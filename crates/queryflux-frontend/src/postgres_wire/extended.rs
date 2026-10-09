//! PostgreSQL prepared statements and resumable portals. Results are bounded to
//! 64 MiB per portal; an Execute resume never repeats the backend statement.
use super::*;
use polyglot_sql::{
    expressions::{BooleanLiteral, Expression, Literal, Null, ParameterStyle},
    traversal::transform,
    DialectType,
};
use std::collections::VecDeque;

type WireResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone)]
struct Statement {
    sql: String,
    types: Vec<i32>,
}
struct Portal {
    sql: String,
    formats: Vec<usize>,
    rows: Option<VecDeque<Vec<u8>>>,
    complete: Option<Vec<u8>>,
}
#[derive(Default)]
pub(super) struct Extended {
    statements: HashMap<String, Statement>,
    portals: HashMap<String, Portal>,
    pub failed: bool,
}
struct Input<'a> {
    bytes: &'a [u8],
    index: usize,
}
impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }
    fn bytes(&mut self, n: usize) -> WireResult<&'a [u8]> {
        let end = self.index.checked_add(n).ok_or("invalid length")?;
        let value = self.bytes.get(self.index..end).ok_or("truncated message")?;
        self.index = end;
        Ok(value)
    }
    fn string(&mut self) -> WireResult<String> {
        let n = self.bytes[self.index..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("missing terminator")?;
        let value = String::from_utf8(self.bytes(n)?.to_vec())?;
        self.bytes(1)?;
        Ok(value)
    }
    fn i16(&mut self) -> WireResult<usize> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into()?) as usize)
    }
    fn i32(&mut self) -> WireResult<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into()?))
    }
    fn end(&self) -> WireResult<()> {
        if self.index == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing message bytes".into())
        }
    }
}
pub(super) fn bind(sql: &str, values: &[Option<String>]) -> WireResult<String> {
    bind_typed(sql, values, &[])
}
fn bind_typed(sql: &str, values: &[Option<String>], types: &[i32]) -> WireResult<String> {
    let sql = sql.to_owned();
    let statements = queryflux_core::polyglot_pool::run(move || {
        polyglot_sql::parse(&sql, DialectType::PostgreSQL)
    })
    .ok_or("parse worker failed")??;
    if statements.len() != 1 {
        return Err("prepared statements must contain one SQL statement".into());
    }
    let missing = std::cell::Cell::new(false);
    let rewritten = transform(statements.into_iter().next().unwrap(), &|expression| {
        if let Expression::Parameter(p) = &expression {
            if p.style == ParameterStyle::Dollar {
                let i = p
                    .index
                    .or_else(|| p.name.as_ref().and_then(|n| n.parse().ok()))
                    .unwrap_or(0) as usize;
                let Some(value) = i.checked_sub(1).and_then(|i| values.get(i)) else {
                    missing.set(true);
                    return Ok(Some(expression));
                };
                return Ok(Some(match value {
                    Some(v) => match types.get(i - 1).copied().unwrap_or(0) {
                        PG_OID_INT2 | PG_OID_INT4 | PG_OID_INT8 | PG_OID_FLOAT4 | PG_OID_FLOAT8
                            if v.parse::<f64>().is_ok_and(|n| n.is_finite()) =>
                        {
                            Expression::Literal(Box::new(Literal::Number(v.clone())))
                        }
                        PG_OID_BOOL
                            if matches!(
                                v.to_ascii_lowercase().as_str(),
                                "t" | "true" | "1" | "on" | "f" | "false" | "0" | "off"
                            ) =>
                        {
                            Expression::Boolean(BooleanLiteral {
                                value: matches!(
                                    v.to_ascii_lowercase().as_str(),
                                    "t" | "true" | "1" | "on"
                                ),
                            })
                        }
                        PG_OID_DATE => Expression::Literal(Box::new(Literal::Date(v.clone()))),
                        PG_OID_TIMESTAMP => {
                            Expression::Literal(Box::new(Literal::Timestamp(v.clone())))
                        }
                        _ => Expression::Literal(Box::new(Literal::String(v.clone()))),
                    },
                    None => Expression::Null(Null),
                }));
            }
        }
        Ok(Some(expression))
    })?;
    if missing.get() {
        return Err("parameter index out of range".into());
    }
    Ok(polyglot_sql::generate(&rewritten, DialectType::PostgreSQL)?)
}
pub(super) async fn route(
    state: &AppState,
    session: &SessionContext,
    sql: &str,
    identity: &queryflux_auth::AuthContext,
) -> WireResult<queryflux_core::query::ClusterGroupName> {
    let live = state.live.read().await;
    let (result, mut trace) = live
        .router_chain
        .route_with_trace(
            sql,
            session,
            &FrontendProtocol::PostgresWire,
            Some(identity),
        )
        .await?;
    drop(live);
    let group = match result {
        ChainRouteResult::Routed(g) => g,
        ChainRouteResult::Denied { message } => return Err(message.into()),
    };
    Ok(state
        .resolve_routed_group(group, &mut trace, identity)
        .await?)
}
pub(super) async fn describe(
    state: &AppState,
    session: &SessionContext,
    sql: &str,
    identity: &queryflux_auth::AuthContext,
) -> WireResult<Arc<Schema>> {
    let group = route(state, session, sql, identity).await?;
    let (authorization, manager, adapters) = {
        let live = state.live.read().await;
        (
            live.authorization.clone(),
            live.cluster_manager.clone(),
            live.adapters.clone(),
        )
    };
    if !authorization.check(identity, &group.0).await {
        return Err("metadata access denied".into());
    }
    if let Some(id) = session.extra.get(crate::transaction::SESSION_KEY) {
        if let Some(t) = state
            .transactions
            .lookup(id, identity, &FrontendProtocol::PostgresWire)?
        {
            if t.group != group {
                return Err("transaction cannot change backend group".into());
            }
            let _operation = t.operation.lock().await;
            return Ok(t.adapter.describe_query(sql).await?);
        }
    }
    let cluster = manager
        .acquire_cluster(&group)
        .await?
        .ok_or("no metadata backend capacity")?;
    let result = match adapters.get(&cluster.0) {
        Some(queryflux_engine_adapters::AdapterKind::Sync(a)) => a.describe_query(sql).await,
        _ => Err(QueryFluxError::Engine(
            "backend does not support prepared-statement metadata".into(),
        )),
    };
    let _ = manager.release_cluster(&group, &cluster).await;
    Ok(result?)
}
struct Collector {
    sink: PostgresResultSink,
    rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    packets: VecDeque<Vec<u8>>,
    bytes: usize,
    oids: Vec<i32>,
}
impl Collector {
    fn new(sql: &str) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            sink: PostgresResultSink::new(tx, sql),
            rx,
            packets: VecDeque::new(),
            bytes: 0,
            oids: Vec::new(),
        }
    }
    fn drain(&mut self) -> Result<()> {
        while let Ok(packet) = self.rx.try_recv() {
            self.bytes += packet.len();
            if self.bytes > 64 * 1024 * 1024 {
                return Err(QueryFluxError::Engine(
                    "portal result exceeds 64 MiB; use simple query streaming".into(),
                ));
            }
            self.packets.push_back(packet);
        }
        Ok(())
    }
}
#[async_trait]
impl ResultSink for Collector {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.oids = schema
            .fields()
            .iter()
            .map(|f| arrow_type_to_pg_oid(f.data_type()))
            .collect();
        self.sink.on_schema(schema).await?;
        self.drain()
    }
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.sink.on_batch(batch).await?;
        self.drain()
    }
    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()> {
        self.sink.on_complete(stats).await?;
        self.drain()
    }
    async fn on_error(&mut self, message: &str) -> Result<()> {
        Err(QueryFluxError::Engine(message.into()))
    }
}
impl Extended {
    pub async fn handle(
        &mut self,
        message: u8,
        body: &[u8],
        writer: &mut crate::wire_tls::WireWriter,
        state: &Arc<AppState>,
        session: &SessionContext,
        creds: &Credentials,
    ) -> WireResult<()> {
        let identity = crate::lease::authenticate(state, creds).await?;
        let mut input = Input::new(body);
        match message {
            b'P' => {
                let name = input.string()?;
                let sql = input.string()?;
                let n = input.i16()?;
                let mut types = Vec::with_capacity(n);
                for _ in 0..n {
                    types.push(input.i32()?);
                }
                input.end()?;
                if self.statements.len() >= 256 {
                    return Err("prepared statement capacity exceeded".into());
                }
                if !name.is_empty() && self.statements.contains_key(&name) {
                    return Err("prepared statement already exists".into());
                }
                // Parse now so malformed SQL cannot be acknowledged as prepared.
                let copy = sql.clone();
                let parsed = queryflux_core::polyglot_pool::run(move || {
                    polyglot_sql::parse(&copy, DialectType::PostgreSQL)
                })
                .ok_or("parse worker failed")??;
                if parsed.len() != 1 {
                    return Err("prepared statements require exactly one statement".into());
                }
                let max_index = std::cell::Cell::new(0usize);
                let _ = transform(parsed.into_iter().next().unwrap(), &|e| {
                    if let Expression::Parameter(p) = &e {
                        if p.style == ParameterStyle::Dollar {
                            let n = p
                                .index
                                .or_else(|| p.name.as_ref().and_then(|n| n.parse().ok()))
                                .unwrap_or(0) as usize;
                            max_index.set(max_index.get().max(n));
                        }
                    }
                    Ok(Some(e))
                })?;
                if max_index.get() > 1024 {
                    return Err("parameter capacity exceeded".into());
                }
                types.resize(types.len().max(max_index.get()), 0);
                self.statements.insert(name, Statement { sql, types });
                write_msg(writer, b'1', &[]).await?;
            }
            b'B' => {
                let name = input.string()?;
                let stmt = input.string()?;
                let statement = self
                    .statements
                    .get(&stmt)
                    .ok_or("prepared statement not found")?;
                let n = input.i16()?;
                let mut formats = Vec::with_capacity(n);
                for _ in 0..n {
                    formats.push(input.i16()?);
                }
                let n = input.i16()?;
                if formats.len() > 1 && formats.len() != n {
                    return Err("invalid parameter format count".into());
                }
                if !statement.types.is_empty() && statement.types.len() != n {
                    return Err("parameter count mismatch".into());
                }
                let mut values = Vec::with_capacity(n);
                for i in 0..n {
                    let len = input.i32()?;
                    let format = formats
                        .get(if formats.len() == 1 { 0 } else { i })
                        .copied()
                        .unwrap_or(0);
                    let value = if len == -1 {
                        None
                    } else if len < -1 {
                        return Err("invalid parameter length".into());
                    } else {
                        let bytes = input.bytes(len as usize)?;
                        Some(match format {
                            0 => String::from_utf8(bytes.to_vec())?,
                            1 => super::binary::input(
                                statement.types.get(i).copied().unwrap_or(0),
                                bytes,
                            )?,
                            _ => return Err("invalid parameter format".into()),
                        })
                    };
                    values.push(value);
                }
                let n = input.i16()?;
                let mut result_formats = Vec::new();
                for _ in 0..n {
                    let f = input.i16()?;
                    if f > 1 {
                        return Err("invalid result format".into());
                    }
                    result_formats.push(f);
                }
                input.end()?;
                if self.portals.len() >= 64 {
                    return Err("portal capacity exceeded".into());
                }
                if !name.is_empty() && self.portals.contains_key(&name) {
                    return Err("portal already exists".into());
                }
                let sql = bind_typed(&statement.sql, &values, &statement.types)?;
                self.portals.insert(
                    name,
                    Portal {
                        sql,
                        formats: result_formats,
                        rows: None,
                        complete: None,
                    },
                );
                write_msg(writer, b'2', &[]).await?;
            }
            b'D' => {
                let kind = input.bytes(1)?[0];
                let name = input.string()?;
                input.end()?;
                let sql = if kind == b'S' {
                    let stmt = self.statements.get(&name).ok_or("statement not found")?;
                    let mut parameters = (stmt.types.len() as i16).to_be_bytes().to_vec();
                    for oid in &stmt.types {
                        parameters.extend_from_slice(&oid.to_be_bytes());
                    }
                    write_msg(writer, b't', &parameters).await?;
                    bind_typed(&stmt.sql, &vec![None; stmt.types.len()], &stmt.types)?
                } else if kind == b'P' {
                    self.portals
                        .get(&name)
                        .ok_or("portal not found")?
                        .sql
                        .clone()
                } else {
                    return Err("invalid describe target".into());
                };
                let schema = describe(state, session, &sql, &identity).await?;
                if schema.fields().is_empty() {
                    write_msg(writer, b'n', &[]).await?;
                } else {
                    let mut sink = Collector::new(&sql);
                    sink.on_schema(&schema).await?;
                    let formats = if kind == b'P' {
                        self.portals.get(&name).unwrap().formats.clone()
                    } else {
                        Vec::new()
                    };
                    for packet in sink.packets {
                        writer
                            .write_all(&super::binary::description(packet, &formats)?)
                            .await?;
                    }
                }
            }
            b'E' => {
                let name = input.string()?;
                let limit = input.i32()?;
                input.end()?;
                if limit < 0 {
                    return Err("invalid Execute row limit".into());
                }
                let portal = self.portals.get_mut(&name).ok_or("portal not found")?;
                if portal.rows.is_none() {
                    if crate::transaction::transaction_command(&portal.sql).is_some()
                        || crate::transaction::transaction_end(&portal.sql).is_some()
                    {
                        return Err(
                            "transaction commands currently require the simple query flow".into(),
                        );
                    }
                    let group = route(state, session, &portal.sql, &identity).await?;
                    let mut sink = Collector::new(&portal.sql);
                    execute_to_sink(
                        state,
                        portal.sql.clone(),
                        vec![],
                        session.clone(),
                        FrontendProtocol::PostgresWire,
                        group,
                        &mut sink,
                        &identity,
                    )
                    .await?;
                    portal.complete = sink.packets.iter().find(|p| p[0] == b'C').cloned();
                    let rows = sink
                        .packets
                        .into_iter()
                        .filter(|p| p[0] == b'D')
                        .map(|p| super::binary::row(p, &sink.oids, &portal.formats))
                        .collect::<WireResult<VecDeque<_>>>()?;
                    portal.rows = Some(rows);
                }
                let rows = portal.rows.as_mut().unwrap();
                let count = if limit == 0 {
                    rows.len()
                } else {
                    (limit as usize).min(rows.len())
                };
                for _ in 0..count {
                    writer.write_all(&rows.pop_front().unwrap()).await?;
                }
                if rows.is_empty() {
                    if let Some(packet) = &portal.complete {
                        writer.write_all(packet).await?;
                    }
                } else {
                    write_msg(writer, b's', &[]).await?;
                }
            }
            b'C' => {
                let kind = input.bytes(1)?[0];
                let name = input.string()?;
                input.end()?;
                if kind == b'S' {
                    self.statements.remove(&name).ok_or("statement not found")?;
                } else if kind == b'P' {
                    self.portals.remove(&name).ok_or("portal not found")?;
                } else {
                    return Err("invalid close target".into());
                }
                write_msg(writer, b'3', &[]).await?;
            }
            _ => return Err("unsupported extended message".into()),
        }
        writer.flush().await?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bind_preserves_strings_and_repeated_indexes() {
        let sql = bind(
            "SELECT $2, '$1', $1, $2",
            &[Some("x'; DROP TABLE users;--".into()), Some("two".into())],
        )
        .unwrap();
        let parsed = polyglot_sql::parse(&sql, DialectType::PostgreSQL).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(sql.contains("'$1'"));
        assert!(sql.contains("x''; DROP"));
    }
    #[test]
    fn truncated_message_is_rejected() {
        assert!(Input::new(&[0, 0]).i32().is_err());
        assert!(Input::new(b"no terminator").string().is_err());
    }
}
