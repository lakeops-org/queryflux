//! Streaming PostgreSQL COPY text over the shared authorization/dispatch path.
//! COPY FROM uses a pinned transaction so a failed upload cannot partially commit.
use super::*;
use polyglot_sql::{expressions::Expression, DialectType};
type WireResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn generate(e: &Expression) -> WireResult<String> {
    Ok(polyglot_sql::generate(e, DialectType::PostgreSQL)?)
}
fn escape(value: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    for byte in value {
        match byte {
            b'\\' => result.extend_from_slice(b"\\\\"),
            b'\t' => result.extend_from_slice(b"\\t"),
            b'\n' => result.extend_from_slice(b"\\n"),
            b'\r' => result.extend_from_slice(b"\\r"),
            b'\x08' => result.extend_from_slice(b"\\b"),
            b'\x0c' => result.extend_from_slice(b"\\f"),
            b'\x0b' => result.extend_from_slice(b"\\v"),
            _ => result.push(*byte),
        }
    }
    result
}
fn decode(field: &[u8]) -> WireResult<Option<String>> {
    if field == b"\\N" {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    let mut i = 0;
    while i < field.len() {
        if field[i] != b'\\' {
            bytes.push(field[i]);
            i += 1;
            continue;
        }
        i += 1;
        let b = *field.get(i).ok_or("unterminated COPY escape")?;
        match b {
            b't' => bytes.push(b'\t'),
            b'n' => bytes.push(b'\n'),
            b'r' => bytes.push(b'\r'),
            b'b' => bytes.push(8),
            b'f' => bytes.push(12),
            b'v' => bytes.push(11),
            b'0'..=b'7' => {
                let mut n = 0u16;
                let mut count = 0;
                while count < 3 && i < field.len() && (b'0'..=b'7').contains(&field[i]) {
                    n = n * 8 + (field[i] - b'0') as u16;
                    i += 1;
                    count += 1;
                }
                if n > 255 {
                    return Err("invalid octal COPY escape".into());
                }
                bytes.push(n as u8);
                continue;
            }
            b'x' => {
                i += 1;
                let start = i;
                while i < field.len() && i - start < 2 && field[i].is_ascii_hexdigit() {
                    i += 1;
                }
                if start == i {
                    return Err("invalid hex COPY escape".into());
                }
                bytes.push(u8::from_str_radix(
                    std::str::from_utf8(&field[start..i])?,
                    16,
                )?);
                continue;
            }
            _ => bytes.push(b),
        }
        i += 1;
    }
    Ok(Some(String::from_utf8(bytes)?))
}
struct Sink<'a> {
    writer: &'a mut crate::wire_tls::WireWriter,
    rows: u64,
    copy_out: bool,
}
#[async_trait]
impl ResultSink for Sink<'_> {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        if self.copy_out {
            let mut body = vec![0];
            body.extend_from_slice(&(schema.fields().len() as i16).to_be_bytes());
            for _ in schema.fields() {
                body.extend_from_slice(&0i16.to_be_bytes());
            }
            write_msg(self.writer, b'H', &body)
                .await
                .map_err(|e| QueryFluxError::Engine(e.to_string()))?;
        }
        Ok(())
    }
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.copy_out {
            return Ok(());
        }
        for row in 0..batch.num_rows() {
            let mut bytes = Vec::new();
            for (i, col) in batch.columns().iter().enumerate() {
                if i > 0 {
                    bytes.push(b'\t');
                }
                match arrow_value_to_pg_text(col.as_ref(), row) {
                    Some(v) => bytes.extend_from_slice(&escape(&v)),
                    None => bytes.extend_from_slice(b"\\N"),
                }
            }
            bytes.push(b'\n');
            write_msg(self.writer, b'd', &bytes)
                .await
                .map_err(|e| QueryFluxError::Engine(e.to_string()))?;
            self.rows += 1;
        }
        Ok(())
    }
    async fn on_complete(&mut self, _: &QueryStats) -> Result<()> {
        Ok(())
    }
    async fn on_error(&mut self, e: &str) -> Result<()> {
        Err(QueryFluxError::Engine(e.into()))
    }
}
async fn insert(
    state: &Arc<AppState>,
    session: &SessionContext,
    identity: &queryflux_auth::AuthContext,
    group: &queryflux_core::query::ClusterGroupName,
    target: &str,
    rows: &[Vec<Option<String>>],
    writer: &mut crate::wire_tls::WireWriter,
    credentials: &Credentials,
) -> WireResult<()> {
    let current = crate::lease::authenticate(state, credentials).await?;
    if current.session_owner() != identity.session_owner() {
        return Err("COPY session identity changed".into());
    }
    if rows.is_empty() {
        return Ok(());
    }
    let mut values = Vec::new();
    let mut params = Vec::new();
    for row in rows {
        let mut p = Vec::new();
        for value in row {
            params.push(value.clone());
            p.push(format!("${}", params.len()));
        }
        values.push(format!("({})", p.join(",")));
    }
    let sql = super::extended::bind(
        &format!("INSERT INTO {target} VALUES {}", values.join(",")),
        &params,
    )?;
    let mut sink = Sink {
        writer,
        rows: 0,
        copy_out: false,
    };
    execute_to_sink(
        state,
        sql,
        vec![],
        session.clone(),
        FrontendProtocol::PostgresWire,
        group.clone(),
        &mut sink,
        &current,
    )
    .await?;
    Ok(())
}
pub(super) async fn run(
    reader: &mut crate::wire_tls::WireReader,
    writer: &mut crate::wire_tls::WireWriter,
    state: &Arc<AppState>,
    session: &SessionContext,
    sql: &str,
    creds: &Credentials,
) -> WireResult<()> {
    let identity = crate::lease::authenticate(state, creds).await?;
    let owned = sql.to_owned();
    let statements = queryflux_core::polyglot_pool::run(move || {
        polyglot_sql::parse(&owned, DialectType::PostgreSQL)
    })
    .ok_or("parse worker failed")??;
    if statements.len() != 1 {
        return Err("COPY requires one statement".into());
    }
    let Expression::Copy(copy) = &statements[0] else {
        return Err("invalid COPY statement".into());
    };
    if copy.files.len() != 1
        || generate(&copy.files[0])?.to_ascii_uppercase()
            != if copy.kind { "STDIN" } else { "STDOUT" }
    {
        return Err("only client STDIN/STDOUT COPY is supported".into());
    }
    for option in &copy.params {
        if !option.name.eq_ignore_ascii_case("FORMAT")
            || option
                .value
                .as_ref()
                .map(generate)
                .transpose()?
                .is_none_or(|v| !v.trim_matches('\'').eq_ignore_ascii_case("text"))
        {
            return Err("COPY currently supports default text format; CSV/binary/options require further implementation".into());
        }
    }
    let (target, select, count) = match &copy.this {
        Expression::Table(_) => {
            let t = generate(&copy.this)?;
            (t.clone(), format!("SELECT * FROM {t}"), None)
        }
        Expression::Schema(s) => {
            let table = s.this.as_deref().ok_or("COPY table missing")?;
            if !matches!(table, Expression::Table(_)) {
                return Err("COPY target must be a table".into());
            }
            let columns = s
                .expressions
                .iter()
                .map(generate)
                .collect::<WireResult<Vec<_>>>()?;
            let table = generate(table)?;
            (
                format!("{table} ({})", columns.join(",")),
                format!("SELECT {} FROM {table}", columns.join(",")),
                Some(columns.len()),
            )
        }
        Expression::Subquery(s) if !copy.kind => (String::new(), generate(&s.this)?, None),
        _ => return Err("unsupported COPY target".into()),
    };
    let group = super::extended::route(state, session, &select, &identity).await?;
    if !copy.kind {
        let mut sink = Sink {
            writer,
            rows: 0,
            copy_out: true,
        };
        execute_to_sink(
            state,
            select,
            vec![],
            session.clone(),
            FrontendProtocol::PostgresWire,
            group,
            &mut sink,
            &identity,
        )
        .await?;
        let rows = sink.rows;
        write_msg(writer, b'c', &[]).await?;
        write_msg(writer, b'C', format!("COPY {rows}\0").as_bytes()).await?;
        return Ok(());
    }
    let count = match count {
        Some(n) => n,
        None => super::extended::describe(state, session, &select, &identity)
            .await?
            .fields()
            .len(),
    };
    let id = session
        .extra
        .get(crate::transaction::SESSION_KEY)
        .ok_or("connection transaction id missing")?;
    let own = state
        .transactions
        .lookup(id, &identity, &FrontendProtocol::PostgresWire)?
        .is_none();
    if own {
        state
            .transactions
            .begin(
                state,
                id,
                group.clone(),
                &identity,
                FrontendProtocol::PostgresWire,
                session,
            )
            .await?;
    }
    let result = async {
        let mut body = vec![0];
        body.extend_from_slice(&(count as i16).to_be_bytes());
        for _ in 0..count {
            body.extend_from_slice(&0i16.to_be_bytes());
        }
        write_msg(writer, b'G', &body).await?;
        writer.flush().await?;
        let mut pending = Vec::new();
        let mut rows = Vec::new();
        let mut total = 0u64;
        loop {
            let kind = read_byte(reader).await?;
            let len = checked_frontend_len(read_i32(reader).await?)?;
            if len < 4 {
                return Err("invalid COPY packet".into());
            }
            let mut bytes = vec![0; len - 4];
            reader.read_exact(&mut bytes).await?;
            match kind {
                b'd' => {
                    pending.extend_from_slice(&bytes);
                    while let Some(end) = pending.iter().position(|b| *b == b'\n') {
                        let line: Vec<u8> = pending.drain(..=end).collect();
                        let line = line[..line.len() - 1]
                            .strip_suffix(b"\r")
                            .unwrap_or(&line[..line.len() - 1]);
                        let fields = line
                            .split(|b| *b == b'\t')
                            .map(decode)
                            .collect::<WireResult<Vec<_>>>()?;
                        if fields.len() != count {
                            return Err("COPY row column count mismatch".into());
                        }
                        rows.push(fields);
                        total += 1;
                        if rows.len() >= 256 {
                            insert(
                                state, session, &identity, &group, &target, &rows, writer, creds,
                            )
                            .await?;
                            rows.clear();
                        }
                    }
                    if pending.len() > 8 * 1024 * 1024 {
                        return Err("COPY row exceeds 8 MiB".into());
                    }
                }
                b'c' if bytes.is_empty() => {
                    if !pending.is_empty() {
                        return Err("COPY final row must end with newline".into());
                    }
                    break;
                }
                b'f' => return Err("client cancelled COPY".into()),
                b'H' if bytes.is_empty() => writer.flush().await?,
                _ => return Err("unexpected message during COPY".into()),
            }
        }
        insert(
            state, session, &identity, &group, &target, &rows, writer, creds,
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(total)
    }
    .await;
    if own {
        state
            .transactions
            .finish(
                id,
                &identity,
                &FrontendProtocol::PostgresWire,
                result.is_ok(),
            )
            .await?;
    } else if result.is_err() {
        if let Some(t) =
            state
                .transactions
                .lookup(id, &identity, &FrontendProtocol::PostgresWire)?
        {
            t.failed.store(true, Ordering::Release);
        }
    }
    let rows = result?;
    write_msg(writer, b'C', format!("COPY {rows}\0").as_bytes()).await?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_copy_roundtrip_and_null() {
        let s = b"tab\tnewline\nslash\\carriage\r";
        assert_eq!(decode(&escape(s)).unwrap().unwrap().as_bytes(), s);
        assert!(decode(b"\\N").unwrap().is_none());
        assert_eq!(decode(b"\\\\N").unwrap(), Some("\\N".into()));
    }
}
