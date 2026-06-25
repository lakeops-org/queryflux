//! PostgreSQL wire protocol frontend (protocol version 3).
//!
//! Accepts connections from any Postgres-compatible client (psql, JDBC, SQLAlchemy,
//! DBeaver, etc.) and dispatches queries through the QueryFlux routing/dispatch pipeline.
//!
//! Implements the simple query flow only (V1):
//!   Startup → AuthenticationOk → ParameterStatus × N → BackendKeyData → ReadyForQuery
//!   Q (SimpleQuery) → RowDescription + DataRow × N + CommandComplete + ReadyForQuery
//!   X (Terminate) → close connection
//!
//! Results are streamed as Arrow RecordBatches and serialised to Postgres text format.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use queryflux_auth::Credentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::parse_query_tags,
};

use crate::dispatch::{execute_to_sink, ResultSink};
use crate::state::AppState;
use crate::{FrontendListenerTrait, ShutdownRx};

// ── Postgres type OIDs (text-format only in V1) ───────────────────────────────

const PG_OID_BOOL: i32 = 16;
const PG_OID_BYTEA: i32 = 17;
const PG_OID_INT8: i32 = 20; // bigint
const PG_OID_INT2: i32 = 21; // smallint
const PG_OID_INT4: i32 = 23; // integer
const PG_OID_TEXT: i32 = 25;
const PG_OID_FLOAT4: i32 = 700;
const PG_OID_FLOAT8: i32 = 701;
const PG_OID_DATE: i32 = 1082;
const PG_OID_TIMESTAMP: i32 = 1114;
const PG_OID_NUMERIC: i32 = 1700;

static CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

// ── Frontend ──────────────────────────────────────────────────────────────────

pub struct PostgresWireFrontend {
    state: Arc<AppState>,
    port: u16,
    max_connections: Option<usize>,
}

impl PostgresWireFrontend {
    pub fn new(state: Arc<AppState>, port: u16, max_connections: Option<usize>) -> Self {
        Self {
            state,
            port,
            max_connections,
        }
    }
}

#[async_trait]
impl FrontendListenerTrait for PostgresWireFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("Postgres wire frontend listening on {addr}");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;

        let active = Arc::new(AtomicUsize::new(0));
        let max_conn = self.max_connections.filter(|&l| l > 0);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result.map_err(|e| QueryFluxError::Other(e.into()))?;
                    if let Some(limit) = max_conn {
                        if active.load(Ordering::Relaxed) >= limit {
                            tracing::warn!(peer = %peer, "Postgres wire: rejecting connection — at limit {limit}");
                            drop(stream);
                            continue;
                        }
                    }
                    debug!(peer = %peer, "Postgres wire: new connection");
                    let state = self.state.clone();
                    let conn_id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    active.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state, conn_id).await {
                            debug!(conn_id, "Postgres wire connection closed: {e}");
                        }
                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = shutdown.changed() => {
                    info!("Postgres wire frontend: shutdown signal received, stopping accept loop");
                    break;
                }
            }
        }
        // Drain: wait for all in-flight connections to finish before returning.
        while active.load(Ordering::Relaxed) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

// ── Connection handler ────────────────────────────────────────────────────────

async fn handle_connection(
    stream: TcpStream,
    state: Arc<AppState>,
    connection_id: u32,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    // ── Startup phase ────────────────────────────────────────────────────────

    // Read the startup message: 4-byte length + 4-byte protocol version + params.
    let startup_len = read_i32(&mut reader).await? as usize;
    if startup_len < 8 {
        return Ok(());
    }
    let mut startup_body = vec![0u8; startup_len - 4];
    reader.read_exact(&mut startup_body).await?;

    let protocol_version = i32::from_be_bytes([
        startup_body[0],
        startup_body[1],
        startup_body[2],
        startup_body[3],
    ]);

    // SSL request (magic number 80877103) — tell client we don't support SSL.
    if protocol_version == 80877103 {
        writer.write_all(b"N").await?; // 'N' = no SSL
        writer.flush().await?;
        // Re-read the real startup message.
        let real_len = read_i32(&mut reader).await? as usize;
        if real_len < 8 {
            return Ok(());
        }
        let mut real_body = vec![0u8; real_len - 4];
        reader.read_exact(&mut real_body).await?;
        startup_body = real_body;
    }

    let params = parse_startup_params(&startup_body[4..]);
    let user = params.get("user").cloned().unwrap_or_default();
    let database = params.get("database").cloned();

    info!(user, database = ?database, conn_id = connection_id, "Postgres wire: client connecting");

    // AuthenticationOk
    write_msg(&mut writer, b'R', &0i32.to_be_bytes()).await?;

    // ParameterStatus messages (clients expect at least a few of these).
    for (k, v) in [
        ("server_version", "16.0-queryflux"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ] {
        let mut body = Vec::new();
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
        write_msg(&mut writer, b'S', &body).await?;
    }

    // BackendKeyData (conn_id as both process ID and secret — clients use this for cancellation).
    let mut bkd = Vec::new();
    bkd.extend_from_slice(&connection_id.to_be_bytes());
    bkd.extend_from_slice(&connection_id.to_be_bytes()); // secret
    write_msg(&mut writer, b'K', &bkd).await?;

    // ReadyForQuery ('I' = idle, not in a transaction).
    write_msg(&mut writer, b'Z', b"I").await?;

    let raw_tags = params
        .get("query_tags")
        .or_else(|| params.get("query_tag"))
        .map(String::as_str)
        .unwrap_or("");
    let (tags, _) = parse_query_tags(raw_tags);
    // Copy all remaining startup params into extra so that resolved_agent_context(),
    // routers, and guards can read them without per-field knowledge here.
    let well_known = ["user", "database", "query_tags", "query_tag"];
    let extra: HashMap<String, String> = params
        .into_iter()
        .filter(|(k, _)| !well_known.contains(&k.as_str()))
        .collect();
    let session = SessionContext {
        user: if user.is_empty() { None } else { Some(user) },
        database,
        tags,
        extra,
        agent_context: None,
    };

    // ── Command loop ─────────────────────────────────────────────────────────

    loop {
        // Each frontend message: 1-byte type + 4-byte length (includes itself).
        let msg_type = match read_byte(&mut reader).await {
            Ok(b) => b,
            Err(_) => break,
        };

        let msg_len = read_i32(&mut reader).await? as usize;
        if msg_len < 4 {
            break;
        }
        let body_len = msg_len - 4;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            reader.read_exact(&mut body).await?;
        }

        match msg_type {
            b'X' => break, // Terminate

            b'Q' => {
                // SimpleQuery: null-terminated SQL string.
                let sql = String::from_utf8_lossy(&body)
                    .trim_end_matches('\0')
                    .trim()
                    .to_string();
                debug!(conn_id = connection_id, sql = %sql, "Postgres wire: query");
                handle_simple_query(&mut writer, &state, &session, &sql).await?;
            }

            b'P' => {
                // Parse (prepared statements) — not supported in V1.
                write_error_response(
                    &mut writer,
                    "42000",
                    "Prepared statements are not supported",
                )
                .await?;
                write_msg(&mut writer, b'Z', b"I").await?;
            }

            b'd' | b'c' | b'f' | b'H' | b'S' => {
                // CopyData, CopyDone, CopyFail, Flush, Sync — acknowledge.
                write_msg(&mut writer, b'Z', b"I").await?;
            }

            _ => {
                warn!(
                    conn_id = connection_id,
                    msg_type, "Postgres wire: unsupported message type"
                );
                write_error_response(
                    &mut writer,
                    "0A000",
                    &format!("Unsupported message type: {}", msg_type as char),
                )
                .await?;
                write_msg(&mut writer, b'Z', b"I").await?;
            }
        }
    }

    debug!(conn_id = connection_id, "Postgres wire: connection closed");
    Ok(())
}

// ── Query execution ───────────────────────────────────────────────────────────

async fn handle_simple_query<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    state: &Arc<AppState>,
    session: &SessionContext,
    sql: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if sql.is_empty() {
        // Empty query: EmptyQueryResponse + ReadyForQuery.
        write_msg(writer, b'I', &[]).await?;
        write_msg(writer, b'Z', b"I").await?;
        return Ok(());
    }

    let sql_lower = sql.trim().to_lowercase();

    // Fast-path: SET statements.
    if sql_lower.starts_with("set ") || sql_lower.starts_with("set\t") {
        write_msg(writer, b'C', b"SET\0").await?;
        write_msg(writer, b'Z', b"I").await?;
        return Ok(());
    }

    let protocol = FrontendProtocol::PostgresWire;

    let creds = Credentials {
        username: session.user().map(|s| s.to_string()),
        ..Default::default()
    };
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            write_error_response(writer, "28000", &e.to_string()).await?;
            write_msg(writer, b'Z', b"I").await?;
            return Ok(());
        }
    };

    let routing_result = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(sql, session, &protocol, Some(&auth_ctx))
            .await
    };
    let (group, _trace) = match routing_result {
        Ok(r) => r,
        Err(e) => {
            write_error_response(writer, "42000", &e.to_string()).await?;
            write_msg(writer, b'Z', b"I").await?;
            return Ok(());
        }
    };

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let mut sink = PostgresResultSink::new(tx);

    let state2 = state.clone();
    let session2 = session.clone();
    let sql2 = sql.to_string();

    let exec_task = tokio::spawn(async move {
        execute_to_sink(
            &state2,
            sql2,
            vec![],
            session2,
            protocol,
            group,
            &mut sink,
            &auth_ctx,
        )
        .await
        // sink drops here, closing tx
    });

    // Forward encoded Postgres messages to the client as they arrive.
    while let Some(msg) = rx.recv().await {
        writer.write_all(&msg).await?;
        writer.flush().await?;
    }

    if let Err(e) = exec_task.await {
        warn!("Postgres query task panicked: {e}");
    }

    // ReadyForQuery after each command.
    write_msg(writer, b'Z', b"I").await?;
    Ok(())
}

// ── PostgresResultSink ────────────────────────────────────────────────────────

/// Streams Arrow RecordBatches as Postgres wire protocol messages over a channel.
///
/// Sends pre-encoded Postgres messages (type byte + length + body) via channel.
/// The query handler drains the channel and writes them to the TCP stream.
struct PostgresResultSink {
    tx: UnboundedSender<Vec<u8>>,
    row_count: u64,
}

impl PostgresResultSink {
    fn new(tx: UnboundedSender<Vec<u8>>) -> Self {
        Self { tx, row_count: 0 }
    }

    fn send_msg(&self, msg_type: u8, body: Vec<u8>) {
        let len = (body.len() + 4) as i32;
        let mut msg = Vec::with_capacity(5 + body.len());
        msg.push(msg_type);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&body);
        let _ = self.tx.send(msg);
    }
}

#[async_trait]
impl ResultSink for PostgresResultSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        // RowDescription: field count (i16) + field descriptors.
        let n = schema.fields().len() as i16;
        let mut body = n.to_be_bytes().to_vec();
        for field in schema.fields() {
            body.extend_from_slice(field.name().as_bytes());
            body.push(0); // NUL terminator
            body.extend_from_slice(&0i32.to_be_bytes()); // table OID (0 = unknown)
            body.extend_from_slice(&0i16.to_be_bytes()); // column attr number
            body.extend_from_slice(&arrow_type_to_pg_oid(field.data_type()).to_be_bytes());
            body.extend_from_slice(&(-1i16).to_be_bytes()); // type size (-1 = variable)
            body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            body.extend_from_slice(&0i16.to_be_bytes()); // format code: 0 = text
        }
        self.send_msg(b'T', body);
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for row in 0..batch.num_rows() {
            // DataRow: column count (i16) + per-column (length i32 + bytes, or -1 for NULL).
            let n = batch.num_columns() as i16;
            let mut body = n.to_be_bytes().to_vec();
            for col in batch.columns() {
                match arrow_value_to_pg_text(col.as_ref(), row) {
                    None => body.extend_from_slice(&(-1i32).to_be_bytes()), // NULL
                    Some(bytes) => {
                        body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        body.extend_from_slice(&bytes);
                    }
                }
            }
            self.send_msg(b'D', body);
            self.row_count += 1;
        }
        Ok(())
    }

    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        // CommandComplete: tag string (e.g. "SELECT 42").
        let tag = format!("SELECT {}\0", self.row_count);
        self.send_msg(b'C', tag.into_bytes());
        Ok(())
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        // ErrorResponse: field type 'M' (message) + NUL terminator.
        let mut body = Vec::new();
        body.push(b'S'); // severity
        body.extend_from_slice(b"ERROR\0");
        body.push(b'C'); // SQLSTATE code
        body.extend_from_slice(b"XX000\0"); // internal error
        body.push(b'M'); // message
        body.extend_from_slice(message.as_bytes());
        body.push(0);
        body.push(0); // terminator
        self.send_msg(b'E', body);
        Ok(())
    }
}

// ── Arrow → Postgres helpers ──────────────────────────────────────────────────

fn arrow_type_to_pg_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => PG_OID_BOOL,
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => PG_OID_INT2,
        DataType::Int32 | DataType::UInt16 => PG_OID_INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => PG_OID_INT8,
        DataType::Float16 | DataType::Float32 => PG_OID_FLOAT4,
        DataType::Float64 => PG_OID_FLOAT8,
        DataType::Decimal128(..) | DataType::Decimal256(..) => PG_OID_NUMERIC,
        DataType::Date32 | DataType::Date64 => PG_OID_DATE,
        DataType::Timestamp(..) => PG_OID_TIMESTAMP,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => PG_OID_BYTEA,
        _ => PG_OID_TEXT, // Utf8, LargeUtf8, List, Map, Struct, ...
    }
}

/// Serialize a single Arrow array cell as UTF-8 text bytes for Postgres text protocol.
/// Returns `None` for SQL NULL.
fn arrow_value_to_pg_text(col: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if col.is_null(row) {
        return None;
    }
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let s = ArrayFormatter::try_new(col, &FormatOptions::default())
        .map(|fmt| fmt.value(row).to_string())
        .unwrap_or_default();
    Some(s.into_bytes())
}

// ── Postgres message I/O ──────────────────────────────────────────────────────

/// Write a Postgres backend message: type byte + i32 length (includes itself) + body.
async fn write_msg<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg_type: u8,
    body: &[u8],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let len = (body.len() + 4) as i32;
    writer.write_all(&[msg_type]).await?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

async fn write_error_response<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    sqlstate: &str,
    message: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0); // terminator
    write_msg(writer, b'E', &body).await
}

async fn read_byte<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::result::Result<u8, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).await?;
    Ok(buf[0])
}

async fn read_i32<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::result::Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(i32::from_be_bytes(buf))
}

// ── Startup message parsing ───────────────────────────────────────────────────

/// Parse Postgres startup params: NUL-separated key=value pairs, terminated by NUL.
fn parse_startup_params(data: &[u8]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut pos = 0;
    loop {
        let key = read_cstr(data, &mut pos);
        if key.is_empty() {
            break;
        }
        let val = read_cstr(data, &mut pos);
        map.insert(key, val);
    }
    map
}

fn read_cstr(data: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*pos]).to_string();
    if *pos < data.len() {
        *pos += 1; // skip NUL
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params_to_extra(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let well_known = ["user", "database", "query_tags", "query_tag"];
        pairs
            .iter()
            .filter(|(k, _)| !well_known.contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn session_from_params(pairs: &[(&str, &str)]) -> SessionContext {
        let extra = params_to_extra(pairs);
        SessionContext {
            user: None,
            database: None,
            tags: queryflux_core::tags::QueryTags::new(),
            extra,
            agent_context: None,
        }
    }

    #[test]
    fn agent_context_from_startup_params() {
        let s = session_from_params(&[
            ("agent_id", "agent-pg"),
            ("conversation_id", "conv-pg"),
            ("step_index", "2"),
            ("tool_call_id", "call_xyz"),
            ("query_intent", "lookup"),
        ]);
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "agent-pg");
        assert_eq!(ctx.conversation_id, "conv-pg");
        assert_eq!(ctx.step_index, Some(2));
        assert_eq!(ctx.tool_call_id.as_deref(), Some("call_xyz"));
        assert_eq!(
            ctx.query_intent,
            queryflux_core::session::QueryIntent::Lookup
        );
    }

    #[test]
    fn agent_context_requires_both_ids() {
        let s = session_from_params(&[("agent_id", "agent-pg")]);
        assert!(s.resolved_agent_context().is_none());
    }

    #[test]
    fn well_known_params_not_in_extra() {
        let extra = params_to_extra(&[
            ("user", "alice"),
            ("database", "mydb"),
            ("query_tags", "team:eng"),
            ("agent_id", "agent-1"),
        ]);
        assert!(!extra.contains_key("user"));
        assert!(!extra.contains_key("database"));
        assert!(!extra.contains_key("query_tags"));
        assert!(extra.contains_key("agent_id"));
    }

    #[test]
    fn unknown_startup_params_flow_through_to_extra() {
        let s = session_from_params(&[
            ("agent_id", "a"),
            ("conversation_id", "c"),
            ("custom_routing_hint", "region-us"),
        ]);
        assert_eq!(
            s.extra.get("custom_routing_hint").map(String::as_str),
            Some("region-us")
        );
    }
}
