//! MySQL wire protocol frontend.
//!
//! Accepts connections from any MySQL-compatible client (StarRocks clients,
//! DBeaver, mysql CLI, JDBC, etc.) and dispatches queries through the normal
//! QueryFlux routing/dispatch pipeline.
//!
//! # Execution paths
//!
//! Results reach the client via one of two paths, chosen by dispatch:
//!
//! **Native path** (zero serialization) — when the backend adapter declares
//! `ConnectionFormat::MysqlWire` (e.g. StarRocks, ClickHouse via `mysql_async`):
//! driver values are text-encoded directly into `NativeResultChunk`s and written
//! as MySQL text-protocol packets with no Arrow allocation in between.
//!
//! **Arrow fallback** — all other backends (DuckDB, Trino, ADBC engines):
//! results arrive as Arrow `RecordBatch`es and are serialised to MySQL text
//! protocol on the fly via `on_schema` / `on_batch`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use queryflux_auth::Credentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    native_result::{NativeColumn, NativeResultChunk, NativeTypeKind},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::{parse_query_tags, QueryTags},
};

use crate::dispatch::{execute_to_sink, ResultSink};
use crate::state::AppState;
use crate::{FrontendListenerTrait, ShutdownRx};

// ── MySQL command bytes ───────────────────────────────────────────────────────

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_PING: u8 = 0x0e;

// ── MySQL column type bytes ───────────────────────────────────────────────────

const MYSQL_TYPE_DECIMAL: u8 = 0;
const MYSQL_TYPE_TINY: u8 = 1;
const MYSQL_TYPE_SHORT: u8 = 2;
const MYSQL_TYPE_LONG: u8 = 3;
const MYSQL_TYPE_FLOAT: u8 = 4;
const MYSQL_TYPE_DOUBLE: u8 = 5;
const MYSQL_TYPE_TIMESTAMP: u8 = 7;
const MYSQL_TYPE_LONGLONG: u8 = 8;
const MYSQL_TYPE_DATE: u8 = 10;
const MYSQL_TYPE_TIME: u8 = 11;
const MYSQL_TYPE_DATETIME: u8 = 12;
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
const MYSQL_TYPE_JSON: u8 = 245;
const MYSQL_TYPE_BLOB: u8 = 252;
const MYSQL_TYPE_VAR_STRING: u8 = 253;

// ── Capability flags (sent in the server handshake) ───────────────────────────

const CLIENT_LONG_PASSWORD: u32 = 1;
const CLIENT_FOUND_ROWS: u32 = 2;
const CLIENT_LONG_FLAG: u32 = 4;
const CLIENT_CONNECT_WITH_DB: u32 = 8;
const CLIENT_NO_SCHEMA: u32 = 16;
const CLIENT_PROTOCOL_41: u32 = 512;
const CLIENT_TRANSACTIONS: u32 = 8192;
const CLIENT_SECURE_CONNECTION: u32 = 32768;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_SSL: u32 = 1 << 11;

static CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

// ── Frontend ──────────────────────────────────────────────────────────────────

pub struct MysqlWireFrontend {
    state: Arc<AppState>,
    port: u16,
    max_connections: Option<usize>,
}

impl MysqlWireFrontend {
    pub fn new(state: Arc<AppState>, port: u16, max_connections: Option<usize>) -> Self {
        Self {
            state,
            port,
            max_connections,
        }
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for MysqlWireFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("MySQL wire frontend listening on {addr}");
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
                            tracing::warn!(peer = %peer, "MySQL wire: rejecting connection — at limit {limit}");
                            drop(stream);
                            continue;
                        }
                    }
                    debug!(peer = %peer, "MySQL wire: new connection");
                    let state = self.state.clone();
                    let conn_id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    active.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state, conn_id).await {
                            debug!(conn_id, "MySQL wire connection closed: {e}");
                        }
                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = shutdown.changed() => {
                    info!("MySQL wire frontend: shutdown signal received, stopping accept loop");
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

    // Send server handshake.
    write_packet(&mut writer, 0, &build_handshake(connection_id)).await?;

    // Read client HandshakeResponse or SSLRequest.
    let (_, payload) = read_packet(&mut reader).await?;

    if is_ssl_request(&payload) {
        warn!(
            conn_id = connection_id,
            "MySQL wire: client requested TLS (SSLRequest); TLS is not supported — closing"
        );
        write_packet(
            &mut writer,
            1,
            &build_err(
                1105,
                "QueryFlux MySQL wire does not support TLS. Disable SSL on the client \
                 (e.g. mysql --ssl-mode=DISABLED, JDBC useSSL=false).",
            ),
        )
        .await?;
        return Ok(());
    }

    let (user, schema) = parse_handshake_response(&payload);

    // Accept any credentials — QueryFlux trusts the network here.
    write_packet(&mut writer, 2, &build_ok(0, 0)).await?;

    info!(
        user,
        schema = ?schema,
        conn_id = connection_id,
        "MySQL wire: client authenticated"
    );

    let mut session = SessionContext {
        user: if user.is_empty() { None } else { Some(user) },
        database: schema,
        extra: HashMap::new(),
        tags: QueryTags::new(),
        agent_context: None,
    };

    // Command loop.
    loop {
        let (seq, payload) = match read_packet(&mut reader).await {
            Ok(p) => p,
            Err(_) => break, // client disconnected
        };
        if payload.is_empty() {
            break;
        }

        let cmd = payload[0];
        let body = &payload[1..];

        match cmd {
            COM_QUIT => break,

            COM_PING => {
                write_packet(&mut writer, seq.wrapping_add(1), &build_ok(0, 0)).await?;
            }

            COM_INIT_DB => {
                let db = String::from_utf8_lossy(body)
                    .trim_end_matches('\0')
                    .to_string();
                session.database = if db.is_empty() { None } else { Some(db) };
                write_packet(&mut writer, seq.wrapping_add(1), &build_ok(0, 0)).await?;
            }

            COM_QUERY => {
                let sql = String::from_utf8_lossy(body)
                    .trim_end_matches('\0')
                    .to_string();
                debug!(conn_id = connection_id, sql = %sql, "MySQL wire: query");
                handle_com_query(&mut writer, &state, &mut session, &sql, seq.wrapping_add(1))
                    .await?;
            }

            COM_FIELD_LIST => {
                write_packet(&mut writer, seq.wrapping_add(1), &build_eof()).await?;
            }

            _ => {
                warn!(
                    conn_id = connection_id,
                    cmd, "MySQL wire: unsupported command"
                );
                write_packet(
                    &mut writer,
                    seq.wrapping_add(1),
                    &build_err(1047, &format!("Unsupported command: {cmd:#04x}")),
                )
                .await?;
            }
        }
    }

    debug!(conn_id = connection_id, "MySQL wire: connection closed");
    Ok(())
}

// ── Query execution ───────────────────────────────────────────────────────────

async fn handle_com_query<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    state: &Arc<AppState>,
    session: &mut SessionContext,
    sql: &str,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Unwrap MySQL conditional comments: /*!40101 SET ... */ → SET ...
    let logical = strip_mysql_conditional_comment(sql);
    let sql_lower = logical.trim().to_lowercase();

    // Fast-path: SET query_tags / SET SESSION query_tags — update session tags and ACK.
    if let Some(new_tags) = try_parse_set_query_tags(logical) {
        session.tags = new_tags;
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: all other SET statements — acknowledge without dispatching.
    // Capture simple single-assignment forms into session.extra so downstream
    // routers and Python scripts can read them (key convention: lowercase var name).
    if sql_lower.starts_with("set ") || sql_lower.starts_with("set\t") {
        if let Some((key, val)) = try_parse_set_kv(logical) {
            session.extra.insert(key, val);
        }
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: USE db sent as COM_QUERY text (mysql CLI does this).
    // Parse from `logical` (original case) so `USE Sales` stores `Sales`, not `sales`.
    if let Some(db) = try_parse_use(logical) {
        session.database = if db.is_empty() { None } else { Some(db) };
        write_packet(writer, start_seq, &build_ok(0, 0)).await?;
        return Ok(());
    }

    // Fast-path: synthetic @@version / @@version_comment (single value, no comma).
    if sql_lower.contains("@@version") && !sql_lower.contains(',') {
        let col = if sql_lower.contains("version_comment") {
            "@@version_comment"
        } else {
            "@@version"
        };
        return write_string_result(writer, col, "8.0.0-queryflux", start_seq).await;
    }

    // Fast-path: SELECT DATABASE()
    if is_select_database(&sql_lower) {
        return write_optional_string_result(writer, "DATABASE()", session.database(), start_seq)
            .await;
    }

    // Fast-path: SHOW WARNINGS (empty result).
    if sql_lower.starts_with("show warnings") {
        return write_empty_show_warnings(writer, start_seq).await;
    }

    // Fast-path: SHOW VARIABLES / SHOW SESSION VARIABLES / SHOW GLOBAL VARIABLES
    // MySQL clients (JDBC, DBeaver, etc.) probe these at startup. Return empty set
    // rather than forwarding to the backend where they'd fail with a parse error.
    if sql_lower.starts_with("show variables")
        || sql_lower.starts_with("show session variables")
        || sql_lower.starts_with("show global variables")
        || sql_lower.starts_with("show status")
        || sql_lower.starts_with("show session status")
        || sql_lower.starts_with("show global status")
    {
        return write_empty_show_variables(writer, start_seq).await;
    }

    // Fast-path: SELECT with only MySQL metadata expressions (@@vars, VERSION(),
    // CURRENT_SCHEMA(), etc.) and no real FROM clause. Covers both the pure
    // @@-variable init queries and the mixed VERSION()/@@/CURRENT_SCHEMA() probe
    // that mysql-connector-j sends right after the comment-prefixed init query.
    if let Some(col_vals) = try_parse_mysql_metadata_select(&sql_lower, session) {
        return write_synthetic_multi_column_row(writer, &col_vals, start_seq).await;
    }

    let protocol = FrontendProtocol::MySqlWire;

    let creds = Credentials {
        username: session.user().map(|s| s.to_string()),
        ..Default::default()
    };
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            write_packet(writer, start_seq, &build_err(1045, &e.to_string())).await?;
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
            write_packet(writer, start_seq, &build_err(1105, &e.to_string())).await?;
            return Ok(());
        }
    };

    // Channel: sink encodes MySQL packets and sends them; we write them to TCP.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let mut sink = MysqlResultSink::new(tx, start_seq);

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
        // `sink` drops here — closes tx — rx.recv() will return None after last packet
    });

    // Forward encoded MySQL packets to the client as they arrive.
    while let Some(pkt) = rx.recv().await {
        writer.write_all(&pkt).await?;
        writer.flush().await?;
    }

    // Log any panic in the execution task; result errors go through on_error → ERR packet.
    if let Err(e) = exec_task.await {
        warn!("MySQL query task panicked: {e}");
    }

    Ok(())
}

// ── Synthetic result helpers ──────────────────────────────────────────────────

/// Write a single-column single-row string result set directly to the writer.
/// Used for synthetic responses (@@version, etc.) that bypass `execute_to_sink`.
async fn write_string_result<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_name: &str,
    value: &str,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 1);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    write_packet(
        writer,
        seq,
        &build_column_def_named(col_name, MYSQL_TYPE_VAR_STRING),
    )
    .await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    let mut row = Vec::new();
    write_lenenc_str(&mut row, value.as_bytes());
    write_packet(writer, seq, &row).await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Single string column; value may be SQL NULL (e.g. `SELECT DATABASE()` with no schema).
async fn write_optional_string_result<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_name: &str,
    value: Option<&str>,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 1);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    write_packet(
        writer,
        seq,
        &build_column_def_named(col_name, MYSQL_TYPE_VAR_STRING),
    )
    .await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    let mut row = Vec::new();
    match value {
        None => row.push(0xfb), // NULL marker
        Some(v) => write_lenenc_str(&mut row, v.as_bytes()),
    }
    write_packet(writer, seq, &row).await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Empty `SHOW WARNINGS` result (three columns, zero rows).
async fn write_empty_show_warnings<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 3);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for (name, ty) in [
        ("Level", MYSQL_TYPE_VAR_STRING),
        ("Code", MYSQL_TYPE_LONGLONG),
        ("Message", MYSQL_TYPE_VAR_STRING),
    ] {
        write_packet(writer, seq, &build_column_def_named(name, ty)).await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    // Zero rows — straight to the closing EOF.
    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Empty two-column result set for `SHOW VARIABLES` / `SHOW STATUS` and similar
/// MySQL-specific commands that the backend doesn't understand.
async fn write_empty_show_variables<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, 2);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for name in ["Variable_name", "Value"] {
        write_packet(
            writer,
            seq,
            &build_column_def_named(name, MYSQL_TYPE_VAR_STRING),
        )
        .await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    // Zero rows.
    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

/// Multi-column single-row result for synthetic MySQL metadata selects.
/// Each entry is `(column_label, value)` where `None` = SQL NULL.
async fn write_synthetic_multi_column_row<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    col_vals: &[(String, Option<String>)],
    start_seq: u8,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seq = start_seq;

    let mut count = Vec::new();
    write_lenenc_int(&mut count, col_vals.len() as u64);
    write_packet(writer, seq, &count).await?;
    seq = seq.wrapping_add(1);

    for (label, _) in col_vals {
        write_packet(
            writer,
            seq,
            &build_column_def_named(label, MYSQL_TYPE_VAR_STRING),
        )
        .await?;
        seq = seq.wrapping_add(1);
    }

    write_packet(writer, seq, &build_eof()).await?;
    seq = seq.wrapping_add(1);

    let mut row = Vec::new();
    for (_, val) in col_vals {
        match val {
            None => row.push(0xfb), // SQL NULL
            Some(s) => write_lenenc_str(&mut row, s.as_bytes()),
        }
    }
    write_packet(writer, seq, &row).await?;
    seq = seq.wrapping_add(1);

    write_packet(writer, seq, &build_eof()).await?;
    Ok(())
}

// ── MysqlResultSink ───────────────────────────────────────────────────────────

/// Streams Arrow RecordBatches as MySQL text-protocol result packets over a channel.
///
/// Each `on_*` call encodes one or more MySQL packets (4-byte header + payload)
/// and sends them to an `UnboundedSender`. The COM_QUERY handler drains the
/// receiver and writes them to the TCP stream, preserving O(1 batch) memory.
struct MysqlResultSink {
    tx: UnboundedSender<Vec<u8>>,
    seq: u8,
}

impl MysqlResultSink {
    fn new(tx: UnboundedSender<Vec<u8>>, start_seq: u8) -> Self {
        Self { tx, seq: start_seq }
    }

    /// Prepend a 4-byte MySQL packet header and send to the channel.
    fn encode_and_send(&mut self, payload: Vec<u8>) {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        let len = payload.len();
        let mut pkt = Vec::with_capacity(4 + len);
        pkt.push((len & 0xff) as u8);
        pkt.push(((len >> 8) & 0xff) as u8);
        pkt.push(((len >> 16) & 0xff) as u8);
        pkt.push(seq);
        pkt.extend_from_slice(&payload);
        let _ = self.tx.send(pkt); // ignore SendError: receiver gone = client disconnected
    }
}

#[async_trait]
impl ResultSink for MysqlResultSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        // Column count packet.
        let mut count_pkt = Vec::new();
        write_lenenc_int(&mut count_pkt, schema.fields().len() as u64);
        self.encode_and_send(count_pkt);

        // One ColumnDefinition41 packet per field.
        for field in schema.fields() {
            self.encode_and_send(build_column_def_from_field(field));
        }

        // EOF separating column defs from rows.
        self.encode_and_send(build_eof());
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for row in 0..batch.num_rows() {
            let mut row_pkt = Vec::new();
            for col in batch.columns() {
                match arrow_value_to_mysql_bytes(col.as_ref(), row) {
                    None => row_pkt.push(0xfb), // NULL marker
                    Some(bytes) => write_lenenc_str(&mut row_pkt, &bytes),
                }
            }
            self.encode_and_send(row_pkt);
        }
        Ok(())
    }

    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        self.encode_and_send(build_eof());
        Ok(())
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.encode_and_send(build_err(1105, message));
        Ok(())
    }

    async fn on_native_chunk(&mut self, chunk: &NativeResultChunk) -> Result<()> {
        // On the first chunk, send column count + column definition packets + EOF.
        if let Some(columns) = &chunk.columns {
            let mut count_pkt = Vec::new();
            write_lenenc_int(&mut count_pkt, columns.len() as u64);
            self.encode_and_send(count_pkt);

            for col in columns {
                self.encode_and_send(build_column_def_from_native(col));
            }

            self.encode_and_send(build_eof());
        }

        // Encode each row as a MySQL text-protocol row data packet.
        for row in &chunk.rows {
            let mut row_pkt = Vec::new();
            for value in &row.0 {
                match value {
                    None => row_pkt.push(0xfb), // NULL marker
                    Some(bytes) => write_lenenc_str(&mut row_pkt, bytes),
                }
            }
            self.encode_and_send(row_pkt);
        }
        Ok(())
    }
}

// ── Native type helpers ───────────────────────────────────────────────────────

fn native_type_to_mysql_type(kind: &NativeTypeKind) -> u8 {
    match kind {
        NativeTypeKind::Boolean | NativeTypeKind::TinyInt => MYSQL_TYPE_TINY,
        NativeTypeKind::SmallInt => MYSQL_TYPE_SHORT,
        NativeTypeKind::Int => MYSQL_TYPE_LONG,
        NativeTypeKind::BigInt => MYSQL_TYPE_LONGLONG,
        NativeTypeKind::Float => MYSQL_TYPE_FLOAT,
        NativeTypeKind::Double => MYSQL_TYPE_DOUBLE,
        NativeTypeKind::Decimal => MYSQL_TYPE_NEWDECIMAL,
        NativeTypeKind::Date => MYSQL_TYPE_DATE,
        NativeTypeKind::Time => MYSQL_TYPE_TIME,
        NativeTypeKind::DateTime => MYSQL_TYPE_DATETIME,
        NativeTypeKind::Timestamp => MYSQL_TYPE_TIMESTAMP,
        NativeTypeKind::Binary | NativeTypeKind::Blob | NativeTypeKind::Text => MYSQL_TYPE_BLOB,
        NativeTypeKind::Json => MYSQL_TYPE_JSON,
        _ => MYSQL_TYPE_VAR_STRING, // Char, Varchar, Unknown
    }
}

/// Build a MySQL ColumnDefinition41 packet from a `NativeColumn`.
fn build_column_def_from_native(col: &NativeColumn) -> Vec<u8> {
    let mut pkt = Vec::new();
    write_lenenc_str(&mut pkt, b"def"); // catalog
    write_lenenc_str(&mut pkt, b""); // schema
    write_lenenc_str(&mut pkt, b""); // table
    write_lenenc_str(&mut pkt, b""); // org_table
    write_lenenc_str(&mut pkt, col.name.as_bytes()); // name
    write_lenenc_str(&mut pkt, col.name.as_bytes()); // org_name
    pkt.push(0x0c); // fixed-length block = 12
    pkt.extend_from_slice(&0x21u16.to_le_bytes()); // charset: utf8mb4 (33)
    pkt.extend_from_slice(&0xffffu32.to_le_bytes()); // max column length
    pkt.push(native_type_to_mysql_type(&col.type_info.kind)); // type byte
    let mut flags: u16 = if col.nullable { 0 } else { 0x0001 }; // NOT_NULL_FLAG
    if col.type_info.unsigned {
        flags |= 0x0020;
    } // UNSIGNED_FLAG
    pkt.extend_from_slice(&flags.to_le_bytes());
    pkt.push(col.type_info.scale.unwrap_or(0)); // decimals
    pkt.extend_from_slice(&[0, 0]); // filler
    pkt
}

// ── Arrow type helpers ────────────────────────────────────────────────────────

fn arrow_type_to_mysql_type(dt: &DataType) -> u8 {
    match dt {
        DataType::Boolean | DataType::Int8 => MYSQL_TYPE_TINY,
        DataType::Int16 | DataType::UInt8 => MYSQL_TYPE_SHORT,
        DataType::Int32 | DataType::UInt16 => MYSQL_TYPE_LONG,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => MYSQL_TYPE_LONGLONG,
        DataType::Float16 | DataType::Float32 => MYSQL_TYPE_FLOAT,
        DataType::Float64 => MYSQL_TYPE_DOUBLE,
        DataType::Decimal128(..) | DataType::Decimal256(..) => MYSQL_TYPE_DECIMAL,
        DataType::Date32 | DataType::Date64 => MYSQL_TYPE_DATE,
        DataType::Timestamp(..) => MYSQL_TYPE_DATETIME,
        DataType::Time32(..) | DataType::Time64(..) => MYSQL_TYPE_TIMESTAMP,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => MYSQL_TYPE_BLOB,
        _ => MYSQL_TYPE_VAR_STRING, // Utf8, LargeUtf8, List, Map, Struct, Duration, ...
    }
}

/// Build a MySQL ColumnDefinition41 packet from an Arrow field.
fn build_column_def_from_field(field: &Field) -> Vec<u8> {
    let mut pkt = Vec::new();
    write_lenenc_str(&mut pkt, b"def"); // catalog
    write_lenenc_str(&mut pkt, b""); // schema
    write_lenenc_str(&mut pkt, b""); // table
    write_lenenc_str(&mut pkt, b""); // org_table
    write_lenenc_str(&mut pkt, field.name().as_bytes()); // name
    write_lenenc_str(&mut pkt, field.name().as_bytes()); // org_name
    pkt.push(0x0c); // fixed-length block = 12
    pkt.extend_from_slice(&0x21u16.to_le_bytes()); // charset: utf8mb4 (33)
    pkt.extend_from_slice(&0xffffu32.to_le_bytes()); // max column length
    pkt.push(arrow_type_to_mysql_type(field.data_type())); // type byte
    let flags: u16 = if field.is_nullable() { 0 } else { 0x0001 }; // NOT_NULL_FLAG
    pkt.extend_from_slice(&flags.to_le_bytes());
    pkt.push(0); // decimals
    pkt.extend_from_slice(&[0u8; 2]); // filler
    pkt
}

/// Build a ColumnDefinition41 packet by name and type byte (for synthetic results).
fn build_column_def_named(name: &str, type_byte: u8) -> Vec<u8> {
    let mut pkt = Vec::new();
    write_lenenc_str(&mut pkt, b"def");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, b"");
    write_lenenc_str(&mut pkt, name.as_bytes());
    write_lenenc_str(&mut pkt, name.as_bytes());
    pkt.push(0x0c);
    pkt.extend_from_slice(&0x21u16.to_le_bytes());
    pkt.extend_from_slice(&0xffffu32.to_le_bytes());
    pkt.push(type_byte);
    pkt.extend_from_slice(&0u16.to_le_bytes()); // nullable
    pkt.push(0);
    pkt.extend_from_slice(&[0u8; 2]);
    pkt
}

/// Serialize a single Arrow array cell as MySQL text protocol bytes.
/// Returns `None` for SQL NULL.
fn arrow_value_to_mysql_bytes(col: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if col.is_null(row) {
        return None;
    }
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let s = ArrayFormatter::try_new(col, &FormatOptions::default())
        .map(|fmt| fmt.value(row).to_string())
        .unwrap_or_default();
    Some(s.into_bytes())
}

// ── Packet I/O ────────────────────────────────────────────────────────────────

async fn read_packet<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::result::Result<(u8, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let seq = header[3];
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    Ok((seq, payload))
}

async fn write_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    seq: u8,
    payload: &[u8],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let len = payload.len();
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];
    writer.write_all(&header).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

// ── Packet builders ───────────────────────────────────────────────────────────

fn build_handshake(connection_id: u32) -> Vec<u8> {
    let caps: u32 = CLIENT_LONG_PASSWORD
        | CLIENT_FOUND_ROWS
        | CLIENT_LONG_FLAG
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_NO_SCHEMA
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;

    let mut pkt = Vec::new();
    pkt.push(0x0a); // protocol version 10
    pkt.extend_from_slice(b"8.0.0-queryflux\0"); // server version + NUL
    pkt.extend_from_slice(&connection_id.to_le_bytes());
    pkt.extend_from_slice(b"12345678"); // auth-plugin-data part 1 (8 bytes)
    pkt.push(0x00); // filler
    pkt.extend_from_slice(&(caps as u16).to_le_bytes()); // capabilities low
    pkt.push(0x21); // charset: utf8mb4 (33)
    pkt.extend_from_slice(&0u16.to_le_bytes()); // status flags
    pkt.extend_from_slice(&((caps >> 16) as u16).to_le_bytes()); // capabilities high
    pkt.push(21); // auth_plugin_data length (8 + 13)
    pkt.extend_from_slice(&[0u8; 10]); // reserved
    pkt.extend_from_slice(b"123456789012\0"); // auth-plugin-data part 2 (12 bytes + NUL)
    pkt.extend_from_slice(b"mysql_native_password\0"); // plugin name + NUL
    pkt
}

fn build_ok(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0x00); // OK marker
    write_lenenc_int(&mut pkt, affected_rows);
    write_lenenc_int(&mut pkt, last_insert_id);
    pkt.extend_from_slice(&0x0002u16.to_le_bytes()); // status: SERVER_STATUS_AUTOCOMMIT
    pkt.extend_from_slice(&0u16.to_le_bytes()); // warning count
    pkt
}

fn build_err(error_code: u16, message: &str) -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0xff); // ERR marker
    pkt.extend_from_slice(&error_code.to_le_bytes());
    pkt.push(b'#'); // SQL state marker
    pkt.extend_from_slice(b"HY000"); // generic SQL state
                                     // Truncate to 512 bytes to keep packets reasonable.
    pkt.extend_from_slice(message.as_bytes().get(..512).unwrap_or(message.as_bytes()));
    pkt
}

fn build_eof() -> Vec<u8> {
    let mut pkt = Vec::new();
    pkt.push(0xfe); // EOF marker
    pkt.extend_from_slice(&0u16.to_le_bytes()); // warning count
    pkt.extend_from_slice(&0x0002u16.to_le_bytes()); // status: SERVER_STATUS_AUTOCOMMIT
    pkt
}

// ── Length-encoded integers and strings ──────────────────────────────────────

fn write_lenenc_int(buf: &mut Vec<u8>, n: u64) {
    if n < 251 {
        buf.push(n as u8);
    } else if n < 65_536 {
        buf.push(0xfc);
        buf.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n < 16_777_216 {
        buf.push(0xfd);
        buf.push((n & 0xff) as u8);
        buf.push(((n >> 8) & 0xff) as u8);
        buf.push(((n >> 16) & 0xff) as u8);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&n.to_le_bytes());
    }
}

fn write_lenenc_str(buf: &mut Vec<u8>, s: &[u8]) {
    write_lenenc_int(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

// ── SQL classification helpers ────────────────────────────────────────────────

/// Strip any number of leading block comments (`/* ... */` and `/*!NN... */`) from
/// the query and return the first non-comment token onwards.
///
/// MySQL connectors prepend metadata comments to every query, e.g.:
///   `/* mysql-connector-j-9.5.0 (...) */ SELECT @@session.auto_increment_increment ...`
/// Without stripping these the query starts with `/*` instead of `SELECT`, so all
/// fast-path checks miss it and it is forwarded to the backend engine unchanged.
///
/// For `/*!NNNNNbody*/` conditional-execution comments the body is extracted and
/// returned (the mysql CLI uses this form for SET init statements).
fn strip_mysql_conditional_comment(sql: &str) -> &str {
    let mut t = sql.trim();
    loop {
        if !t.starts_with("/*") {
            return t;
        }
        // `/*!NNNNNbody*/` — conditional execution: return the body itself.
        if let Some(inner) = t.strip_prefix("/*!") {
            if let Some((before_close, after_close)) = inner.split_once("*/") {
                let skip = before_close
                    .char_indices()
                    .find(|(_, c)| !c.is_ascii_digit())
                    .map(|(i, _)| i)
                    .unwrap_or(before_close.len());
                let body = before_close[skip..].trim();
                if !body.is_empty() {
                    return body;
                }
                // Empty body (e.g. `/*!*/`) — skip and keep going.
                t = after_close.trim();
                continue;
            }
        }
        // Plain `/* ... */` comment — skip it entirely.
        if let Some((_, after_close)) = t.split_once("*/") {
            t = after_close.trim();
        } else {
            // Unterminated comment — return as-is.
            return t;
        }
    }
}

/// Parse `SET query_tags = '...'` / `SET SESSION query_tags = '...'` (and `query_tag` spelling).
/// Returns the parsed `QueryTags` on match, `None` otherwise.
/// Case-insensitive; tolerant of extra whitespace and a trailing semicolon.
fn try_parse_set_query_tags(sql: &str) -> Option<QueryTags> {
    let s = sql.trim().trim_end_matches(';').trim();
    // Must start with SET (case-insensitive).
    let rest = s.strip_prefix_ci("SET")?;
    let rest = rest.trim_start();
    // Optionally skip SESSION keyword.
    let rest = if rest.to_ascii_lowercase().starts_with("session") {
        rest["SESSION".len()..].trim_start()
    } else {
        rest
    };
    // Match query_tags or query_tag key.
    let rest = if rest.to_ascii_lowercase().starts_with("query_tags") {
        &rest["query_tags".len()..]
    } else if rest.to_ascii_lowercase().starts_with("query_tag") {
        &rest["query_tag".len()..]
    } else {
        return None;
    };
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    // Strip surrounding single or double quotes.
    let value = rest
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| rest.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(rest);
    let (tags, _) = parse_query_tags(value);
    Some(tags)
}

trait StripPrefixCi {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str>;
}
impl StripPrefixCi for str {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str> {
        if self.len() >= prefix.len() && self[..prefix.len()].eq_ignore_ascii_case(prefix) {
            Some(&self[prefix.len()..])
        } else {
            None
        }
    }
}

/// Parse `USE db` / `USE \`db\`` sent as COM_QUERY text. Returns the database name
/// in the original case as sent by the client.
fn try_parse_use(sql: &str) -> Option<String> {
    let s = sql.trim().trim_end_matches(';');
    let s_lower = s.to_lowercase();
    // Detect the "USE" keyword case-insensitively, then extract db from the original string.
    let rest = if s_lower == "use" {
        ""
    } else if s_lower.starts_with("use ") || s_lower.starts_with("use\t") {
        &s[4..]
    } else {
        return None;
    };
    let rest = rest.trim();
    if rest.is_empty() {
        return Some(String::new());
    }
    Some(rest.trim_matches('`').to_string())
}

/// Parse a simple `SET [SESSION] [@@[session.]]var = value` statement into a
/// `(key, value)` pair suitable for storing in `SessionContext::extra`.
///
/// Returns `None` for multi-assignment forms (containing `,` before `=`),
/// unparseable input, or empty keys — those are still ACK'd but not stored.
/// The key is lowercased; `@@session.` / `@@` / `@` prefixes are stripped.
fn try_parse_set_kv(sql: &str) -> Option<(String, String)> {
    let s = sql.trim().trim_end_matches(';');
    let s_lower = s.to_lowercase();
    let rest = if s_lower.starts_with("set session ") || s_lower.starts_with("set session\t") {
        &s[12..]
    } else if s_lower.starts_with("set ") || s_lower.starts_with("set\t") {
        &s[4..]
    } else {
        return None;
    };
    let rest = rest.trim();
    // Strip @@ prefix and optional `session.` qualifier, then any remaining @ (user vars).
    let rest = rest.trim_start_matches("@@");
    let rest = if rest.to_lowercase().starts_with("session.") {
        &rest[8..]
    } else {
        rest
    };
    let rest = rest.trim_start_matches('@');
    // Reject multi-assignment: any unquoted comma in the assignment text.
    // Session variable values virtually never contain bare commas, so a simple
    // scan suffices without full quote-aware parsing.
    let eq_pos = rest.find('=')?;
    {
        let mut in_q = false;
        let mut q_ch = '\0';
        for ch in rest.chars() {
            if in_q {
                if ch == q_ch {
                    in_q = false;
                }
            } else {
                match ch {
                    '\'' | '"' => {
                        in_q = true;
                        q_ch = ch;
                    }
                    ',' => return None,
                    _ => {}
                }
            }
        }
    }
    let key = rest[..eq_pos].trim().to_lowercase();
    if key.is_empty() {
        return None;
    }
    let raw_val = rest[eq_pos + 1..].trim();
    // Strip uniform surrounding single or double quotes.
    let val = if raw_val.len() >= 2
        && ((raw_val.starts_with('\'') && raw_val.ends_with('\''))
            || (raw_val.starts_with('"') && raw_val.ends_with('"')))
    {
        raw_val[1..raw_val.len() - 1].to_string()
    } else {
        raw_val.to_string()
    };
    Some((key, val))
}

/// Detect `SELECT DATABASE()` (with optional whitespace variations).
fn is_select_database(sql_lower: &str) -> bool {
    let compact: String = sql_lower
        .trim()
        .trim_end_matches(';')
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    compact.starts_with("selectdatabase()") && !compact.contains("from")
}

/// Strip a trailing `FROM dual` / `FROM DUAL` (with any whitespace) from a SELECT
/// body. MySQL/JDBC often appends `FROM dual` to pure variable selects.
fn strip_from_dual(select_body: &str) -> &str {
    // Work on the already-lowercased input. Strip trailing whitespace, then look
    // at the last two non-empty tokens. If they are `from` and `dual`, strip
    // them (and any preceding whitespace); otherwise, return the original body.
    let tail = select_body.trim_end();

    // Fast path: if there's nothing (or only whitespace), or fewer than two
    // tokens, we can't have a trailing `from dual`.
    let mut tokens: Vec<(&str, usize)> = Vec::new();
    for token in tail.split_whitespace() {
        // Compute the byte offset of this token within `tail`.
        let start = token.as_ptr() as usize - tail.as_ptr() as usize;
        tokens.push((token, start));
    }

    if tokens.len() >= 2 {
        let (last_tok, _) = tokens[tokens.len() - 1];
        let (prev_tok, prev_start) = tokens[tokens.len() - 2];

        // `select_body` is already lowercased by the caller, so we can do
        // direct string comparisons.
        if last_tok == "dual" && prev_tok == "from" {
            // Slice everything before the `from` token, then trim any
            // whitespace left at the end of that prefix.
            let before_from = &tail[..prev_start];
            let stripped = before_from.trim_end();
            return stripped;
        }
    }
    select_body
}

/// Default value for a MySQL `@@variable`. Strips `@@`, `@@session.`, `@@global.`
/// prefixes before matching. Returns a static string suitable for mysql-connector-j
/// to parse — numeric variables get digit strings so the Java JDBC driver doesn't
/// throw `NumberFormatException: For input string: ""`.
fn mysql_var_default(expr_lower: &str) -> &'static str {
    let name = expr_lower
        .trim_start_matches("@@")
        .trim_start_matches("session.")
        .trim_start_matches("global.");
    match name {
        "auto_increment_increment" | "auto_increment_offset" => "1",
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server" => "utf8mb4",
        "collation_server" | "collation_connection" => "utf8mb4_0900_ai_ci",
        "init_connect" => "",
        "interactive_timeout" | "wait_timeout" => "28800",
        "license" => "GPL",
        "lower_case_table_names" => "0",
        "max_allowed_packet" => "67108864",
        "net_buffer_length" => "16384",
        "net_write_timeout" => "60",
        "performance_schema" => "1",
        "query_cache_size" => "1048576",
        "query_cache_type" => "0",
        "sql_mode" => "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
        "system_time_zone" => "UTC",
        "time_zone" => "SYSTEM",
        "transaction_isolation" | "tx_isolation" => "REPEATABLE-READ",
        // Boolean / integer variables that mysql-connector-j 9.x reads per-query
        // (e.g. before each statement to detect read-only mode). Returning "" causes
        // Java's parseInt("") → NumberFormatException: For input string: "".
        "transaction_read_only" | "read_only" | "innodb_read_only" => "0",
        "foreign_key_checks" | "unique_checks" => "1",
        "sql_auto_is_null" | "max_execution_time" | "session_track_state_change" => "0",
        "version" => "8.0.0-queryflux",
        "version_comment" => "QueryFlux",
        _ => "",
    }
}

/// If `expr_lower` (already lowercased, AS alias already stripped) is a MySQL-only
/// synthetic expression, return its value. `Some(Some(s))` = string, `Some(None)` = NULL.
/// Returns `None` if it's a real expression that must be dispatched to the backend.
fn mysql_synthetic_value(expr_lower: &str, session: &SessionContext) -> Option<Option<String>> {
    if expr_lower.starts_with("@@") {
        return Some(Some(mysql_var_default(expr_lower).to_string()));
    }
    match expr_lower {
        "version()" => Some(Some("8.0.0-queryflux".to_string())),
        "current_schema()" | "schema()" | "database()" => {
            Some(session.database().map(|s| s.to_string()))
        }
        "current_user()" | "user()" | "system_user()" | "session_user()" => {
            Some(Some(session.user().unwrap_or("").to_string()))
        }
        "connection_id()" => Some(Some("1".to_string())),
        _ => None,
    }
}

/// Detect a SELECT whose column list is composed entirely of MySQL-only metadata
/// expressions (`@@vars`, `VERSION()`, `CURRENT_SCHEMA()`, etc.) with no real FROM
/// clause. Returns `(column_label, value)` pairs on success, `None` if any column
/// requires actual dispatch.
///
/// Handles:
///   - Pure `@@var` lists (mysql-connector-j JDBC init query)
///   - Mixed `VERSION(), @@version_comment, CURRENT_SCHEMA()` probes (DataGrip)
///   - Optional trailing `FROM dual` or `LIMIT n`
fn try_parse_mysql_metadata_select(
    sql_lower: &str,
    session: &SessionContext,
) -> Option<Vec<(String, Option<String>)>> {
    let trimmed = sql_lower.trim().trim_end_matches(';');
    let rest = trimmed.strip_prefix("select")?.trim_start();
    let rest = strip_from_dual(rest);

    // Real FROM clause (not dual) — fall through to dispatch.
    if rest.split_whitespace().any(|w| w == "from") {
        return None;
    }

    let main = if let Some(i) = rest.find(" limit ") {
        &rest[..i]
    } else {
        rest
    };

    let parts: Vec<&str> = main.split(',').map(str::trim).collect();
    if parts.is_empty() {
        return None;
    }

    let mut col_vals = Vec::with_capacity(parts.len());
    for part in &parts {
        // Derive the column label (the AS alias, or the expression itself).
        let label = {
            if let Some(pos) = part.rfind(" as ") {
                part[pos + 4..].trim().to_string()
            } else {
                part.trim().to_string()
            }
        };
        // Base expression without alias.
        let expr = if let Some(pos) = part.rfind(" as ") {
            part[..pos].trim().to_lowercase()
        } else {
            part.trim().to_lowercase()
        };
        match mysql_synthetic_value(&expr, session) {
            Some(val) => col_vals.push((label, val)),
            None => return None, // unknown expression — needs real dispatch
        }
    }

    if col_vals.is_empty() {
        return None;
    }
    Some(col_vals)
}

// ── HandshakeResponse / SSLRequest parsing ────────────────────────────────────

/// SSLRequest is a 32-byte packet with CLIENT_SSL set but no username.
/// If we mistake it for a login and send OK, the client starts TLS while
/// we expect MySQL commands — both sides block indefinitely.
fn is_ssl_request(payload: &[u8]) -> bool {
    if payload.len() != 32 {
        return false;
    }
    let caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    (caps & CLIENT_SSL) != 0
}

/// Extracts (username, optional database) from a MySQL 4.1+ HandshakeResponse packet.
fn parse_handshake_response(payload: &[u8]) -> (String, Option<String>) {
    // Layout: capabilities(4) + max_packet_size(4) + charset(1) + reserved(23) = 32 bytes
    if payload.len() < 32 {
        return (String::new(), None);
    }
    // Read the client's capability flags from the first 4 bytes of the response.
    let client_caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut pos = 32usize;

    // Null-terminated username.
    let user = read_nul_str(payload, &mut pos);

    // Auth response: 1-byte length prefix (CLIENT_SECURE_CONNECTION).
    if pos < payload.len() {
        let auth_len = payload[pos] as usize;
        pos += 1 + auth_len;
    }

    // Optional null-terminated database — only present if CLIENT_CONNECT_WITH_DB is set
    // by the client. Without this check, the auth-plugin name ("mysql_native_password")
    // that immediately follows would be misread as the schema.
    let database = if (client_caps & CLIENT_CONNECT_WITH_DB) != 0 && pos < payload.len() {
        let db = read_nul_str(payload, &mut pos);
        if db.is_empty() {
            None
        } else {
            Some(db)
        }
    } else {
        None
    };

    (user, database)
}

fn read_nul_str(payload: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < payload.len() && payload[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&payload[start..*pos]).to_string();
    if *pos < payload.len() {
        *pos += 1; // consume NUL terminator
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::native_result::{NativeColumn, NativeTypeInfo, NativeTypeKind};

    // ── native_type_to_mysql_type ─────────────────────────────────────────────

    #[test]
    fn tinyint_maps_to_mysql_tiny() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::TinyInt),
            MYSQL_TYPE_TINY
        );
    }

    #[test]
    fn bigint_maps_to_mysql_longlong() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::BigInt),
            MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn decimal_maps_to_mysql_newdecimal() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Decimal),
            MYSQL_TYPE_NEWDECIMAL
        );
    }

    #[test]
    fn date_maps_to_mysql_date() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Date),
            MYSQL_TYPE_DATE
        );
    }

    #[test]
    fn time_maps_to_mysql_time() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Time),
            MYSQL_TYPE_TIME
        );
    }

    #[test]
    fn datetime_maps_to_mysql_datetime() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::DateTime),
            MYSQL_TYPE_DATETIME
        );
    }

    #[test]
    fn timestamp_maps_to_mysql_timestamp() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Timestamp),
            MYSQL_TYPE_TIMESTAMP
        );
    }

    #[test]
    fn json_maps_to_mysql_json() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Json),
            MYSQL_TYPE_JSON
        );
    }

    #[test]
    fn text_maps_to_mysql_blob() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Text),
            MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn varchar_maps_to_mysql_var_string() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Varchar),
            MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn unknown_maps_to_mysql_var_string() {
        assert_eq!(
            native_type_to_mysql_type(&NativeTypeKind::Unknown),
            MYSQL_TYPE_VAR_STRING
        );
    }

    // ── build_column_def_from_native — packet structure ───────────────────────
    //
    // ColumnDefinition41 layout for a column named "id" (2 bytes):
    //   4  bytes: lenenc "def"       (1 len + 3 data)
    //   1  byte:  lenenc ""          (schema)
    //   1  byte:  lenenc ""          (table)
    //   1  byte:  lenenc ""          (org_table)
    //   3  bytes: lenenc "id"        (1 len + 2 data)
    //   3  bytes: lenenc "id"        (org_name)
    //   1  byte:  0x0c               (fixed-length-fields marker)
    //   2  bytes: charset            (0x21 = utf8mb4)
    //   4  bytes: max col len        (0xffff)
    //   1  byte:  type
    //   2  bytes: flags
    //   1  byte:  decimals
    //   2  bytes: filler             (0x00 0x00)
    //
    //  total = 26 bytes for name "id"

    fn make_col(name: &str, kind: NativeTypeKind, nullable: bool, unsigned: bool) -> NativeColumn {
        NativeColumn {
            name: name.to_string(),
            type_info: NativeTypeInfo {
                kind,
                precision: None,
                scale: None,
                unsigned,
            },
            nullable,
        }
    }

    // Compute type-byte offset for a given column name length.
    fn type_byte_offset(name_len: usize) -> usize {
        4               // lenenc "def"
        + 1             // lenenc ""  (schema)
        + 1             // lenenc ""  (table)
        + 1             // lenenc ""  (org_table)
        + 1 + name_len  // lenenc name
        + 1 + name_len  // lenenc org_name
        + 1             // 0x0c marker
        + 2             // charset
        + 4 // max col len
    }

    #[test]
    fn bigint_column_type_byte_is_longlong() {
        let col = make_col("id", NativeTypeKind::BigInt, true, false);
        let pkt = build_column_def_from_native(&col);
        let type_pos = type_byte_offset("id".len());
        assert_eq!(
            pkt[type_pos], MYSQL_TYPE_LONGLONG,
            "type byte should be LONGLONG"
        );
    }

    #[test]
    fn decimal_column_type_byte_is_newdecimal() {
        let col = make_col("price", NativeTypeKind::Decimal, true, false);
        let pkt = build_column_def_from_native(&col);
        let type_pos = type_byte_offset("price".len());
        assert_eq!(pkt[type_pos], MYSQL_TYPE_NEWDECIMAL);
    }

    #[test]
    fn not_null_column_sets_not_null_flag() {
        let col = make_col("id", NativeTypeKind::BigInt, false, false);
        let pkt = build_column_def_from_native(&col);
        let flags_pos = type_byte_offset("id".len()) + 1;
        let flags = u16::from_le_bytes([pkt[flags_pos], pkt[flags_pos + 1]]);
        assert_ne!(flags & 0x0001, 0, "NOT_NULL_FLAG should be set");
    }

    #[test]
    fn nullable_column_clears_not_null_flag() {
        let col = make_col("id", NativeTypeKind::BigInt, true, false);
        let pkt = build_column_def_from_native(&col);
        let flags_pos = type_byte_offset("id".len()) + 1;
        let flags = u16::from_le_bytes([pkt[flags_pos], pkt[flags_pos + 1]]);
        assert_eq!(
            flags & 0x0001,
            0,
            "NOT_NULL_FLAG should be clear for nullable"
        );
    }

    #[test]
    fn unsigned_column_sets_unsigned_flag() {
        let col = make_col("cnt", NativeTypeKind::BigInt, true, true);
        let pkt = build_column_def_from_native(&col);
        let flags_pos = type_byte_offset("cnt".len()) + 1;
        let flags = u16::from_le_bytes([pkt[flags_pos], pkt[flags_pos + 1]]);
        assert_ne!(flags & 0x0020, 0, "UNSIGNED_FLAG should be set");
    }

    #[test]
    fn charset_is_utf8mb4() {
        let col = make_col("s", NativeTypeKind::Varchar, true, false);
        let pkt = build_column_def_from_native(&col);
        // scan for the 0x0c fixed-length-fields marker and read the 2 charset bytes after it
        let marker_pos = pkt.iter().position(|&b| b == 0x0c).expect("0x0c marker");
        let charset = u16::from_le_bytes([pkt[marker_pos + 1], pkt[marker_pos + 2]]);
        assert_eq!(charset, 0x21, "charset should be utf8mb4 (33 / 0x21)");
    }

    #[test]
    fn packet_ends_with_zero_filler() {
        let col = make_col("x", NativeTypeKind::Int, true, false);
        let pkt = build_column_def_from_native(&col);
        assert_eq!(pkt[pkt.len() - 2], 0, "penultimate filler byte should be 0");
        assert_eq!(pkt[pkt.len() - 1], 0, "last filler byte should be 0");
    }

    #[test]
    fn column_name_appears_in_packet() {
        let col = make_col("my_column", NativeTypeKind::Varchar, true, false);
        let pkt = build_column_def_from_native(&col);
        let name_bytes = b"my_column";
        let found = pkt.windows(name_bytes.len()).any(|w| w == name_bytes);
        assert!(found, "column name bytes should appear in packet");
    }

    // ── try_parse_set_kv ──────────────────────────────────────────────────────

    #[test]
    fn set_bare_variable() {
        assert_eq!(
            try_parse_set_kv("SET time_zone = '+00:00'"),
            Some(("time_zone".to_string(), "+00:00".to_string()))
        );
    }

    #[test]
    fn set_session_prefix_stripped() {
        assert_eq!(
            try_parse_set_kv("SET SESSION time_zone = 'UTC'"),
            Some(("time_zone".to_string(), "UTC".to_string()))
        );
    }

    #[test]
    fn set_double_at_prefix_stripped() {
        assert_eq!(
            try_parse_set_kv("SET @@time_zone = 'UTC'"),
            Some(("time_zone".to_string(), "UTC".to_string()))
        );
    }

    #[test]
    fn set_double_at_session_dot_prefix_stripped() {
        assert_eq!(
            try_parse_set_kv("SET @@session.time_zone = 'UTC'"),
            Some(("time_zone".to_string(), "UTC".to_string()))
        );
    }

    #[test]
    fn set_unquoted_value() {
        assert_eq!(
            try_parse_set_kv("SET autocommit = 1"),
            Some(("autocommit".to_string(), "1".to_string()))
        );
    }

    #[test]
    fn set_key_is_lowercased() {
        assert_eq!(
            try_parse_set_kv("SET TimeZone = 'UTC'"),
            Some(("timezone".to_string(), "UTC".to_string()))
        );
    }

    #[test]
    fn set_trailing_semicolon_handled() {
        assert_eq!(
            try_parse_set_kv("SET autocommit = 0;"),
            Some(("autocommit".to_string(), "0".to_string()))
        );
    }

    #[test]
    fn set_multi_assignment_returns_none() {
        assert_eq!(try_parse_set_kv("SET a = 1, b = 2"), None);
    }

    #[test]
    fn non_set_statement_returns_none() {
        assert_eq!(try_parse_set_kv("SELECT 1"), None);
    }

    // ── agent context via SET statements ─────────────────────────────────────

    fn session_after_sets(stmts: &[&str]) -> queryflux_core::session::SessionContext {
        let mut session = queryflux_core::session::SessionContext {
            user: None,
            database: None,
            tags: queryflux_core::tags::QueryTags::new(),
            extra: std::collections::HashMap::new(),
            agent_context: None,
        };
        for stmt in stmts {
            if let Some((key, val)) = try_parse_set_kv(stmt) {
                session.extra.insert(key, val);
            }
        }
        session
    }

    #[test]
    fn agent_context_from_set_statements() {
        let s = session_after_sets(&[
            "SET agent_id = 'agent-mysql'",
            "SET conversation_id = 'conv-mysql'",
            "SET step_index = '5'",
            "SET tool_call_id = 'call_abc'",
            "SET query_intent = 'aggregation'",
        ]);
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "agent-mysql");
        assert_eq!(ctx.conversation_id, "conv-mysql");
        assert_eq!(ctx.step_index, Some(5));
        assert_eq!(ctx.tool_call_id.as_deref(), Some("call_abc"));
        assert_eq!(
            ctx.query_intent,
            queryflux_core::session::QueryIntent::Aggregation
        );
    }

    #[test]
    fn agent_context_set_session_prefix() {
        let s = session_after_sets(&[
            "SET SESSION agent_id = 'a'",
            "SET SESSION conversation_id = 'c'",
        ]);
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "a");
        assert_eq!(ctx.conversation_id, "c");
    }

    #[test]
    fn agent_context_requires_both_ids() {
        let s = session_after_sets(&["SET agent_id = 'only-agent'"]);
        assert!(s.resolved_agent_context().is_none());
    }

    #[test]
    fn agent_context_later_set_overwrites_earlier() {
        let s = session_after_sets(&[
            "SET agent_id = 'first'",
            "SET conversation_id = 'conv'",
            "SET agent_id = 'second'",
        ]);
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "second");
    }
}
