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

mod binary;
mod copy;
mod extended;

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

use crate::abort::{wait_client_gone, AbortOnDrop};
use crate::dispatch::{execute_to_sink, ResultSink};
use crate::state::AppState;
use crate::{FrontendListenerTrait, ShutdownRx, MAX_FRONTEND_MESSAGE_BYTES};
use queryflux_routing::ChainRouteResult;

async fn read_startup(
    stream: &mut crate::wire_tls::BoxIo,
) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let len = checked_frontend_len(read_i32(stream).await?)?;
    if len < 8 {
        return Err("invalid startup length".into());
    }
    let mut body = vec![0; len - 4];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn read_password(
    reader: &mut crate::wire_tls::WireReader,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if read_byte(reader).await? != b'p' {
        return Err("expected PasswordMessage".into());
    }
    let len = checked_frontend_len(read_i32(reader).await?)?;
    if !(5..=16389).contains(&len) {
        return Err("invalid credential length".into());
    }
    let mut body = vec![0; len - 4];
    reader.read_exact(&mut body).await?;
    if body.pop() != Some(0) || body.contains(&0) {
        return Err("invalid password framing".into());
    }
    Ok(String::from_utf8(body)?)
}

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
static CANCELS: std::sync::LazyLock<dashmap::DashMap<(u32, u32), tokio::task::AbortHandle>> =
    std::sync::LazyLock::new(dashmap::DashMap::new);
struct CancelRegistration((u32, u32));
impl Drop for CancelRegistration {
    fn drop(&mut self) {
        CANCELS.remove(&self.0);
    }
}

// ── Frontend ──────────────────────────────────────────────────────────────────

pub struct PostgresWireFrontend {
    state: Arc<AppState>,
    port: u16,
    max_connections: Option<usize>,
    tls: Option<queryflux_core::config::FrontendTlsConfig>,
}

impl PostgresWireFrontend {
    pub fn new(state: Arc<AppState>, port: u16, max_connections: Option<usize>) -> Self {
        Self {
            state,
            port,
            max_connections,
            tls: None,
        }
    }
    pub fn with_tls(mut self, tls: Option<queryflux_core::config::FrontendTlsConfig>) -> Self {
        self.tls = tls;
        self
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

        let tls = crate::wire_tls::load_tls(self.tls.as_ref())?;
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
                    let tls = tls.clone();
                    let state = self.state.clone();
                    let conn_id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    active.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state, conn_id, tls).await {
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
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;
    let mut transport: crate::wire_tls::BoxIo = Box::new(stream);
    let mut secure = false;

    // ── Startup phase ────────────────────────────────────────────────────────

    let mut startup_body = read_startup(&mut transport).await?;
    let mut protocol_version = i32::from_be_bytes(startup_body[..4].try_into()?);
    if protocol_version == 80877103 {
        if let Some(tls) = tls {
            transport.write_all(b"S").await?;
            transport.flush().await?;
            transport = Box::new(
                tokio::time::timeout(std::time::Duration::from_secs(30), tls.accept(transport))
                    .await??,
            );
            secure = true;
        } else {
            transport.write_all(b"N").await?;
            transport.flush().await?;
        }
        startup_body = read_startup(&mut transport).await?;
        protocol_version = i32::from_be_bytes(startup_body[..4].try_into()?);
    }
    if protocol_version == 80877102 {
        if startup_body.len() == 12 {
            let pid = u32::from_be_bytes(startup_body[4..8].try_into()?);
            let secret = u32::from_be_bytes(startup_body[8..12].try_into()?);
            if let Some(query) = CANCELS.get(&(pid, secret)) {
                query.abort();
            }
        }
        return Ok(());
    }
    if protocol_version != 196608 {
        return Err("unsupported PostgreSQL protocol version".into());
    }
    let (mut reader, mut writer) = crate::wire_tls::split(transport);

    let params = parse_startup_params(&startup_body[4..]);
    let user = params.get("user").cloned().unwrap_or_default();
    let database = params.get("database").cloned();

    info!(user, database = ?database, conn_id = connection_id, "Postgres wire: client connecting");

    let provider = state.live.read().await.auth_provider.clone();
    let kind = provider.wire_auth_kind();
    let mut creds = Credentials {
        username: Some(user.clone()),
        ..Default::default()
    };
    if kind != queryflux_auth::provider::WireAuthKind::None {
        if !secure {
            write_error_response(
                &mut writer,
                "28000",
                "TLS is required for credential authentication",
            )
            .await?;
            return Ok(());
        }
        write_msg(&mut writer, b'R', &3i32.to_be_bytes()).await?;
        writer.flush().await?;
        let password = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            read_password(&mut reader),
        )
        .await??;
        if kind == queryflux_auth::provider::WireAuthKind::Jwt {
            creds.bearer_token = Some(password);
        } else {
            creds.password = Some(password);
        }
    }
    let identity = match crate::lease::authenticate_initial(&state, &mut creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            write_error_response(&mut writer, "28000", &e.to_string()).await?;
            return Ok(());
        }
    };

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

    // BackendKeyData: an unpredictable per-connection cancellation secret.
    let cancel_secret = uuid::Uuid::new_v4().as_u128() as u32;
    let mut bkd = Vec::new();
    bkd.extend_from_slice(&connection_id.to_be_bytes());
    bkd.extend_from_slice(&cancel_secret.to_be_bytes()); // secret
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
    let transaction_id = uuid::Uuid::new_v4().to_string();
    let _rollback = crate::transaction::DisconnectRollback {
        state: state.clone(),
        id: transaction_id.clone(),
    };
    let mut extra = extra;
    extra.insert(crate::transaction::SESSION_KEY.into(), transaction_id);
    extra.insert("queryflux.pg_cancel_pid".into(), connection_id.to_string());
    extra.insert(
        "queryflux.pg_cancel_secret".into(),
        cancel_secret.to_string(),
    );
    let session = SessionContext {
        user: Some(identity.user),
        database,
        // Postgres wire has no catalog concept — just a database.
        catalog: None,
        tags,
        extra,
        agent_context: None,
    };

    let mut extended = extended::Extended::default();

    // ── Command loop ─────────────────────────────────────────────────────────

    loop {
        // Each frontend message: 1-byte type + 4-byte length (includes itself).
        let msg_type = match read_byte(&mut reader).await {
            Ok(b) => b,
            Err(_) => break,
        };

        let msg_len = checked_frontend_len(read_i32(&mut reader).await?)?;
        if msg_len < 4 {
            break;
        }
        let body_len = msg_len - 4;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            reader.read_exact(&mut body).await?;
        }

        if extended.failed && !matches!(msg_type, b'S' | b'X' | b'H') {
            continue;
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
                handle_simple_query(&mut reader, &mut writer, &state, &session, &sql, &creds)
                    .await?;
            }

            b'P' | b'B' | b'D' | b'E' | b'C' => {
                if !extended.failed {
                    if let Err(e) = extended
                        .handle(msg_type, &body, &mut writer, &state, &session, &creds)
                        .await
                    {
                        extended.failed = true;
                        write_error_response(&mut writer, "0A000", &e.to_string()).await?;
                        if let Ok(identity) = crate::lease::authenticate(&state, &creds).await {
                            if let Some(id) = session.extra.get(crate::transaction::SESSION_KEY) {
                                if let Ok(Some(t)) = state.transactions.lookup(
                                    id,
                                    &identity,
                                    &FrontendProtocol::PostgresWire,
                                ) {
                                    t.failed.store(true, Ordering::Release);
                                }
                            }
                        }
                    }
                }
            }

            b'H' => {
                writer.flush().await?;
            }
            b'S' => {
                extended.failed = false;
                write_ready(&mut writer, &state, &session, &creds).await?;
            }
            b'd' | b'c' | b'f' => {
                write_error_response(
                    &mut writer,
                    "0A000",
                    "COPY data requires an active COPY operation",
                )
                .await?;
                write_ready(&mut writer, &state, &session, &creds).await?;
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
                write_ready(&mut writer, &state, &session, &creds).await?;
            }
        }
    }

    debug!(conn_id = connection_id, "Postgres wire: connection closed");
    Ok(())
}

// ── Query execution ───────────────────────────────────────────────────────────

async fn handle_simple_query(
    reader: &mut crate::wire_tls::WireReader,
    writer: &mut crate::wire_tls::WireWriter,
    state: &Arc<AppState>,
    session: &SessionContext,
    sql: &str,
    creds: &Credentials,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if sql.is_empty() {
        // Empty query: EmptyQueryResponse + ReadyForQuery.
        write_msg(writer, b'I', &[]).await?;
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }

    let parse_sql = sql.to_owned();
    let tokens = queryflux_core::polyglot_pool::run(move || {
        polyglot_sql::dialects::Dialect::get(polyglot_sql::DialectType::PostgreSQL)
            .tokenize(&parse_sql)
    });
    let tokens = match tokens {
        Some(Ok(tokens)) => tokens,
        _ => {
            write_error_response(writer, "42601", "Invalid SQL token framing").await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
    };
    if tokens.iter().enumerate().any(|(i, token)| {
        token.token_type == polyglot_sql::tokens::TokenType::Semicolon && i + 1 < tokens.len()
    }) {
        write_error_response(writer, "0A000", "Send one SQL statement per request; multi-statement simple query batches are not supported yet").await?;
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }
    let sql_lower = sql.trim().to_lowercase();

    let protocol = FrontendProtocol::PostgresWire;

    let auth_ctx = match crate::lease::authenticate(state, creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::PostgresWire));
            write_error_response(writer, "28000", &e.to_string()).await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
    };

    if queryflux_core::sql_classify::strip_leading_sql_comments(sql)
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("COPY ")
    {
        if let Err(e) = copy::run(reader, writer, state, session, sql, creds).await {
            write_error_response(writer, "0A000", &e.to_string()).await?;
        }
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }

    // Auth-complete fast path: `SET` statements are handled locally by the proxy,
    // but must still go through authentication when `auth.required=true`.
    if sql_lower.starts_with("set ") || sql_lower.starts_with("set\t") {
        write_msg(writer, b'C', b"SET\0").await?;
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }

    let routing_result = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(sql, session, &protocol, Some(&auth_ctx))
            .await
    };
    let (chain_result, mut routing_trace) = match routing_result {
        Ok(r) => r,
        Err(e) => {
            write_error_response(writer, "42000", &e.to_string()).await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
    };
    let mut group = match chain_result {
        ChainRouteResult::Routed(g) => g,
        ChainRouteResult::Denied { message } => {
            state.record_routing_deny(sql, session, protocol, &message, Some(routing_trace));
            // 42501 = insufficient_privilege
            write_error_response(writer, "42501", &message).await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
    };
    group = match state
        .resolve_routed_group(group, &mut routing_trace, &auth_ctx)
        .await
    {
        Ok(g) => g,
        Err(QueryFluxError::Unauthorized(msg)) => {
            write_error_response(writer, "42501", &msg).await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
        Err(e) => {
            write_error_response(writer, "42000", &e.to_string()).await?;
            write_ready(writer, state, session, creds).await?;
            return Ok(());
        }
    };

    let id = session
        .extra
        .get(crate::transaction::SESSION_KEY)
        .expect("connection transaction id");
    if crate::transaction::transaction_command(sql).is_some() {
        match state
            .transactions
            .begin(state, id, group, &auth_ctx, protocol, session)
            .await
        {
            Ok(()) => write_msg(writer, b'C', b"BEGIN\0").await?,
            Err(e) => write_error_response(writer, "0A000", &e.to_string()).await?,
        }
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }
    if let Some(commit) = crate::transaction::transaction_end(sql) {
        let failed = state
            .transactions
            .lookup(id, &auth_ctx, &protocol)
            .map(|t| t.is_some_and(|t| t.failed.load(Ordering::Acquire)))
            .unwrap_or(true);
        match state
            .transactions
            .finish(id, &auth_ctx, &protocol, commit)
            .await
        {
            Ok(()) => {
                write_msg(
                    writer,
                    b'C',
                    if commit && !failed {
                        b"COMMIT\0"
                    } else {
                        b"ROLLBACK\0"
                    },
                )
                .await?
            }
            Err(e) => write_error_response(writer, "XX000", &e.to_string()).await?,
        }
        write_ready(writer, state, session, creds).await?;
        return Ok(());
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let mut sink = PostgresResultSink::new(tx, sql);

    let state2 = state.clone();
    let session2 = session.clone();
    let sql2 = sql.to_string();

    let cancel_group = group.clone();
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

    let cancellation_supported = {
        let live = state.live.read().await;
        live.group_members
            .get(&cancel_group.0)
            .is_some_and(|members| {
                !members.is_empty()
                    && members.iter().all(|name| match live.adapters.get(name) {
                        Some(queryflux_engine_adapters::AdapterKind::Sync(a)) => {
                            a.supports_cancellation()
                        }
                        Some(queryflux_engine_adapters::AdapterKind::Async(_)) => true,
                        None => false,
                    })
            })
    };
    let cancel_key = session
        .extra
        .get("queryflux.pg_cancel_pid")
        .and_then(|p| p.parse::<u32>().ok())
        .zip(
            session
                .extra
                .get("queryflux.pg_cancel_secret")
                .and_then(|p| p.parse::<u32>().ok()),
        );
    let _registration = if cancellation_supported {
        cancel_key.map(|key| {
            CANCELS.insert(key, exec_task.abort_handle());
            CancelRegistration(key)
        })
    } else {
        None
    };
    let exec_task = AbortOnDrop::new(exec_task);

    // Forward encoded Postgres messages. Abort the engine query if the client
    // closes (or sends another message) while we are still waiting for results.
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        writer.write_all(&msg).await?;
                        writer.flush().await?;
                    }
                    None => break,
                }
            }
            _ = wait_client_gone(reader) => {
                debug!("Postgres wire: client disconnected during query — aborting");
                return Ok(());
            }
        }
    }

    if let Err(e) = exec_task.join().await {
        if e.is_cancelled() {
            write_error_response(writer, "57014", "canceling statement due to user request")
                .await?;
        } else {
            warn!("Postgres query task panicked: {e}");
            write_error_response(writer, "XX000", "query task failed").await?;
        }
    }

    // ReadyForQuery after each command.
    write_ready(writer, state, session, creds).await?;
    Ok(())
}

async fn write_ready(
    writer: &mut crate::wire_tls::WireWriter,
    state: &AppState,
    session: &SessionContext,
    creds: &Credentials,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let status = match crate::lease::authenticate(state, creds).await {
        Ok(identity) => match session.extra.get(crate::transaction::SESSION_KEY) {
            Some(id) => {
                match state
                    .transactions
                    .lookup(id, &identity, &FrontendProtocol::PostgresWire)
                {
                    Ok(Some(t)) => {
                        if t.failed.load(Ordering::Acquire) {
                            b'E'
                        } else {
                            b'T'
                        }
                    }
                    _ => b'I',
                }
            }
            None => b'I',
        },
        Err(_) => b'I',
    };
    write_msg(writer, b'Z', &[status]).await
}

// ── PostgresResultSink ────────────────────────────────────────────────────────

/// The `CommandComplete` tag for a non-result-set statement: a verb (or
/// verb + object, e.g. "CREATE TABLE") and whether Postgres appends a row count.
///
/// Real Postgres tags: `INSERT 0 <rows>`, `UPDATE <rows>`, `DELETE <rows>`,
/// `MERGE <rows>` carry a count; DDL tags (`CREATE TABLE`, `DROP INDEX`, …) and
/// everything else do not.
struct PostgresCommandTag {
    verb: String,
    has_row_count: bool,
}

impl PostgresCommandTag {
    fn classify(sql: &str) -> Self {
        let s = queryflux_core::sql_classify::strip_leading_sql_comments(sql);
        let mut words = s.split_whitespace();
        // Trim trailing punctuation (e.g. a semicolon on a single-statement,
        // single-word command like `COMMIT;`) — real Postgres never includes it
        // in the tag, and some drivers match the tag text to track transaction
        // state, so a stray `;` can change client behavior.
        let first = words
            .next()
            .unwrap_or("")
            .trim_end_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_uppercase();
        match first.as_str() {
            "INSERT" => Self {
                verb: "INSERT 0".to_string(),
                has_row_count: true,
            },
            "UPDATE" | "DELETE" | "MERGE" => Self {
                verb: first,
                has_row_count: true,
            },
            "CREATE" | "DROP" | "ALTER" => {
                // Modifiers between the verb and the object type that Postgres
                // drops from the tag entirely (e.g. `CREATE OR REPLACE VIEW` →
                // "CREATE VIEW", `CREATE UNIQUE INDEX` → "CREATE INDEX").
                const SKIPPED_QUALIFIERS: &[&str] = &[
                    "OR",
                    "REPLACE",
                    "TEMP",
                    "TEMPORARY",
                    "UNLOGGED",
                    "UNIQUE",
                    "GLOBAL",
                    "LOCAL",
                ];
                // Object-type words that are themselves a prefix of a two-word
                // type (`CREATE MATERIALIZED VIEW`, `CREATE FOREIGN TABLE`) —
                // keep scanning for the next word instead of stopping here.
                const COMPOUND_TYPE_PREFIXES: &[&str] = &["MATERIALIZED", "FOREIGN"];

                let mut parts = vec![first];
                for word in words {
                    let upper = word.to_uppercase();
                    if SKIPPED_QUALIFIERS.contains(&upper.as_str()) {
                        continue;
                    }
                    let is_prefix = COMPOUND_TYPE_PREFIXES.contains(&upper.as_str());
                    parts.push(upper);
                    if !is_prefix {
                        break;
                    }
                }
                Self {
                    verb: parts.join(" "),
                    has_row_count: false,
                }
            }
            "TRUNCATE" => Self {
                verb: "TRUNCATE TABLE".to_string(),
                has_row_count: false,
            },
            "" => Self {
                verb: "OK".to_string(),
                has_row_count: false,
            },
            other => Self {
                verb: other.to_string(),
                has_row_count: false,
            },
        }
    }
}

/// Streams Arrow RecordBatches as Postgres wire protocol messages over a channel.
///
/// Sends pre-encoded Postgres messages (type byte + length + body) via channel.
/// The query handler drains the channel and writes them to the TCP stream.
struct PostgresResultSink {
    tx: UnboundedSender<Vec<u8>>,
    row_count: u64,
    schema_sent: bool,
    /// Command tag verb (e.g. "CREATE TABLE", "INSERT 0", "UPDATE") for the
    /// non-result-set path, derived from the original SQL at construction.
    command_tag: PostgresCommandTag,
}

impl PostgresResultSink {
    fn new(tx: UnboundedSender<Vec<u8>>, sql: &str) -> Self {
        Self {
            tx,
            row_count: 0,
            schema_sent: false,
            command_tag: PostgresCommandTag::classify(sql),
        }
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
        if schema.fields().is_empty() {
            return Ok(());
        }

        self.schema_sent = true;

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
        // Do not emit DataRow packets without a prior RowDescription (e.g. after
        // an empty schema / DDL path).
        if !self.schema_sent {
            return Ok(());
        }
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

    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()> {
        let tag = if self.schema_sent {
            format!("SELECT {}\0", self.row_count)
        } else if self.command_tag.has_row_count {
            format!(
                "{} {}\0",
                self.command_tag.verb,
                stats.affected_rows.unwrap_or(0)
            )
        } else {
            format!("{}\0", self.command_tag.verb)
        };
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

    async fn on_translated_sql(&mut self, sql: &str) -> Result<()> {
        // Translation can rewrite the leading verb (e.g. MySQL `REPLACE INTO` →
        // target-dialect `INSERT ... ON CONFLICT`) — reclassify from what's
        // actually executed, not the pre-translation SQL used at construction.
        self.command_tag = PostgresCommandTag::classify(sql);
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

/// Validate a Postgres length-prefixed message size before allocating.
fn checked_frontend_len(
    len: i32,
) -> std::result::Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if len <= 0 {
        return Err(format!("invalid Postgres message length {len}").into());
    }
    let len = len as usize;
    if len > MAX_FRONTEND_MESSAGE_BYTES {
        return Err(format!(
            "Postgres message length {len} exceeds max allowed {MAX_FRONTEND_MESSAGE_BYTES} bytes"
        )
        .into());
    }
    Ok(len)
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
            catalog: None,
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

    // ── PostgresResultSink: no-result vs result-set framing ───────────────────

    use queryflux_core::query::QueryStats;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn no_schema_complete_emits_insert_command_complete() {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "INSERT INTO t VALUES (1)");

        let stats = QueryStats {
            affected_rows: Some(1),
            ..Default::default()
        };
        sink.on_complete(&stats).await.unwrap();

        let msg = rx.try_recv().expect("should have CommandComplete");
        assert_eq!(msg[0], b'C', "CommandComplete message type");
        let body = &msg[5..]; // skip type + i32 length
        let tag = String::from_utf8_lossy(body);
        assert!(
            tag.starts_with("INSERT 0 1"),
            "tag should be 'INSERT 0 1', got: {tag}"
        );
    }

    #[tokio::test]
    async fn ddl_with_no_affected_rows_emits_bare_command_tag() {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "CREATE TABLE t (id INT)");

        let stats = QueryStats::default();
        sink.on_complete(&stats).await.unwrap();

        let msg = rx.try_recv().expect("CommandComplete");
        let body = &msg[5..];
        let tag = String::from_utf8_lossy(body);
        assert!(
            tag.starts_with("CREATE TABLE\0"),
            "tag should be 'CREATE TABLE', got: {tag}"
        );
    }

    #[tokio::test]
    async fn empty_schema_skips_row_description() {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "CREATE TABLE t (id INT)");

        sink.on_schema(&Schema::empty()).await.unwrap();
        assert!(
            rx.try_recv().is_err(),
            "empty schema should not emit RowDescription"
        );
    }

    #[tokio::test]
    async fn non_empty_schema_sends_row_description_and_select_tag() {
        use arrow::datatypes::{DataType, Field};
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "SELECT * FROM t");

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        sink.on_schema(&schema).await.unwrap();

        let row_desc = rx.try_recv().expect("RowDescription");
        assert_eq!(row_desc[0], b'T', "RowDescription message type");

        let stats = QueryStats::default();
        sink.on_complete(&stats).await.unwrap();

        let cmd = rx.try_recv().expect("CommandComplete");
        assert_eq!(cmd[0], b'C');
        let tag = String::from_utf8_lossy(&cmd[5..]);
        assert!(
            tag.starts_with("SELECT 0"),
            "result set should have SELECT tag, got: {tag}"
        );
    }

    #[tokio::test]
    async fn empty_schema_batch_does_not_emit_datarow() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "CREATE TABLE t (id INT)");

        sink.on_schema(&Schema::empty()).await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1])) as _]).unwrap();
        sink.on_batch(&batch).await.unwrap();
        assert!(
            rx.try_recv().is_err(),
            "batch without RowDescription must not emit DataRow"
        );
    }

    /// Regression for lakeops-org/queryflux#97 (Postgres wire).
    /// DDL/DML with no result set must emit a CommandComplete with the real
    /// Postgres command tag, not SELECT 0 after a zero-field RowDescription.
    #[tokio::test]
    async fn issue97_ddl_path_emits_command_complete_without_row_description() {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "UPDATE t SET x = 1");

        let stats = QueryStats {
            affected_rows: Some(2),
            ..Default::default()
        };
        sink.on_complete(&stats).await.unwrap();

        let msg = rx.try_recv().expect("CommandComplete");
        assert_eq!(msg[0], b'C');
        let tag = String::from_utf8_lossy(&msg[5..]);
        assert!(
            tag.starts_with("UPDATE 2"),
            "expected UPDATE tag with affected_rows, got: {tag}"
        );
        assert!(
            !tag.starts_with("SELECT"),
            "DDL/DML must not use SELECT tag"
        );
        assert!(rx.try_recv().is_err(), "only one CommandComplete expected");
    }

    #[tokio::test]
    async fn command_tag_classifies_ddl_dml_verbs() {
        let cases: &[(&str, &str, bool)] = &[
            ("CREATE TABLE t (id INT)", "CREATE TABLE", false),
            ("create table t (id int)", "CREATE TABLE", false),
            ("DROP TABLE t", "DROP TABLE", false),
            ("ALTER TABLE t ADD COLUMN y INT", "ALTER TABLE", false),
            ("TRUNCATE t", "TRUNCATE TABLE", false),
            ("INSERT INTO t VALUES (1)", "INSERT 0", true),
            ("UPDATE t SET x = 1", "UPDATE", true),
            ("DELETE FROM t WHERE id = 1", "DELETE", true),
            ("  -- comment\nDROP TABLE t", "DROP TABLE", false),
            ("CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE VIEW", false),
            ("CREATE TEMP TABLE t (id INT)", "CREATE TABLE", false),
            ("CREATE TEMPORARY TABLE t (id INT)", "CREATE TABLE", false),
            ("CREATE UNLOGGED TABLE t (id INT)", "CREATE TABLE", false),
            ("CREATE UNIQUE INDEX idx ON t (id)", "CREATE INDEX", false),
            (
                "CREATE MATERIALIZED VIEW v AS SELECT 1",
                "CREATE MATERIALIZED VIEW",
                false,
            ),
            (
                "CREATE FOREIGN TABLE t (id INT) SERVER s",
                "CREATE FOREIGN TABLE",
                false,
            ),
            (
                "CREATE OR REPLACE FUNCTION f() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE sql",
                "CREATE FUNCTION",
                false,
            ),
            // Trailing punctuation on a single-token statement must not leak into
            // the tag — real Postgres sends "COMMIT"/"VACUUM", never "COMMIT;".
            ("COMMIT;", "COMMIT", false),
            ("VACUUM;", "VACUUM", false),
            ("BEGIN;", "BEGIN", false),
        ];
        for (sql, expected_verb, has_row_count) in cases {
            let tag = super::PostgresCommandTag::classify(sql);
            assert_eq!(tag.verb, *expected_verb, "sql: {sql}");
            assert_eq!(tag.has_row_count, *has_row_count, "sql: {sql}");
        }
    }

    /// `on_translated_sql` must override the construction-time classification —
    /// dispatch calls it once the SQL is fully translated, and translation can
    /// rewrite the leading verb (e.g. MySQL `REPLACE INTO` on the client side
    /// becomes a target-dialect `INSERT ... ON CONFLICT`).
    #[tokio::test]
    async fn on_translated_sql_reclassifies_the_command_tag() {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut sink = PostgresResultSink::new(tx, "REPLACE INTO t VALUES (1)");
        sink.on_translated_sql("INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET x = 1")
            .await
            .unwrap();

        let stats = QueryStats {
            affected_rows: Some(1),
            ..Default::default()
        };
        sink.on_complete(&stats).await.unwrap();

        let msg = rx.try_recv().expect("CommandComplete");
        let tag = String::from_utf8_lossy(&msg[5..]);
        assert!(
            tag.starts_with("INSERT 0 1"),
            "expected reclassified INSERT tag, got: {tag}"
        );
    }

    #[tokio::test]
    async fn set_fast_path_rejects_unauthenticated_when_required() {
        use tokio::io::AsyncReadExt;
        use tokio::net::{TcpListener, TcpStream};

        use crate::state::test_fixtures;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = test_fixtures::app_state(true);
        let session = SessionContext::default();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (mut reader, mut writer) = crate::wire_tls::split(Box::new(stream));
            super::handle_simple_query(
                &mut reader,
                &mut writer,
                &state,
                &session,
                "SET search_path = public",
                &Credentials::default(),
            )
            .await
            .expect("handle_simple_query");
        });

        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let mut msg_type = [0u8; 1];
        stream.read_exact(&mut msg_type).await.expect("read type");
        server.await.expect("server");

        assert_eq!(
            msg_type[0], b'E',
            "expected Postgres ErrorResponse when auth is required"
        );
    }

    #[test]
    fn checked_frontend_len_rejects_negative_and_oversized() {
        assert!(checked_frontend_len(0).is_err());
        assert!(checked_frontend_len(-1).is_err());
        assert!(checked_frontend_len((MAX_FRONTEND_MESSAGE_BYTES + 1) as i32).is_err());
        assert_eq!(checked_frontend_len(8).unwrap(), 8);
    }
}

#[cfg(test)]
mod native_integration_tests {
    use super::*;
    async fn fixture() -> (Arc<AppState>, TcpListener) {
        use queryflux_core::query::{ClusterGroupName, ClusterName};
        use queryflux_engine_adapters::{
            duckdb::{DuckDbAdapter, DuckDbConfig},
            AdapterKind,
        };
        let state = crate::state::test_fixtures::app_state(false);
        let adapter = Arc::new(
            DuckDbAdapter::new(
                ClusterName("trino".into()),
                ClusterGroupName("default".into()),
                DuckDbConfig {
                    database_path: None,
                    motherduck_token: None,
                    pool_size: 1,
                    max_result_buffer_bytes: 16 * 1024 * 1024,
                },
            )
            .unwrap(),
        );
        state
            .live
            .write()
            .await
            .adapters
            .insert("trino".into(), AdapterKind::Sync(adapter));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        (state, listener)
    }
    async fn send(stream: &mut TcpStream, kind: u8, body: &[u8]) {
        let mut packet = vec![kind];
        packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        packet.extend_from_slice(body);
        stream.write_all(&packet).await.unwrap();
    }
    async fn receive(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let kind = tokio::time::timeout(std::time::Duration::from_secs(10), stream.read_u8())
            .await
            .unwrap()
            .unwrap();
        let length = stream.read_i32().await.unwrap();
        let mut bytes = vec![0; length as usize - 4];
        stream.read_exact(&mut bytes).await.unwrap();
        (kind, bytes)
    }
    async fn connect_fixture() -> (TcpStream, tokio::task::JoinHandle<()>) {
        let (state, listener) = fixture().await;
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, state, 901, None).await.unwrap();
        });
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut body = 196608i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0alice\0database\0default\0\0");
        stream
            .write_all(&((body.len() + 4) as i32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&body).await.unwrap();
        loop {
            if receive(&mut stream).await.0 == b'Z' {
                break;
            }
        }
        (stream, server)
    }
    #[tokio::test]
    async fn protocol_portal_suspends_and_resumes_without_reexecution() {
        let (mut stream, server) = connect_fixture().await;
        let mut parse = b"stmt\0SELECT n FROM (VALUES (1), (2), (3)) AS t(n)\0".to_vec();
        parse.extend_from_slice(&0i16.to_be_bytes());
        send(&mut stream, b'P', &parse).await;
        let mut bind = b"portal\0stmt\0".to_vec();
        bind.extend_from_slice(&[0; 6]);
        send(&mut stream, b'B', &bind).await;
        send(&mut stream, b'D', b"Pportal\0").await;
        let mut exec = b"portal\0".to_vec();
        exec.extend_from_slice(&2i32.to_be_bytes());
        send(&mut stream, b'E', &exec).await;
        for kind in [b'1', b'2', b'T', b'D', b'D', b's'] {
            let (got, body) = receive(&mut stream).await;
            assert_eq!(got, kind, "{}", String::from_utf8_lossy(&body));
        }
        let mut exec = b"portal\0".to_vec();
        exec.extend_from_slice(&0i32.to_be_bytes());
        send(&mut stream, b'E', &exec).await;
        send(&mut stream, b'S', &[]).await;
        for kind in [b'D', b'C', b'Z'] {
            let (got, body) = receive(&mut stream).await;
            assert_eq!(got, kind, "{}", String::from_utf8_lossy(&body));
        }
        send(&mut stream, b'X', &[]).await;
        drop(stream);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn protocol_binary_integer_parameter_and_result() {
        let (mut stream, server) = connect_fixture().await;
        let mut parse = b"stmt\0SELECT $1::INTEGER + 1 AS answer\0".to_vec();
        parse.extend_from_slice(&1i16.to_be_bytes());
        parse.extend_from_slice(&PG_OID_INT4.to_be_bytes());
        send(&mut stream, b'P', &parse).await;
        let mut bind = b"portal\0stmt\0".to_vec();
        bind.extend_from_slice(&1i16.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes());
        bind.extend_from_slice(&4i32.to_be_bytes());
        bind.extend_from_slice(&41i32.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes());
        send(&mut stream, b'B', &bind).await;
        send(&mut stream, b'D', b"Pportal\0").await;
        let mut exec = b"portal\0".to_vec();
        exec.extend_from_slice(&0i32.to_be_bytes());
        send(&mut stream, b'E', &exec).await;
        send(&mut stream, b'S', &[]).await;
        for kind in [b'1', b'2', b'T'] {
            let (got, body) = receive(&mut stream).await;
            assert_eq!(got, kind, "{}", String::from_utf8_lossy(&body));
        }
        let (kind, body) = receive(&mut stream).await;
        assert_eq!(kind, b'D');
        assert_eq!(body, [0, 1, 0, 0, 0, 4, 0, 0, 0, 42]);
        assert_eq!(receive(&mut stream).await.0, b'C');
        assert_eq!(receive(&mut stream).await.0, b'Z');
        send(&mut stream, b'X', &[]).await;
        drop(stream);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn native_psql_tls_jwt_validates_jwks_and_rejects_forgery() {
        use base64::Engine;
        if std::process::Command::new("psql")
            .arg("--version")
            .output()
            .is_err()
            || std::process::Command::new("openssl")
                .arg("version")
                .output()
                .is_err()
        {
            eprintln!("psql/openssl unavailable; TLS OIDC integration skipped");
            return;
        }
        struct Temp(std::path::PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let temp = Temp(
            std::env::temp_dir().join(format!("queryflux-pg-tls-test-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir_all(&temp.0).unwrap();
        let cert = temp.0.join("cert.pem");
        let key = temp.0.join("key.pem");
        let result = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .output()
            .unwrap();
        assert!(result.status.success());
        let jwks: serde_json::Value = serde_json::from_str(include_str!(
            "../../../queryflux-auth/tests/fixtures/oidc-test-jwks.json"
        ))
        .unwrap();
        let jwks_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let jwks_port = jwks_listener.local_addr().unwrap().port();
        let jwks_server = AbortOnDrop::new(tokio::spawn(async move {
            axum::serve(
                jwks_listener,
                axum::Router::new().route(
                    "/jwks",
                    axum::routing::get(move || {
                        let keys = jwks.clone();
                        async move { axum::Json(keys) }
                    }),
                ),
            )
            .await
            .unwrap();
        }));
        let (state, listener) = fixture().await;
        let port = listener.local_addr().unwrap().port();
        state.live.write().await.auth_provider = Arc::new(queryflux_auth::OidcAuthProvider::new(
            queryflux_core::config::OidcConfig {
                issuer: "https://test.knorket.invalid".into(),
                jwks_uri: format!("http://127.0.0.1:{jwks_port}/jwks"),
                audience: Some("cluster-a".into()),
                groups_claim: "groups".into(),
                roles_claim: Some("role".into()),
                attribute_claims: vec!["tenant_id".into(), "cluster_id".into()],
            },
            true,
        ));
        state.live.write().await.authorization = Arc::new(
            queryflux_auth::SimpleAuthorizationPolicy::new(HashMap::from([(
                "default".into(),
                queryflux_core::config::ClusterGroupAuthorizationConfig {
                    allow_groups: vec![],
                    allow_users: vec!["verified-user".into()],
                },
            )])),
        );
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"RS256","typ":"JWT","kid":"test-only"}"#);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({"iss":"https://test.knorket.invalid","aud":"cluster-a","sub":"verified-user","exp":now+1800,"iat":now,"tenant_id":"tenant-a","cluster_id":"cluster-a","token_type":"cluster_access","role":"reader"});
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
        let unsigned = format!("{header}.{payload}");
        let fixture_key = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../queryflux-auth/tests/fixtures/oidc-test-private.pem");
        let mut signer = std::process::Command::new("openssl")
            .args(["dgst", "-sha256", "-sign"])
            .arg(fixture_key)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        std::io::Write::write_all(&mut signer.stdin.take().unwrap(), unsigned.as_bytes()).unwrap();
        let signature = signer.wait_with_output().unwrap();
        assert!(signature.status.success());
        let jwt = format!(
            "{unsigned}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.stdout)
        );
        let tls = crate::wire_tls::load_tls(Some(&queryflux_core::config::FrontendTlsConfig {
            cert_file: cert.to_string_lossy().into(),
            key_file: key.to_string_lossy().into(),
        }))
        .unwrap();
        let server = tokio::spawn(async move {
            for id in 910..912 {
                let (socket, _) = listener.accept().await.unwrap();
                let _ = handle_connection(socket, state.clone(), id, tls.clone()).await;
            }
        });
        for (token, success) in [(jwt, true), (format!("{unsigned}.AAAA"), false)] {
            let output = tokio::process::Command::new("psql")
                .args([
                    "-X",
                    "-w",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &port.to_string(),
                    "-U",
                    "spoofed-user",
                    "-d",
                    "default",
                    "-At",
                    "-c",
                    "SELECT 1",
                ])
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", &cert)
                .env("PGGSSENCMODE", "disable")
                .env("PGPASSWORD", token)
                .env_remove("PGSERVICE")
                .output()
                .await
                .unwrap();
            assert_eq!(
                output.status.success(),
                success,
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if success {
                assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "1");
            }
        }
        server.await.unwrap();
        drop(jwks_server);
    }

    #[tokio::test]
    async fn native_psql_transactions_and_copy_text() {
        if std::process::Command::new("psql")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("psql is unavailable; native integration skipped");
            return;
        }
        let (state, listener) = fixture().await;
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, state, 900, None).await.unwrap();
        });
        let mut command = tokio::process::Command::new("psql");
        command
            .args([
                "-X",
                "-w",
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                "alice",
                "-d",
                "default",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
            ])
            .args([
                "-c",
                "CREATE TABLE native_rows (n INTEGER, label VARCHAR)",
                "-c",
                "BEGIN",
                "-c",
                "INSERT INTO native_rows VALUES (99, 'rollback')",
                "-c",
                "ROLLBACK",
                "-c",
                "\\copy native_rows FROM STDIN",
                "-c",
                "SELECT n, label FROM native_rows ORDER BY n",
                "-c",
                "\\copy native_rows TO STDOUT",
            ])
            .env("PGSSLMODE", "disable")
            .env("PGGSSENCMODE", "disable")
            .env_remove("PGPASSWORD")
            .env_remove("PGSERVICE")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"1\tfirst\n2\tsecond\n")
            .await
            .unwrap();
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        assert!(
            output.status.success(),
            "psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("1|first"), "{stdout}");
        assert!(stdout.contains("2\tsecond"), "{stdout}");
        assert!(!stdout.contains("rollback"), "{stdout}");
        server.await.unwrap();
    }
}
