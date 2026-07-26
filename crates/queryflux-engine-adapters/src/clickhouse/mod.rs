//! ClickHouse backend adapter — SQL over the HTTP interface, Arrow results.
//!
//! Queries are POSTed to the ClickHouse HTTP endpoint (default port 8123) with
//! `default_format=ArrowStream`; the response is Arrow IPC decoded with the
//! `arrow` crate. `default_format` is used instead of a `FORMAT` suffix so DDL
//! and INSERT statements (where a trailing FORMAT clause would name the *input*
//! format) pass through unchanged.
//!
//! Execution is eager like the other sync adapters: the whole response body is
//! buffered before any batch is surfaced. ClickHouse can fail a query *after*
//! sending HTTP 200 (it aborts the chunked encoding and appends an exception
//! frame to the body), so the body is scanned for the `__exception__` frame —
//! whose random tag is pre-announced in the `X-ClickHouse-Exception-Tag`
//! response header — before Arrow decoding. A failed query therefore surfaces
//! as an error, never as partial results.
//!
//! ClickHouse has no catalog level (its hierarchy is database → table), so a
//! single synthetic `default` catalog is exposed. Requires ClickHouse 24.3+
//! (String columns arrive as Arrow `Utf8` by default from that version).

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use queryflux_core::catalog::{ColumnDef, TableSchema};
use queryflux_core::config::{ClusterAuth, ClusterConfig};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use queryflux_core::session::SessionContext;
use queryflux_core::tags::QueryTags;

use crate::{AdapterKind, SyncExecution};

/// Timeout for control-plane requests (health checks, catalog listing).
/// Query execution has no overall timeout — analytics queries can be long.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on error-body text included in error messages.
const MAX_ERROR_BODY: usize = 2000;

/// Default cap on the buffered result body (`maxResultBufferBytes` config).
/// ClickHouse has infinite virtual tables (`system.numbers`,
/// `generateRandom()`, …) — without a cap, one unbounded SELECT would grow
/// the buffer until the process is OOM-killed.
pub const DEFAULT_MAX_RESULT_BUFFER_BYTES: usize = 1 << 30; // 1 GiB

/// Parsed and validated configuration for a ClickHouse cluster.
pub struct ClickHouseConfig {
    pub endpoint: String,
    pub auth: Option<ClusterAuth>,
    pub tls_skip_verify: bool,
    /// Per-query buffered-result cap in bytes.
    pub max_result_buffer_bytes: usize,
}

/// Parse the optional `maxResultBufferBytes` JSON field (positive integer).
fn parse_max_result_buffer_from_json(
    json: &serde_json::Value,
    cluster_name: &str,
) -> Result<usize> {
    match json.get("maxResultBufferBytes") {
        None => Ok(DEFAULT_MAX_RESULT_BUFFER_BYTES),
        Some(v) => v
            .as_u64()
            .filter(|&n| n >= 1)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': maxResultBufferBytes must be a positive integer"
                ))
            }),
    }
}

impl ClickHouseConfig {
    fn reject_non_basic_auth(auth: &Option<ClusterAuth>, cluster_name: &str) -> Result<()> {
        if let Some(a) = auth {
            if !matches!(a, ClusterAuth::Basic { .. }) {
                return Err(QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': ClickHouse only supports basic auth (username + password)"
                )));
            }
        }
        Ok(())
    }
}

impl crate::EngineConfigParseable for ClickHouseConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> Result<Self> {
        use queryflux_core::engine_registry::{
            json_str, json_tls_insecure_skip_verify, parse_auth_from_config_json,
        };
        let endpoint = json_str(json, "endpoint").ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name}': missing endpoint"))
        })?;
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            QueryFluxError::Engine(format!("cluster '{cluster_name}': invalid auth ({e})"))
        })?;
        Self::reject_non_basic_auth(&auth, cluster_name)?;
        Ok(Self {
            endpoint,
            auth,
            tls_skip_verify: json_tls_insecure_skip_verify(json),
            max_result_buffer_bytes: parse_max_result_buffer_from_json(json, cluster_name)?,
        })
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> Result<Self> {
        let endpoint = cfg.endpoint.clone().ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name}': missing endpoint"))
        })?;
        Self::reject_non_basic_auth(&cfg.auth, cluster_name)?;
        let max_result_buffer_bytes = match cfg.max_result_buffer_bytes {
            None => DEFAULT_MAX_RESULT_BUFFER_BYTES,
            Some(0) => {
                return Err(QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': maxResultBufferBytes must be a positive integer"
                )))
            }
            Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
        };
        Ok(Self {
            endpoint,
            auth: cfg.auth.clone(),
            tls_skip_verify: cfg.tls.as_ref().is_some_and(|t| t.insecure_skip_verify),
            max_result_buffer_bytes,
        })
    }
}

/// ClickHouse adapter — executes SQL over the HTTP interface (default port
/// 8123) and decodes `ArrowStream` responses; sync/eager like the other sync
/// adapters. See the module docs for exception-frame handling and catalog
/// mapping.
pub struct ClickHouseAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    endpoint: String,
    /// Basic-auth credentials (username, password) when configured.
    basic_auth: Option<(String, String)>,
    /// Per-query buffered-result cap in bytes (`maxResultBufferBytes`).
    max_result_buffer_bytes: usize,
    client: reqwest::Client,
}

impl ClickHouseAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: ClickHouseConfig,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder().connect_timeout(CONNECT_TIMEOUT);
        if config.tls_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder.build().map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': ClickHouse HTTP client build failed: {e}",
                cluster_name.0
            ))
        })?;
        let basic_auth = match config.auth {
            Some(ClusterAuth::Basic { username, password }) => Some((username, password)),
            _ => None,
        };
        Ok(Self {
            cluster_name,
            group_name,
            endpoint: config.endpoint.trim_end_matches('/').to_string(),
            basic_auth,
            max_result_buffer_bytes: config.max_result_buffer_bytes,
            client,
        })
    }

    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "clickHouse",
            display_name: "ClickHouse",
            description: "Real-time OLAP database. Connects via the ClickHouse HTTP interface.",
            hex: "FFCC01",
            connection_type: ConnectionType::Http,
            default_port: Some(8123),
            endpoint_example: Some("http://clickhouse:8123"),
            supported_auth: vec![AuthType::Basic],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "HTTP base URL of the ClickHouse server.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("http://clickhouse:8123"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Must be 'basic' for ClickHouse (username + password).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("basic"),
                },
                ConfigField {
                    key: "auth.username",
                    label: "Username",
                    description: "ClickHouse username.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("default"),
                },
                ConfigField {
                    key: "auth.password",
                    label: "Password",
                    description: "ClickHouse password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "maxResultBufferBytes",
                    label: "Max result buffer (bytes)",
                    description: "Per-query cap on the result bytes QueryFlux buffers in memory. \
                                  Defaults to 1 GiB when omitted.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("1073741824"),
                },
                ConfigField {
                    key: "tls.insecureSkipVerify",
                    label: "Skip TLS verification",
                    description: "Disable TLS certificate verification. Use only in development.",
                    field_type: FieldType::Boolean,
                    required: false,
                    example: Some("false"),
                },
            ],
        }
    }

    /// Build a query request: SQL as the POST body, everything else as URL params.
    ///
    /// `query_id` is always set (one UUID per request) so queries are traceable
    /// in `system.query_log` and killable via `KILL QUERY WHERE query_id = …`.
    fn query_request(&self, sql: &str, format: &str) -> reqwest::RequestBuilder {
        let mut req = self
            .client
            .post(format!("{}/", self.endpoint))
            .query(&[
                ("default_format", format),
                ("query_id", &uuid::Uuid::new_v4().to_string()),
            ])
            .body(sql.to_string());
        if let Some((user, pass)) = &self.basic_auth {
            req = req.basic_auth(user, Some(pass));
        }
        req
    }

    /// Run a control-plane query (catalog listing, process counts) and return
    /// the response body as non-empty TSV lines. Values are NOT unescaped —
    /// callers that surface identifiers apply [`tsv_unescape`].
    async fn run_query_lines(&self, sql: &str) -> Result<Vec<String>> {
        let resp = self
            .query_request(sql, "TSV")
            .timeout(CONTROL_TIMEOUT)
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("ClickHouse request failed: {e}")))?;
        let status = resp.status();
        let exception_code = header_string(&resp, "x-clickhouse-exception-code");
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(clickhouse_http_error(status, exception_code, &body));
        }
        Ok(body
            .lines()
            .filter(|l| !l.is_empty())
            .map(String::from)
            .collect())
    }
}

/// Decode ClickHouse TSV escape sequences (`\\`, `\t`, `\n`, `\r`, `\b`,
/// `\f`, `\0`, `\'`). Unknown escapes pass the escaped character through.
fn tsv_unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('0') => out.push('\0'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

/// Format a non-200 ClickHouse response as an engine error.
fn clickhouse_http_error(
    status: reqwest::StatusCode,
    exception_code: Option<String>,
    body: &str,
) -> QueryFluxError {
    let code = exception_code
        .map(|c| format!(", exception code {c}"))
        .unwrap_or_default();
    // Truncate on char boundaries — exception bodies can contain multi-byte text.
    let body: String = body.trim().chars().take(MAX_ERROR_BODY).collect();
    QueryFluxError::Engine(format!(
        "ClickHouse query failed (HTTP {status}{code}): {body}"
    ))
}

fn header_string(resp: &reqwest::Response, name: &str) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// Locate a ClickHouse `__exception__` frame appended to a response body.
///
/// Since 25.11, a query that fails after HTTP 200 was sent appends
/// `\r\n__exception__\r\n<TAG>\r\n<message>\r\n<len> <TAG>\r\n__exception__\r\n`
/// to the body, where `<TAG>` is a random per-response tag pre-announced in the
/// `X-ClickHouse-Exception-Tag` header. Returns the error message when the
/// frame is present. Older servers splice plain error text into the stream
/// instead; those surface as an Arrow decode / transfer error.
fn find_exception_frame(body: &[u8], tag: &str) -> Option<String> {
    let open = format!("\r\n__exception__\r\n{tag}\r\n");
    let start = find_last(body, open.as_bytes())?;
    let rest = &body[start + open.len()..];
    // The frame closes with `\r\n<message_length> <TAG>\r\n__exception__\r\n`.
    let close_suffix = format!(" {tag}\r\n__exception__\r\n");
    let close_at = find_last(rest, close_suffix.as_bytes())?;
    // Walk back over the message-length digits and the newline preceding them.
    // Observed on 26.7 the separator is a bare `\n` (the docs say `\r\n`) —
    // accept either.
    let mut msg_end = close_at;
    while msg_end > 0 && rest[msg_end - 1].is_ascii_digit() {
        msg_end -= 1;
    }
    let sep_len = if msg_end >= 2 && &rest[msg_end - 2..msg_end] == b"\r\n" {
        2
    } else if msg_end >= 1 && rest[msg_end - 1] == b'\n' {
        1
    } else {
        return None;
    };
    Some(String::from_utf8_lossy(&rest[..msg_end - sep_len]).into_owned())
}

fn find_last(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    (0..=haystack.len() - needle.len())
        .rev()
        .find(|&i| &haystack[i..i + needle.len()] == needle)
}

/// Decode a complete ArrowStream response body into record batches.
/// An empty body (DDL, INSERT) decodes to no batches.
fn decode_arrow_stream(body: &[u8]) -> Result<Vec<RecordBatch>> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let reader = StreamReader::try_new(Cursor::new(body), None).map_err(|e| {
        QueryFluxError::Engine(format!("ClickHouse Arrow stream schema read failed: {e}"))
    })?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| QueryFluxError::Engine(format!("ClickHouse Arrow decode failed: {e}")))
}

/// Split a `DESCRIBE TABLE` TSV line into a column definition.
/// Line shape: `name\ttype\tdefault_type\tdefault_expression\t…`.
fn parse_describe_line(line: &str) -> Option<ColumnDef> {
    let mut fields = line.split('\t');
    let name = tsv_unescape(fields.next()?);
    let raw_type = fields.next().unwrap_or("String");
    let (data_type, nullable) = strip_nullable(raw_type);
    Some(ColumnDef {
        name,
        data_type: data_type.to_string(),
        nullable,
    })
}

/// `Nullable(String)` → (`String`, true); anything else passes through as not null.
fn strip_nullable(raw: &str) -> (&str, bool) {
    raw.strip_prefix("Nullable(")
        .and_then(|s| s.strip_suffix(')'))
        .map_or((raw, false), |inner| (inner, true))
}

/// Backtick-escape a ClickHouse identifier. Backslashes are escaped too —
/// ClickHouse interprets backslash escapes inside back-quoted identifiers.
fn escape_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('\\', "\\\\").replace('`', "``"))
}

#[async_trait]
impl crate::SyncAdapter for ClickHouseAdapter {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
        // Dispatch interpolates params into the SQL before calling — this
        // adapter reports `supports_native_params() == false` (the default).
        _params: &queryflux_core::params::QueryParams,
    ) -> Result<SyncExecution> {
        let mut req = self.query_request(sql, "ArrowStream");
        if let Some(db) = session.database() {
            req = req.query(&[("database", db)]);
        }
        if !tags.is_empty() {
            // `log_comment` surfaces the tags in system.query_log for audits.
            // Note: rejected by servers for users with the readonly=1 profile;
            // use readonly=2 (or a non-readonly service account) with tags.
            let tag_json = serde_json::to_string(tags).unwrap_or_default();
            req = req.query(&[("log_comment", tag_json.as_str())]);
        }

        let mut resp = req
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("ClickHouse request failed: {e}")))?;
        let status = resp.status();
        let exception_code = header_string(&resp, "x-clickhouse-exception-code");
        let exception_tag = header_string(&resp, "x-clickhouse-exception-tag");

        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(clickhouse_http_error(status, exception_code, &body));
        }

        // Buffer the body, tolerating a mid-stream abort: ClickHouse kills the
        // chunked encoding when a query fails after 200 was already sent, and
        // the exception frame is in the bytes received so far.
        let mut body: Vec<u8> = Vec::new();
        let mut transfer_error: Option<reqwest::Error> = None;
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    if body.len() + chunk.len() > self.max_result_buffer_bytes {
                        return Err(QueryFluxError::Engine(format!(
                            "ClickHouse result exceeded the {}-byte buffered-result cap; \
                             add a LIMIT, narrow the query, or raise maxResultBufferBytes",
                            self.max_result_buffer_bytes
                        )));
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(e) => {
                    transfer_error = Some(e);
                    break;
                }
            }
        }

        // A post-200 failure appends a tag-framed exception block — check for
        // it before decoding so a failed query never yields partial batches.
        if let Some(tag) = &exception_tag {
            if let Some(msg) = find_exception_frame(&body, tag) {
                return Err(QueryFluxError::Engine(format!(
                    "ClickHouse query failed mid-stream: {}",
                    msg.trim()
                )));
            }
        }
        if let Some(e) = transfer_error {
            return Err(QueryFluxError::Engine(format!(
                "ClickHouse response aborted mid-stream: {e}"
            )));
        }

        let batches = decode_arrow_stream(&body)?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        // X-ClickHouse-Summary is a snapshot taken at header-flush time and
        // undercounts in streaming mode — send None rather than wrong numbers.
        let _ = tx.send(None);
        Ok(SyncExecution {
            stream: Box::pin(stream::iter(batches.into_iter().map(Ok))),
            stats: rx,
        })
    }

    fn engine_type(&self) -> EngineType {
        EngineType::ClickHouse
    }

    async fn health_check(&self) -> bool {
        // `/ping` is ClickHouse's dedicated auth-free liveness endpoint.
        let url = format!("{}/ping", self.endpoint);
        match self.client.get(&url).timeout(CONTROL_TIMEOUT).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                tracing::warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "ClickHouse health check ping failed"
                );
                false
            }
        }
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        // All actively executing queries on the server — not just those routed
        // through QueryFlux — so the reconciler sees true engine load.
        let lines = self
            .run_query_lines("SELECT count() FROM system.processes")
            .await
            .ok()?;
        lines.first()?.parse().ok()
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        // ClickHouse has no catalog level — expose one synthetic catalog.
        Ok(vec!["default".to_string()])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        let lines = self.run_query_lines("SHOW DATABASES").await?;
        Ok(lines.iter().map(|l| tsv_unescape(l)).collect())
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let sql = format!("SHOW TABLES FROM {}", escape_ident(database));
        let lines = self.run_query_lines(&sql).await?;
        Ok(lines.iter().map(|l| tsv_unescape(l)).collect())
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let sql = format!(
            "DESCRIBE TABLE {}.{}",
            escape_ident(database),
            escape_ident(table)
        );
        let lines = match self.run_query_lines(&sql).await {
            Ok(l) => l,
            Err(_) => return Ok(None),
        };
        let columns = lines
            .iter()
            .filter_map(|l| parse_describe_line(l))
            .collect();
        Ok(Some(TableSchema {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

pub struct ClickHouseFactory;

#[async_trait]
impl crate::EngineAdapterFactory for ClickHouseFactory {
    fn engine_key(&self) -> &'static str {
        "clickHouse"
    }

    fn descriptor(&self) -> EngineDescriptor {
        ClickHouseAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = ClickHouseConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(ClickHouseAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineConfigParseable;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use serde_json::json;

    // --- config parsing ---

    #[test]
    fn from_json_parses_endpoint_auth_and_tls() {
        let cfg = ClickHouseConfig::from_json(
            &json!({
                "endpoint": "http://ch:8123",
                "authType": "basic",
                "authUsername": "qf",
                "authPassword": "secret",
                "tls": { "insecureSkipVerify": true }
            }),
            "ch-1",
        )
        .expect("valid config");
        assert_eq!(cfg.endpoint, "http://ch:8123");
        assert!(matches!(cfg.auth, Some(ClusterAuth::Basic { .. })));
        assert!(cfg.tls_skip_verify);
    }

    #[test]
    fn from_json_missing_endpoint_errors() {
        let err = ClickHouseConfig::from_json(&json!({}), "ch-1")
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("missing endpoint"));
    }

    #[test]
    fn result_buffer_cap_defaults_and_overrides() {
        // Omitted → 1 GiB default.
        let cfg =
            ClickHouseConfig::from_json(&json!({"endpoint": "http://ch:8123"}), "ch-1").unwrap();
        assert_eq!(cfg.max_result_buffer_bytes, DEFAULT_MAX_RESULT_BUFFER_BYTES);
        // Explicit value wins.
        let cfg = ClickHouseConfig::from_json(
            &json!({"endpoint": "http://ch:8123", "maxResultBufferBytes": 65536}),
            "ch-1",
        )
        .unwrap();
        assert_eq!(cfg.max_result_buffer_bytes, 65536);
        // Zero and non-integers are rejected.
        for bad in [json!(0), json!("big"), json!(-1)] {
            let err = ClickHouseConfig::from_json(
                &json!({"endpoint": "http://ch:8123", "maxResultBufferBytes": bad}),
                "ch-1",
            )
            .map(|_| ())
            .unwrap_err();
            assert!(err.to_string().contains("maxResultBufferBytes"));
        }
    }

    #[test]
    fn from_json_rejects_bearer_auth() {
        let err = ClickHouseConfig::from_json(
            &json!({
                "endpoint": "http://ch:8123",
                "authType": "bearer",
                "authToken": "tok"
            }),
            "ch-1",
        )
        .map(|_| ())
        .unwrap_err();
        assert!(err.to_string().contains("only supports basic auth"));
    }

    #[test]
    fn from_cluster_config_requires_endpoint_and_basic_auth() {
        let mut cfg = ClusterConfig {
            engine: Some(queryflux_core::config::EngineConfig::ClickHouse),
            enabled: true,
            max_running_queries: None,
            pool_size: None,
            endpoint: None,
            database_path: None,
            region: None,
            s3_output_location: None,
            workgroup: None,
            catalog: None,
            tls: None,
            max_result_buffer_bytes: None,
            auth: None,
            query_auth: None,
        };
        assert!(ClickHouseConfig::from_cluster_config(&cfg, "ch-1").is_err());

        cfg.endpoint = Some("http://ch:8123".to_string());
        cfg.auth = Some(ClusterAuth::Bearer {
            token: "tok".to_string(),
        });
        let err = ClickHouseConfig::from_cluster_config(&cfg, "ch-1")
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("only supports basic auth"));

        cfg.auth = Some(ClusterAuth::Basic {
            username: "qf".to_string(),
            password: "pw".to_string(),
        });
        assert!(ClickHouseConfig::from_cluster_config(&cfg, "ch-1").is_ok());
    }

    // --- exception frame scanning ---

    const TAG: &str = "yqftqimeegydpouu";

    /// Byte-exact real-server framing (verified against ClickHouse 26.7):
    /// bare `\n` between message and length line.
    fn framed(message: &str) -> Vec<u8> {
        let mut body = b"some arrow bytes".to_vec();
        body.extend_from_slice(
            format!(
                "\r\n__exception__\r\n{TAG}\r\n{message}\n{} {TAG}\r\n__exception__\r\n",
                message.len()
            )
            .as_bytes(),
        );
        body
    }

    /// The framing the ClickHouse docs describe (`\r\n` separator) — accepted
    /// too in case other versions emit it.
    fn framed_crlf(message: &str) -> Vec<u8> {
        let mut body = b"some arrow bytes".to_vec();
        body.extend_from_slice(
            format!(
                "\r\n__exception__\r\n{TAG}\r\n{message}\r\n{} {TAG}\r\n__exception__\r\n",
                message.len()
            )
            .as_bytes(),
        );
        body
    }

    #[test]
    fn exception_frame_is_extracted() {
        let body = framed("Code: 395. DB::Exception: Value passed to 'throwIf'.");
        assert_eq!(
            find_exception_frame(&body, TAG).as_deref(),
            Some("Code: 395. DB::Exception: Value passed to 'throwIf'.")
        );
    }

    #[test]
    fn exception_frame_with_crlf_separator_is_extracted() {
        let body = framed_crlf("Code: 395. DB::Exception: boom.");
        assert_eq!(
            find_exception_frame(&body, TAG).as_deref(),
            Some("Code: 395. DB::Exception: boom.")
        );
    }

    #[test]
    fn multiline_exception_message_is_preserved() {
        let body = framed("line one\r\nline two");
        assert_eq!(
            find_exception_frame(&body, TAG).as_deref(),
            Some("line one\r\nline two")
        );
    }

    #[test]
    fn body_without_frame_returns_none() {
        assert_eq!(find_exception_frame(b"plain arrow bytes", TAG), None);
    }

    #[test]
    fn frame_with_wrong_tag_returns_none() {
        let body = framed("boom");
        assert_eq!(find_exception_frame(&body, "aaaaaaaaaaaaaaaa"), None);
    }

    #[test]
    fn truncated_frame_returns_none() {
        let mut body = framed("boom");
        body.truncate(body.len() - 10);
        assert_eq!(find_exception_frame(&body, TAG), None);
    }

    // --- arrow stream decoding ---

    fn ipc_stream(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            for b in batches {
                writer.write(b).unwrap();
            }
            writer.finish().unwrap();
        }
        buf
    }

    #[test]
    fn decode_roundtrips_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
                .unwrap();
        let decoded = decode_arrow_stream(&ipc_stream(std::slice::from_ref(&batch))).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], batch);
    }

    #[test]
    fn decode_empty_body_is_no_batches() {
        assert!(decode_arrow_stream(&[]).unwrap().is_empty());
    }

    #[test]
    fn decode_garbage_errors() {
        assert!(decode_arrow_stream(b"definitely not arrow").is_err());
    }

    // --- describe parsing / identifiers ---

    #[test]
    fn describe_line_strips_nullable() {
        let col = parse_describe_line("age\tNullable(UInt8)\t\t\t\t\t").unwrap();
        assert_eq!(col.name, "age");
        assert_eq!(col.data_type, "UInt8");
        assert!(col.nullable);
    }

    #[test]
    fn describe_line_non_nullable_passthrough() {
        let col = parse_describe_line("name\tString\tDEFAULT\t''\t\t\t").unwrap();
        assert_eq!(col.name, "name");
        assert_eq!(col.data_type, "String");
        assert!(!col.nullable);
    }

    #[test]
    fn nested_nullable_only_strips_outer_layer() {
        assert_eq!(
            strip_nullable("Array(Nullable(String))"),
            ("Array(Nullable(String))", false)
        );
        assert_eq!(
            strip_nullable("Nullable(Decimal(10, 2))"),
            ("Decimal(10, 2)", true)
        );
    }

    #[test]
    fn idents_are_backtick_escaped() {
        assert_eq!(escape_ident("db"), "`db`");
        assert_eq!(escape_ident("we`ird"), "`we``ird`");
        assert_eq!(escape_ident(r"back\slash"), r"`back\\slash`");
    }

    #[test]
    fn tsv_escapes_are_decoded() {
        assert_eq!(tsv_unescape(r"plain"), "plain");
        assert_eq!(tsv_unescape(r"tab\there"), "tab\there");
        assert_eq!(tsv_unescape(r"line\nbreak"), "line\nbreak");
        assert_eq!(tsv_unescape(r"back\\slash"), r"back\slash");
        assert_eq!(tsv_unescape(r"quote\'s"), "quote's");
        // Unknown escape passes the escaped character through; trailing
        // backslash is preserved.
        assert_eq!(tsv_unescape(r"odd\z"), "oddz");
        assert_eq!(tsv_unescape("trailing\\"), "trailing\\");
    }

    /// A TSV-escaped identifier from SHOW DATABASES round-trips through
    /// decode + re-escape into a form ClickHouse parses back to the original.
    #[test]
    fn tsv_identifier_roundtrip() {
        let listed = r"back\\slash"; // SHOW DATABASES output for `back\slash`
        let decoded = tsv_unescape(listed);
        assert_eq!(decoded, r"back\slash");
        assert_eq!(escape_ident(&decoded), r"`back\\slash`");
    }
}
