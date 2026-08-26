use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use queryflux_auth::QueryCredentials;
use queryflux_core::{
    catalog::TableSchema,
    config::ClusterConfig,
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::QueryTags,
};
use reqwest::Client;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use crate::{AdapterKind, BackendQueryIdSlot, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Parsed and validated configuration for a DuckDB HTTP cluster.
pub struct DuckDbHttpConfig {
    pub endpoint: String,
    pub tls_skip_verify: bool,
    pub auth: Option<queryflux_core::config::ClusterAuth>,
    pub max_result_buffer_bytes: usize,
}

impl crate::EngineConfigParseable for DuckDbHttpConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        use queryflux_core::config::ClusterAuth;
        use queryflux_core::engine_registry::{
            json_str, json_tls_insecure_skip_verify, parse_auth_from_config_json,
        };
        let endpoint = json_str(json, "endpoint").ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing endpoint"
            ))
        })?;
        let tls_skip_verify = json_tls_insecure_skip_verify(json);
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': invalid auth ({e})"
            ))
        })?;
        if let Some(ref a) = auth {
            if !matches!(a, ClusterAuth::Basic { .. } | ClusterAuth::Bearer { .. }) {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': DuckDB HTTP supports only basic or bearer auth"
                )));
            }
        }
        Ok(Self {
            endpoint,
            tls_skip_verify,
            auth,
            max_result_buffer_bytes: crate::duckdb::parse_max_result_buffer_from_json(
                json,
                cluster_name,
            )?,
        })
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        let endpoint = cfg.endpoint.clone().ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing endpoint"
            ))
        })?;
        let tls_skip_verify = cfg
            .tls
            .as_ref()
            .map(|t| t.insecure_skip_verify)
            .unwrap_or(false);
        use queryflux_core::config::ClusterAuth;
        let auth = cfg.auth.clone();
        if let Some(ref a) = auth {
            if !matches!(a, ClusterAuth::Basic { .. } | ClusterAuth::Bearer { .. }) {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': DuckDB HTTP supports only basic or bearer auth"
                )));
            }
        }
        Ok(Self {
            endpoint,
            tls_skip_verify,
            auth,
            max_result_buffer_bytes: crate::duckdb::parse_max_result_buffer_bytes(
                cfg.max_result_buffer_bytes,
                cluster_name,
            )?,
        })
    }
}

/// DuckDB remote HTTP server adapter.
///
/// Targets the DuckDB community `httpserver` extension API:
/// - POST `{endpoint}/?default_format=JSONCompact` with the raw SQL as the request body.
/// - Response: a single JSON object `{"meta": [{"name","type"}, ...], "data": [[...], ...],
///   "rows": N, "statistics": {...}}` (see `result_serializer_compact_json.hpp` in
///   quackscience/duckdb-extension-httpserver). Unlike the default `JSONEachRow`
///   (NDJSON-per-row) format, `meta` always reflects the query's real result schema —
///   even when `data` is empty — so a zero-row `SELECT` can still be framed as a proper
///   empty result set instead of falling back to a DDL/DML-style OK response.
///
/// Start a DuckDB HTTP server with:
/// ```sql
/// INSTALL httpserver FROM community;
/// LOAD httpserver;
/// SELECT httpserve_start('0.0.0.0', 4321, '');
/// ```
pub struct DuckDbHttpAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    endpoint: String,
    client: Client,
    max_result_buffer_bytes: usize,
}

/// Parsed JSONCompact response from the DuckDB HTTP server.
#[derive(Debug)]
struct HttpQueryResponse {
    /// Column name + Arrow type, in positional order — always present (even for a
    /// zero-row result) since it comes from `meta`, not inferred from row data.
    columns: Vec<(String, DataType)>,
    /// Row data: outer vec = rows, inner vec = column values in `columns` order.
    rows: Vec<Vec<serde_json::Value>>,
}

impl HttpQueryResponse {
    fn parse(body: &str) -> Result<Self> {
        let root: serde_json::Value = serde_json::from_str(body).map_err(|e| {
            QueryFluxError::Engine(format!(
                "Failed to parse DuckDB HTTP JSONCompact response: {e}"
            ))
        })?;

        let meta = root.get("meta").and_then(|m| m.as_array()).ok_or_else(|| {
            QueryFluxError::Engine(
                "DuckDB HTTP JSONCompact response missing 'meta' — expected \
                 ?default_format=JSONCompact"
                    .to_string(),
            )
        })?;
        let columns: Vec<(String, DataType)> = meta
            .iter()
            .map(|col| {
                let name = col
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let ty = col.get("type").and_then(|t| t.as_str()).unwrap_or("");
                (name, duckdb_type_to_arrow(ty))
            })
            .collect();

        let rows: Vec<Vec<serde_json::Value>> = root
            .get("data")
            .and_then(|d| d.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|row| row.as_array().cloned())
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self { columns, rows })
    }
}

/// Map a DuckDB `LogicalType::ToString()` name (as emitted in JSONCompact `meta[].type`,
/// e.g. `"INTEGER"`, `"DECIMAL(10,2)"`, `"STRUCT(a INTEGER)"`) to an Arrow `DataType`.
/// Only maps the types `build_array` below actually specializes for (Boolean, the
/// integer family, the float family) — everything else DuckDB's serializer already
/// emits as a JSON string (dates, timestamps, UUIDs, blobs, lists, structs, ...), so
/// falling back to `Utf8` matches what's actually on the wire.
fn duckdb_type_to_arrow(ty: &str) -> DataType {
    if ty.contains('[') {
        // LIST/ARRAY element type, e.g. "INTEGER[]". DuckDB's serializer emits
        // these as nested JSON arrays, not scalars — mapping the element type
        // here (e.g. to Int64) would feed a JSON array into `build_array`'s
        // scalar-only Int64 parser, silently turning every value into NULL.
        // Stringify via the Utf8 fallback instead.
        return DataType::Utf8;
    }
    let base = ty.split('(').next().unwrap_or(ty).trim().to_uppercase();
    match base.as_str() {
        "BOOLEAN" => DataType::Boolean,
        // TINYINT..BIGINT and the unsigned types up to UINTEGER all fit in i64
        // (BIGINT's max is exactly i64::MAX). HUGEINT/UHUGEINT are 128-bit and
        // UBIGINT's max (~1.8e19) exceeds i64::MAX (~9.2e18) — DuckDB's own
        // serializer already emits those three as JSON strings to avoid losing
        // precision, so map them to Utf8 rather than feeding out-of-range values
        // into `build_array`'s Int64 parser, which would silently turn them NULL.
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "UTINYINT" | "USMALLINT" | "UINTEGER" => {
            DataType::Int64
        }
        "FLOAT" | "DOUBLE" | "DECIMAL" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

impl DuckDbHttpAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: DuckDbHttpConfig,
    ) -> Result<Self> {
        let mut builder = Client::builder().timeout(std::time::Duration::from_secs(120));

        if config.tls_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }

        // Apply default authorization header if configured.
        if let Some(auth) = config.auth {
            use queryflux_core::config::ClusterAuth;
            let mut headers = reqwest::header::HeaderMap::new();
            match auth {
                ClusterAuth::Bearer { token } => {
                    let val = format!("Bearer {token}");
                    headers.insert(
                        reqwest::header::AUTHORIZATION,
                        val.parse().map_err(|_| {
                            QueryFluxError::Engine("Invalid bearer token for DuckDB HTTP".into())
                        })?,
                    );
                }
                ClusterAuth::Basic { username, password } => {
                    let encoded = base64_encode(&format!("{username}:{password}"));
                    let val = format!("Basic {encoded}");
                    headers.insert(
                        reqwest::header::AUTHORIZATION,
                        val.parse().map_err(|_| {
                            QueryFluxError::Engine("Invalid basic auth for DuckDB HTTP".into())
                        })?,
                    );
                }
                _ => {}
            }
            builder = builder.default_headers(headers);
        }

        let client = builder
            .build()
            .map_err(|e| QueryFluxError::Engine(format!("Failed to build HTTP client: {e}")))?;

        let endpoint = config.endpoint.trim_end_matches('/').to_string();
        Ok(Self {
            cluster_name,
            group_name,
            endpoint,
            client,
            max_result_buffer_bytes: config.max_result_buffer_bytes,
        })
    }

    async fn read_body_capped(&self, sql: &str, cap: usize) -> Result<Vec<u8>> {
        let url = format!("{}/?default_format=JSONCompact", self.endpoint);
        let mut resp = self
            .client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("DuckDB HTTP request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Engine(format!(
                "DuckDB HTTP server returned {status}: {body}"
            )));
        }

        let mut body: Vec<u8> = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("DuckDB HTTP read failed: {e}")))?
        {
            if body.len() + chunk.len() > cap {
                return Err(QueryFluxError::Engine(format!(
                    "DuckDB HTTP result exceeded the {cap}-byte buffered-result cap; \
                     add a LIMIT, narrow the query, or raise maxResultBufferBytes"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    fn clone_for_task(&self) -> Self {
        Self {
            cluster_name: self.cluster_name.clone(),
            group_name: self.group_name.clone(),
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            max_result_buffer_bytes: self.max_result_buffer_bytes,
        }
    }

    async fn run_query(&self, sql: &str) -> Result<HttpQueryResponse> {
        let body = self
            .read_body_capped(sql, self.max_result_buffer_bytes)
            .await?;
        let text = std::str::from_utf8(&body).map_err(|e| {
            QueryFluxError::Engine(format!("DuckDB HTTP response is not valid UTF-8: {e}"))
        })?;
        HttpQueryResponse::parse(text)
    }

    /// Chunk a parsed JSONCompact response into `RecordBatch`es of at most
    /// `BATCH_SIZE` rows. The whole body is already fully buffered by
    /// `read_body_capped` (bounded by `max_result_buffer_bytes`) before this runs —
    /// unlike the old NDJSON parser, JSONCompact is one JSON object, not an
    /// incrementally-parseable line stream — so chunking here only bounds how much
    /// gets copied into a single Arrow batch, not how much memory the response uses.
    ///
    /// Always sends at least one batch (possibly zero rows) carrying the real
    /// schema from `meta`, so a genuinely empty `SELECT` is still framed as an
    /// empty result set by dispatch, not misread as a DDL/DML OK response.
    async fn stream_json_to_batches(
        body: &[u8],
        batch_tx: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        const BATCH_SIZE: usize = 8_192;
        let text = std::str::from_utf8(body).map_err(|e| {
            QueryFluxError::Engine(format!("DuckDB HTTP response is not valid UTF-8: {e}"))
        })?;
        let HttpQueryResponse { columns, mut rows } = HttpQueryResponse::parse(text)?;

        if rows.is_empty() {
            let batch = response_to_record_batch(HttpQueryResponse { columns, rows })?;
            let _ = batch_tx.send(Ok(batch)).await;
            return Ok(());
        }

        while !rows.is_empty() {
            let chunk: Vec<_> = if rows.len() > BATCH_SIZE {
                rows.drain(..BATCH_SIZE).collect()
            } else {
                std::mem::take(&mut rows)
            };
            let batch = response_to_record_batch(HttpQueryResponse {
                columns: columns.clone(),
                rows: chunk,
            })?;
            if batch_tx.send(Ok(batch)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SyncAdapter for DuckDbHttpAdapter {
    async fn health_check(&self) -> bool {
        self.run_query("SELECT 1").await.is_ok()
    }

    fn engine_type(&self) -> EngineType {
        EngineType::DuckDbHttp
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        _credentials: &QueryCredentials,
        _tags: &QueryTags,
        _params: &queryflux_core::params::QueryParams,
        _hints: queryflux_core::sql_classify::ExecutionHints,
        _id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        // Community httpserver has no cancel API — leave the slot unset so
        // dispatch does not record a fake backend id or spawn a no-op cancel.
        // Dropping this future aborts the HTTP request (best-effort).
        debug!(
            cluster = %self.cluster_name,
            attempt_id = %uuid::Uuid::new_v4(),
            "Executing DuckDB HTTP query"
        );

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(32);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let adapter = self.clone_for_task();
        let sql = sql.to_string();

        tokio::spawn(async move {
            let result = async {
                let body = adapter
                    .read_body_capped(&sql, adapter.max_result_buffer_bytes)
                    .await?;
                DuckDbHttpAdapter::stream_json_to_batches(&body, &batch_tx).await
            }
            .await;
            if let Err(e) = result {
                let _ = batch_tx.send(Err(e)).await;
            }
            let _ = stats_tx.send(None);
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
            affected_rows: None,
        })
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let resp = self
            .run_query("SELECT catalog_name FROM information_schema.schemata GROUP BY catalog_name")
            .await?;
        Ok(extract_string_column(&resp, 0))
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        let resp = self
            .run_query("SELECT schema_name FROM information_schema.schemata")
            .await?;
        Ok(extract_string_column(&resp, 0))
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}'"
        );
        let resp = self.run_query(&sql).await?;
        Ok(extract_string_column(&resp, 0))
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let sql = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = '{database}' AND table_name = '{table}' \
             ORDER BY ordinal_position"
        );
        let resp = self.run_query(&sql).await?;
        if resp.rows.is_empty() {
            return Ok(None);
        }
        let columns = resp
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.first()?.as_str()?.to_string();
                let data_type = row.get(1)?.as_str()?.to_uppercase();
                let nullable = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_uppercase() != "NO")
                    .unwrap_or(true);
                Some(queryflux_core::catalog::ColumnDef {
                    name,
                    data_type,
                    nullable,
                })
            })
            .collect();
        Ok(Some(TableSchema {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

// ---------------------------------------------------------------------------
// JSON → Arrow conversion
// ---------------------------------------------------------------------------

/// Convert a parsed DuckDB HTTP JSONCompact response into a single Arrow RecordBatch.
/// `response.columns` (from `meta`) is always populated with the real schema, even
/// when `response.rows` is empty, so this builds a correctly-typed zero-row batch
/// for an empty `SELECT` rather than an untyped, columnless one.
fn response_to_record_batch(response: HttpQueryResponse) -> Result<RecordBatch> {
    let n_cols = response.columns.len();
    let n_rows = response.rows.len();

    if n_cols == 0 {
        let schema = Arc::new(Schema::empty());
        return RecordBatch::new_empty(schema)
            .pipe_ok()
            .map_err(|e| QueryFluxError::Engine(format!("Arrow error: {e}")));
    }

    let mut fields: Vec<Field> = Vec::with_capacity(n_cols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_cols);

    for (col_idx, (col_name, arrow_type)) in response.columns.iter().enumerate() {
        let col_values: Vec<Option<&serde_json::Value>> =
            response.rows.iter().map(|row| row.get(col_idx)).collect();

        fields.push(Field::new(col_name, arrow_type.clone(), true));
        let array = build_array(arrow_type, &col_values, n_rows)?;
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| QueryFluxError::Engine(format!("Failed to build RecordBatch: {e}")))
}

/// Build an Arrow array from a column of JSON values.
fn build_array(
    arrow_type: &DataType,
    values: &[Option<&serde_json::Value>],
    n_rows: usize,
) -> Result<ArrayRef> {
    match arrow_type {
        DataType::Boolean => {
            let arr: BooleanArray = values.iter().map(|v| v.and_then(|v| v.as_bool())).collect();
            Ok(Arc::new(arr))
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            // Use Int64 for all integer types; Arrow will cast if needed.
            let arr: Int64Array = values
                .iter()
                .map(|v| {
                    v.and_then(|v| match v {
                        serde_json::Value::Number(n) => n.as_i64(),
                        serde_json::Value::String(s) => s.parse().ok(),
                        _ => None,
                    })
                })
                .collect();
            // If the target type isn't Int64, cast.
            if *arrow_type == DataType::Int64 {
                Ok(Arc::new(arr))
            } else {
                arrow::compute::cast(&arr, arrow_type)
                    .map_err(|e| QueryFluxError::Engine(format!("Arrow cast failed: {e}")))
            }
        }
        DataType::Float32 | DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| {
                    v.and_then(|v| match v {
                        serde_json::Value::Number(n) => n.as_f64(),
                        serde_json::Value::String(s) => s.parse().ok(),
                        _ => None,
                    })
                })
                .collect();
            if *arrow_type == DataType::Float64 {
                Ok(Arc::new(arr))
            } else {
                arrow::compute::cast(&arr, arrow_type)
                    .map_err(|e| QueryFluxError::Engine(format!("Arrow cast failed: {e}")))
            }
        }
        DataType::Null => Ok(Arc::new(NullArray::new(n_rows))),
        // Default: stringify everything as Utf8
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| {
                    v.map(|v| match v {
                        serde_json::Value::String(s) => s.as_str().to_string(),
                        serde_json::Value::Null => String::new(),
                        other => other.to_string(),
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
    }
}

fn extract_string_column(response: &HttpQueryResponse, col_idx: usize) -> Vec<String> {
    response
        .rows
        .iter()
        .filter_map(|row| {
            row.get(col_idx)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

fn base64_encode(input: &str) -> String {
    use std::fmt::Write;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = match chunk.len() {
            1 => [chunk[0], 0, 0],
            2 => [chunk[0], chunk[1], 0],
            _ => [chunk[0], chunk[1], chunk[2]],
        };
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        let _ = write!(
            out,
            "{}{}{}{}",
            CHARS[((n >> 18) & 0x3f) as usize] as char,
            CHARS[((n >> 12) & 0x3f) as usize] as char,
            if chunk.len() > 1 {
                CHARS[((n >> 6) & 0x3f) as usize] as char
            } else {
                '='
            },
            if chunk.len() > 2 {
                CHARS[(n & 0x3f) as usize] as char
            } else {
                '='
            },
        );
    }
    out
}

// Small helper trait to pipe Ok through a Result chain without a closure.
trait PipeOk: Sized {
    fn pipe_ok(self) -> std::result::Result<Self, arrow::error::ArrowError>;
}
impl PipeOk for RecordBatch {
    fn pipe_ok(self) -> std::result::Result<Self, arrow::error::ArrowError> {
        Ok(self)
    }
}

impl DuckDbHttpAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "duckDbHttp",
            display_name: "DuckDB HTTP Server",
            description: "Remote DuckDB instance running the community httpserver extension. Connects via HTTP REST API.",
            hex: "E8AC00",
            connection_type: ConnectionType::Http,
            default_port: Some(4321),
            endpoint_example: Some("http://duckdb-server:4321"),
            supported_auth: vec![AuthType::Basic, AuthType::Bearer],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "HTTP base URL of the DuckDB HTTP server.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("http://duckdb-server:4321"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Authentication mechanism used by the DuckDB HTTP server.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("bearer"),
                },
                ConfigField {
                    key: "auth.token",
                    label: "Bearer token",
                    description: "Bearer token for the DuckDB HTTP server.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "tls.insecureSkipVerify",
                    label: "Skip TLS verification",
                    description: "Disable TLS certificate verification. Use only in development.",
                    field_type: FieldType::Boolean,
                    required: false,
                    example: Some("false"),
                },
                ConfigField {
                    key: "maxResultBufferBytes",
                    label: "Max result buffer (bytes)",
                    description: "Per-query cap on buffered HTTP response / Arrow bytes. Defaults to 1 GiB.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("1073741824"),
                },
            ],
        }
    }
}

pub struct DuckDbHttpFactory;

#[async_trait]
impl crate::EngineAdapterFactory for DuckDbHttpFactory {
    fn engine_key(&self) -> &'static str {
        "duckDbHttp"
    }

    fn descriptor(&self) -> EngineDescriptor {
        DuckDbHttpAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<crate::AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = DuckDbHttpConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(DuckDbHttpAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }

    async fn build_from_cluster_config(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        cfg: &ClusterConfig,
        cluster_name_str: &str,
    ) -> Result<crate::AdapterKind> {
        use crate::EngineConfigParseable;
        let config = DuckDbHttpConfig::from_cluster_config(cfg, cluster_name_str)?;
        Ok(AdapterKind::Sync(Arc::new(DuckDbHttpAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape produced by `ResultSerializerCompactJson` in
    /// quackscience/duckdb-extension-httpserver for a query matching zero rows —
    /// `meta` still lists the real columns, `data` is an empty array.
    const EMPTY_SELECT_RESPONSE: &str = r#"{
        "meta": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"}
        ],
        "data": [],
        "rows": 0,
        "statistics": {"elapsed": 0.001, "rows_read": 0, "bytes_read": 0}
    }"#;

    const TWO_ROW_RESPONSE: &str = r#"{
        "meta": [
            {"name": "id", "type": "BIGINT"},
            {"name": "score", "type": "DOUBLE"},
            {"name": "active", "type": "BOOLEAN"},
            {"name": "label", "type": "VARCHAR"}
        ],
        "data": [
            [1, 1.5, true, "a"],
            [2, null, false, null]
        ],
        "rows": 2,
        "statistics": {"elapsed": 0.001, "rows_read": 2, "bytes_read": 0}
    }"#;

    #[test]
    fn duckdb_type_mapping_covers_the_common_families() {
        assert_eq!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(duckdb_type_to_arrow("INTEGER"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("UINTEGER"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("FLOAT"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("DECIMAL(10,2)"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("DATE"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("TIMESTAMP"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("STRUCT(a INTEGER)"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("INTEGER[]"), DataType::Utf8);
    }

    /// Regression: UBIGINT/HUGEINT/UHUGEINT can exceed i64::MAX. DuckDB's own
    /// serializer already emits them as JSON strings for this reason — mapping
    /// them to Int64 would feed an out-of-range value into `build_array`'s
    /// `as_i64()`/`parse::<i64>()` parsing, silently turning it into NULL.
    #[test]
    fn wide_integer_types_map_to_utf8_not_int64() {
        assert_eq!(duckdb_type_to_arrow("UBIGINT"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("HUGEINT"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("UHUGEINT"), DataType::Utf8);
    }

    /// A UBIGINT value beyond i64::MAX must round-trip as text, not silently
    /// become NULL.
    #[test]
    fn out_of_i64_range_ubigint_value_is_preserved_as_text() {
        let body = r#"{
            "meta": [{"name": "n", "type": "UBIGINT"}],
            "data": [["18446744073709551615"]],
            "rows": 1
        }"#;
        let resp = HttpQueryResponse::parse(body).expect("parse");
        let batch = response_to_record_batch(resp).expect("build batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("UBIGINT column must be Utf8");
        assert_eq!(col.value(0), "18446744073709551615");
    }

    /// Regression: a zero-row SELECT must still carry its real column schema —
    /// this is the whole reason for switching from JSONEachRow (NDJSON, columns
    /// only derivable from row data) to JSONCompact (`meta` always present).
    #[test]
    fn parse_empty_select_keeps_real_schema() {
        let resp = HttpQueryResponse::parse(EMPTY_SELECT_RESPONSE).expect("parse");
        assert_eq!(
            resp.columns,
            vec![
                ("id".to_string(), DataType::Int64),
                ("name".to_string(), DataType::Utf8),
            ]
        );
        assert!(resp.rows.is_empty());

        let batch = response_to_record_batch(resp).expect("build batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
    }

    #[test]
    fn parse_populates_rows_and_types() {
        let resp = HttpQueryResponse::parse(TWO_ROW_RESPONSE).expect("parse");
        assert_eq!(resp.rows.len(), 2);
        let batch = response_to_record_batch(resp).expect("build batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn parse_rejects_ndjson_missing_meta() {
        // The old NDJSON-per-row format has no top-level `meta` — must fail loudly
        // rather than silently misinterpreting the response.
        let err = HttpQueryResponse::parse(r#"{"id": 1, "name": "a"}"#).unwrap_err();
        assert!(err.to_string().contains("meta"), "got: {err}");
    }

    #[tokio::test]
    async fn stream_json_to_batches_empty_select_sends_one_zero_row_batch_with_schema() {
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        DuckDbHttpAdapter::stream_json_to_batches(EMPTY_SELECT_RESPONSE.as_bytes(), &tx)
            .await
            .expect("stream");
        drop(tx);
        let batches: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
            .collect()
            .await;
        assert_eq!(batches.len(), 1, "expected exactly one batch");
        let batch = batches[0].as_ref().expect("batch ok");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 2);
    }

    #[tokio::test]
    async fn stream_json_to_batches_chunks_large_results() {
        use futures::StreamExt;
        let meta = r#"[{"name": "n", "type": "INTEGER"}]"#;
        let data: Vec<String> = (0..20_000).map(|i| format!("[{i}]")).collect();
        let body = format!(
            r#"{{"meta": {meta}, "data": [{}], "rows": 20000}}"#,
            data.join(",")
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        DuckDbHttpAdapter::stream_json_to_batches(body.as_bytes(), &tx)
            .await
            .expect("stream");
        drop(tx);
        let batches: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
            .collect()
            .await;
        assert!(batches.len() > 1, "20000 rows must span multiple batches");
        let total: usize = batches
            .iter()
            .map(|b| b.as_ref().expect("batch ok").num_rows())
            .sum();
        assert_eq!(total, 20_000);
    }
}
