use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::stream;
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
use tracing::debug;

use crate::{AdapterKind, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Parsed and validated configuration for a DuckDB HTTP cluster.
pub struct DuckDbHttpConfig {
    pub endpoint: String,
    pub tls_skip_verify: bool,
    pub auth: Option<queryflux_core::config::ClusterAuth>,
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
        })
    }
}

/// DuckDB remote HTTP server adapter.
///
/// Targets the DuckDB community `httpserver` extension API:
/// - POST `{endpoint}/query` with JSON body `{"query": "..."}`
/// - Response: JSON with `columns` metadata and `rows` data
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
}

/// Parsed NDJSON response from the DuckDB HTTP server.
/// Each row is a JSON object `{"col": value, ...}`.
struct HttpQueryResponse {
    /// Column names in order (derived from the first row's keys).
    column_names: Vec<String>,
    /// Row data: outer vec = rows, inner vec = column values in column_names order.
    rows: Vec<Vec<serde_json::Value>>,
}

impl HttpQueryResponse {
    fn parse(body: &str) -> Result<Self> {
        let mut column_names: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)
                .map_err(|e| {
                    QueryFluxError::Engine(format!("Failed to parse DuckDB HTTP NDJSON line: {e}"))
                })?;

            if column_names.is_empty() {
                column_names = obj.keys().cloned().collect();
            }

            let row: Vec<serde_json::Value> = column_names
                .iter()
                .map(|k| obj.get(k).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            rows.push(row);
        }

        Ok(Self { column_names, rows })
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
        })
    }

    async fn run_query(&self, sql: &str) -> Result<HttpQueryResponse> {
        let url = format!("{}/", self.endpoint);

        let resp = self
            .client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("DuckDB HTTP request failed: {e}")))?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(QueryFluxError::Engine(format!(
                "DuckDB HTTP server returned {status}: {body}"
            )));
        }

        HttpQueryResponse::parse(&body)
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
    ) -> Result<SyncExecution> {
        debug!(cluster = %self.cluster_name, "Executing DuckDB HTTP query");
        let response = self.run_query(sql).await?;
        let batch = response_to_record_batch(response)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(None);
        let stream = Box::pin(stream::iter(vec![Ok(batch)]));
        Ok(SyncExecution {
            stream,
            stats: rx,
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

/// Convert a DuckDB HTTP NDJSON response into a single Arrow RecordBatch.
fn response_to_record_batch(response: HttpQueryResponse) -> Result<RecordBatch> {
    let n_cols = response.column_names.len();
    let n_rows = response.rows.len();

    if n_cols == 0 {
        let schema = Arc::new(Schema::empty());
        return RecordBatch::new_empty(schema)
            .pipe_ok()
            .map_err(|e| QueryFluxError::Engine(format!("Arrow error: {e}")));
    }

    let mut fields: Vec<Field> = Vec::with_capacity(n_cols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_cols);

    for (col_idx, col_name) in response.column_names.iter().enumerate() {
        let col_values: Vec<Option<&serde_json::Value>> =
            response.rows.iter().map(|row| row.get(col_idx)).collect();

        // Infer Arrow type from the first non-null value in the column.
        let arrow_type = infer_arrow_type(&col_values);
        fields.push(Field::new(col_name, arrow_type.clone(), true));

        let array = build_array(&arrow_type, &col_values, n_rows)?;
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| QueryFluxError::Engine(format!("Failed to build RecordBatch: {e}")))
}

/// Infer an Arrow DataType from the first non-null JSON value in a column.
fn infer_arrow_type(values: &[Option<&serde_json::Value>]) -> DataType {
    let Some(v) = values.iter().flatten().next() else {
        return DataType::Utf8;
    };
    match v {
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_f64() && n.as_i64().is_none() {
                DataType::Float64
            } else {
                DataType::Int64
            }
        }
        _ => DataType::Utf8,
    }
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
}
