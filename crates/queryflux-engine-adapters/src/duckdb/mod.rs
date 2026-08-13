pub mod http;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::Connection;
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    params::{QueryParam, QueryParams},
    query::{BackendQueryId, ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::QueryTags,
};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use crate::{AdapterKind, BackendQueryIdSlot, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Default per-query buffered-result cap (1 GiB), matching ClickHouse.
pub const DEFAULT_MAX_RESULT_BUFFER_BYTES: usize = 1 << 30;
const DEFAULT_DUCKDB_POOL_SIZE: usize = 1;

/// Parsed and validated configuration for a DuckDB cluster.
pub struct DuckDbConfig {
    pub database_path: Option<String>,
    pub motherduck_token: Option<String>,
    pub pool_size: usize,
    pub max_result_buffer_bytes: usize,
}

fn parse_pool_size(raw: Option<usize>, cluster_name: &str) -> Result<usize> {
    match raw {
        None => Ok(DEFAULT_DUCKDB_POOL_SIZE),
        Some(0) => Err(QueryFluxError::Engine(format!(
            "cluster '{cluster_name}': poolSize must be a positive integer"
        ))),
        Some(n) => Ok(n),
    }
}

pub fn parse_max_result_buffer_bytes(raw: Option<u64>, cluster_name: &str) -> Result<usize> {
    match raw {
        None => Ok(DEFAULT_MAX_RESULT_BUFFER_BYTES),
        Some(0) => Err(QueryFluxError::Engine(format!(
            "cluster '{cluster_name}': maxResultBufferBytes must be a positive integer"
        ))),
        Some(n) => usize::try_from(n).map_err(|_| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': maxResultBufferBytes is too large for this platform"
            ))
        }),
    }
}

pub fn parse_max_result_buffer_from_json(
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

fn parse_pool_size_from_json(json: &serde_json::Value, cluster_name: &str) -> Result<usize> {
    match json.get("poolSize") {
        None => Ok(DEFAULT_DUCKDB_POOL_SIZE),
        Some(v) => {
            let n = v.as_u64().filter(|&n| n >= 1).ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': poolSize must be a positive integer"
                ))
            })?;
            usize::try_from(n).map_err(|_| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': poolSize is too large for this platform"
                ))
            })
        }
    }
}

impl crate::EngineConfigParseable for DuckDbConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        use queryflux_core::engine_registry::{json_str, parse_auth_from_config_json};
        let database_path = json_str(json, "databasePath");
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': invalid auth ({e})"
            ))
        })?;
        let motherduck_token = match auth {
            None => None,
            Some(ClusterAuth::Bearer { token }) => Some(token),
            Some(_) => {
                return Err(QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': DuckDB supports only bearer auth (Motherduck token)"
                )));
            }
        };
        Ok(Self {
            database_path,
            motherduck_token,
            pool_size: parse_pool_size_from_json(json, cluster_name)?,
            max_result_buffer_bytes: parse_max_result_buffer_from_json(json, cluster_name)?,
        })
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        let motherduck_token = match cfg.auth.clone() {
            None => None,
            Some(ClusterAuth::Bearer { token }) => Some(token),
            Some(_) => {
                return Err(QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': DuckDB supports only bearer auth (Motherduck token)"
                )));
            }
        };
        Ok(Self {
            database_path: cfg.database_path.clone(),
            motherduck_token,
            pool_size: parse_pool_size(cfg.pool_size, cluster_name)?,
            max_result_buffer_bytes: parse_max_result_buffer_bytes(
                cfg.max_result_buffer_bytes,
                cluster_name,
            )?,
        })
    }
}

struct ConnectionSlot {
    conn: Arc<Mutex<Connection>>,
    interrupt: Arc<duckdb::InterruptHandle>,
    inflight: Arc<Mutex<Option<BackendQueryId>>>,
}

/// DuckDB embedded engine adapter.
///
/// DuckDB is Arrow-native and runs in-process. Queries stream batches over an
/// mpsc channel; a small connection pool allows limited concurrent queries.
pub struct DuckDbAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    slots: Arc<Vec<ConnectionSlot>>,
    checkout: Arc<Semaphore>,
    next_slot: AtomicUsize,
    max_result_buffer_bytes: usize,
}

fn open_duckdb_connection(config: &DuckDbConfig) -> Result<Connection> {
    let resolved_path = build_connection_string(
        config.database_path.clone(),
        config.motherduck_token.clone(),
    );
    match resolved_path.as_deref() {
        Some(path) => Connection::open(path),
        None => Connection::open_in_memory(),
    }
    .map_err(|e| QueryFluxError::Engine(format!("DuckDB open failed: {e}")))
}

impl DuckDbAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: DuckDbConfig,
    ) -> Result<Self> {
        let mut slots = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            let conn = open_duckdb_connection(&config)?;
            let interrupt = conn.interrupt_handle();
            slots.push(ConnectionSlot {
                conn: Arc::new(Mutex::new(conn)),
                interrupt,
                inflight: Arc::new(Mutex::new(None)),
            });
        }
        let permits = u32::try_from(config.pool_size).unwrap_or(u32::MAX);
        Ok(Self {
            cluster_name,
            group_name,
            slots: Arc::new(slots),
            checkout: Arc::new(Semaphore::new(permits as usize)),
            next_slot: AtomicUsize::new(0),
            max_result_buffer_bytes: config.max_result_buffer_bytes,
        })
    }

    fn pick_slot(&self) -> ConnectionSlot {
        let idx = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let slot = &self.slots[idx];
        ConnectionSlot {
            conn: Arc::clone(&slot.conn),
            interrupt: Arc::clone(&slot.interrupt),
            inflight: Arc::clone(&slot.inflight),
        }
    }
}

/// Build the DuckDB connection string.
///
/// For MotherDuck (`md:` prefix) with a token, appends `motherduck_token=<token>` as a
/// query parameter. Local file paths and in-memory (None) are returned unchanged.
fn build_connection_string(
    database_path: Option<String>,
    motherduck_token: Option<String>,
) -> Option<String> {
    match (database_path, motherduck_token) {
        (None, _) => None,
        (Some(path), None) => Some(path),
        (Some(path), Some(token)) if path.starts_with("md:") => {
            // Append token to the connection string.
            // md:dbname  →  md:dbname?motherduck_token=<token>
            // md:        →  md:?motherduck_token=<token>
            if path.contains('?') {
                Some(format!("{path}&motherduck_token={token}"))
            } else {
                Some(format!("{path}?motherduck_token={token}"))
            }
        }
        (Some(path), Some(_)) => Some(path), // token ignored for non-MotherDuck paths
    }
}

#[async_trait]
impl SyncAdapter for DuckDbAdapter {
    async fn health_check(&self) -> bool {
        let conn = Arc::clone(&self.slots[0].conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            guard.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    fn engine_type(&self) -> EngineType {
        EngineType::DuckDb
    }

    fn supports_native_params(&self) -> bool {
        true
    }

    async fn cancel_query(&self, backend_id: &BackendQueryId) -> Result<()> {
        for slot in self.slots.iter() {
            let matches = slot
                .inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                == Some(backend_id);
            if matches {
                slot.interrupt.interrupt();
                debug!(cluster = %self.cluster_name, query_id = %backend_id, "DuckDB interrupt issued");
                break;
            }
        }
        Ok(())
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
        params: &QueryParams,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        debug!(cluster = %self.cluster_name, "Executing DuckDB query as Arrow");
        let query_id = BackendQueryId(uuid::Uuid::new_v4().to_string());
        id_slot.publish(query_id.0.clone());

        let _permit = self
            .checkout
            .acquire()
            .await
            .map_err(|_| QueryFluxError::Engine("DuckDB connection pool closed".into()))?;
        let slot = self.pick_slot();
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(32);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let sql = sql.to_string();
        let duckdb_params: Vec<duckdb::types::Value> =
            params.iter().map(query_param_to_duckdb).collect();
        let id_for_task = query_id.clone();
        let max_bytes = self.max_result_buffer_bytes;

        tokio::task::spawn_blocking(move || {
            let conn = slot.conn;
            let inflight = slot.inflight;
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            *inflight.lock().unwrap_or_else(|e| e.into_inner()) = Some(id_for_task.clone());
            let stream_result = (|| {
                let mut stmt = guard
                    .prepare(&sql)
                    .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
                let arrow = stmt
                    .query_arrow(duckdb::params_from_iter(duckdb_params))
                    .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
                let mut buffered_bytes = 0usize;
                for batch in arrow {
                    buffered_bytes = buffered_bytes.saturating_add(batch.get_array_memory_size());
                    if buffered_bytes > max_bytes {
                        return Err(QueryFluxError::Engine(format!(
                            "DuckDB result exceeded the {max_bytes}-byte buffered-result cap; \
                             add a LIMIT, narrow the query, or raise maxResultBufferBytes"
                        )));
                    }
                    if batch_tx.blocking_send(Ok(batch)).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })();
            {
                let mut slot = inflight.lock().unwrap_or_else(|e| e.into_inner());
                if slot.as_ref() == Some(&id_for_task) {
                    *slot = None;
                }
            }
            if let Err(e) = stream_result {
                let _ = batch_tx.blocking_send(Err(e));
            }
            let _ = stats_tx.send(None);
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
        })
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let rows = self
            .run_show_query(
                "SELECT catalog_name FROM information_schema.schemata GROUP BY catalog_name",
            )
            .await?;
        if rows.is_empty() {
            Ok(vec!["memory".to_string()])
        } else {
            Ok(rows)
        }
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        self.run_show_query("SELECT schema_name FROM information_schema.schemata")
            .await
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}'"
        ))
        .await
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
        let conn = Arc::clone(&self.slots[0].conn);
        let rows: Vec<(String, String, bool)> = tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow([])
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            use duckdb::arrow::array::{Array, StringArray};
            let mut rows = Vec::new();
            for batch in arrow.collect::<Vec<_>>() {
                let names = batch.column(0).as_any().downcast_ref::<StringArray>();
                let types = batch.column(1).as_any().downcast_ref::<StringArray>();
                let nullables = batch.column(2).as_any().downcast_ref::<StringArray>();
                for i in 0..batch.num_rows() {
                    let name = names.and_then(|a| {
                        if !a.is_null(i) {
                            Some(a.value(i).to_string())
                        } else {
                            None
                        }
                    });
                    let data_type = types.and_then(|a| {
                        if !a.is_null(i) {
                            Some(a.value(i).to_uppercase())
                        } else {
                            None
                        }
                    });
                    let nullable = nullables
                        .map(|a| a.is_null(i) || a.value(i).to_uppercase() != "NO")
                        .unwrap_or(true);
                    if let (Some(name), Some(data_type)) = (name, data_type) {
                        rows.push((name, data_type, nullable));
                    }
                }
            }
            Ok::<_, QueryFluxError>(rows)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))??;

        if rows.is_empty() {
            return Ok(None);
        }
        let columns = rows
            .into_iter()
            .map(
                |(name, data_type, nullable)| queryflux_core::catalog::ColumnDef {
                    name,
                    data_type,
                    nullable,
                },
            )
            .collect();
        Ok(Some(TableSchema {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

impl DuckDbAdapter {
    /// Execute a batch of setup statements (INSTALL, LOAD, ATTACH, CREATE SECRET, etc.).
    ///
    /// Used by the test harness to prepare the Iceberg catalog extension before
    /// queries run. Runs on a blocking thread since DuckDB is synchronous.
    pub async fn setup_batch(&self, sql: &str) -> Result<()> {
        let conn = Arc::clone(&self.slots[0].conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .execute_batch(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB setup_batch failed: {e}")))
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))?
    }

    /// Run a query and collect the first column of each row as strings.
    /// Used internally for catalog discovery queries.
    async fn run_show_query(&self, sql: &str) -> Result<Vec<String>> {
        let conn = Arc::clone(&self.slots[0].conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow([])
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            let mut results = Vec::new();
            for batch in arrow.collect::<Vec<_>>() {
                if batch.num_columns() == 0 {
                    continue;
                }
                let col = batch.column(0);
                use duckdb::arrow::array::{Array, StringArray};
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            results.push(arr.value(i).to_string());
                        }
                    }
                } else {
                    use duckdb::arrow::util::display::array_value_to_string;
                    for i in 0..col.len() {
                        if !col.is_null(i) {
                            results
                                .push(array_value_to_string(col.as_ref(), i).unwrap_or_default());
                        }
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))?
    }
}

impl DuckDbAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "duckDb",
            display_name: "DuckDB",
            description: "Embedded in-process OLAP database. Use databasePath for a local file or 'md:' prefix for MotherDuck (cloud DuckDB).",
            hex: "FCC021",
            connection_type: ConnectionType::Embedded,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::Bearer],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "databasePath",
                    label: "Database path",
                    description: "Local DuckDB file path, 'md:' for MotherDuck default database, or 'md:mydb' for a named MotherDuck database. Omit for an in-memory database.",
                    field_type: FieldType::Path,
                    required: false,
                    example: Some("md:my_database"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Set to 'bearer' for MotherDuck (requires a MotherDuck token). Leave unset for local DuckDB.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("bearer"),
                },
                ConfigField {
                    key: "auth.token",
                    label: "MotherDuck token",
                    description: "MotherDuck access token. Required when databasePath starts with 'md:'.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "poolSize",
                    label: "Connection pool size",
                    description: "Number of embedded DuckDB connections for concurrent queries. Defaults to 1.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("1"),
                },
                ConfigField {
                    key: "maxResultBufferBytes",
                    label: "Max result buffer (bytes)",
                    description: "Per-query cap on buffered Arrow result bytes. Defaults to 1 GiB.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("1073741824"),
                },
            ],
        }
    }
}

/// Convert a [`QueryParam`] to a DuckDB native value.
fn query_param_to_duckdb(p: &QueryParam) -> duckdb::types::Value {
    use duckdb::types::Value;
    match p {
        QueryParam::Text(s) => Value::Text(s.clone()),
        QueryParam::Numeric(s) => {
            if let Ok(n) = s.parse::<i64>() {
                Value::BigInt(n)
            } else if let Ok(f) = s.parse::<f64>() {
                Value::Double(f)
            } else {
                // Genuinely non-numeric string — pass as text and let DuckDB error.
                Value::Text(s.clone())
            }
        }
        QueryParam::Boolean(b) => Value::Boolean(*b),
        QueryParam::Date(s) | QueryParam::Timestamp(s) | QueryParam::Time(s) => {
            Value::Text(s.clone())
        }
        QueryParam::Null => Value::Null,
    }
}

pub struct DuckDbFactory;

#[async_trait]
impl crate::EngineAdapterFactory for DuckDbFactory {
    fn engine_key(&self) -> &'static str {
        "duckDb"
    }

    fn descriptor(&self) -> EngineDescriptor {
        DuckDbAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<crate::AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = DuckDbConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(DuckDbAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::types::Value;
    use queryflux_core::params::QueryParam;

    #[test]
    fn text_maps_to_text() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Text("hello".into())),
            Value::Text("hello".into())
        );
    }

    #[test]
    fn integer_numeric_maps_to_bigint() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Numeric("42".into())),
            Value::BigInt(42)
        );
    }

    #[test]
    fn negative_integer_numeric_maps_to_bigint() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Numeric("-7".into())),
            Value::BigInt(-7)
        );
    }

    #[test]
    fn float_numeric_maps_to_double() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Numeric("2.5".into())),
            Value::Double(2.5)
        );
    }

    #[test]
    fn non_parseable_numeric_falls_back_to_text() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Numeric("not_a_number".into())),
            Value::Text("not_a_number".into())
        );
    }

    #[test]
    fn boolean_true_maps_correctly() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Boolean(true)),
            Value::Boolean(true)
        );
    }

    #[test]
    fn boolean_false_maps_correctly() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Boolean(false)),
            Value::Boolean(false)
        );
    }

    #[test]
    fn date_maps_to_text() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Date("2025-01-15".into())),
            Value::Text("2025-01-15".into())
        );
    }

    #[test]
    fn timestamp_maps_to_text() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Timestamp("2025-01-15 12:00:00".into())),
            Value::Text("2025-01-15 12:00:00".into())
        );
    }

    #[test]
    fn time_maps_to_text() {
        assert_eq!(
            query_param_to_duckdb(&QueryParam::Time("08:30:00".into())),
            Value::Text("08:30:00".into())
        );
    }

    #[test]
    fn null_maps_to_null() {
        assert_eq!(query_param_to_duckdb(&QueryParam::Null), Value::Null);
    }

    #[tokio::test]
    async fn interrupt_stops_a_long_query() {
        use futures::StreamExt;
        use queryflux_auth::QueryCredentials;
        use queryflux_core::query::{ClusterGroupName, ClusterName};

        let adapter = DuckDbAdapter::new(
            ClusterName("duck".into()),
            ClusterGroupName("g".into()),
            DuckDbConfig {
                database_path: None,
                motherduck_token: None,
                pool_size: 1,
                max_result_buffer_bytes: DEFAULT_MAX_RESULT_BUFFER_BYTES,
            },
        )
        .expect("open in-memory duckdb");

        let slot = BackendQueryIdSlot::new();
        let session = SessionContext::default();
        let creds = QueryCredentials::ServiceAccount;
        let tags = QueryTags::new();
        let params: QueryParams = vec![];

        let exec_fut = adapter.execute_as_arrow(
            "SELECT count(*) FROM range(1000000000)",
            &session,
            &creds,
            &tags,
            &params,
            &slot,
        );
        let cancel = async {
            for _ in 0..200 {
                if slot.get().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let id = slot.get().expect("backend id published");
            adapter.cancel_query(&id).await.expect("cancel");
        };

        let (exec_result, _) = tokio::join!(exec_fut, cancel);
        let mut exec = exec_result.expect("execute returns stream handle");
        let mut failed = false;
        while let Some(item) = exec.stream.next().await {
            if item.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed, "interrupted range() query stream should error");
    }

    #[tokio::test]
    async fn interrupt_targets_the_running_query() {
        use futures::StreamExt;
        use queryflux_auth::QueryCredentials;
        use queryflux_core::query::{ClusterGroupName, ClusterName};

        let adapter = std::sync::Arc::new(
            DuckDbAdapter::new(
                ClusterName("duck".into()),
                ClusterGroupName("g".into()),
                DuckDbConfig {
                    database_path: None,
                    motherduck_token: None,
                    pool_size: 1,
                    max_result_buffer_bytes: DEFAULT_MAX_RESULT_BUFFER_BYTES,
                },
            )
            .expect("open in-memory duckdb"),
        );

        let session = SessionContext::default();
        let creds = QueryCredentials::ServiceAccount;
        let tags = QueryTags::new();
        let params: QueryParams = vec![];

        let slot_a = BackendQueryIdSlot::new();
        let slot_b = BackendQueryIdSlot::new();

        let exec_a = {
            let adapter = Arc::clone(&adapter);
            let session = session.clone();
            let creds = creds.clone();
            let tags = tags.clone();
            let params = params.clone();
            let slot_a = slot_a.clone();
            async move {
                adapter
                    .execute_as_arrow(
                        "SELECT count(*) FROM range(1000000000)",
                        &session,
                        &creds,
                        &tags,
                        &params,
                        &slot_a,
                    )
                    .await
            }
        };
        let exec_b = {
            let adapter = Arc::clone(&adapter);
            let session = session.clone();
            let creds = creds.clone();
            let tags = tags.clone();
            let params = params.clone();
            let slot_b = slot_b.clone();
            async move {
                // Let A acquire the connection first.
                tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                adapter
                    .execute_as_arrow("SELECT 1", &session, &creds, &tags, &params, &slot_b)
                    .await
            }
        };
        let cancel_a = {
            let adapter = Arc::clone(&adapter);
            async move {
                for _ in 0..200 {
                    if slot_a.get().is_some() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let id = slot_a.get().expect("backend id published");
                adapter.cancel_query(&id).await.expect("cancel");
            }
        };

        let (result_a, result_b, _) = tokio::join!(exec_a, exec_b, cancel_a);
        let mut exec_a = result_a.expect("first query stream handle");
        let mut a_failed = false;
        while let Some(item) = exec_a.stream.next().await {
            if item.is_err() {
                a_failed = true;
                break;
            }
        }
        assert!(a_failed, "canceled first query stream should error");

        let mut exec_b = result_b.expect("second query stream handle");
        while let Some(item) = exec_b.stream.next().await {
            item.expect("second query batch");
        }
    }

    #[tokio::test]
    async fn execute_as_arrow_streams_multiple_batches() {
        use futures::StreamExt;
        use queryflux_auth::QueryCredentials;
        use queryflux_core::query::{ClusterGroupName, ClusterName};

        let adapter = DuckDbAdapter::new(
            ClusterName("duck".into()),
            ClusterGroupName("g".into()),
            DuckDbConfig {
                database_path: None,
                motherduck_token: None,
                pool_size: 1,
                max_result_buffer_bytes: DEFAULT_MAX_RESULT_BUFFER_BYTES,
            },
        )
        .expect("open in-memory duckdb");

        let params: QueryParams = vec![];

        let exec = adapter
            .execute_as_arrow(
                "SELECT * FROM range(2500)",
                &SessionContext::default(),
                &QueryCredentials::ServiceAccount,
                &QueryTags::new(),
                &params,
                &BackendQueryIdSlot::new(),
            )
            .await
            .expect("execute");

        let mut stream = exec.stream;
        let mut batches = 0usize;
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            batches += 1;
            rows += batch.num_rows();
        }
        assert!(
            batches >= 2,
            "expected multiple Arrow batches, got {batches}"
        );
        assert_eq!(rows, 2500);
    }

    #[tokio::test]
    async fn execute_as_arrow_enforces_result_buffer_cap() {
        use futures::StreamExt;
        use queryflux_auth::QueryCredentials;
        use queryflux_core::query::{ClusterGroupName, ClusterName};

        let adapter = DuckDbAdapter::new(
            ClusterName("duck".into()),
            ClusterGroupName("g".into()),
            DuckDbConfig {
                database_path: None,
                motherduck_token: None,
                pool_size: 1,
                max_result_buffer_bytes: 4096,
            },
        )
        .expect("open in-memory duckdb");

        let params: QueryParams = vec![];

        let exec = adapter
            .execute_as_arrow(
                "SELECT * FROM range(100000)",
                &SessionContext::default(),
                &QueryCredentials::ServiceAccount,
                &QueryTags::new(),
                &params,
                &BackendQueryIdSlot::new(),
            )
            .await
            .expect("execute");

        let mut stream = exec.stream;
        let mut saw_error = false;
        while let Some(item) = stream.next().await {
            if item.is_err() {
                saw_error = true;
                assert!(
                    item.unwrap_err()
                        .to_string()
                        .contains("buffered-result cap"),
                    "expected cap error"
                );
                break;
            }
        }
        assert!(saw_error, "expected buffered-result cap error");
    }
}
