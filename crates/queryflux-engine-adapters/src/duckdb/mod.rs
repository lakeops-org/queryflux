pub mod http;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::Connection;
use futures::stream;
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    params::{QueryParam, QueryParams},
    query::{ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::QueryTags,
};
use tracing::debug;

use crate::{AdapterKind, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Parsed and validated configuration for a DuckDB cluster.
pub struct DuckDbConfig {
    pub database_path: Option<String>,
    pub motherduck_token: Option<String>,
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
        })
    }
}

/// DuckDB embedded engine adapter.
///
/// DuckDB is Arrow-native and runs in-process. All query execution goes through
/// `execute_as_arrow` — `submit_query` is not used for this adapter.
pub struct DuckDbAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    /// Thread-safe DuckDB connection. Wrapped in Arc<Mutex> so it can be shared
    /// across `spawn_blocking` calls without moving out of `&self`.
    conn: Arc<Mutex<Connection>>,
}

impl DuckDbAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: DuckDbConfig,
    ) -> Result<Self> {
        let resolved_path = build_connection_string(config.database_path, config.motherduck_token);
        let conn = match resolved_path.as_deref() {
            Some(path) => Connection::open(path),
            None => Connection::open_in_memory(),
        }
        .map_err(|e| QueryFluxError::Engine(format!("DuckDB open failed: {e}")))?;

        Ok(Self {
            cluster_name,
            group_name,
            conn: Arc::new(Mutex::new(conn)),
        })
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
        let conn = Arc::clone(&self.conn);
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

    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
        params: &QueryParams,
    ) -> Result<SyncExecution> {
        debug!(cluster = %self.cluster_name, "Executing DuckDB query as Arrow");
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let duckdb_params: Vec<duckdb::types::Value> =
            params.iter().map(query_param_to_duckdb).collect();

        let batches = tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow(duckdb::params_from_iter(duckdb_params))
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            Ok::<_, QueryFluxError>(arrow.collect::<Vec<_>>())
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))??;

        let (tx, rx) = tokio::sync::oneshot::channel();
        // DuckDB does not expose structured engine stats (CPU time, bytes scanned, etc.)
        // via the query_arrow API — send None and establish the pattern for future use.
        let _ = tx.send(None);
        let stream = Box::pin(stream::iter(batches.into_iter().map(Ok)));
        Ok(SyncExecution { stream, stats: rx, affected_rows: None })
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
        let conn = Arc::clone(&self.conn);
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
        let conn = Arc::clone(&self.conn);
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
        let conn = Arc::clone(&self.conn);
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
}
