//! Native Snowflake adapter built on `snowflake-connector-rs` — no ADBC driver
//! shared library needed. Executes over Snowflake's REST protocol and converts
//! each result partition into one Arrow `RecordBatch`.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{NaiveDate, Timelike};
use dashmap::DashMap;
use queryflux_core::{
    catalog::{ColumnDef, TableSchema},
    config::{ClusterAuth, ClusterConfig},
    engine_registry::{AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType},
    error::{QueryFluxError, Result},
    query::{BackendQueryId, ClusterGroupName, ClusterName, EngineType},
};
use snowflake_connector_rs::{
    AuthConfig, CellValue, Client, ClientConfig, ColumnType, EndpointConfig, ErrorKind,
    KeyPairConfig, QueryCanceller, QueryConfig, ResultCursor, ResultTable, Session, SessionConfig,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use crate::adbc::snowflake::SnowflakeIntrospection;
use crate::adbc::sql_helpers;
use crate::{AdapterKind, BackendQueryIdSlot, SyncAdapter, SyncExecution};

/// Default client-side wait for a query response (`maxWaitSecs`).
pub const DEFAULT_SNOWFLAKE_MAX_WAIT_SECS: u64 = 600;

/// Parsed and validated configuration for a native Snowflake cluster.
pub struct SnowflakeConfig {
    pub account: String,
    pub auth: ClusterAuth,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    /// Overrides the account-derived `https://<account>.snowflakecomputing.com`
    /// (PrivateLink hosts, test servers).
    pub endpoint: Option<String>,
    pub max_wait_secs: u64,
}

impl SnowflakeConfig {
    #[allow(clippy::too_many_arguments)]
    fn validate(
        cluster_name: &str,
        account: Option<String>,
        auth: Option<ClusterAuth>,
        warehouse: Option<String>,
        role: Option<String>,
        database: Option<String>,
        schema: Option<String>,
        endpoint: Option<String>,
        max_wait_secs: Option<u64>,
    ) -> Result<Self> {
        let err = |m: &str| QueryFluxError::Engine(format!("cluster '{cluster_name}': {m}"));
        let account = account
            .filter(|a| !a.is_empty())
            .ok_or_else(|| err("missing 'account' for Snowflake"))?;
        let auth = match auth {
            Some(a @ ClusterAuth::Basic { .. }) | Some(a @ ClusterAuth::KeyPair { .. }) => a,
            Some(_) => return Err(err("Snowflake only supports basic or keyPair auth")),
            None => return Err(err("missing 'auth' for Snowflake (basic or keyPair)")),
        };
        let max_wait_secs = match max_wait_secs {
            None => DEFAULT_SNOWFLAKE_MAX_WAIT_SECS,
            Some(n) if n >= 1 => n,
            Some(_) => return Err(err("maxWaitSecs must be a positive integer")),
        };
        Ok(Self {
            account,
            auth,
            warehouse,
            role,
            database,
            schema,
            endpoint,
            max_wait_secs,
        })
    }
}

impl crate::EngineConfigParseable for SnowflakeConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> Result<Self> {
        use queryflux_core::engine_registry::{json_str, parse_auth_from_config_json};
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            QueryFluxError::Engine(format!("cluster '{cluster_name}': invalid auth ({e})"))
        })?;
        // Non-integer values map to 0 so validation rejects them.
        let max_wait_secs = json.get("maxWaitSecs").map(|v| v.as_u64().unwrap_or(0));
        Self::validate(
            cluster_name,
            json_str(json, "account"),
            auth,
            json_str(json, "warehouse"),
            json_str(json, "role"),
            json_str(json, "database"),
            json_str(json, "schema"),
            json_str(json, "endpoint"),
            max_wait_secs,
        )
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> Result<Self> {
        Self::validate(
            cluster_name,
            cfg.account.clone(),
            cfg.auth.clone(),
            cfg.warehouse.clone(),
            cfg.role.clone(),
            cfg.database.clone(),
            cfg.schema.clone(),
            cfg.endpoint.clone(),
            cfg.max_wait_secs,
        )
    }
}

fn sf_err(context: &str, e: snowflake_connector_rs::Error) -> QueryFluxError {
    let message = format!("Snowflake {context}: {e}");
    match e.kind() {
        ErrorKind::Timeout => QueryFluxError::backend_timeout(message),
        ErrorKind::Network => QueryFluxError::backend_failure(message),
        _ => QueryFluxError::Engine(message),
    }
}

/// Removes a query's canceller once its execution (including result streaming) ends.
struct CancellerGuard {
    cancellers: Arc<DashMap<String, QueryCanceller>>,
    request_id: String,
}

impl Drop for CancellerGuard {
    fn drop(&mut self) {
        self.cancellers.remove(&self.request_id);
    }
}

pub struct SnowflakeAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    client: Client,
    /// One logged-in session shared by all queries (Snowflake sessions accept
    /// concurrent statements). Re-created on `SessionExpired`.
    // ponytail: shared service-account session; per-user sessions when queryAuth lands.
    session: tokio::sync::Mutex<Option<Arc<Session>>>,
    /// In-flight queries by client request id, for `cancel_query`.
    cancellers: Arc<DashMap<String, QueryCanceller>>,
    warehouse: Option<String>,
    database: Option<String>,
}

impl SnowflakeAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: SnowflakeConfig,
    ) -> Result<Self> {
        let (username, auth) = match config.auth {
            ClusterAuth::Basic { username, password } => (username, AuthConfig::password(password)),
            ClusterAuth::KeyPair {
                username,
                private_key_pem,
                private_key_passphrase,
            } => {
                let kp = match private_key_passphrase {
                    Some(p) => KeyPairConfig::from_encrypted_pem(private_key_pem, p.into_bytes()),
                    None => KeyPairConfig::from_pem(private_key_pem),
                };
                (username, AuthConfig::key_pair(kp))
            }
            _ => unreachable!("validated in SnowflakeConfig"),
        };

        let mut session_cfg = SessionConfig::new();
        if let Some(v) = &config.warehouse {
            session_cfg = session_cfg.with_warehouse(v);
        }
        if let Some(v) = &config.role {
            session_cfg = session_cfg.with_role(v);
        }
        if let Some(v) = &config.database {
            session_cfg = session_cfg.with_database(v);
        }
        if let Some(v) = &config.schema {
            session_cfg = session_cfg.with_schema(v);
        }

        let mut client_cfg = ClientConfig::new(username, &config.account, auth)
            .with_session(session_cfg)
            .with_query(
                QueryConfig::new()
                    .with_query_response_timeout(Duration::from_secs(config.max_wait_secs)),
            );
        if let Some(endpoint) = &config.endpoint {
            let url = url::Url::parse(endpoint).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': invalid Snowflake endpoint '{endpoint}': {e}",
                    cluster_name.0
                ))
            })?;
            client_cfg = client_cfg.with_endpoint(EndpointConfig::custom_base_url(url));
        }
        let client = Client::new(client_cfg).map_err(|e| sf_err("client config", e))?;

        Ok(Self {
            cluster_name,
            group_name,
            client,
            session: tokio::sync::Mutex::new(None),
            cancellers: Arc::new(DashMap::new()),
            warehouse: config.warehouse,
            database: config.database,
        })
    }

    async fn session(&self) -> Result<Arc<Session>> {
        let mut guard = self.session.lock().await;
        if let Some(s) = guard.as_ref() {
            return Ok(Arc::clone(s));
        }
        let s = Arc::new(
            self.client
                .create_session()
                .await
                .map_err(|e| sf_err("login", e))?,
        );
        *guard = Some(Arc::clone(&s));
        Ok(s)
    }

    /// Submit `sql`, publishing the request id to `id_slot` before the wait.
    /// Retries once with a fresh session if the shared one expired.
    async fn start(
        &self,
        sql: &str,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<(ResultCursor, CancellerGuard)> {
        let mut retried = false;
        loop {
            let session = self.session().await?;
            let handle = session
                .query_handle(sql.to_string())
                .map_err(|e| sf_err("query", e))?;
            let canceller = handle.canceller();
            let request_id = canceller.request_id().to_string();
            self.cancellers
                .insert(request_id.clone(), canceller.clone());
            let guard = CancellerGuard {
                cancellers: Arc::clone(&self.cancellers),
                request_id: request_id.clone(),
            };
            id_slot.publish(request_id);

            match handle.execute().await {
                Ok(cursor) => return Ok((cursor, guard)),
                Err(e) if e.is_session_expired() && !retried => {
                    retried = true;
                    self.session.lock().await.take();
                }
                Err(e) => {
                    // The response timeout is client-side only — stop the server-side query too.
                    if e.is_timeout() {
                        if let Err(ce) = canceller.cancel().await {
                            warn!(cluster = %self.cluster_name, error = %ce, "Snowflake cancel after timeout failed");
                        }
                    }
                    return Err(sf_err("query", e));
                }
            }
        }
    }

    /// Run a statement to completion and return all rows as Arrow batches.
    async fn query_batches(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let (cursor, _guard) = self.start(sql, &BackendQueryIdSlot::new()).await?;
        let schema = arrow_schema(cursor.schema());
        let table = cursor
            .collect_table()
            .await
            .map_err(|e| sf_err("fetch", e))?;
        Ok(vec![table_to_batch(&schema, &table)?])
    }

    async fn names(&self, sql: &str) -> Result<Vec<String>> {
        let batches = self.query_batches(sql).await?;
        Ok(batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).filter_map(|r| sql_helpers::cell_str(b, "name", r)))
            .collect())
    }

    fn catalog_or_default<'a>(&'a self, catalog: &'a str) -> Result<&'a str> {
        if !catalog.is_empty() {
            return Ok(catalog);
        }
        self.database.as_deref().ok_or_else(|| {
            QueryFluxError::Engine(
                "Snowflake: no database given and no default 'database' configured".to_string(),
            )
        })
    }

    pub fn descriptor() -> EngineDescriptor {
        let text = |key, label, description, required, example| ConfigField {
            key,
            label,
            description,
            field_type: FieldType::Text,
            required,
            example,
        };
        EngineDescriptor {
            engine_key: "snowflake",
            display_name: "Snowflake",
            description: "Snowflake via the native Rust connector (no ADBC driver needed).",
            hex: "29B5E8",
            connection_type: ConnectionType::ManagedApi,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::Basic, AuthType::KeyPair],
            implemented: true,
            config_fields: vec![
                text(
                    "account",
                    "Account",
                    "Snowflake account identifier (orgname-accountname).",
                    true,
                    Some("myorg-myaccount"),
                ),
                text(
                    "warehouse",
                    "Warehouse",
                    "Default warehouse. Also used for health checks and running-query counts.",
                    false,
                    Some("COMPUTE_WH"),
                ),
                text("role", "Role", "Default role.", false, Some("ANALYST")),
                text("database", "Database", "Default database.", false, Some("ANALYTICS")),
                text("schema", "Schema", "Default schema.", false, Some("PUBLIC")),
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint override",
                    description: "Base URL override (e.g. PrivateLink). Defaults to https://<account>.snowflakecomputing.com.",
                    field_type: FieldType::Url,
                    required: false,
                    example: Some("https://myorg-myaccount.privatelink.snowflakecomputing.com"),
                },
                ConfigField {
                    key: "maxWaitSecs",
                    label: "Max wait (seconds)",
                    description: "Max seconds to wait for a query response before cancelling it. Defaults to 600.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("600"),
                },
            ],
        }
    }
}

#[async_trait]
impl SyncAdapter for SnowflakeAdapter {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &queryflux_core::session::SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &queryflux_core::tags::QueryTags,
        _params: &queryflux_core::params::QueryParams,
        _hints: queryflux_core::sql_classify::ExecutionHints,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        let (mut cursor, guard) = self.start(sql, id_slot).await?;
        let schema = arrow_schema(cursor.schema());

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(8);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _guard = guard;
            let mut sent_any = false;
            loop {
                let item = match cursor.next_table().await {
                    Ok(Some(table)) => table_to_batch(&schema, &table),
                    Ok(None) => break,
                    Err(e) => Err(sf_err("fetch", e)),
                };
                let failed = item.is_err();
                sent_any = true;
                if batch_tx.send(item).await.is_err() || failed {
                    break;
                }
            }
            if !sent_any {
                let _ = batch_tx.send(Ok(RecordBatch::new_empty(schema))).await;
            }
            let _ = stats_tx.send(None);
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
            affected_rows: None,
        })
    }

    fn engine_type(&self) -> EngineType {
        EngineType::Snowflake
    }

    async fn cancel_query(&self, backend_id: &BackendQueryId) -> Result<()> {
        let Some(canceller) = self.cancellers.get(&backend_id.0).map(|c| c.clone()) else {
            return Ok(()); // already finished
        };
        canceller
            .cancel()
            .await
            .map(|_| ())
            .map_err(|e| sf_err("cancel", e))
    }

    async fn health_check(&self) -> bool {
        let result = match &self.warehouse {
            // SHOW WAREHOUSES runs in cloud services — it never resumes the warehouse.
            Some(wh) => self
                .query_batches(&SnowflakeIntrospection::show_warehouses_sql(wh))
                .await
                .map(|b| SnowflakeIntrospection::parse_show_warehouses_health(&b, wh)),
            None => self.query_batches("SELECT 1").await.map(|_| true),
        };
        result.unwrap_or_else(|e| {
            warn!(cluster = %self.cluster_name, error = %e, "Snowflake health check failed");
            false
        })
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        let wh = self.warehouse.as_deref()?;
        let batches = self
            .query_batches(&SnowflakeIntrospection::show_warehouses_sql(wh))
            .await
            .ok()?;
        SnowflakeIntrospection::parse_show_warehouses_running(&batches, wh)
    }

    async fn execute_custom_health_check(&self, sql: &str) -> bool {
        self.query_batches(sql).await.is_ok()
    }

    async fn execute_custom_reconcile_query(&self, sql: &str) -> Option<u64> {
        let batches = self.query_batches(sql).await.ok()?;
        sql_helpers::first_cell_u64(&batches)
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        self.names("SHOW DATABASES").await
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let catalog = self.catalog_or_default(catalog)?;
        self.names(&format!(
            "SHOW SCHEMAS IN DATABASE {}",
            quote_ident(catalog)
        ))
        .await
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let catalog = self.catalog_or_default(catalog)?;
        // SHOW OBJECTS covers tables and views.
        self.names(&format!(
            "SHOW OBJECTS IN SCHEMA {}.{}",
            quote_ident(catalog),
            quote_ident(database)
        ))
        .await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let catalog = self.catalog_or_default(catalog)?;
        let sql = format!(
            "DESCRIBE TABLE {}.{}.{}",
            quote_ident(catalog),
            quote_ident(database),
            quote_ident(table)
        );
        let Ok(batches) = self.query_batches(&sql).await else {
            return Ok(None);
        };
        let columns = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).filter_map(move |r| {
                    Some(ColumnDef {
                        name: sql_helpers::cell_str(b, "name", r)?,
                        data_type: sql_helpers::cell_str(b, "type", r)?,
                        nullable: sql_helpers::cell_str(b, "null?", r).as_deref() != Some("N"),
                    })
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

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

fn arrow_type(ty: &ColumnType) -> DataType {
    match ty {
        ColumnType::Fixed {
            precision, scale, ..
        } => match scale {
            Some(s) if *s > 0 => DataType::Decimal128(precision.unwrap_or(38), *s as i8),
            _ => DataType::Int64,
        },
        ColumnType::Real => DataType::Float64,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Date => DataType::Date32,
        ColumnType::Time { .. } => DataType::Time64(TimeUnit::Microsecond),
        // Microseconds, not nanoseconds: Snowflake allows years far outside ns range.
        ColumnType::TimestampNtz { .. } => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::TimestampLtz { .. } | ColumnType::TimestampTz { .. } => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        ColumnType::Binary { .. } => DataType::Binary,
        // Text, VARIANT/OBJECT/ARRAY (JSON text), GEOGRAPHY, VECTOR, unknown.
        _ => DataType::Utf8,
    }
}

fn arrow_schema(schema: &snowflake_connector_rs::Schema) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(
        schema
            .columns()
            .iter()
            .map(|c| Field::new(c.name(), arrow_type(c.ty()), true))
            .collect::<Vec<_>>(),
    ))
}

fn table_to_batch(schema: &Arc<ArrowSchema>, table: &ResultTable) -> Result<RecordBatch> {
    let ncols = schema.fields().len();
    let mut cols: Vec<Vec<CellValue>> = (0..ncols)
        .map(|_| Vec::with_capacity(table.row_count()))
        .collect();
    for row in table.dynamic_rows().map_err(|e| sf_err("decode", e))? {
        let row = row.map_err(|e| sf_err("decode", e))?;
        for (i, v) in row.into_parts().1.into_vec().into_iter().enumerate() {
            cols[i].push(v);
        }
    }
    let arrays = schema
        .fields()
        .iter()
        .zip(cols)
        .map(|(f, cells)| build_column(f.data_type(), cells))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| QueryFluxError::Engine(format!("Snowflake RecordBatch failed: {e}")))
}

fn unexpected(dt: &DataType, v: &CellValue) -> QueryFluxError {
    QueryFluxError::Engine(format!("Snowflake: unexpected value {v:?} for {dt} column"))
}

fn build_column(dt: &DataType, cells: Vec<CellValue>) -> Result<ArrayRef> {
    let n = cells.len();
    macro_rules! typed {
        ($builder:expr, $($pat:pat => $val:expr),+) => {{
            let mut b = $builder;
            for v in &cells {
                match v {
                    CellValue::Null => b.append_null(),
                    $($pat => b.append_value($val),)+
                    other => return Err(unexpected(dt, other)),
                }
            }
            Result::<ArrayRef>::Ok(Arc::new(b.finish()))
        }};
    }
    match dt {
        DataType::Int64 => typed!(Int64Builder::with_capacity(n),
            CellValue::Integer(i) => i64::try_from(*i).map_err(|_| {
                QueryFluxError::Engine(format!("Snowflake: integer {i} overflows Int64"))
            })?),
        DataType::Float64 => typed!(Float64Builder::with_capacity(n), CellValue::Float(f) => *f),
        DataType::Boolean => typed!(BooleanBuilder::with_capacity(n), CellValue::Boolean(b) => *b),
        DataType::Date32 => typed!(Date32Builder::with_capacity(n),
            CellValue::Date(d) => (*d - NaiveDate::default()).num_days() as i32),
        DataType::Time64(_) => typed!(Time64MicrosecondBuilder::with_capacity(n),
            CellValue::Time(t) => t.num_seconds_from_midnight() as i64 * 1_000_000
                + (t.nanosecond() / 1_000) as i64),
        DataType::Timestamp(_, None) => typed!(TimestampMicrosecondBuilder::with_capacity(n),
            CellValue::TimestampNtz(t) => t.and_utc().timestamp_micros()),
        DataType::Timestamp(_, Some(tz)) => {
            let arr: ArrayRef = typed!(TimestampMicrosecondBuilder::with_capacity(n),
                CellValue::TimestampLtz(t) => t.timestamp_micros(),
                CellValue::TimestampTz(t) => t.timestamp_micros())?;
            let ts = arr
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .expect("built above")
                .clone()
                .with_timezone(tz.clone());
            Ok(Arc::new(ts))
        }
        DataType::Binary => typed!(BinaryBuilder::with_capacity(n, n * 16),
            CellValue::Binary(b) => b.as_bytes()),
        DataType::Decimal128(p, s) => {
            // Decimals arrive as exact text; Arrow's cast parses them losslessly.
            let text: ArrayRef = typed!(StringBuilder::with_capacity(n, n * 16),
                CellValue::Decimal(d) => d.raw(),
                CellValue::Integer(i) => i.to_string())?;
            arrow::compute::cast(&text, &DataType::Decimal128(*p, *s))
                .map_err(|e| QueryFluxError::Engine(format!("Snowflake decimal cast: {e}")))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for v in cells {
                match v {
                    CellValue::Null => b.append_null(),
                    CellValue::String(s) => b.append_value(s),
                    CellValue::Json(j) => b.append_value(j.to_string()),
                    other => b.append_value(format!("{other:?}")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

pub struct SnowflakeFactory;

#[async_trait]
impl crate::EngineAdapterFactory for SnowflakeFactory {
    fn engine_key(&self) -> &'static str {
        "snowflake"
    }

    fn descriptor(&self) -> EngineDescriptor {
        SnowflakeAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind> {
        use crate::EngineConfigParseable;
        let config = SnowflakeConfig::from_json(json, &cluster_name.0)?;
        Ok(AdapterKind::Sync(Arc::new(SnowflakeAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests;
