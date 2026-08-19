use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use mysql_async::{
    consts::ColumnType, prelude::Queryable, Conn, Opts, OptsBuilder, Pool, PoolConstraints,
    PoolOpts, Row, Value,
};
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    query::{BackendQueryId, ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::{tags_to_json, QueryTags},
};
use tokio_stream::wrappers::ReceiverStream;

use crate::{AdapterKind, BackendQueryIdSlot, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Default MySQL connection pool size for StarRocks when [`ClusterConfig::pool_size`] / JSON `poolSize` is omitted.
/// Independent of `max_running_queries`: not all load goes through QueryFlux.
const DEFAULT_STARROCKS_POOL_SIZE: usize = 8;

fn parse_pool_size_from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<usize> {
    match json.get("poolSize") {
        None => Ok(DEFAULT_STARROCKS_POOL_SIZE),
        Some(v) => {
            let n = parse_positive_json_integer(v).ok_or_else(|| {
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

fn parse_positive_json_integer(v: &serde_json::Value) -> Option<u64> {
    if let Some(u) = v.as_u64() {
        return (u >= 1).then_some(u);
    }
    if let Some(i) = v.as_i64() {
        return (i >= 1).then_some(i as u64);
    }
    let f = v.as_f64()?;
    (f.fract() == 0.0 && f >= 1.0).then_some(f as u64)
}

/// Parsed and validated configuration for a StarRocks cluster.
pub struct StarRocksConfig {
    pub endpoint: String,
    pub auth: Option<ClusterAuth>,
    pub pool_size: usize,
    /// `passthrough` (LDAP `COM_CHANGE_USER`) or `None`/`serviceAccount` — `impersonate`/
    /// `tokenExchange` are already rejected by `query_auth_supported` before construction.
    pub query_auth: Option<queryflux_core::config::QueryAuthConfig>,
}

impl crate::EngineConfigParseable for StarRocksConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        use queryflux_core::engine_registry::{
            json_str, parse_auth_from_config_json, parse_query_auth_from_config_json,
        };
        let endpoint = json_str(json, "endpoint").ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing endpoint"
            ))
        })?;
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': invalid auth ({e})"
            ))
        })?;
        if let Some(ref a) = auth {
            if !matches!(a, ClusterAuth::Basic { .. }) {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': StarRocks only supports basic auth (MySQL username/password)"
                )));
            }
        }
        let query_auth = parse_query_auth_from_config_json(json).map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': invalid queryAuth ({e})"
            ))
        })?;
        let pool_size = parse_pool_size_from_json(json, cluster_name)?;
        Ok(Self {
            endpoint,
            auth,
            pool_size,
            query_auth,
        })
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        let endpoint = cfg.endpoint.clone().ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing endpoint"
            ))
        })?;
        if let Some(ref a) = cfg.auth {
            if !matches!(a, ClusterAuth::Basic { .. }) {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': StarRocks only supports basic auth (MySQL username/password)"
                )));
            }
        }
        Ok(Self {
            endpoint,
            auth: cfg.auth.clone(),
            pool_size: cfg.pool_size.unwrap_or(DEFAULT_STARROCKS_POOL_SIZE).max(1),
            query_auth: cfg.query_auth.clone(),
        })
    }
}

/// StarRocks adapter — connects over the MySQL wire protocol to a StarRocks FE node.
///
/// StarRocks is synchronous: `submit_query` executes the full query and returns
/// `QueryExecution::Sync` with the complete result. `poll_query` is never called.
///
/// The `endpoint` must be a MySQL connection URL, e.g.:
///   `mysql://root:password@sr-fe-host:9030`
///
/// Alternatively, omit credentials from the URL and supply them via `auth: basic`.
///
/// The query pool uses lazy connections: `mysql_async` opens connections on demand up to
/// the configured max; setting min == max only caps the upper bound, not eager opens.
pub struct StarRocksAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pool: Pool,
    /// Dedicated 1×1 pool for `health_check` so probes do not compete with query traffic.
    control_pool: Pool,
    /// Connection ids that currently own an in-flight query. `cancel_query`
    /// skips `KILL QUERY` once the id is released (connection back in the pool).
    inflight: crate::mysql_native::InflightConnIds,
}

impl StarRocksAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: StarRocksConfig,
    ) -> Result<Self> {
        // `passthrough` re-authenticates connections mid-flight via COM_CHANGE_USER using a
        // real StarRocks/LDAP password (see `mysql_native::apply_passthrough_identity`).
        // That requires the `mysql_clear_password` plugin (StarRocks LDAP auth negotiates
        // it), which sends the password with no hashing or encryption at the MySQL
        // protocol layer — safe only over TLS. `enable_cleartext_plugin` is therefore
        // gated on the endpoint already requesting TLS (`?require_ssl=true`), checked once
        // below against the built `Opts`, not assumed.
        let requires_passthrough = matches!(
            config.query_auth,
            Some(queryflux_core::config::QueryAuthConfig::Passthrough)
        );

        let make_builder = || -> Result<OptsBuilder> {
            // Always disable Unix socket preference — StarRocks doesn't support the
            // `@@socket` system variable that mysql_async queries when prefer_socket=true.
            let base_opts = Opts::from_url(&config.endpoint).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': StarRocks invalid endpoint URL: {e}",
                    cluster_name.0
                ))
            })?;
            let mut b = OptsBuilder::from_opts(base_opts).prefer_socket(false);
            if let Some(ClusterAuth::Basic { username, password }) = &config.auth {
                b = b.user(Some(username.clone())).pass(Some(password.clone()));
            }
            if requires_passthrough {
                b = b.enable_cleartext_plugin(true);
            }
            Ok(b)
        };

        let pool_opts_for = |size: usize| -> Result<PoolOpts> {
            let size = size.max(1);
            let constraints = PoolConstraints::new(size, size).ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': invalid StarRocks pool size",
                    cluster_name.0
                ))
            })?;
            Ok(PoolOpts::default()
                .with_constraints(constraints)
                .with_abs_conn_ttl(Some(Duration::from_secs(1800))))
        };

        let main_builder = make_builder()?;
        let main_opts = Opts::from(
            main_builder
                .clone()
                .pool_opts(pool_opts_for(config.pool_size)?),
        );
        if requires_passthrough && main_opts.ssl_opts().is_none() {
            return Err(QueryFluxError::Engine(format!(
                "cluster '{}': queryAuth: passthrough requires TLS on the StarRocks endpoint \
                 (add ?require_ssl=true to the connection URL) — LDAP passthrough sends the \
                 password in cleartext at the MySQL protocol layer and must not run \
                 unencrypted",
                cluster_name.0
            )));
        }
        let pool = Pool::new(main_opts);

        let control_opts = Opts::from(make_builder()?.pool_opts(pool_opts_for(1)?));
        let control_pool = Pool::new(control_opts);

        Ok(Self {
            cluster_name,
            group_name,
            pool,
            control_pool,
            inflight: crate::mysql_native::InflightConnIds::default(),
        })
    }

    /// Execute a DDL/setup statement that returns no rows (CREATE EXTERNAL CATALOG, etc.).
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let mut conn = self.acquire_conn().await?;
        conn.query_drop(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks DDL failed: {e}")))
    }

    /// Check out a connection from the pool, with a 10-second timeout.
    async fn acquire_conn(&self) -> Result<Conn> {
        tokio::time::timeout(Duration::from_secs(10), self.pool.get_conn())
            .await
            .map_err(|_| {
                QueryFluxError::Engine("StarRocks pool checkout timed out (10s)".to_string())
            })?
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks pool get_conn failed: {e}")))
    }

    async fn acquire_control_conn(&self) -> Result<Conn> {
        tokio::time::timeout(Duration::from_secs(10), self.control_pool.get_conn())
            .await
            .map_err(|_| {
                QueryFluxError::Engine(
                    "StarRocks control pool checkout timed out (10s)".to_string(),
                )
            })?
            .map_err(|e| {
                QueryFluxError::Engine(format!("StarRocks control pool get_conn failed: {e}"))
            })
    }

    async fn run_query(&self, sql: &str) -> Result<Vec<Row>> {
        let mut conn = self.acquire_conn().await?;
        conn.query::<Row, _>(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks query failed: {e}")))
    }

    /// Run a query and return the first column of each row as strings.
    async fn run_first_col(&self, sql: &str) -> Result<Vec<String>> {
        let rows = self.run_query(sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut row| {
                row.take::<String, usize>(0)
                    .or_else(|| row.take::<i64, usize>(0).map(|i| i.to_string()))
                    .or_else(|| row.take::<u64, usize>(0).map(|u| u.to_string()))
            })
            .collect())
    }

    async fn stream_arrow_on_conn(
        conn: &mut Conn,
        sql: &str,
        session: &SessionContext,
        tags: &QueryTags,
        params: &queryflux_core::params::QueryParams,
        batch_tx: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        if let Some(db) = session.database() {
            let use_sql = format!("USE `{}`", db.replace('`', "``"));
            conn.query_drop(&use_sql)
                .await
                .map_err(|e| QueryFluxError::Engine(format!("StarRocks USE failed: {e}")))?;
        }

        if !tags.is_empty() {
            let tag_json = tags_to_json(tags).to_string();
            let escaped = Value::from(tag_json).as_sql(false);
            let set_sql = format!("SET @query_tag = {escaped}");
            conn.query_drop(&set_sql).await.map_err(|e| {
                QueryFluxError::Engine(format!("StarRocks SET @query_tag failed: {e}"))
            })?;
        }

        if params.is_empty() {
            // Text protocol — required for SHOW/DDL that StarRocks rejects via prepared statements.
            let query_result = conn
                .query_iter(sql)
                .await
                .map_err(|e| QueryFluxError::Engine(format!("StarRocks query failed: {e}")))?;
            Self::stream_arrow_from_query_result(query_result, batch_tx).await
        } else {
            let mysql_params = mysql_async::Params::Positional(
                params.iter().map(query_param_to_mysql_value).collect(),
            );
            let query_result = conn
                .exec_iter(sql, mysql_params)
                .await
                .map_err(|e| QueryFluxError::Engine(format!("StarRocks exec failed: {e}")))?;
            Self::stream_arrow_from_query_result(query_result, batch_tx).await
        }
    }

    async fn stream_arrow_from_query_result<P: mysql_async::prelude::Protocol>(
        mut query_result: mysql_async::QueryResult<'_, '_, P>,
        batch_tx: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        const BATCH_SIZE: usize = 1_000;

        let fields: Vec<Field> = query_result
            .columns_ref()
            .iter()
            .map(|c| {
                Field::new(
                    c.name_str().to_string(),
                    mysql_column_type_to_arrow(c.column_type()),
                    true,
                )
            })
            .collect();

        if fields.is_empty() {
            return Ok(());
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        let num_cols = schema.fields().len();
        let mut batch_rows: Vec<Row> = Vec::with_capacity(BATCH_SIZE);
        let mut sent_any = false;

        while let Some(row) = query_result
            .next()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks read row failed: {e}")))?
        {
            batch_rows.push(row);
            if batch_rows.len() >= BATCH_SIZE {
                let batch = starrocks_rows_to_batch(&schema, &mut batch_rows, num_cols)?;
                batch_rows.clear();
                sent_any = true;
                if batch_tx.send(Ok(batch)).await.is_err() {
                    query_result.drop_result().await.ok();
                    return Ok(());
                }
            }
        }

        if !batch_rows.is_empty() {
            let batch = starrocks_rows_to_batch(&schema, &mut batch_rows, num_cols)?;
            let _ = batch_tx.send(Ok(batch)).await;
        } else if !sent_any {
            let batch = RecordBatch::new_empty(Arc::clone(&schema));
            let _ = batch_tx.send(Ok(batch)).await;
        }

        query_result
            .drop_result()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks drop_result failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl SyncAdapter for StarRocksAdapter {
    async fn health_check(&self) -> bool {
        use mysql_async::prelude::Queryable;
        match self.acquire_control_conn().await {
            Ok(mut conn) => match conn.ping().await {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(
                        cluster = %self.cluster_name,
                        error = %e,
                        "StarRocks health check ping failed"
                    );
                    false
                }
            },
            Err(e) => {
                tracing::warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "StarRocks health check: pool checkout failed"
                );
                false
            }
        }
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        // Query the FE's processlist for all actively executing queries — not just the ones
        // routed through QueryFlux. This gives a true picture of StarRocks load and prevents
        // the reconciler from overcorrecting when the engine is busy with external traffic.
        let rows = self
            .run_query(
                "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND = 'Query'",
            )
            .await
            .ok()?;
        rows.into_iter()
            .next()
            .and_then(|mut row| row.take::<u64, usize>(0))
    }

    fn engine_type(&self) -> EngineType {
        EngineType::StarRocks
    }

    fn connection_format(&self) -> crate::ConnectionFormat {
        crate::ConnectionFormat::MysqlWire
    }

    fn supports_native_params(&self) -> bool {
        true
    }

    async fn execute_native(
        &self,
        _protocol: &queryflux_core::query::FrontendProtocol,
        sql: &str,
        session: &SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
        params: &queryflux_core::params::QueryParams,
        id_slot: &BackendQueryIdSlot,
    ) -> crate::Result<crate::NativeExecution> {
        crate::mysql_native::execute(
            &self.pool,
            sql,
            session,
            credentials,
            tags,
            params,
            id_slot,
            &self.inflight,
        )
        .await
    }

    async fn cancel_query(&self, backend_id: &BackendQueryId) -> Result<()> {
        let Some(sql) = kill_query_sql(&backend_id.0) else {
            tracing::debug!(
                cluster = %self.cluster_name,
                query_id = %backend_id,
                "StarRocks cancel skipped: connection id is not a safe integer"
            );
            return Ok(());
        };
        if !self.inflight.contains_published(&backend_id.0) {
            tracing::debug!(
                cluster = %self.cluster_name,
                query_id = %backend_id,
                "StarRocks cancel skipped: connection no longer owns an in-flight query"
            );
            return Ok(());
        }
        let mut conn = match self.acquire_control_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(
                    cluster = %self.cluster_name,
                    query_id = %backend_id,
                    error = %e,
                    "StarRocks KILL QUERY: control pool checkout failed (ignored)"
                );
                return Ok(());
            }
        };
        match conn.query_drop(&sql).await {
            Ok(()) => {
                tracing::debug!(
                    cluster = %self.cluster_name,
                    query_id = %backend_id,
                    "StarRocks KILL QUERY issued"
                );
            }
            Err(e) => {
                tracing::debug!(
                    cluster = %self.cluster_name,
                    query_id = %backend_id,
                    error = %e,
                    "StarRocks KILL QUERY failed (ignored)"
                );
            }
        }
        self.inflight.release_published(&backend_id.0);
        Ok(())
    }

    // `credentials` is `ServiceAccount` or `Passthrough` here — `query_auth_supported`
    // rejects `impersonate`/`tokenExchange` for StarRocks at startup (StarRocks' JWT MySQL
    // auth plugin is a separate, confirmed-infeasible path for `mysql_async` — see
    // auth-authz-design.md "StarRocks"). `Passthrough` here means LDAP `COM_CHANGE_USER`,
    // applied by `apply_passthrough_identity` below.
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
        params: &queryflux_core::params::QueryParams,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        let mut conn = self.acquire_conn().await?;
        crate::mysql_native::apply_passthrough_identity(&mut conn, credentials, session).await?;
        let conn_id = conn.id();
        id_slot.publish(conn_id.to_string());

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(32);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let inflight = self.inflight.clone();
        let sql = sql.to_string();
        let session = session.clone();
        let tags = tags.clone();
        let params = params.to_vec();

        tokio::spawn(async move {
            let mut claim = inflight.claim(conn_id);
            if let Err(e) = StarRocksAdapter::stream_arrow_on_conn(
                &mut conn, &sql, &session, &tags, &params, &batch_tx,
            )
            .await
            {
                let _ = batch_tx.send(Err(e)).await;
            }
            claim.finish();
            drop(conn);
            let _ = stats_tx.send(None);
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
        })
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        // SHOW CATALOGS available in StarRocks 3.0+. Fall back gracefully.
        match self.run_first_col("SHOW CATALOGS").await {
            Ok(catalogs) if !catalogs.is_empty() => Ok(catalogs),
            _ => Ok(vec!["default_catalog".to_string()]),
        }
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let sql = if catalog.is_empty() || catalog == "default_catalog" {
            "SHOW DATABASES".to_string()
        } else {
            format!("SHOW DATABASES FROM `{}`", catalog.replace('`', "``"))
        };
        self.run_first_col(&sql).await
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let sql = format!("SHOW TABLES FROM `{}`", database.replace('`', "``"));
        self.run_first_col(&sql).await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        // DESC [catalog.]db.table — returns Field, Type, Null, Key, Default, Extra
        let qualified = if catalog.is_empty() || catalog == "default_catalog" {
            format!(
                "`{}`.`{}`",
                database.replace('`', "``"),
                table.replace('`', "``")
            )
        } else {
            format!(
                "`{}`.`{}`.`{}`",
                catalog.replace('`', "``"),
                database.replace('`', "``"),
                table.replace('`', "``"),
            )
        };

        let mut rows = match self.run_query(&format!("DESC {qualified}")).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let columns = rows
            .iter_mut()
            .filter_map(|row| {
                let name: String = row.take(0)?;
                let data_type = row
                    .take::<String, usize>(1)
                    .unwrap_or_else(|| "VARCHAR".to_string())
                    .to_uppercase();
                let nullable = row
                    .take::<String, usize>(2)
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

/// `KILL QUERY <connection_id>` — `connection_id` must be a decimal integer.
fn kill_query_sql(id: &str) -> Option<String> {
    let trimmed = id.trim();
    if trimmed.is_empty() || trimmed.len() > 20 || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(format!("KILL QUERY {trimmed}"))
}

// ---------------------------------------------------------------------------
// Parameter binding
// ---------------------------------------------------------------------------

/// Convert a [`QueryParam`] to a `mysql_async` native value for prepared-statement binding.
fn query_param_to_mysql_value(p: &queryflux_core::params::QueryParam) -> Value {
    use queryflux_core::params::QueryParam;
    match p {
        QueryParam::Text(s) => Value::Bytes(s.as_bytes().to_vec()),
        QueryParam::Numeric(s) => {
            if let Ok(n) = s.parse::<i64>() {
                Value::Int(n)
            } else if let Ok(f) = s.parse::<f64>() {
                Value::Double(f)
            } else {
                Value::Bytes(s.as_bytes().to_vec())
            }
        }
        QueryParam::Boolean(b) => Value::Int(*b as i64),
        QueryParam::Date(s) | QueryParam::Timestamp(s) | QueryParam::Time(s) => {
            Value::Bytes(s.as_bytes().to_vec())
        }
        QueryParam::Null => Value::NULL,
    }
}

// ---------------------------------------------------------------------------
// Type mapping helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Arrow conversion helpers for execute_as_arrow
// ---------------------------------------------------------------------------

fn mysql_column_type_to_arrow(ct: ColumnType) -> DataType {
    match ct {
        ColumnType::MYSQL_TYPE_TINY => DataType::Int8,
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => DataType::Int16,
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => DataType::Int32,
        ColumnType::MYSQL_TYPE_LONGLONG => DataType::Int64,
        ColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
        ColumnType::MYSQL_TYPE_BIT => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn starrocks_rows_to_batch(
    schema: &Arc<ArrowSchema>,
    rows: &mut [Row],
    num_cols: usize,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let dt = schema.field(col_idx).data_type();
        columns.push(starrocks_build_column(dt, rows, col_idx)?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| QueryFluxError::Engine(format!("StarRocks RecordBatch failed: {e}")))
}

fn starrocks_build_column(dt: &DataType, rows: &mut [Row], col_idx: usize) -> Result<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i != 0),
                    Value::UInt(u) => b.append_value(u != 0),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(i) => b.append_value(i != 0),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i8),
                    Value::UInt(u) => b.append_value(u as i8),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i8>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i16),
                    Value::UInt(u) => b.append_value(u as i16),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i16>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i32),
                    Value::UInt(u) => b.append_value(u as i32),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i32>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i),
                    Value::UInt(u) => b.append_value(u as i64),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Float(f) => b.append_value(f),
                    Value::Double(d) => b.append_value(d as f32),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<f32>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Float(f) => b.append_value(f as f64),
                    Value::Double(d) => b.append_value(d),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Bytes(bytes) => match String::from_utf8(bytes) {
                        Ok(s) => b.append_value(s),
                        Err(e) => {
                            b.append_value(format!("<binary:{} bytes>", e.into_bytes().len()))
                        }
                    },
                    Value::Int(i) => b.append_value(i.to_string()),
                    Value::UInt(u) => b.append_value(u.to_string()),
                    Value::Float(f) => b.append_value(f.to_string()),
                    Value::Double(d) => b.append_value(d.to_string()),
                    Value::Date(y, mo, d, h, mi, s, us) => b.append_value(format!(
                        "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                    )),
                    Value::Time(neg, days, h, mi, s, us) => {
                        let sign = if neg { "-" } else { "" };
                        let total_h = days * 24 + h as u32;
                        b.append_value(format!("{sign}{total_h:02}:{mi:02}:{s:02}.{us:06}"))
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

impl StarRocksAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "starRocks",
            display_name: "StarRocks",
            description: "High-performance OLAP database. Connects via the MySQL wire protocol.",
            hex: "EF4444",
            connection_type: ConnectionType::MySqlWire,
            default_port: Some(9030),
            endpoint_example: Some("mysql://starrocks-fe:9030"),
            supported_auth: vec![AuthType::Basic],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "MySQL-protocol connection URL for the StarRocks front-end node.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("mysql://starrocks-fe:9030"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Must be 'basic' for StarRocks (username + password).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("basic"),
                },
                ConfigField {
                    key: "auth.username",
                    label: "Username",
                    description: "MySQL username for the StarRocks connection.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("root"),
                },
                ConfigField {
                    key: "auth.password",
                    label: "Password",
                    description: "MySQL password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "poolSize",
                    label: "Connection pool size",
                    description: "Max concurrent MySQL connections QueryFlux opens to StarRocks. Defaults to 8 when poolSize is omitted in JSON or in typed cluster/YAML config; set poolSize to override. Not derived from max_running_queries.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("8"),
                },
            ],
        }
    }
}

pub struct StarRocksFactory;

#[async_trait]
impl crate::EngineAdapterFactory for StarRocksFactory {
    fn engine_key(&self) -> &'static str {
        "starRocks"
    }

    fn descriptor(&self) -> EngineDescriptor {
        StarRocksAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<crate::AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = StarRocksConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(StarRocksAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::params::QueryParam;

    #[test]
    fn text_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Text("alice".into())),
            Value::Bytes(b"alice".to_vec())
        );
    }

    #[test]
    fn integer_numeric_maps_to_int() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("99".into())),
            Value::Int(99)
        );
    }

    #[test]
    fn negative_integer_numeric_maps_to_int() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("-3".into())),
            Value::Int(-3)
        );
    }

    #[test]
    fn float_numeric_maps_to_double() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("1.5".into())),
            Value::Double(1.5)
        );
    }

    #[test]
    fn non_parseable_numeric_falls_back_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("bad".into())),
            Value::Bytes(b"bad".to_vec())
        );
    }

    #[test]
    fn boolean_true_maps_to_int_one() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Boolean(true)),
            Value::Int(1)
        );
    }

    #[test]
    fn boolean_false_maps_to_int_zero() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Boolean(false)),
            Value::Int(0)
        );
    }

    #[test]
    fn date_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Date("2025-06-01".into())),
            Value::Bytes(b"2025-06-01".to_vec())
        );
    }

    #[test]
    fn timestamp_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Timestamp("2025-06-01 09:00:00".into())),
            Value::Bytes(b"2025-06-01 09:00:00".to_vec())
        );
    }

    #[test]
    fn time_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Time("14:30:00".into())),
            Value::Bytes(b"14:30:00".to_vec())
        );
    }

    #[test]
    fn null_maps_to_null() {
        assert_eq!(query_param_to_mysql_value(&QueryParam::Null), Value::NULL);
    }

    #[test]
    fn kill_query_sql_accepts_decimal_ids() {
        assert_eq!(kill_query_sql("42").as_deref(), Some("KILL QUERY 42"));
        assert_eq!(kill_query_sql(" 7 ").as_deref(), Some("KILL QUERY 7"));
    }

    #[test]
    fn kill_query_sql_rejects_non_integers() {
        assert!(kill_query_sql("").is_none());
        assert!(kill_query_sql("12; DROP").is_none());
        assert!(kill_query_sql("-1").is_none());
        assert!(kill_query_sql("1e2").is_none());
    }

    // ── passthrough requires TLS ──────────────────────────────────────────────
    // `mysql_async::Pool::new` doesn't eagerly connect, so these construct a real
    // `StarRocksAdapter` against an unreachable host without needing a live server.

    fn config(
        endpoint: &str,
        query_auth: Option<queryflux_core::config::QueryAuthConfig>,
    ) -> StarRocksConfig {
        StarRocksConfig {
            endpoint: endpoint.to_string(),
            auth: None,
            pool_size: 1,
            query_auth,
        }
    }

    #[test]
    fn passthrough_without_tls_is_rejected_at_construction() {
        let cfg = config(
            "mysql://svc@127.0.0.1:1/db",
            Some(queryflux_core::config::QueryAuthConfig::Passthrough),
        );
        let result =
            StarRocksAdapter::new(ClusterName("sr".into()), ClusterGroupName("g".into()), cfg);
        match result {
            Ok(_) => panic!("passthrough without require_ssl must fail construction"),
            Err(e) => assert!(
                e.to_string().contains("TLS"),
                "expected a TLS-requirement error, got: {e}"
            ),
        }
    }

    #[test]
    fn passthrough_with_require_ssl_is_accepted_at_construction() {
        let cfg = config(
            "mysql://svc@127.0.0.1:1/db?require_ssl=true",
            Some(queryflux_core::config::QueryAuthConfig::Passthrough),
        );
        assert!(
            StarRocksAdapter::new(ClusterName("sr".into()), ClusterGroupName("g".into()), cfg)
                .is_ok()
        );
    }

    #[test]
    fn service_account_does_not_require_tls() {
        let cfg = config("mysql://svc@127.0.0.1:1/db", None);
        assert!(
            StarRocksAdapter::new(ClusterName("sr".into()), ClusterGroupName("g".into()), cfg)
                .is_ok()
        );
    }
}
