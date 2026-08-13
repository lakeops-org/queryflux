use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_athena::types::QueryExecutionState;
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
    tags::QueryTags,
};
use tracing::warn;

use crate::{AdapterKind, AsyncAdapter, BackendQueryIdSlot};

/// Parsed and validated configuration for an Athena cluster.
pub struct AthenaConfig {
    pub region: String,
    pub s3_output_location: String,
    pub workgroup: Option<String>,
    pub catalog: Option<String>,
    /// Max seconds to poll for query completion before `StopQueryExecution`.
    pub max_wait_secs: u64,
    pub auth: Option<ClusterAuth>,
}

/// Default proxy-side wait for Athena completion (`maxWaitSecs`).
pub const DEFAULT_ATHENA_MAX_WAIT_SECS: u64 = 600;
const ATHENA_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Bound for best-effort `StopQueryExecution` after the main wait deadline expires.
const ATHENA_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Parse optional `maxWaitSecs` (positive integer seconds). Defaults to
/// [`DEFAULT_ATHENA_MAX_WAIT_SECS`].
fn parse_max_wait_secs_from_json(json: &serde_json::Value, cluster_name: &str) -> Result<u64> {
    match json.get("maxWaitSecs") {
        None => Ok(DEFAULT_ATHENA_MAX_WAIT_SECS),
        Some(v) => v.as_u64().filter(|&n| n >= 1).ok_or_else(|| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': maxWaitSecs must be a positive integer"
            ))
        }),
    }
}

fn resolve_max_wait_secs(raw: Option<u64>, cluster_name: &str) -> Result<u64> {
    match raw {
        None => Ok(DEFAULT_ATHENA_MAX_WAIT_SECS),
        Some(n) if n >= 1 => Ok(n),
        Some(_) => Err(QueryFluxError::Engine(format!(
            "cluster '{cluster_name}': maxWaitSecs must be a positive integer"
        ))),
    }
}

impl crate::EngineConfigParseable for AthenaConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        use queryflux_core::engine_registry::{json_str, parse_auth_from_config_json};
        let region = json_str(json, "region").ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing 'region' for Athena"
            ))
        })?;
        let s3_output_location = json_str(json, "s3OutputLocation").ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing 's3OutputLocation' for Athena"
            ))
        })?;
        let raw_auth = parse_auth_from_config_json(json).map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': invalid auth ({e})"
            ))
        })?;
        let auth = match raw_auth {
            Some(a @ ClusterAuth::AccessKey { .. }) | Some(a @ ClusterAuth::RoleArn { .. }) => {
                Some(a)
            }
            Some(_) => {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': Athena only supports accessKey or roleArn auth"
                )));
            }
            None => None,
        };
        Ok(Self {
            region,
            s3_output_location,
            workgroup: json_str(json, "workgroup"),
            catalog: json_str(json, "catalog"),
            max_wait_secs: parse_max_wait_secs_from_json(json, cluster_name)?,
            auth,
        })
    }

    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        let region = cfg.region.clone().ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing 'region' for Athena"
            ))
        })?;
        let s3_output_location = cfg.s3_output_location.clone().ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': missing 's3OutputLocation' for Athena"
            ))
        })?;
        let auth = match cfg.auth.clone() {
            Some(a @ ClusterAuth::AccessKey { .. }) | Some(a @ ClusterAuth::RoleArn { .. }) => {
                Some(a)
            }
            Some(_) => {
                return Err(queryflux_core::error::QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': Athena only supports accessKey or roleArn auth"
                )));
            }
            None => None,
        };
        Ok(Self {
            region,
            s3_output_location,
            workgroup: cfg.workgroup.clone(),
            catalog: cfg.catalog.clone(),
            max_wait_secs: resolve_max_wait_secs(cfg.max_wait_secs, cluster_name)?,
            auth,
        })
    }
}

/// Walk the `std::error::Error` source chain and join all messages with ": ".
/// AWS SDK errors expose the real Athena message (e.g. "InvalidRequestException:
/// Query string is null or empty") only via the source chain; `Display` on the
/// outer `SdkError` just says "service error".
fn aws_err(e: &impl std::error::Error) -> String {
    let mut parts = vec![e.to_string()];
    let mut src = e.source();
    while let Some(s) = src {
        let s_str = s.to_string();
        if !parts.contains(&s_str) {
            parts.push(s_str);
        }
        src = s.source();
    }
    parts.join(": ")
}
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// Amazon Athena adapter.
///
/// Submits queries via `StartQueryExecution`, polls `GetQueryExecution` until complete,
/// then pages through `GetQueryResults` and converts the response to Arrow RecordBatches.
///
/// Sync engines use `execute_as_arrow` (Trino HTTP falls back to it when `supports_async` is false).
///
/// Auth: set `auth.type: accessKey` for static credentials. When omitted the default
/// AWS credential chain is used (env vars `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`,
/// ECS task role, EC2 instance profile, etc.). RoleArn auth uses a refreshing
/// `AssumeRoleProvider` so temporary STS credentials renew before expiry.
pub struct AthenaAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    client: aws_sdk_athena::Client,
    pub region: String,
    pub s3_output_location: String,
    workgroup: String,
    /// Default Glue catalog. Defaults to `AwsDataCatalog`.
    catalog: String,
    /// Proxy-side max wait for completion (`maxWaitSecs`).
    max_wait: Duration,
}

impl AthenaAdapter {
    /// Build an `AthenaAdapter`. The AWS client config is loaded asynchronously
    /// so the constructor is async.
    pub async fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: AthenaConfig,
    ) -> Result<Self> {
        use aws_config::BehaviorVersion;
        use aws_credential_types::Credentials;
        use aws_types::region::Region;

        let aws_region = Region::new(config.region.clone());

        let sdk_config = match config.auth {
            Some(ClusterAuth::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                let creds = Credentials::new(
                    access_key_id,
                    secret_access_key,
                    session_token,
                    None,
                    "queryflux-static",
                );
                aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_region.clone())
                    .credentials_provider(creds)
                    .load()
                    .await
            }
            Some(ClusterAuth::RoleArn {
                role_arn,
                external_id,
            }) => {
                // Refreshing AssumeRole provider: temporary STS credentials expire
                // (default 1h). A one-shot AssumeRole left the Athena client with
                // static creds that never refreshed.
                let base_config = aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_region.clone())
                    .load()
                    .await;
                let mut role_builder = aws_config::sts::AssumeRoleProvider::builder(&role_arn)
                    .session_name("queryflux-session")
                    .configure(&base_config);
                if let Some(eid) = external_id {
                    role_builder = role_builder.external_id(eid);
                }
                let provider = role_builder.build().await;
                aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_region.clone())
                    .credentials_provider(provider)
                    .load()
                    .await
            }
            _ => {
                // Default credential chain (env vars, ECS task role, EC2 instance profile, …)
                aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_region.clone())
                    .load()
                    .await
            }
        };

        let mut athena_builder = aws_sdk_athena::config::Builder::from(&sdk_config);
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
            if !endpoint.is_empty() {
                athena_builder = athena_builder.endpoint_url(endpoint);
            }
        }
        let client = aws_sdk_athena::Client::from_conf(athena_builder.build());

        Ok(Self {
            cluster_name,
            group_name,
            client,
            region: config.region,
            s3_output_location: config.s3_output_location,
            workgroup: config.workgroup.unwrap_or_else(|| "primary".to_string()),
            catalog: config
                .catalog
                .unwrap_or_else(|| "AwsDataCatalog".to_string()),
            max_wait: Duration::from_secs(config.max_wait_secs),
        })
    }

    async fn stop_query_best_effort(&self, execution_id: &str) {
        let stop = self
            .client
            .stop_query_execution()
            .query_execution_id(execution_id)
            .send();
        match tokio::time::timeout(ATHENA_STOP_TIMEOUT, stop).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                warn!(
                    execution_id = %execution_id,
                    error = %aws_err(&e),
                    "Athena StopQueryExecution failed (ignored)"
                );
            }
            Err(_) => {
                warn!(
                    execution_id = %execution_id,
                    "Athena StopQueryExecution timed out (ignored)"
                );
            }
        }
    }

    /// Poll until the given query execution reaches a terminal state.
    /// Returns `Ok(())` on success, `Err` on failure, cancellation, or timeout.
    ///
    /// Athena workgroups may enforce their own runtime limits; `maxWaitSecs`
    /// also stops the proxy from looping forever if status stays non-terminal,
    /// and issues `StopQueryExecution` on timeout.
    async fn wait_for_completion(&self, execution_id: &str) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.max_wait;
        loop {
            if tokio::time::Instant::now() >= deadline {
                self.stop_query_best_effort(execution_id).await;
                return Err(QueryFluxError::Engine(format!(
                    "Athena query {execution_id} exceeded max wait of {}s",
                    self.max_wait.as_secs()
                )));
            }

            let get = self
                .client
                .get_query_execution()
                .query_execution_id(execution_id)
                .send();
            let resp = match tokio::time::timeout_at(deadline, get).await {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    return Err(QueryFluxError::Engine(format!(
                        "Athena GetQueryExecution: {}",
                        aws_err(&e)
                    )));
                }
                Err(_) => {
                    self.stop_query_best_effort(execution_id).await;
                    return Err(QueryFluxError::Engine(format!(
                        "Athena query {execution_id} exceeded max wait of {}s",
                        self.max_wait.as_secs()
                    )));
                }
            };

            let state = resp
                .query_execution()
                .and_then(|e| e.status())
                .and_then(|s| s.state())
                .cloned()
                .unwrap_or(QueryExecutionState::Running);

            match state {
                QueryExecutionState::Succeeded => return Ok(()),
                QueryExecutionState::Failed => {
                    let reason = resp
                        .query_execution()
                        .and_then(|e| e.status())
                        .and_then(|s| s.state_change_reason())
                        .unwrap_or("unknown reason")
                        .to_string();
                    return Err(QueryFluxError::Engine(format!(
                        "Athena query failed: {reason}"
                    )));
                }
                QueryExecutionState::Cancelled => {
                    return Err(QueryFluxError::Engine(
                        "Athena query was cancelled".to_string(),
                    ));
                }
                _ => {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let sleep_for = ATHENA_POLL_INTERVAL.min(remaining);
                    if sleep_for.is_zero() {
                        continue;
                    }
                    tokio::time::sleep(sleep_for).await;
                }
            }
        }
    }

    /// Stream `GetQueryResults` pages as Arrow RecordBatches (one batch per API page).
    async fn stream_results(
        client: aws_sdk_athena::Client,
        execution_id: String,
        batch_tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
        stats_tx: tokio::sync::oneshot::Sender<Option<queryflux_core::query::QueryEngineStats>>,
    ) {
        let mut col_names: Vec<String> = Vec::new();
        let mut col_types: Vec<String> = Vec::new();
        let mut schema: Option<Arc<ArrowSchema>> = None;
        let mut next_token: Option<String> = None;
        let mut first_page = true;
        let mut sent_any = false;

        loop {
            let mut req = client.get_query_results().query_execution_id(&execution_id);
            if let Some(ref t) = next_token {
                req = req.next_token(t.clone());
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = batch_tx
                        .send(Err(QueryFluxError::Engine(format!(
                            "Athena GetQueryResults: {}",
                            aws_err(&e)
                        ))))
                        .await;
                    let _ = stats_tx.send(None);
                    return;
                }
            };

            if first_page {
                if let Some(meta) = resp.result_set().and_then(|rs| rs.result_set_metadata()) {
                    for col in meta.column_info() {
                        col_names.push(col.name().to_string());
                        col_types.push(col.r#type().to_string());
                    }
                }
                first_page = false;
                if col_names.is_empty() {
                    let _ = stats_tx.send(None);
                    return;
                }
                let fields: Vec<Field> = col_names
                    .iter()
                    .zip(col_types.iter())
                    .map(|(name, ty)| Field::new(name, athena_type_to_arrow(ty), true))
                    .collect();
                schema = Some(Arc::new(ArrowSchema::new(fields)));
            }

            let mut page_rows: Vec<Vec<Option<String>>> = Vec::new();
            if let Some(rs) = resp.result_set() {
                for (row_idx, row) in rs.rows().iter().enumerate() {
                    if !sent_any && row_idx == 0 {
                        continue;
                    }
                    let cells: Vec<Option<String>> = row
                        .data()
                        .iter()
                        .map(|d| d.var_char_value().map(|s| s.to_string()))
                        .collect();
                    page_rows.push(cells);
                }
            }

            if let Some(ref schema) = schema {
                if !page_rows.is_empty() {
                    match rows_to_record_batch(schema, &page_rows) {
                        Ok(batch) => {
                            sent_any = true;
                            if batch_tx.send(Ok(batch)).await.is_err() {
                                let _ = stats_tx.send(None);
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = batch_tx.send(Err(e)).await;
                            let _ = stats_tx.send(None);
                            return;
                        }
                    }
                }
            }

            next_token = resp.next_token().map(|t| t.to_string());
            if next_token.is_none() {
                break;
            }
        }

        if !sent_any {
            if let Some(schema) = schema {
                let batch = RecordBatch::new_empty(schema);
                let _ = batch_tx.send(Ok(batch)).await;
            }
        }

        let _ = stats_tx.send(None);
    }
}

#[async_trait]
impl AsyncAdapter for AthenaAdapter {
    async fn submit_query(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
        _params: &queryflux_core::params::QueryParams,
    ) -> Result<QueryExecution> {
        Err(QueryFluxError::Engine(
            "Athena requires execute_as_arrow; use a non-Trino-HTTP frontend".to_string(),
        ))
    }

    async fn poll_query(
        &self,
        _backend_id: &BackendQueryId,
        _poll_token: Option<&str>,
    ) -> Result<QueryPollResult> {
        Err(QueryFluxError::Engine(
            "Athena does not support async polling via this interface".to_string(),
        ))
    }

    async fn cancel_query(&self, backend_id: &BackendQueryId) -> Result<()> {
        let _ = self
            .client
            .stop_query_execution()
            .query_execution_id(&backend_id.0)
            .send()
            .await;
        Ok(())
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &queryflux_core::session::SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &queryflux_core::tags::QueryTags,
        params: &queryflux_core::params::QueryParams,
        id_slot: &BackendQueryIdSlot,
    ) -> crate::Result<crate::SyncExecution> {
        use crate::SyncExecution;
        use tokio_stream::wrappers::ReceiverStream;

        let ctx = aws_sdk_athena::types::QueryExecutionContext::builder()
            .catalog(&self.catalog)
            .set_database(session.database().map(|s| s.to_string()))
            .build();
        let result_cfg = aws_sdk_athena::types::ResultConfiguration::builder()
            .output_location(&self.s3_output_location)
            .build();

        let athena_params: Option<Vec<String>> = if params.is_empty() {
            None
        } else {
            Some(params.iter().map(query_param_to_athena_string).collect())
        };

        let resp = self
            .client
            .start_query_execution()
            .query_string(sql)
            .query_execution_context(ctx)
            .set_execution_parameters(athena_params)
            .result_configuration(result_cfg)
            .work_group(&self.workgroup)
            .send()
            .await
            .map_err(|e| {
                QueryFluxError::Engine(format!("Athena StartQueryExecution: {}", aws_err(&e)))
            })?;

        let execution_id = resp
            .query_execution_id()
            .ok_or_else(|| QueryFluxError::Engine("Athena returned no execution ID".to_string()))?
            .to_string();
        id_slot.publish(execution_id.clone());

        self.wait_for_completion(&execution_id).await?;

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(8);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let client = self.client.clone();
        tokio::spawn(async move {
            AthenaAdapter::stream_results(client, execution_id, batch_tx, stats_tx).await;
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
        })
    }

    async fn health_check(&self) -> bool {
        match self
            .client
            .get_work_group()
            .work_group(&self.workgroup)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "Athena health check failed"
                );
                false
            }
        }
    }

    fn engine_type(&self) -> EngineType {
        EngineType::Athena
    }

    fn supports_native_params(&self) -> bool {
        true
    }

    // --- Catalog discovery via Glue ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let resp = self.client.list_data_catalogs().send().await.map_err(|e| {
            QueryFluxError::Engine(format!("Athena ListDataCatalogs: {}", aws_err(&e)))
        })?;
        let mut names: Vec<String> = resp
            .data_catalogs_summary()
            .iter()
            .map(|c| c.catalog_name().unwrap_or_default().to_string())
            .collect();
        if names.is_empty() {
            names.push(self.catalog.clone());
        }
        Ok(names)
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let cat = if catalog.is_empty() {
            &self.catalog
        } else {
            catalog
        };
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self.client.list_databases().catalog_name(cat);
            if let Some(t) = next_token {
                req = req.next_token(t);
            }
            let resp = req.send().await.map_err(|e| {
                QueryFluxError::Engine(format!("Athena ListDatabases: {}", aws_err(&e)))
            })?;
            for db in resp.database_list() {
                names.push(db.name().to_string());
            }
            next_token = resp.next_token().map(|t| t.to_string());
            if next_token.is_none() {
                break;
            }
        }
        Ok(names)
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let cat = if catalog.is_empty() {
            &self.catalog
        } else {
            catalog
        };
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_table_metadata()
                .catalog_name(cat)
                .database_name(database);
            if let Some(t) = next_token {
                req = req.next_token(t);
            }
            let resp = req.send().await.map_err(|e| {
                QueryFluxError::Engine(format!("Athena ListTableMetadata: {}", aws_err(&e)))
            })?;
            for tbl in resp.table_metadata_list() {
                names.push(tbl.name().to_string());
            }
            next_token = resp.next_token().map(|t| t.to_string());
            if next_token.is_none() {
                break;
            }
        }
        Ok(names)
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let cat = if catalog.is_empty() {
            &self.catalog
        } else {
            catalog
        };
        let resp = match self
            .client
            .get_table_metadata()
            .catalog_name(cat)
            .database_name(database)
            .table_name(table)
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let columns = resp
            .table_metadata()
            .map(|t| t.columns())
            .unwrap_or_default()
            .iter()
            .map(|c| queryflux_core::catalog::ColumnDef {
                name: c.name().to_string(),
                data_type: c.r#type().unwrap_or("varchar").to_uppercase(),
                nullable: true,
            })
            .collect();

        Ok(Some(TableSchema {
            catalog: cat.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Convert a [`QueryParam`] to the string representation Athena expects in
/// `execution_parameters`. Athena passes these as positional `?` bindings and
/// handles quoting itself — we send the raw value string for all types.
fn query_param_to_athena_string(p: &queryflux_core::params::QueryParam) -> String {
    use queryflux_core::params::QueryParam;
    match p {
        QueryParam::Text(s) => s.clone(),
        QueryParam::Numeric(s) => s.clone(),
        QueryParam::Boolean(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        QueryParam::Date(s) | QueryParam::Timestamp(s) | QueryParam::Time(s) => s.clone(),
        QueryParam::Null => "NULL".to_string(),
    }
}

fn athena_type_to_arrow(athena_type: &str) -> DataType {
    match athena_type
        .to_lowercase()
        .trim_start_matches("varchar(")
        .split('(')
        .next()
        .unwrap_or("")
    {
        "bigint" | "long" => DataType::Int64,
        "integer" | "int" => DataType::Int64,
        "smallint" => DataType::Int64,
        "tinyint" => DataType::Int64,
        "double" | "float8" => DataType::Float64,
        "float" | "real" | "float4" => DataType::Float32,
        "boolean" => DataType::Boolean,
        // Everything else (varchar, char, string, date, timestamp, decimal, array, map, struct…)
        // is returned as a UTF-8 string — callers can parse further if needed.
        _ => DataType::Utf8,
    }
}

fn rows_to_record_batch(
    schema: &Arc<ArrowSchema>,
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        columns.push(build_column(field.data_type(), rows, col_idx)?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| QueryFluxError::Engine(format!("Athena RecordBatch failed: {e}")))
}

fn build_column(dt: &DataType, rows: &[Vec<Option<String>>], col_idx: usize) -> Result<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx).and_then(|v| v.as_deref()) {
                    None | Some("") => b.append_null(),
                    Some(s) => b.append_value(s.eq_ignore_ascii_case("true") || s == "1"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .and_then(|v| v.as_deref())
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .and_then(|v| v.as_deref())
                    .and_then(|s| s.parse::<f32>().ok())
                {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row
                    .get(col_idx)
                    .and_then(|v| v.as_deref())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.get(col_idx).and_then(|v| v.as_ref()) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

// ---------------------------------------------------------------------------
// Engine descriptor
// ---------------------------------------------------------------------------

impl AthenaAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "athena",
            display_name: "Amazon Athena",
            description: "Serverless SQL over S3 via the AWS SDK (async submit/poll).",
            hex: "FF9900",
            connection_type: ConnectionType::ManagedApi,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::AccessKey, AuthType::RoleArn],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "region",
                    label: "AWS Region",
                    description: "AWS region where Athena runs (e.g. us-east-1).",
                    field_type: FieldType::Text,
                    required: true,
                    example: Some("us-east-1"),
                },
                ConfigField {
                    key: "s3OutputLocation",
                    label: "S3 Output Location",
                    description: "S3 URI where Athena writes query results.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("s3://my-bucket/athena-results/"),
                },
                ConfigField {
                    key: "workgroup",
                    label: "Workgroup",
                    description: "Athena workgroup to submit queries to. Defaults to 'primary'.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("primary"),
                },
                ConfigField {
                    key: "catalog",
                    label: "Catalog",
                    description: "Default Glue catalog. Defaults to 'AwsDataCatalog'.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("AwsDataCatalog"),
                },
                ConfigField {
                    key: "maxWaitSecs",
                    label: "Max wait (seconds)",
                    description: "Max seconds QueryFlux polls Athena before cancelling the query. Defaults to 600 (10 minutes).",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("600"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Use 'accessKey' for static credentials. Omit to use the default AWS credential chain.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("accessKey"),
                },
                ConfigField {
                    key: "auth.accessKeyId",
                    label: "Access Key ID",
                    description: "AWS access key ID (required when auth.type = accessKey).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("AKIAIOSFODNN7EXAMPLE"),
                },
                ConfigField {
                    key: "auth.secretAccessKey",
                    label: "Secret Access Key",
                    description: "AWS secret access key.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
            ],
        }
    }
}

pub struct AthenaFactory;

#[async_trait]
impl crate::EngineAdapterFactory for AthenaFactory {
    fn engine_key(&self) -> &'static str {
        "athena"
    }

    fn descriptor(&self) -> EngineDescriptor {
        AthenaAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<crate::AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = AthenaConfig::from_json(json, &name)?;
        Ok(AdapterKind::Async(Arc::new(
            AthenaAdapter::new(cluster_name, group, config).await?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::params::QueryParam;

    #[test]
    fn text_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Text("alice".into())),
            "alice"
        );
    }

    #[test]
    fn integer_numeric_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Numeric("42".into())),
            "42"
        );
    }

    #[test]
    fn float_numeric_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Numeric("3.14".into())),
            "3.14"
        );
    }

    #[test]
    fn boolean_true_is_lowercase_true() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Boolean(true)),
            "true"
        );
    }

    #[test]
    fn boolean_false_is_lowercase_false() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Boolean(false)),
            "false"
        );
    }

    #[test]
    fn date_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Date("2025-01-15".into())),
            "2025-01-15"
        );
    }

    #[test]
    fn timestamp_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Timestamp("2025-01-15 12:00:00".into())),
            "2025-01-15 12:00:00"
        );
    }

    #[test]
    fn time_passes_through() {
        assert_eq!(
            query_param_to_athena_string(&QueryParam::Time("08:30:00".into())),
            "08:30:00"
        );
    }

    #[test]
    fn null_emits_null_string() {
        assert_eq!(query_param_to_athena_string(&QueryParam::Null), "NULL");
    }

    #[test]
    fn wait_for_completion_bound_defaults_to_ten_minutes() {
        assert_eq!(DEFAULT_ATHENA_MAX_WAIT_SECS, 600);
        assert_eq!(ATHENA_POLL_INTERVAL, Duration::from_millis(500));
        assert!(ATHENA_POLL_INTERVAL < Duration::from_secs(DEFAULT_ATHENA_MAX_WAIT_SECS));
    }

    #[test]
    fn max_wait_secs_parses_from_json() {
        use crate::EngineConfigParseable;
        use serde_json::json;

        let cfg = AthenaConfig::from_json(
            &json!({
                "region": "us-east-1",
                "s3OutputLocation": "s3://bucket/results/"
            }),
            "athena-1",
        )
        .expect("defaults");
        assert_eq!(cfg.max_wait_secs, DEFAULT_ATHENA_MAX_WAIT_SECS);

        let cfg = AthenaConfig::from_json(
            &json!({
                "region": "us-east-1",
                "s3OutputLocation": "s3://bucket/results/",
                "maxWaitSecs": 120
            }),
            "athena-1",
        )
        .expect("override");
        assert_eq!(cfg.max_wait_secs, 120);

        for bad in [json!(0), json!(-1), json!("600"), json!(null)] {
            let err = AthenaConfig::from_json(
                &json!({
                    "region": "us-east-1",
                    "s3OutputLocation": "s3://bucket/results/",
                    "maxWaitSecs": bad
                }),
                "athena-1",
            )
            .map(|_| ())
            .unwrap_err();
            assert!(err.to_string().contains("maxWaitSecs"));
        }
    }

    #[test]
    fn rows_to_record_batch_builds_typed_columns() {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::{DataType, Field, Int64Type, Schema as ArrowSchema};

        let fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ];
        let schema = Arc::new(ArrowSchema::new(fields));
        let rows = vec![
            vec![Some("1".into()), Some("alice".into())],
            vec![Some("2".into()), None],
        ];
        let batch = rows_to_record_batch(&schema, &rows).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let names = batch.column(1).as_string::<i32>();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1));
    }

    #[test]
    fn rows_to_record_batch_empty_yields_zero_rows() {
        let fields = vec![Field::new("x", DataType::Utf8, true)];
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = rows_to_record_batch(&schema, &[]).expect("batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }
}
