//! OpenLineage events for every query (`plans/openlineage-plan.md`).
//!
//! [`OpenLineageStore`] sits in the metrics chain and in the control plane's worker-history
//! ingest. Each finished query that read or wrote a table becomes one `RunEvent` (`COMPLETE`,
//! `FAIL` or `ABORT`), queued in a [`Spool`] and posted to `{url}{endpoint}`. The run ID is
//! the query's proxy ID, so an event delivered twice, or emitted by two processes, is still
//! one run to the consumer.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use queryflux_core::config::{ClusterConfig, OpenLineageConfig};
use queryflux_core::error::Result;
use queryflux_core::lineage::{ColumnLineage, InputColumn};
use queryflux_core::query::QueryStatus;
use queryflux_core::worker_snapshot::HistoryRedaction;
use queryflux_frontend::state::LiveConfig;
use queryflux_persistence::{ClusterSnapshot, MetricsStore, QueryDataset, QueryRecord};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

use crate::history_uplink::mask_literals;
use crate::spool::{retry_after, Spool, UploadError, Uploader};

const RUN_EVENT_SCHEMA: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
/// Schemas of the facets QueryFlux defines (`queryflux`, `queryflux_columnsRead`).
const QUERYFLUX_FACETS_SCHEMA: &str = "https://raw.githubusercontent.com/lakeops-org/queryflux/main/crates/queryflux/openlineage/queryflux-facets.json";
/// Events taken from the spool per round.
const BATCH_SIZE: usize = 200;
/// Events posted at once (OpenLineage's HTTP transport takes one event per request).
const CONCURRENCY: usize = 8;

/// Query tags a caller (dbt, Airflow, an agent framework) sets to name the job and link the
/// run to its own.
const TAG_JOB: &str = "openlineage.job";
const TAG_PARENT_RUN_ID: &str = "openlineage.parent_run_id";
const TAG_PARENT_JOB_NAMESPACE: &str = "openlineage.parent_job_namespace";
const TAG_PARENT_JOB_NAME: &str = "openlineage.parent_job_name";

fn producer() -> String {
    format!(
        "https://github.com/lakeops-org/queryflux/tree/v{}",
        env!("CARGO_PKG_VERSION")
    )
}

pub(crate) struct OpenLineageStore {
    spool: Spool<Value>,
    client: reqwest::Client,
    url: String,
    api_key: Option<String>,
    namespace: String,
    sql: HistoryRedaction,
    /// Cluster configs, for dataset namespaces.
    live: Arc<RwLock<LiveConfig>>,
}

impl OpenLineageStore {
    pub(crate) fn new(config: &OpenLineageConfig, live: Arc<RwLock<LiveConfig>>) -> Arc<Self> {
        let url = format!(
            "{}/{}",
            config.url.trim_end_matches('/'),
            config.endpoint.trim_start_matches('/')
        );
        let api_key = config
            .api_key
            .clone()
            .or_else(|| std::env::var("OPENLINEAGE_API_KEY").ok())
            .filter(|k| !k.is_empty());
        tracing::info!(url = %url, namespace = %config.namespace, "OpenLineage events enabled");
        Arc::new(Self {
            spool: Spool::new(config.spool_max_events),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            url,
            api_key,
            namespace: config.namespace.clone(),
            sql: config.sql,
            live,
        })
    }

    /// Post one event. `Ok(false)`: the consumer rejected it for good (a 4xx other than
    /// 408/429), so it is dropped rather than retried forever at the front of the spool.
    async fn post(&self, event: &Value) -> std::result::Result<bool, UploadError> {
        let mut req = self.client.post(&self.url).json(event);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => Ok(true),
            Ok(resp) => {
                let status = resp.status();
                let retry_after = retry_after(resp.headers());
                let body = resp.text().await.unwrap_or_default();
                let permanent = status.is_client_error()
                    && status != reqwest::StatusCode::REQUEST_TIMEOUT
                    && status != reqwest::StatusCode::TOO_MANY_REQUESTS;
                if permanent {
                    tracing::warn!(%status, "OpenLineage consumer rejected an event, dropped: {body}");
                    return Ok(false);
                }
                Err(UploadError {
                    message: format!("OpenLineage endpoint returned {status}: {body}"),
                    retry_after,
                })
            }
            Err(e) => Err(UploadError {
                message: format!("OpenLineage endpoint unreachable: {e}"),
                retry_after: None,
            }),
        }
    }
}

impl Uploader for OpenLineageStore {
    type Item = Value;
    const WHAT: &'static str = "OpenLineage events";

    fn spool(&self) -> &Spool<Value> {
        &self.spool
    }

    async fn flush(&self) -> std::result::Result<(), UploadError> {
        loop {
            let batch = self.spool.take(BATCH_SIZE);
            if batch.is_empty() {
                return Ok(());
            }
            let results: Vec<_> = futures::stream::iter(batch)
                .map(|event| async move {
                    let result = self.post(&event).await;
                    (event, result)
                })
                .buffered(CONCURRENCY)
                .collect()
                .await;
            let mut failed = Vec::new();
            let mut error = None;
            for (event, result) in results {
                if let Err(e) = result {
                    failed.push(event);
                    error.get_or_insert(e);
                }
            }
            if let Some(e) = error {
                let (_, dropped) = self.spool.push(failed, true);
                if dropped > 0 {
                    tracing::warn!(dropped, "OpenLineage spool full, dropped the oldest events");
                }
                return Err(e);
            }
        }
    }
}

#[async_trait]
impl MetricsStore for OpenLineageStore {
    fn wants_datasets(&self) -> bool {
        true
    }

    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        let cluster = self
            .live
            .read()
            .await
            .cluster_configs
            .get(&record.cluster_name.0)
            .cloned();
        if let Some(event) = run_event(&record, &self.namespace, self.sql, cluster.as_ref()) {
            let (queued, dropped) = self.spool.push([event], false);
            if dropped > 0 {
                tracing::warn!(dropped, "OpenLineage spool full, dropped the oldest events");
            }
            if queued >= BATCH_SIZE {
                self.spool.wake();
            }
        }
        Ok(())
    }

    async fn record_cluster_snapshot(&self, _snapshot: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}

/// `body` with the `_producer` and `_schemaURL` every facet carries.
fn facet(schema_url: &str, body: Value) -> Value {
    let mut facet = Map::new();
    facet.insert("_producer".into(), producer().into());
    facet.insert("_schemaURL".into(), schema_url.into());
    if let Value::Object(fields) = body {
        facet.extend(fields);
    }
    Value::Object(facet)
}

/// A string as an OpenLineage run ID (a UUID): itself when it is one, else a UUID derived
/// from it, so the same string always gives the same ID.
fn run_id(id: &str) -> String {
    uuid::Uuid::parse_str(id)
        .unwrap_or_else(|_| uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, id.as_bytes()))
        .to_string()
}

fn tag<'a>(record: &'a QueryRecord, key: &str) -> Option<&'a str> {
    record
        .query_tags
        .get(key)
        .and_then(|v| v.as_deref())
        .filter(|v| !v.is_empty())
}

/// The `RunEvent` for a finished query, or `None` when there is nothing to report: no
/// datasets (unparseable SQL, `SELECT 1`, DDL), blocked by a guard, or denied by routing.
pub(crate) fn run_event(
    record: &QueryRecord,
    namespace: &str,
    sql_policy: HistoryRedaction,
    cluster: Option<&ClusterConfig>,
) -> Option<Value> {
    if record.datasets.is_empty() || record.was_guard_blocked {
        return None;
    }
    let event_type = match record.status {
        QueryStatus::Success => "COMPLETE",
        QueryStatus::Failed => "FAIL",
        QueryStatus::Cancelled => "ABORT",
        QueryStatus::Denied => return None,
    };

    let mut run_facets = Map::new();
    run_facets.insert(
        "processing_engine".into(),
        facet(
            "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
            json!({ "name": "queryflux", "version": env!("CARGO_PKG_VERSION") }),
        ),
    );
    if let (QueryStatus::Failed, Some(message)) = (&record.status, &record.error_message) {
        let message = match sql_policy {
            HistoryRedaction::Full => message.clone(),
            // Engine errors can quote the query.
            HistoryRedaction::MaskLiterals => mask_literals(message),
            HistoryRedaction::Metadata => "query failed".to_string(),
        };
        run_facets.insert(
            "errorMessage".into(),
            facet(
                "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
                json!({ "message": message, "programmingLanguage": "SQL" }),
            ),
        );
    }
    if let Some(parent) = parent(record) {
        run_facets.insert("parent".into(), parent);
    }
    run_facets.insert(
        "queryflux".into(),
        facet(
            &format!("{QUERYFLUX_FACETS_SCHEMA}#/$defs/QueryFluxRunFacet"),
            json!({
                "clusterGroup": record.cluster_group.0,
                "cluster": record.cluster_name.0,
                "engine": record.engine_type,
                "sourceDialect": record.source_dialect,
                "targetDialect": record.target_dialect,
                "translated": record.was_translated,
                "rewritten": record.was_rewritten,
                "cacheHit": record.cache_hit,
                "user": record.user,
                "agentId": record.agent_id,
                "backendQueryId": record.backend_query_id,
                "queueMs": record.queue_duration_ms,
                "executionMs": record.execution_duration_ms,
                "rowsReturned": record.rows_returned,
            }),
        ),
    );

    let mut job_facets = Map::new();
    job_facets.insert(
        "jobType".into(),
        facet(
            "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
            json!({ "processingType": "BATCH", "integration": "QUERYFLUX", "jobType": "QUERY" }),
        ),
    );
    let sql = match sql_policy {
        HistoryRedaction::Full => record.sql.clone(),
        HistoryRedaction::MaskLiterals => record
            .digest_text
            .clone()
            .or_else(|| record.sql.as_deref().map(mask_literals)),
        HistoryRedaction::Metadata => None,
    };
    if let Some(query) = sql {
        job_facets.insert(
            "sql".into(),
            facet(
                "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json#/$defs/SQLJobFacet",
                json!({ "query": query, "dialect": record.source_dialect }),
            ),
        );
    }
    let job_name = tag(record, TAG_JOB)
        .map(String::from)
        .or_else(|| {
            record
                .query_parameterized_hash
                .map(|h| format!("query.{:016x}", h as u64))
        })
        .unwrap_or_else(|| format!("query.{}", record.proxy_query_id));

    let (outputs, inputs): (Vec<_>, Vec<_>) = record.datasets.iter().partition(|d| d.is_write);
    let inputs: Vec<Value> = inputs
        .into_iter()
        .map(|d| {
            let (ns, name) = dataset_name(parts(d), record, cluster);
            let mut dataset = json!({ "namespace": ns, "name": name });
            if !d.columns.is_empty() {
                dataset["facets"] = json!({
                    "queryflux_columnsRead": facet(
                        &format!("{QUERYFLUX_FACETS_SCHEMA}#/$defs/ColumnsReadDatasetFacet"),
                        json!({ "fields": d.columns }),
                    ),
                });
            }
            dataset
        })
        .collect();
    let outputs: Vec<Value> = outputs
        .into_iter()
        .map(|d| {
            let (ns, name) = dataset_name(parts(d), record, cluster);
            let mut dataset = json!({ "namespace": ns, "name": name });
            if let Some(lineage) = &d.column_lineage {
                dataset["facets"] = json!({
                    "columnLineage": column_lineage_facet(lineage, record, cluster),
                });
            }
            dataset
        })
        .collect();

    Some(json!({
        "eventType": event_type,
        "eventTime": record.created_at.to_rfc3339(),
        "producer": producer(),
        "schemaURL": RUN_EVENT_SCHEMA,
        "run": { "runId": run_id(&record.proxy_query_id), "facets": run_facets },
        "job": { "namespace": namespace, "name": job_name, "facets": job_facets },
        "inputs": inputs,
        "outputs": outputs,
    }))
}

fn parts(d: &QueryDataset) -> (Option<&str>, Option<&str>, &str) {
    (d.catalog.as_deref(), d.database.as_deref(), &d.table)
}

/// The `columnLineage` facet of a written dataset. An input column that access control
/// masked for this query is marked `masking`.
fn column_lineage_facet(
    lineage: &ColumnLineage,
    record: &QueryRecord,
    cluster: Option<&ClusterConfig>,
) -> Value {
    // `table.column` / `schema.table.column`, as the access-control guard records them.
    let masked: HashSet<String> = record
        .guard_actions
        .iter()
        .filter_map(|g| g.metadata.as_ref()?.get("masked_columns"))
        .flat_map(|list| list.split(','))
        .filter(|c| !c.is_empty())
        .map(str::to_lowercase)
        .collect();
    let field = |c: &InputColumn, kind: &str| {
        let (namespace, name) = dataset_name(
            (c.catalog.as_deref(), c.schema.as_deref(), &c.table),
            record,
            cluster,
        );
        let bare = format!("{}.{}", c.table, c.column).to_lowercase();
        let qualified = c
            .schema
            .as_ref()
            .map(|s| format!("{s}.{bare}").to_lowercase());
        let masking = masked.contains(&bare) || qualified.is_some_and(|q| masked.contains(&q));
        json!({
            "namespace": namespace,
            "name": name,
            "field": c.column,
            "transformations": [{ "type": kind, "subtype": c.subtype, "masking": masking }],
        })
    };
    let fields: Map<String, Value> = lineage
        .fields
        .iter()
        .map(|(column, inputs)| {
            let inputs: Vec<Value> = inputs.iter().map(|c| field(c, "DIRECT")).collect();
            (column.clone(), json!({ "inputFields": inputs }))
        })
        .collect();
    let indirect: Vec<Value> = lineage
        .indirect
        .iter()
        .map(|c| field(c, "INDIRECT"))
        .collect();
    facet(
        "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
        json!({ "fields": fields, "dataset": indirect }),
    )
}

/// The `parent` facet: the caller's own run from query tags, else the agent conversation
/// the query belongs to.
fn parent(record: &QueryRecord) -> Option<Value> {
    let schema =
        "https://openlineage.io/spec/facets/1-2-0/ParentRunFacet.json#/$defs/ParentRunFacet";
    if let (Some(run), Some(namespace), Some(name)) = (
        tag(record, TAG_PARENT_RUN_ID),
        tag(record, TAG_PARENT_JOB_NAMESPACE),
        tag(record, TAG_PARENT_JOB_NAME),
    ) {
        return Some(facet(
            schema,
            json!({
                "run": { "runId": run_id(run) },
                "job": { "namespace": namespace, "name": name },
            }),
        ));
    }
    let (Some(agent), Some(conversation)) = (&record.agent_id, &record.conversation_id) else {
        return None;
    };
    Some(facet(
        schema,
        json!({
            "run": { "runId": run_id(conversation) },
            "job": { "namespace": "queryflux-agents", "name": format!("agent/{agent}") },
        }),
    ))
}

/// A dataset's OpenLineage `(namespace, name)`. A catalog mapped in the cluster's
/// `lineageCatalogs` gives the catalog's own namespace and `schema.table`, so engines sharing
/// that catalog name its tables alike. Anything else is named after the engine:
/// `<engine>://<host:port>` and `catalog.schema.table`.
fn dataset_name(
    (catalog, database, table): (Option<&str>, Option<&str>, &str),
    record: &QueryRecord,
    cluster: Option<&ClusterConfig>,
) -> (String, String) {
    let catalog = catalog.or(record.catalog.as_deref());
    let database = database.or(record.database.as_deref());
    let join = |parts: &[Option<&str>]| {
        parts
            .iter()
            .flatten()
            .filter(|p| !p.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join(".")
    };
    let mapped = catalog.and_then(|c| {
        cluster?
            .lineage_catalogs
            .iter()
            .find(|(alias, _)| alias.eq_ignore_ascii_case(c))
            .map(|(_, ns)| ns.clone())
    });
    match mapped {
        Some(namespace) => (namespace, join(&[database, Some(table)])),
        None => (
            engine_namespace(record, cluster),
            join(&[catalog, database, Some(table)]),
        ),
    }
}

/// The engine's namespace, after the OpenLineage naming spec where it has one.
// ponytail: host:port from `endpoint` for every engine, plus Athena and BigQuery. Add the
// spec's other engine forms (`snowflake://<account>`, `databricks://<workspace>`) when
// someone needs to join with those engines' own OpenLineage integrations.
fn engine_namespace(record: &QueryRecord, cluster: Option<&ClusterConfig>) -> String {
    let engine = serde_json::to_value(&record.engine_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_lowercase))
        .unwrap_or_else(|| "unknown".to_string());
    let scheme = match engine.as_str() {
        "mssql" => "sqlserver",
        "duckdbhttp" => "duckdb",
        other => other,
    };
    match scheme {
        "bigquery" => return "bigquery".to_string(),
        "athena" => {
            if let Some(region) = cluster.and_then(|c| c.region.as_deref()) {
                return format!("awsathena://athena.{region}.amazonaws.com");
            }
        }
        _ => {}
    }
    let authority = cluster
        .and_then(|c| c.endpoint.as_deref())
        .and_then(|e| reqwest::Url::parse(e).ok())
        .and_then(|u| {
            let host = u.host_str()?.to_string();
            Some(match u.port_or_known_default() {
                Some(port) => format!("{host}:{port}"),
                None => host,
            })
        })
        .unwrap_or_else(|| record.cluster_name.0.clone());
    format!("{scheme}://{authority}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, SqlDialect,
    };

    fn dataset(catalog: Option<&str>, database: &str, table: &str, is_write: bool) -> QueryDataset {
        QueryDataset {
            catalog: catalog.map(String::from),
            database: Some(database.into()),
            table: table.into(),
            columns: vec![],
            is_write,
            column_lineage: None,
        }
    }

    fn record(engine: EngineType, cluster: &str) -> QueryRecord {
        QueryRecord {
            proxy_query_id: "6f1c1f0e-3c1a-4c55-9a43-1d6a0f3c2b10".into(),
            backend_query_id: Some("20261021_000000_00001_abcde".into()),
            cluster_group: ClusterGroupName("analytics".into()),
            cluster_name: ClusterName(cluster.into()),
            cluster_group_config_id: None,
            cluster_config_id: None,
            engine_type: engine,
            frontend_protocol: FrontendProtocol::TrinoHttp,
            source_dialect: SqlDialect::Trino,
            target_dialect: SqlDialect::Trino,
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: false,
            translation: None,
            translated_sql: None,
            user: Some("alice".into()),
            catalog: Some("iceberg".into()),
            database: Some("sales".into()),
            sql_preview: String::new(),
            status: QueryStatus::Success,
            routing_trace: None,
            queue_duration_ms: 3,
            execution_duration_ms: 120,
            rows_returned: Some(10),
            error_message: None,
            created_at: chrono::DateTime::parse_from_rfc3339("2026-10-21T10:00:00Z")
                .unwrap()
                .into(),
            engine_stats: None,
            query_tags: Default::default(),
            query_hash: None,
            query_parameterized_hash: Some(0x1234),
            translated_query_hash: None,
            digest_text: Some("INSERT INTO mart.daily SELECT * FROM orders WHERE id > ?".into()),
            translated_digest_text: None,
            agent_id: None,
            conversation_id: None,
            step_index: None,
            tool_call_id: None,
            query_intent: None,
            guard_actions: vec![],
            was_guard_blocked: false,
            cache_hit: false,
            worker: None,
            datasets: vec![
                QueryDataset {
                    columns: vec!["id".into(), "amount".into()],
                    ..dataset(None, "sales", "orders", false)
                },
                dataset(None, "mart", "daily", true),
            ],
            sql: Some("INSERT INTO mart.daily SELECT * FROM orders WHERE id > 5".into()),
        }
    }

    fn trino() -> ClusterConfig {
        ClusterConfig {
            endpoint: Some("https://trino.example.com:8443".into()),
            lineage_catalogs: [(
                "iceberg".to_string(),
                "iceberg+rest://polaris.example.com/api/catalog".to_string(),
            )]
            .into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_write_becomes_a_complete_event_with_mapped_catalog_names() {
        let event = run_event(
            &record(EngineType::Trino, "trino-prod"),
            "queryflux-prod",
            HistoryRedaction::MaskLiterals,
            Some(&trino()),
        )
        .unwrap();
        let polaris = "iceberg+rest://polaris.example.com/api/catalog";
        assert_eq!(event["eventType"], "COMPLETE");
        assert_eq!(event["eventTime"], "2026-10-21T10:00:00+00:00");
        assert_eq!(
            event["run"]["runId"],
            "6f1c1f0e-3c1a-4c55-9a43-1d6a0f3c2b10"
        );
        assert_eq!(event["job"]["namespace"], "queryflux-prod");
        assert_eq!(event["job"]["name"], "query.0000000000001234");
        assert_eq!(
            event["job"]["facets"]["sql"]["query"],
            "INSERT INTO mart.daily SELECT * FROM orders WHERE id > ?",
            "SQL is masked by default"
        );
        assert_eq!(event["inputs"][0]["namespace"], polaris);
        assert_eq!(event["inputs"][0]["name"], "sales.orders");
        assert_eq!(
            event["inputs"][0]["facets"]["queryflux_columnsRead"]["fields"],
            json!(["id", "amount"])
        );
        assert_eq!(event["outputs"][0]["namespace"], polaris);
        assert_eq!(event["outputs"][0]["name"], "mart.daily");
        assert_eq!(event["run"]["facets"]["queryflux"]["cluster"], "trino-prod");
        assert!(event["run"]["facets"]["parent"].is_null());
        for facet in [
            &event["run"]["facets"]["queryflux"],
            &event["job"]["facets"]["sql"],
        ] {
            assert!(facet["_producer"].is_string() && facet["_schemaURL"].is_string());
        }
    }

    /// Two engines reading one table through a shared catalog name it alike.
    #[test]
    fn engines_sharing_a_catalog_name_its_tables_alike() {
        let snowflake = ClusterConfig {
            lineage_catalogs: [(
                "ANALYTICS".to_string(),
                "iceberg+rest://polaris.example.com/api/catalog".to_string(),
            )]
            .into(),
            ..Default::default()
        };
        let mut sf = record(EngineType::Snowflake, "snowflake-prod");
        sf.catalog = Some("analytics".into());
        let a = run_event(
            &record(EngineType::Trino, "trino-prod"),
            "qf",
            HistoryRedaction::Full,
            Some(&trino()),
        )
        .unwrap();
        let b = run_event(&sf, "qf", HistoryRedaction::Full, Some(&snowflake)).unwrap();
        assert_eq!(a["inputs"][0]["namespace"], b["inputs"][0]["namespace"]);
        assert_eq!(a["inputs"][0]["name"], b["inputs"][0]["name"]);
    }

    #[test]
    fn unmapped_catalogs_are_named_after_the_engine() {
        let mut r = record(EngineType::Trino, "trino-prod");
        r.catalog = Some("hive".into());
        let event = run_event(&r, "qf", HistoryRedaction::Full, Some(&trino())).unwrap();
        assert_eq!(
            event["inputs"][0]["namespace"],
            "trino://trino.example.com:8443"
        );
        assert_eq!(event["inputs"][0]["name"], "hive.sales.orders");

        let athena = ClusterConfig {
            region: Some("eu-west-1".into()),
            ..Default::default()
        };
        let event = run_event(
            &record(EngineType::Athena, "athena"),
            "qf",
            HistoryRedaction::Full,
            Some(&athena),
        )
        .unwrap();
        assert_eq!(
            event["inputs"][0]["namespace"],
            "awsathena://athena.eu-west-1.amazonaws.com"
        );
    }

    #[test]
    fn sql_and_errors_follow_the_redaction_policy() {
        let mut r = record(EngineType::Trino, "t");
        r.status = QueryStatus::Failed;
        r.error_message = Some("bad value 'secret' at 42".into());
        let full = run_event(&r, "qf", HistoryRedaction::Full, None).unwrap();
        assert_eq!(full["eventType"], "FAIL");
        assert_eq!(
            full["job"]["facets"]["sql"]["query"],
            "INSERT INTO mart.daily SELECT * FROM orders WHERE id > 5"
        );
        assert_eq!(
            full["run"]["facets"]["errorMessage"]["message"],
            "bad value 'secret' at 42"
        );
        let masked = run_event(&r, "qf", HistoryRedaction::MaskLiterals, None).unwrap();
        assert_eq!(
            masked["run"]["facets"]["errorMessage"]["message"],
            "bad value ? at ?"
        );
        let metadata = run_event(&r, "qf", HistoryRedaction::Metadata, None).unwrap();
        assert!(metadata["job"]["facets"]["sql"].is_null());
        assert_eq!(
            metadata["run"]["facets"]["errorMessage"]["message"],
            "query failed"
        );
    }

    #[test]
    fn nothing_to_report_means_no_event() {
        let mut r = record(EngineType::Trino, "t");
        r.was_guard_blocked = true;
        assert!(run_event(&r, "qf", HistoryRedaction::Full, None).is_none());
        let mut r = record(EngineType::Trino, "t");
        r.datasets.clear();
        assert!(run_event(&r, "qf", HistoryRedaction::Full, None).is_none());
        let mut r = record(EngineType::Trino, "t");
        r.status = QueryStatus::Denied;
        assert!(run_event(&r, "qf", HistoryRedaction::Full, None).is_none());
        let mut r = record(EngineType::Trino, "t");
        r.status = QueryStatus::Cancelled;
        assert_eq!(
            run_event(&r, "qf", HistoryRedaction::Full, None).unwrap()["eventType"],
            "ABORT"
        );
    }

    #[test]
    fn callers_name_the_job_and_parent_through_tags_else_the_agent_conversation() {
        let mut r = record(EngineType::Trino, "t");
        for (k, v) in [
            (TAG_JOB, "dbt.mart.daily"),
            (TAG_PARENT_RUN_ID, "0b6b3f62-2e7b-4c5e-8a2b-6c1d1f0e9a77"),
            (TAG_PARENT_JOB_NAMESPACE, "dbt-prod"),
            (TAG_PARENT_JOB_NAME, "mart.daily"),
        ] {
            r.query_tags.insert(k.into(), Some(v.into()));
        }
        r.agent_id = Some("analyst-bot".into());
        r.conversation_id = Some("conv-42".into());
        let event = run_event(&r, "qf", HistoryRedaction::Full, None).unwrap();
        assert_eq!(event["job"]["name"], "dbt.mart.daily");
        let parent = &event["run"]["facets"]["parent"];
        assert_eq!(
            parent["run"]["runId"],
            "0b6b3f62-2e7b-4c5e-8a2b-6c1d1f0e9a77"
        );
        assert_eq!(parent["job"]["name"], "mart.daily");

        r.query_tags.clear();
        let event = run_event(&r, "qf", HistoryRedaction::Full, None).unwrap();
        let parent = &event["run"]["facets"]["parent"];
        assert_eq!(parent["job"]["name"], "agent/analyst-bot");
        assert_eq!(parent["run"]["runId"], run_id("conv-42"));
        assert!(uuid::Uuid::parse_str(parent["run"]["runId"].as_str().unwrap()).is_ok());
    }

    #[test]
    fn a_written_table_carries_column_lineage_with_masking() {
        use queryflux_persistence::GuardAction;
        let input = |table: &str, column: &str, subtype: &str| InputColumn {
            catalog: None,
            schema: Some("sales".into()),
            table: table.into(),
            column: column.into(),
            subtype: subtype.into(),
        };
        let mut r = record(EngineType::Trino, "trino-prod");
        r.datasets[1].column_lineage = Some(ColumnLineage {
            fields: [
                (
                    "revenue".to_string(),
                    vec![input("orders", "amount", "AGGREGATION")],
                ),
                (
                    "email".to_string(),
                    vec![input("customers", "email", "IDENTITY")],
                ),
            ]
            .into(),
            indirect: vec![input("orders", "status", "FILTER")],
        });
        r.guard_actions = vec![GuardAction {
            guard: "access_control".into(),
            action: "rewrite".into(),
            reason: None,
            code: None,
            metadata: Some(
                [(
                    "masked_columns".to_string(),
                    "sales.customers.email".to_string(),
                )]
                .into(),
            ),
        }];
        let event = run_event(&r, "qf", HistoryRedaction::Full, Some(&trino())).unwrap();
        let facet = &event["outputs"][0]["facets"]["columnLineage"];
        let revenue = &facet["fields"]["revenue"]["inputFields"][0];
        assert_eq!(
            revenue["namespace"],
            "iceberg+rest://polaris.example.com/api/catalog"
        );
        assert_eq!(revenue["name"], "sales.orders");
        assert_eq!(revenue["field"], "amount");
        assert_eq!(
            revenue["transformations"][0],
            json!({ "type": "DIRECT", "subtype": "AGGREGATION", "masking": false })
        );
        assert_eq!(
            facet["fields"]["email"]["inputFields"][0]["transformations"][0]["masking"],
            true
        );
        assert_eq!(
            facet["dataset"][0]["transformations"][0]["type"],
            "INDIRECT"
        );
        assert_eq!(facet["dataset"][0]["field"], "status");
    }

    /// A consumer that is down keeps events spooled; one that rejects an event drops it.
    #[tokio::test]
    async fn delivery_retries_outages_and_drops_rejected_events() {
        use axum::http::StatusCode;
        use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

        let status = Arc::new(AtomicU16::new(503));
        let received = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new().route(
            "/api/v1/lineage",
            axum::routing::post({
                let (status, received) = (status.clone(), received.clone());
                move |headers: axum::http::HeaderMap, axum::Json(_event): axum::Json<Value>| async move {
                    assert_eq!(headers["authorization"], "Bearer k");
                    let code = StatusCode::from_u16(status.load(Ordering::SeqCst)).unwrap();
                    if code.is_success() {
                        received.fetch_add(1, Ordering::SeqCst);
                    }
                    code
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config: OpenLineageConfig =
            serde_yaml::from_str(&format!("url: http://{addr}/\napiKey: k\n")).unwrap();
        let live = Arc::new(RwLock::new(crate::live_config::empty_live_config()));
        let store = OpenLineageStore::new(&config, live);
        store
            .record_query(record(EngineType::Trino, "t"))
            .await
            .unwrap();
        store
            .record_query(record(EngineType::Trino, "t"))
            .await
            .unwrap();

        assert!(store.flush().await.is_err());
        assert_eq!(store.spool.len(), 2, "kept for retry");

        status.store(200, Ordering::SeqCst);
        store.flush().await.unwrap();
        assert_eq!((received.load(Ordering::SeqCst), store.spool.len()), (2, 0));

        status.store(400, Ordering::SeqCst);
        store
            .record_query(record(EngineType::Trino, "t"))
            .await
            .unwrap();
        store.flush().await.unwrap();
        assert_eq!(store.spool.len(), 0, "a rejected event is not retried");
    }
}
