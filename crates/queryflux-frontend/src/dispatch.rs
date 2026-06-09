use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use queryflux_auth::{AuthContext, QueryCredentials};
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::native_result::NativeResultChunk;
use queryflux_core::params::{interpolate_params, QueryParams};
use queryflux_core::tags::{merge_tags, QueryTags};
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{
        ClusterGroupName, ClusterName, EngineType, ExecutingQuery, FrontendProtocol, ProxyQueryId,
        QueryEngineStats, QueryExecution, QueryStats, QueryStatus, QueuedQuery,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::trino::api::TrinoResponse;
use queryflux_engine_adapters::{AdapterKind, AsyncAdapter, ConnectionFormat, SyncAdapter};
use queryflux_guardrails::{GuardContext, GuardLayer};
use queryflux_metrics::MetricsStore;
use queryflux_translation::SchemaContext;

use tracing::{debug, info, warn};

use crate::state::{AppState, QueryContext, QueryOutcome};

// ---------------------------------------------------------------------------
// ResultSink — universal streaming output interface
// ---------------------------------------------------------------------------

/// Implemented by each frontend protocol to receive query results.
///
/// `execute_to_sink` calls these in order:
///   on_schema (once) → on_batch (N times) → on_complete (once)
///   or on_error (once on failure).
///
/// Text-protocol sinks (MySQL, Postgres) format values as strings.
/// Arrow-native sinks (Flight SQL) pass RecordBatch through without inspection.
#[async_trait]
pub trait ResultSink: Send {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()>;
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()>;
    async fn on_error(&mut self, message: &str) -> Result<()>;

    /// Receive a native result chunk (non-Arrow path).
    ///
    /// Called by `execute_native_to_sink` only when
    /// `adapter.connection_format().matches_frontend(protocol)` is true — i.e. only for
    /// sinks whose frontend protocol matches the backend's connection format.
    /// The default returns `Err` to surface misconfiguration during development.
    async fn on_native_chunk(&mut self, _chunk: &NativeResultChunk) -> Result<()> {
        Err(queryflux_core::error::QueryFluxError::Engine(
            "on_native_chunk not implemented for this sink".to_string(),
        ))
    }
}

/// Protocol-agnostic result of dispatching a query to an async (Trino) backend.
pub enum DispatchOutcome {
    /// No cluster capacity available — query was queued. Client should poll `queued_next_uri`.
    Queued { queued_next_uri: String },
    /// Query submitted to Trino; executing state stored in persistence.
    /// Client should poll `proxy_next_uri`. `initial_body` may contain the first response page.
    Async {
        initial_body: Option<Bytes>,
        proxy_next_uri: Option<String>,
    },
}

/// Rewrite a Trino-origin URL to point to QueryFlux instead, keeping the full path.
/// `http://trino:8080/v1/statement/executing/{id}/{token}` →
/// `http://queryflux:9000/v1/statement/executing/{id}/{token}`
///
/// Any instance can then reconstruct the Trino URL by looking up the stored
/// `trino_endpoint` and re-joining it with the path.
async fn cluster_db_ids(
    mgr: &std::sync::Arc<dyn ClusterGroupManager>,
    group: &ClusterGroupName,
    cluster: &ClusterName,
) -> (Option<i64>, Option<i64>) {
    match mgr.cluster_state(group, cluster).await {
        Ok(Some(s)) => (s.cluster_group_config_id, s.cluster_config_id),
        _ => (None, None),
    }
}

pub fn rewrite_trino_uri(trino_uri: &str, external_address: &str) -> String {
    // Find the path portion starting at /v1/
    if let Some(path_start) = trino_uri.find("/v1/") {
        format!(
            "{}{}",
            external_address.trim_end_matches('/'),
            &trino_uri[path_start..]
        )
    } else {
        trino_uri.to_string()
    }
}

/// Core dispatch logic shared across all frontend protocol implementations.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_query(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    params: QueryParams,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    already_queued: bool,
    sequence: u64,
    auth_ctx: &AuthContext,
) -> Result<DispatchOutcome> {
    // Authorization check — first gate before any resource acquisition.
    // Phase 1: AllowAllAuthorization always returns true (no behavior change).
    if !state.authorization.check(auth_ctx, &group.0).await {
        return Err(QueryFluxError::Unauthorized(format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        )));
    }

    // Clone the manager, group translation fixups, default tags, and guard chains in one snapshot.
    let (cluster_manager, group_fixups, group_default_tags, guard_chain, group_guard_chain) = {
        let live = state.live.read().await;
        (
            live.cluster_manager.clone(),
            live.group_translation_scripts
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
            live.group_default_tags
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
            live.guard_chain.clone(),
            live.group_guard_chains.get(&group.0).cloned(),
        )
    };
    let effective_tags = merge_tags(&group_default_tags, &session.tags().clone());

    let cluster_name = match cluster_manager.acquire_cluster(&group).await? {
        Some(c) => c,
        None => {
            let uri = persist_queued_query(
                state,
                query_id,
                sql,
                session,
                protocol,
                group,
                already_queued,
                sequence,
            )
            .await?;
            return Ok(DispatchOutcome::Queued {
                queued_next_uri: uri,
            });
        }
    };

    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&cluster_manager, &group, &cluster_name).await;

    state.metrics.on_query_started(&group.0, &cluster_name.0);

    let cluster_cfg = state.cluster_config_cloned(&cluster_name.0).await;
    let credentials = state
        .identity_resolver
        .resolve(auth_ctx, cluster_cfg.as_ref())
        .await;

    let adapter_kind = match state.adapter(&cluster_name.0).await {
        Some(a) => a,
        None => {
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(QueryFluxError::Engine(format!(
                "No adapter for {group}/{cluster_name}"
            )));
        }
    };

    let src_dialect = protocol.default_dialect();
    let tgt_dialect = adapter_kind.translation_target_dialect();
    let engine_type = adapter_kind.engine_type();
    let original_sql = sql.clone();
    let sql = match state
        .translation
        .maybe_translate(
            &sql,
            &src_dialect,
            &tgt_dialect,
            &SchemaContext::default(),
            &group_fixups,
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(id = %query_id, "Translation error: {e}");
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(e);
        }
    };
    let was_translated = sql != original_sql;
    if was_translated {
        info!(id = %query_id, src = ?src_dialect, tgt = ?tgt_dialect, "SQL translated");
    }

    // Fallback interpolation for async adapters that don't support native params.
    let (sql, effective_params) = if !params.is_empty() {
        (interpolate_params(&sql, &params, &tgt_dialect)?, vec![])
    } else {
        (sql, params)
    };

    // Guard chain: runs after translation (SQL is final), before engine submission.
    // Global guards run first; per-group guards are appended after.
    let resolved_agent_ctx = session.resolved_agent_context();
    let mut all_guard_actions: Vec<queryflux_persistence::GuardAction> = Vec::new();

    let guard_ctx = GuardContext {
        sql: &original_sql,
        translated_sql: &sql,
        engine_type: &engine_type,
        cluster_group: &group,
        user: session.user(),
        agent_context: resolved_agent_ctx.as_ref(),
        query_tags: &effective_tags,
    };

    macro_rules! guard_deny {
        ($actions:expr) => {{
            let deny_reason = $actions
                .iter()
                .find(|a| a.action == "deny")
                .and_then(|a| a.reason.clone())
                .unwrap_or_else(|| "query blocked by guardrail".to_string());
            let ctx = QueryContext {
                query_id: query_id.clone(),
                sql: original_sql.clone(),
                session: session.clone(),
                protocol: protocol.clone(),
                group: group.clone(),
                cluster: cluster_name.clone(),
                cluster_group_config_id,
                cluster_config_id,
                engine_type: engine_type.clone(),
                src_dialect: src_dialect.clone(),
                tgt_dialect: tgt_dialect.clone(),
                was_translated,
                translated_sql: if was_translated {
                    Some(sql.clone())
                } else {
                    None
                },
                query_tags: effective_tags.clone(),
                query_params: vec![],
                agent_context: resolved_agent_ctx.clone(),
            };
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id: None,
                    status: QueryStatus::Failed,
                    execution_ms: 0,
                    rows: None,
                    error: Some(deny_reason.clone()),
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: $actions,
                    was_guard_blocked: true,
                },
            );
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(QueryFluxError::Engine(deny_reason));
        }};
    }

    if let Some(chain) = &guard_chain {
        let (actions, was_blocked) = chain.run(&guard_ctx, GuardLayer::Plan).await;
        all_guard_actions.extend(actions);
        if was_blocked {
            guard_deny!(std::mem::take(&mut all_guard_actions));
        }
    }

    if let Some(chain) = &group_guard_chain {
        let (actions, was_blocked) = chain.run(&guard_ctx, GuardLayer::Plan).await;
        all_guard_actions.extend(actions);
        if was_blocked {
            guard_deny!(std::mem::take(&mut all_guard_actions));
        }
    }

    // Serialize guard actions for storage in ExecutingQuery (retrieved at poll time).
    let submitted_guard_actions: Vec<serde_json::Value> = all_guard_actions
        .iter()
        .filter_map(|a| serde_json::to_value(a).ok())
        .collect();

    match adapter_kind {
        AdapterKind::Async(adapter) => {
            let execution = match adapter
                .submit_query(
                    &sql,
                    &session,
                    &credentials,
                    &effective_tags,
                    &effective_params,
                )
                .await
            {
                Ok(e) => e,
                Err(e) => {
                    state.metrics.on_query_finished(&group.0, &cluster_name.0);
                    let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
                    warn!(id = %query_id, "Submit error: {e}");
                    return Err(e);
                }
            };

            if already_queued {
                let _ = state.persistence.delete_queued(&query_id).await;
            }

            let QueryExecution::Async {
                backend_query_id,
                next_uri,
                initial_body,
            } = execution;
            let now = Utc::now();
            let executing = ExecutingQuery {
                id: query_id.clone(),
                sql,
                translated_sql: if was_translated {
                    Some(original_sql)
                } else {
                    None
                },
                cluster_group: group.clone(),
                cluster_name: cluster_name.clone(),
                cluster_group_config_id,
                cluster_config_id,
                backend_query_id: backend_query_id.clone(),
                trino_endpoint: adapter.base_url().to_string(),
                creation_time: now,
                last_accessed: now,
                query_tags: effective_tags,
                agent_context: resolved_agent_ctx,
                submitted_guard_actions,
                was_guard_blocked: false,
            };
            let _ = state.persistence.upsert(executing.clone()).await;
            info!(id = %query_id, backend = %backend_query_id, cluster = %cluster_name, "Query submitted (async)");

            if next_uri.is_none() {
                if let Some(ref ib) = initial_body {
                    if engine_type == EngineType::Trino {
                        finalize_trino_async_terminal_on_submit(
                            state,
                            &cluster_manager,
                            &executing,
                            &adapter,
                            &session,
                            protocol,
                            ib,
                        )
                        .await;
                    }
                }
            }

            let proxy_next_uri = next_uri
                .as_deref()
                .map(|uri| rewrite_trino_uri(uri, &state.external_address));
            Ok(DispatchOutcome::Async {
                initial_body,
                proxy_next_uri,
            })
        }
        AdapterKind::Sync(sync_adapter) => {
            if already_queued {
                let _ = state.persistence.delete_queued(&query_id).await;
            }

            info!(id = %query_id, cluster = %cluster_name, "Query executing (sync via dispatch)");
            let start = Instant::now();

            let mut sink = crate::trino_http::result_sink::TrinoHttpResultSink::new(&query_id.0);

            debug!(id = %query_id, "sync dispatch: calling execute_as_arrow");
            let (status, rows, error) = match sync_adapter
                .execute_as_arrow(
                    &sql,
                    &session,
                    &credentials,
                    &effective_tags,
                    &effective_params,
                )
                .await
            {
                Ok(execution) => {
                    debug!(id = %query_id, "sync dispatch: execute_as_arrow returned stream");
                    let mut stream = execution.stream;
                    let mut schema_sent = false;
                    let mut total_rows: u64 = 0;
                    let mut stream_err: Option<String> = None;
                    let mut batch_count: u64 = 0;

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(batch) => {
                                if !schema_sent {
                                    debug!(id = %query_id, cols = batch.num_columns(), "sync dispatch: on_schema");
                                    let _ = sink.on_schema(batch.schema_ref()).await;
                                    schema_sent = true;
                                }
                                total_rows += batch.num_rows() as u64;
                                batch_count += 1;
                                debug!(id = %query_id, batch = batch_count, rows = batch.num_rows(), "sync dispatch: on_batch");
                                let _ = sink.on_batch(&batch).await;
                                debug!(id = %query_id, batch = batch_count, "sync dispatch: on_batch done");
                            }
                            Err(e) => {
                                stream_err = Some(e.to_string());
                                let _ = sink.on_error(stream_err.as_ref().unwrap()).await;
                                break;
                            }
                        }
                    }

                    if !schema_sent {
                        debug!(id = %query_id, "sync dispatch: empty schema");
                        let _ = sink.on_schema(&Schema::empty()).await;
                    }

                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    let stats = QueryStats {
                        execution_duration_ms: elapsed_ms,
                        rows_returned: total_rows,
                        ..Default::default()
                    };
                    debug!(id = %query_id, total_rows, "sync dispatch: on_complete");
                    let _ = sink.on_complete(&stats).await;

                    if let Some(err_msg) = stream_err {
                        (QueryStatus::Failed, None, Some(err_msg))
                    } else {
                        (QueryStatus::Success, Some(total_rows), None)
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    warn!(id = %query_id, cluster = %cluster_name, "Sync execute_as_arrow failed: {msg}");
                    let _ = sink.on_error(&msg).await;
                    (QueryStatus::Failed, None, Some(msg))
                }
            };

            let elapsed_ms = start.elapsed().as_millis() as u64;

            let ctx = QueryContext {
                query_id,
                sql: original_sql,
                session,
                protocol,
                group: group.clone(),
                cluster: cluster_name.clone(),
                cluster_group_config_id,
                cluster_config_id,
                engine_type,
                src_dialect,
                tgt_dialect,
                was_translated,
                translated_sql: if was_translated { Some(sql) } else { None },
                query_tags: effective_tags,
                query_params: effective_params,
                agent_context: resolved_agent_ctx,
            };
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id: None,
                    status,
                    execution_ms: elapsed_ms,
                    rows,
                    error,
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: all_guard_actions,
                    was_guard_blocked: false,
                },
            );

            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;

            debug!(id = %ctx.query_id, "sync dispatch: calling into_bytes");
            let body_bytes = sink.into_bytes();
            debug!(id = %ctx.query_id, bytes = body_bytes.len(), "sync dispatch: into_bytes done");
            Ok(DispatchOutcome::Async {
                initial_body: Some(body_bytes),
                proxy_next_uri: None,
            })
        }
    }
}

/// Determine the terminal `QueryOutcome` from a Trino submit response body.
///
/// Parses the body to determine success vs failure. `engine_stats` is passed in
/// from `adapter.terminal_stats_from_body()` — Trino-specific stats parsing lives
/// in the adapter, not here.
///
/// Returns `(outcome, Option<warn_log_message>)`.
fn trino_submit_terminal_outcome(
    body: &Bytes,
    elapsed_ms: u64,
    backend_id: String,
    engine_stats: Option<QueryEngineStats>,
) -> (QueryOutcome, Option<String>) {
    let trino_resp: TrinoResponse = match serde_json::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            let warn_msg = format!(
                "trino submit terminal body JSON parse failed: {e}; releasing cluster + clearing persistence"
            );
            return (
                QueryOutcome {
                    backend_query_id: Some(backend_id),
                    status: QueryStatus::Failed,
                    execution_ms: elapsed_ms,
                    rows: None,
                    error: Some(format!("failed to parse Trino response: {e}")),
                    routing_trace: None,
                    engine_stats,
                    guard_actions: vec![],
                    was_guard_blocked: false,
                },
                Some(warn_msg),
            );
        }
    };

    let backend_id = Some(backend_id);

    if let Some(err) = &trino_resp.error {
        (
            QueryOutcome {
                backend_query_id: backend_id,
                status: QueryStatus::Failed,
                execution_ms: elapsed_ms,
                rows: None,
                error: Some(err.message.clone()),
                routing_trace: None,
                engine_stats,
                guard_actions: vec![],
                was_guard_blocked: false,
            },
            None,
        )
    } else if trino_resp.stats.state == "FAILED" {
        (
            QueryOutcome {
                backend_query_id: backend_id,
                status: QueryStatus::Failed,
                execution_ms: elapsed_ms,
                rows: None,
                error: Some("Trino query FAILED".to_string()),
                routing_trace: None,
                engine_stats,
                guard_actions: vec![],
                was_guard_blocked: false,
            },
            None,
        )
    } else {
        (
            QueryOutcome {
                backend_query_id: backend_id,
                status: QueryStatus::Success,
                execution_ms: elapsed_ms,
                rows: None,
                error: None,
                routing_trace: None,
                engine_stats,
                guard_actions: vec![],
                was_guard_blocked: false,
            },
            None,
        )
    }
}

/// Trino may return `FINISHED` with no `nextUri` on the initial POST `/v1/statement` response.
/// Clients then never call GET `/v1/statement/...`, so `get_executing_statement` never runs —
/// mirror its metrics, `record_query`, and persistence cleanup here.
///
/// Collapsed from 4 branches (including JSON parse error) to a single `record_query` call.
async fn finalize_trino_async_terminal_on_submit(
    state: &Arc<AppState>,
    cluster_manager: &Arc<dyn ClusterGroupManager>,
    executing: &ExecutingQuery,
    adapter: &Arc<dyn AsyncAdapter>,
    session: &SessionContext,
    protocol: FrontendProtocol,
    body: &Bytes,
) {
    let elapsed_ms = (Utc::now() - executing.creation_time)
        .num_milliseconds()
        .max(0) as u64;

    let was_translated = executing.translated_sql.is_some();
    let src_dialect = protocol.default_dialect();
    let ctx = QueryContext {
        query_id: executing.id.clone(),
        sql: executing
            .translated_sql
            .as_deref()
            .unwrap_or(&executing.sql)
            .to_string(),
        session: session.clone(),
        protocol,
        group: executing.cluster_group.clone(),
        cluster: executing.cluster_name.clone(),
        cluster_group_config_id: executing.cluster_group_config_id,
        cluster_config_id: executing.cluster_config_id,
        engine_type: adapter.engine_type(),
        src_dialect,
        tgt_dialect: adapter.translation_target_dialect(),
        was_translated,
        translated_sql: if was_translated {
            Some(executing.sql.clone())
        } else {
            None
        },
        query_tags: executing.query_tags.clone(),
        query_params: vec![],
        agent_context: executing.agent_context.clone(),
    };

    let engine_stats = adapter.terminal_stats_from_body(body);
    let (mut outcome, warn_msg) = trino_submit_terminal_outcome(
        body,
        elapsed_ms,
        executing.backend_query_id.0.clone(),
        engine_stats,
    );

    // Inject guard actions captured at submit time into the final audit record.
    let stored_actions: Vec<queryflux_persistence::GuardAction> = serde_json::from_value(
        serde_json::Value::Array(executing.submitted_guard_actions.clone()),
    )
    .unwrap_or_default();
    if !stored_actions.is_empty() {
        outcome.guard_actions = stored_actions;
        outcome.was_guard_blocked = executing.was_guard_blocked;
    }

    if let Some(msg) = warn_msg {
        warn!(proxy_id = %executing.id, "{msg}");
    }

    state
        .metrics
        .on_query_finished(&executing.cluster_group.0, &executing.cluster_name.0);
    state.record_query(&ctx, outcome);
    let _ = cluster_manager
        .release_cluster(&executing.cluster_group, &executing.cluster_name)
        .await;
    let _ = state.persistence.delete(&executing.backend_query_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn persist_queued_query(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    _already_stored: bool,
    sequence: u64,
) -> Result<String> {
    let now = Utc::now();
    let queued = QueuedQuery {
        id: query_id.clone(),
        sql,
        session,
        frontend_protocol: protocol,
        cluster_group: group,
        creation_time: now,
        last_accessed: now,
        sequence,
    };
    let _ = state.persistence.upsert_queued(queued).await;
    let next_seq = sequence + 1;
    Ok(format!(
        "{}/v1/statement/qf/queued/{}/{}",
        state.external_address, query_id, next_seq
    ))
}

// ---------------------------------------------------------------------------
// execute_to_sink — shared Arrow execution driver for non-Trino-HTTP frontends
// ---------------------------------------------------------------------------

/// How long to wait between queue retries (exponential backoff, capped at 2s).
async fn queued_backoff_delay(seq: u64) {
    let ms = (100u64 * (1 << seq.min(4))).min(2000);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

// ---------------------------------------------------------------------------
// ClusterSlotGuard — RAII wrapper ensuring the cluster slot is always released
// ---------------------------------------------------------------------------

/// Holds a cluster slot acquired from the ClusterGroupManager.
/// Releases the slot automatically on drop — even on tokio future cancellation.
///
/// On the normal path, call `release().await` explicitly. On cancellation,
/// the `Drop` impl spawns a best-effort release so the slot is never leaked.
struct ClusterSlotGuard {
    cluster_manager: Arc<dyn ClusterGroupManager>,
    group: ClusterGroupName,
    cluster: ClusterName,
    metrics: Arc<dyn MetricsStore>,
    released: bool,
}

impl ClusterSlotGuard {
    fn new(
        cluster_manager: Arc<dyn ClusterGroupManager>,
        group: ClusterGroupName,
        cluster: ClusterName,
        metrics: Arc<dyn MetricsStore>,
    ) -> Self {
        Self {
            cluster_manager,
            group,
            cluster,
            metrics,
            released: false,
        }
    }

    /// Release the slot on the normal path. Idempotent — safe to call twice.
    async fn release(&mut self) {
        if !self.released {
            self.released = true;
            let _ = self
                .cluster_manager
                .release_cluster(&self.group, &self.cluster)
                .await;
            self.metrics
                .on_query_finished(&self.group.0, &self.cluster.0);
        }
    }
}

impl Drop for ClusterSlotGuard {
    fn drop(&mut self) {
        if !self.released {
            // Cancellation path: the future was dropped while holding the slot.
            // Spawn a best-effort release. record_query is not called here —
            // there is no outcome to record.
            let mgr = self.cluster_manager.clone();
            let group = self.group.clone();
            let cluster = self.cluster.clone();
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                let _ = mgr.release_cluster(&group, &cluster).await;
                metrics.on_query_finished(&group.0, &cluster.0);
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Sync execution path — shared by MySQL wire, Postgres wire, Flight SQL
// ---------------------------------------------------------------------------

/// Holds either a native sync adapter or an async adapter that bridges to the sync path.
///
/// Async engines (Trino) implement `execute_as_arrow` internally by driving their own
/// submit+poll loop — allowing MySQL/Postgres clients to query them without needing a
/// separate execution path in dispatch.
enum DispatchAdapter {
    Sync(Arc<dyn SyncAdapter>),
    Async(Arc<dyn AsyncAdapter>),
}

impl DispatchAdapter {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        credentials: &QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
        params: &QueryParams,
    ) -> Result<queryflux_engine_adapters::SyncExecution> {
        match self {
            Self::Sync(a) => {
                a.execute_as_arrow(sql, session, credentials, tags, params)
                    .await
            }
            Self::Async(a) => {
                a.execute_as_arrow(sql, session, credentials, tags, params)
                    .await
            }
        }
    }

    fn supports_native_params(&self) -> bool {
        match self {
            Self::Sync(a) => a.supports_native_params(),
            Self::Async(a) => a.supports_native_params(),
        }
    }

    fn engine_type(&self) -> queryflux_core::query::EngineType {
        match self {
            Self::Sync(a) => a.engine_type(),
            Self::Async(a) => a.engine_type(),
        }
    }

    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        match self {
            Self::Sync(a) => a.translation_target_dialect(),
            Self::Async(a) => a.translation_target_dialect(),
        }
    }

    fn connection_format(&self) -> ConnectionFormat {
        match self {
            Self::Sync(a) => a.connection_format(),
            Self::Async(a) => a.connection_format(),
        }
    }
}

/// Everything resolved before execution begins on the sync path.
/// Holds the cluster slot, resolved credentials, translated SQL, and query context.
struct SyncQuerySetup {
    adapter: DispatchAdapter,
    /// SQL to send to the adapter: translated + params interpolated when the adapter
    /// does not support native parameter binding.
    translated: String,
    start: Instant,
    /// Holds the acquired cluster slot — released on drop or via `slot.release().await`.
    slot: ClusterSlotGuard,
    /// Fully-built context for record_query — all strings owned.
    ctx: QueryContext,
    credentials: QueryCredentials,
    /// Typed parameters — empty when the adapter interpolated them into `translated`.
    params: QueryParams,
    /// Guard actions collected by the guard chain (allow/warn). Merged into QueryOutcome.
    guard_actions: Vec<queryflux_persistence::GuardAction>,
}

/// The outcome of executing a sync query — everything record_query needs.
struct SyncOutcome {
    status: QueryStatus,
    rows: Option<u64>,
    error: Option<String>,
    elapsed_ms: u64,
    /// Engine-reported execution stats received via `SyncExecution.stats` after stream exhaustion.
    /// `None` for engines that do not expose structured stats (DuckDB, StarRocks today).
    engine_stats: Option<QueryEngineStats>,
}

impl From<SyncOutcome> for QueryOutcome {
    fn from(o: SyncOutcome) -> QueryOutcome {
        QueryOutcome {
            backend_query_id: None,
            status: o.status,
            execution_ms: o.elapsed_ms,
            rows: o.rows,
            error: o.error,
            routing_trace: None,
            engine_stats: o.engine_stats,
            guard_actions: vec![],
            was_guard_blocked: false,
        }
    }
}

/// Acquire a cluster slot, resolve credentials, translate SQL, and build the full
/// query context. If translation fails, records the failure and releases the slot
/// before returning Err — the caller has no cleanup to do.
///
/// When `params` is non-empty and the selected adapter does not support native parameter
/// binding, the params are interpolated into the translated SQL before returning, and
/// `SyncQuerySetup.params` is left empty so the adapter receives no raw params.
///
/// Failures before slot acquisition (no adapter) return Err without recording.
async fn setup_sync_query(
    state: &Arc<AppState>,
    sql: String,
    params: QueryParams,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    auth_ctx: &AuthContext,
) -> Result<SyncQuerySetup> {
    let query_id = ProxyQueryId::new();

    let (cluster_manager, group_fixups, group_default_tags) = {
        let live = state.live.read().await;
        (
            live.cluster_manager.clone(),
            live.group_translation_scripts
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
            live.group_default_tags
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
        )
    };
    let effective_tags: QueryTags = merge_tags(&group_default_tags, &session.tags().clone());

    // Queue loop: spin until a cluster slot is available.
    let mut seq: u64 = 0;
    let (cluster_name, adapter) = loop {
        match cluster_manager.acquire_cluster(&group).await? {
            Some(name) => match state.adapter(&name.0).await {
                Some(AdapterKind::Sync(a)) => break (name, DispatchAdapter::Sync(a)),
                Some(AdapterKind::Async(a)) => break (name, DispatchAdapter::Async(a)),
                None => {
                    let _ = cluster_manager.release_cluster(&group, &name).await;
                    return Err(QueryFluxError::Engine(format!(
                        "No adapter for {group}/{name}"
                    )));
                }
            },
            None => {
                queued_backoff_delay(seq).await;
                seq += 1;
            }
        }
    };

    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&cluster_manager, &group, &cluster_name).await;

    // Fix Bug A: on_query_started was missing from the sync path.
    state.metrics.on_query_started(&group.0, &cluster_name.0);
    info!(id = %query_id, group = %group, cluster = %cluster_name, "Query executing (sync)");

    let mut slot = ClusterSlotGuard::new(
        cluster_manager.clone(),
        group.clone(),
        cluster_name.clone(),
        state.metrics.clone(),
    );

    let src_dialect = protocol.default_dialect();
    let tgt_dialect = adapter.translation_target_dialect();
    let engine_type = adapter.engine_type();
    let start = Instant::now();

    // Translate SQL. On failure: record the query, release the slot, propagate the error.
    // The caller (execute_to_sink) will notify the sink via on_error.
    let translated = match state
        .translation
        .maybe_translate(
            &sql,
            &src_dialect,
            &tgt_dialect,
            &SchemaContext::default(),
            &group_fixups,
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            let err_msg = e.to_string();
            warn!(id = %query_id, "Translation error: {err_msg}");
            let ctx = QueryContext {
                query_id: query_id.clone(),
                sql: sql.clone(),
                session: session.clone(),
                protocol: protocol.clone(),
                group: group.clone(),
                cluster: cluster_name.clone(),
                cluster_group_config_id,
                cluster_config_id,
                engine_type: engine_type.clone(),
                src_dialect: src_dialect.clone(),
                tgt_dialect: tgt_dialect.clone(),
                was_translated: false,
                translated_sql: None,
                query_tags: effective_tags,
                query_params: params,
                agent_context: session.resolved_agent_context(),
            };
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id: None,
                    status: QueryStatus::Failed,
                    execution_ms: start.elapsed().as_millis() as u64,
                    rows: None,
                    error: Some(err_msg),
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: vec![],
                    was_guard_blocked: false,
                },
            );
            slot.release().await;
            return Err(e);
        }
    };

    let was_translated = translated != sql;

    let cluster_cfg = state.cluster_config_cloned(&cluster_name.0).await;
    let credentials = state
        .identity_resolver
        .resolve(auth_ctx, cluster_cfg.as_ref())
        .await;

    // Fallback interpolation: when the adapter does not support native params,
    // substitute `?` placeholders with typed literals now so the adapter receives
    // a fully-resolved SQL string and empty params.
    let (translated, effective_params) = if !params.is_empty() && !adapter.supports_native_params()
    {
        (
            interpolate_params(&translated, &params, &tgt_dialect)?,
            vec![],
        )
    } else {
        (translated, params)
    };

    let agent_context = session.resolved_agent_context();
    let ctx = QueryContext {
        query_id,
        sql,
        session,
        protocol,
        group,
        cluster: cluster_name,
        cluster_group_config_id,
        cluster_config_id,
        engine_type,
        src_dialect,
        tgt_dialect,
        was_translated,
        translated_sql: if was_translated {
            Some(translated.clone())
        } else {
            None
        },
        query_tags: effective_tags,
        query_params: effective_params.clone(),
        agent_context,
    };

    Ok(SyncQuerySetup {
        adapter,
        translated,
        start,
        slot,
        ctx,
        credentials,
        params: effective_params,
        guard_actions: vec![],
    })
}

/// Run the Arrow stream to completion. Never returns early.
///
/// Returns `(SyncOutcome, sink_result)`:
/// - `SyncOutcome` is always populated — passed to `record_query` by the caller.
/// - `sink_result` is `Ok(())` on success or `Err(e)` when a sink protocol error occurs.
///
/// Fixes Bug B: sink errors (on_schema, on_batch) now produce a SyncOutcome and are
/// included in `record_query` rather than silently dropped.
async fn execute_stream(
    setup: &SyncQuerySetup,
    sink: &mut impl ResultSink,
) -> (SyncOutcome, Result<()>) {
    let elapsed = || setup.start.elapsed().as_millis() as u64;

    let execution = match setup
        .adapter
        .execute_as_arrow(
            &setup.translated,
            &setup.ctx.session,
            &setup.credentials,
            &setup.ctx.query_tags,
            &setup.params,
        )
        .await
    {
        Ok(e) => e,
        Err(e) => {
            let msg = e.to_string();
            warn!(
                id = %setup.ctx.query_id,
                cluster = %setup.ctx.cluster,
                "execute_as_arrow failed: {msg}"
            );
            debug!(
                id = %setup.ctx.query_id,
                sql = %setup.translated,
                "execute_as_arrow failed with translated SQL"
            );
            let outcome = SyncOutcome {
                status: QueryStatus::Failed,
                rows: None,
                error: Some(msg.clone()),
                elapsed_ms: elapsed(),
                engine_stats: None,
            };
            return (outcome, sink.on_error(&msg).await);
        }
    };

    let mut stream = execution.stream;
    let mut stats_rx = execution.stats;

    let mut schema_sent = false;
    let mut rows_returned: u64 = 0;

    while let Some(result) = stream.next().await {
        match result {
            Err(e) => {
                let msg = e.to_string();
                let outcome = SyncOutcome {
                    status: QueryStatus::Failed,
                    rows: None,
                    error: Some(msg.clone()),
                    elapsed_ms: elapsed(),
                    engine_stats: None,
                };
                return (outcome, sink.on_error(&msg).await);
            }
            Ok(batch) => {
                if !schema_sent {
                    if let Err(e) = sink.on_schema(batch.schema_ref()).await {
                        let outcome = SyncOutcome {
                            status: QueryStatus::Failed,
                            rows: None,
                            error: Some("client disconnected during schema send".to_string()),
                            elapsed_ms: elapsed(),
                            engine_stats: None,
                        };
                        return (outcome, Err(e));
                    }
                    schema_sent = true;
                }
                rows_returned += batch.num_rows() as u64;
                if let Err(e) = sink.on_batch(&batch).await {
                    let msg = e.to_string();
                    let _ = sink.on_error(&msg).await;
                    let outcome = SyncOutcome {
                        status: QueryStatus::Failed,
                        rows: Some(rows_returned),
                        error: Some(msg),
                        elapsed_ms: elapsed(),
                        engine_stats: None,
                    };
                    return (outcome, Err(e));
                }
            }
        }
    }

    let elapsed_ms = elapsed();

    // Stream exhausted — read engine stats now. The adapter sends into the oneshot
    // before or during stream production, so try_recv() is always sufficient here.
    let engine_stats = stats_rx.try_recv().ok().flatten();

    if !schema_sent {
        if let Err(e) = sink.on_schema(&Schema::empty()).await {
            let outcome = SyncOutcome {
                status: QueryStatus::Failed,
                rows: Some(0),
                error: Some("client disconnected during empty schema send".to_string()),
                elapsed_ms,
                engine_stats,
            };
            return (outcome, Err(e));
        }
    }

    let stats = QueryStats {
        execution_duration_ms: elapsed_ms,
        rows_returned,
        ..Default::default()
    };

    let outcome = SyncOutcome {
        status: QueryStatus::Success,
        rows: Some(rows_returned),
        error: None,
        elapsed_ms,
        engine_stats,
    };

    (outcome, sink.on_complete(&stats).await)
}

/// Execute a query via the native (non-Arrow) path and stream `NativeResultChunk`s to `sink`.
///
/// Only called when `adapter.connection_format().matches_frontend(protocol)` is true.
/// Mirrors the structure of `execute_stream` so metrics, error handling, and stats are identical.
async fn execute_native_to_sink(
    setup: &SyncQuerySetup,
    protocol: &FrontendProtocol,
    sink: &mut impl ResultSink,
) -> (SyncOutcome, Result<()>) {
    let elapsed = || setup.start.elapsed().as_millis() as u64;

    // Native execution is only available on SyncAdapters — AsyncAdapters use their own
    // Raw-bytes passthrough in dispatch_query and never reach execute_to_sink.
    let sync_adapter = match &setup.adapter {
        DispatchAdapter::Sync(a) => a,
        DispatchAdapter::Async(_) => {
            // Should never happen: async adapters don't match MysqlWire/PostgresWire formats.
            // Fall through to a clear error rather than silently producing wrong results.
            let msg = "execute_native_to_sink called for an async adapter — this is a bug";
            warn!(id = %setup.ctx.query_id, "{msg}");
            let outcome = SyncOutcome {
                status: QueryStatus::Failed,
                rows: None,
                error: Some(msg.to_string()),
                elapsed_ms: elapsed(),
                engine_stats: None,
            };
            return (outcome, sink.on_error(msg).await);
        }
    };

    let execution = match sync_adapter
        .execute_native(
            protocol,
            &setup.translated,
            &setup.ctx.session,
            &setup.credentials,
            &setup.ctx.query_tags,
            &setup.params,
        )
        .await
    {
        Ok(e) => e,
        Err(e) => {
            let msg = e.to_string();
            warn!(
                id = %setup.ctx.query_id,
                cluster = %setup.ctx.cluster,
                "execute_native failed: {msg}"
            );
            let outcome = SyncOutcome {
                status: QueryStatus::Failed,
                rows: None,
                error: Some(msg.clone()),
                elapsed_ms: elapsed(),
                engine_stats: None,
            };
            return (outcome, sink.on_error(&msg).await);
        }
    };

    let mut stream = execution.stream;
    let mut stats_rx = execution.stats;
    let mut rows_returned: u64 = 0;

    while let Some(result) = stream.next().await {
        match result {
            Err(e) => {
                let msg = e.to_string();
                let outcome = SyncOutcome {
                    status: QueryStatus::Failed,
                    rows: None,
                    error: Some(msg.clone()),
                    elapsed_ms: elapsed(),
                    engine_stats: None,
                };
                return (outcome, sink.on_error(&msg).await);
            }
            Ok(chunk) => {
                rows_returned += chunk.rows.len() as u64;
                if let Err(e) = sink.on_native_chunk(&chunk).await {
                    let msg = e.to_string();
                    let outcome = SyncOutcome {
                        status: QueryStatus::Failed,
                        rows: Some(rows_returned),
                        error: Some(msg.clone()),
                        elapsed_ms: elapsed(),
                        engine_stats: None,
                    };
                    return (outcome, Err(e));
                }
            }
        }
    }

    let elapsed_ms = elapsed();
    let engine_stats = stats_rx.try_recv().ok().flatten();

    let stats = QueryStats {
        execution_duration_ms: elapsed_ms,
        rows_returned,
        ..Default::default()
    };

    let outcome = SyncOutcome {
        status: QueryStatus::Success,
        rows: Some(rows_returned),
        error: None,
        elapsed_ms,
        engine_stats,
    };

    (outcome, sink.on_complete(&stats).await)
}

/// Execute a query against any backend and stream RecordBatches to `sink`.
///
/// Used by all non-Trino-HTTP frontends (MySQL wire, Postgres wire, Flight SQL).
/// The Trino HTTP frontend keeps its raw-bytes passthrough path unchanged.
///
/// Guarantees:
/// - `record_query` is called **exactly once** per query at the terminal state.
/// - The cluster slot is always released — even on tokio future cancellation (via Drop).
#[allow(clippy::too_many_arguments)]
pub async fn execute_to_sink(
    state: &Arc<AppState>,
    sql: String,
    params: QueryParams,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    sink: &mut impl ResultSink,
    auth_ctx: &AuthContext,
) -> Result<()> {
    if !state.authorization.check(auth_ctx, &group.0).await {
        let msg = format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        );
        return sink.on_error(&msg).await;
    }

    let (guard_chain, group_guard_chain) = {
        let live = state.live.read().await;
        (
            live.guard_chain.clone(),
            live.group_guard_chains.get(&group.0).cloned(),
        )
    };

    let mut setup = match setup_sync_query(
        state,
        sql,
        params,
        session,
        protocol.clone(),
        group,
        auth_ctx,
    )
    .await
    {
        Ok(s) => s,
        // Setup failed (no adapter, or translation error already recorded inside).
        // No slot is held at this point — just notify the sink.
        Err(e) => return sink.on_error(&e.to_string()).await,
    };

    // Guard chain: runs after translation (SQL is final) and after routing (group is known),
    // before submitting to the engine. Global guards run first; per-group guards are appended.
    {
        let ctx = &setup.ctx;
        let guard_ctx = GuardContext {
            sql: &ctx.sql,
            translated_sql: ctx.translated_sql.as_deref().unwrap_or(&setup.translated),
            engine_type: &ctx.engine_type,
            cluster_group: &ctx.group,
            user: ctx.session.user(),
            agent_context: ctx.agent_context.as_ref(),
            query_tags: &ctx.query_tags,
        };

        let mut all_actions: Vec<queryflux_persistence::GuardAction> = Vec::new();

        for chain in [guard_chain.as_ref(), group_guard_chain.as_ref()]
            .into_iter()
            .flatten()
        {
            let (actions, was_blocked) = chain.run(&guard_ctx, GuardLayer::Plan).await;
            all_actions.extend(actions);
            if was_blocked {
                let deny_reason = all_actions
                    .iter()
                    .find(|a| a.action == "deny")
                    .and_then(|a| a.reason.clone())
                    .unwrap_or_else(|| "query blocked by guardrail".to_string());
                setup.slot.release().await;
                state.record_query(
                    ctx,
                    QueryOutcome {
                        backend_query_id: None,
                        status: QueryStatus::Failed,
                        execution_ms: setup.start.elapsed().as_millis() as u64,
                        rows: None,
                        error: Some(deny_reason.clone()),
                        routing_trace: None,
                        engine_stats: None,
                        guard_actions: all_actions,
                        was_guard_blocked: true,
                    },
                );
                return sink.on_error(&deny_reason).await;
            }
        }

        // Attach non-blocking guard actions (allow/warn) to the setup context so they
        // flow into record_query at the normal exit point below.
        setup.guard_actions = all_actions;
    }

    // Native path: skip Arrow when backend connection format matches frontend protocol.
    // All other guarantees (slot release, record_query) are upheld by this function's
    // outer structure — only the inner execution subroutine is swapped.
    let (outcome, sink_result) = if setup
        .adapter
        .connection_format()
        .matches_frontend(&protocol)
    {
        execute_native_to_sink(&setup, &protocol, sink).await
    } else {
        execute_stream(&setup, sink).await
    };

    // Guaranteed single exit: release slot, then record.
    // slot.release() is idempotent and sets released=true so Drop is a no-op.
    setup.slot.release().await;
    let mut final_outcome: QueryOutcome = outcome.into();
    // Prepend guard actions (allow/warn) collected before execution.
    if !setup.guard_actions.is_empty() {
        setup.guard_actions.extend(final_outcome.guard_actions);
        final_outcome.guard_actions = setup.guard_actions;
    }
    state.record_query(&setup.ctx, final_outcome);

    sink_result
}
