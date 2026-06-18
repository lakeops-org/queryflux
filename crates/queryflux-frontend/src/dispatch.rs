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
        QueryEngineStats, QueryExecution, QueryStats, QueryStatus, QueuedQuery, SqlDialect,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::{AdapterKind, AsyncAdapter, ConnectionFormat, SyncAdapter};
use queryflux_guardrails::{GuardChain, GuardContext, GuardLayer};
use queryflux_metrics::MetricsStore;
use queryflux_translation::SchemaContext;

use tracing::{debug, info, warn};

use queryflux_routing::chain::RoutingTrace;

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

// ---------------------------------------------------------------------------
// Shared query preparation helpers (used by both async and sync paths)
// ---------------------------------------------------------------------------

/// Snapshot of per-group live config needed for query preparation.
struct GroupLiveConfig {
    cluster_manager: Arc<dyn ClusterGroupManager>,
    group_fixups: Vec<String>,
    group_default_tags: QueryTags,
    guard_chain: Option<Arc<GuardChain>>,
    group_guard_chain: Option<Arc<GuardChain>>,
    /// Max time (ms) a sync/wire query waits for a cluster slot. `None` → no limit.
    queue_timeout_ms: Option<u64>,
}

/// Read the per-group live config snapshot from `AppState`.
async fn read_group_live_config(state: &AppState, group: &str) -> GroupLiveConfig {
    let live = state.live.read().await;
    GroupLiveConfig {
        cluster_manager: live.cluster_manager.clone(),
        group_fixups: live
            .group_translation_scripts
            .get(group)
            .cloned()
            .unwrap_or_default(),
        group_default_tags: live
            .group_default_tags
            .get(group)
            .cloned()
            .unwrap_or_default(),
        guard_chain: live.guard_chain.clone(),
        group_guard_chain: live.group_guard_chains.get(group).cloned(),
        queue_timeout_ms: live.group_queue_timeouts.get(group).copied().flatten(),
    }
}

/// Translate SQL and optionally interpolate parameters.
///
/// Shared by both the async (`dispatch_query`) and sync (`setup_sync_query`) paths.
/// Returns `(translated_sql, was_translated, effective_params)`.
async fn translate_and_prepare(
    state: &AppState,
    sql: &str,
    params: QueryParams,
    src_dialect: &SqlDialect,
    tgt_dialect: &SqlDialect,
    group_fixups: &[String],
    supports_native_params: bool,
) -> Result<(String, bool, QueryParams)> {
    let translated = state
        .translation
        .maybe_translate(
            sql,
            src_dialect,
            tgt_dialect,
            &SchemaContext::default(),
            group_fixups,
        )
        .await?;

    let was_translated = translated != sql;

    let (translated, effective_params) = if !params.is_empty() && !supports_native_params {
        (
            interpolate_params(&translated, &params, tgt_dialect)?,
            vec![],
        )
    } else {
        (translated, params)
    };

    Ok((translated, was_translated, effective_params))
}

/// Centralized routing: resolve the target cluster group for a query.
///
/// Evaluates the router chain, applies authorization-aware fallback resolution, and returns
/// the final group name along with the routing trace. All frontends should use this (or
/// [`route_and_execute`]) instead of calling `router_chain.route_with_trace` directly.
pub async fn resolve_route(
    state: &Arc<AppState>,
    sql: &str,
    session: &SessionContext,
    protocol: &FrontendProtocol,
    auth_ctx: &AuthContext,
) -> Result<(ClusterGroupName, RoutingTrace)> {
    let routing_result = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(sql, session, protocol, Some(auth_ctx))
            .await
    };
    let (group, trace) = routing_result?;

    // When the router chain fell back to the static default, honour it if the user is
    // authorized. Otherwise find the first group the user can access (restrictive ACLs).
    let group = if trace.used_fallback {
        resolve_group_for_user(state, auth_ctx, group).await
    } else {
        group
    };
    Ok((group, trace))
}

/// Authorization-aware fallback resolution: if the user is authorized for the configured
/// fallback group, use it. Otherwise scan groups in config order for the first allowed one.
async fn resolve_group_for_user(
    state: &AppState,
    auth_ctx: &AuthContext,
    fallback: ClusterGroupName,
) -> ClusterGroupName {
    if state.authorization.check(auth_ctx, &fallback.0).await {
        return fallback;
    }
    let group_order = state.live.read().await.group_order.clone();
    for group_name in &group_order {
        if state.authorization.check(auth_ctx, group_name).await {
            return ClusterGroupName(group_name.clone());
        }
    }
    fallback
}

/// Route a query and execute it to a sink in one call.
///
/// Combines [`resolve_route`] + [`execute_to_sink`] — the standard entry point for
/// frontends that use the sink-based (sync/Arrow) execution path (MySQL wire, Postgres wire,
/// FlightSQL). Trino HTTP uses [`resolve_route`] directly because it branches on async vs sync.
///
/// Routing errors are reported to the sink via [`ResultSink::on_error`] so the client always
/// receives a protocol-native error message regardless of where the failure occurred.
pub async fn route_and_execute(
    state: &Arc<AppState>,
    sql: String,
    params: QueryParams,
    session: SessionContext,
    protocol: FrontendProtocol,
    sink: &mut impl ResultSink,
    auth_ctx: &AuthContext,
) -> Result<()> {
    let (group, trace) = match resolve_route(state, &sql, &session, &protocol, auth_ctx).await {
        Ok(r) => r,
        Err(e) => return sink.on_error(&e.to_string()).await,
    };
    execute_to_sink(
        state,
        sql,
        params,
        session,
        protocol,
        group,
        Some(trace),
        sink,
        auth_ctx,
    )
    .await
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
    routing_trace: Option<RoutingTrace>,
) -> Result<DispatchOutcome> {
    // Authorization check — first gate before any resource acquisition.
    // Phase 1: AllowAllAuthorization always returns true (no behavior change).
    if !state.authorization.check(auth_ctx, &group.0).await {
        return Err(QueryFluxError::Unauthorized(format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        )));
    }

    let glc = read_group_live_config(state, &group.0).await;
    let cluster_manager = &glc.cluster_manager;
    let group_fixups = &glc.group_fixups;
    let guard_chain = &glc.guard_chain;
    let group_guard_chain = &glc.group_guard_chain;
    let effective_tags = merge_tags(&glc.group_default_tags, &session.tags().clone());

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
        cluster_db_ids(cluster_manager, &group, &cluster_name).await;

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

    // Async adapters never support native params — always interpolate.
    let (sql, was_translated, effective_params) = match translate_and_prepare(
        state,
        &sql,
        params,
        &src_dialect,
        &tgt_dialect,
        group_fixups,
        false,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(id = %query_id, "Translation error: {e}");
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(e);
        }
    };
    if was_translated {
        info!(id = %query_id, src = ?src_dialect, tgt = ?tgt_dialect, "SQL translated");
    }

    // Guard chain: runs after translation (SQL is final), before engine submission.
    // Global guards run first; per-group guards are appended after.
    let resolved_agent_ctx = session.resolved_agent_context();
    let mut all_guard_actions: Vec<queryflux_core::query::GuardAction> = Vec::new();

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
                    queue_duration_ms: 0,
                    execution_ms: 0,
                    rows: None,
                    error: Some(deny_reason.clone()),
                    routing_trace: routing_trace.clone(),
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
                        crate::trino_http::trino_dispatch::finalize_trino_async_terminal_on_submit(
                            state,
                            cluster_manager,
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

            let proxy_next_uri = next_uri.as_deref().map(|uri| {
                crate::trino_http::trino_dispatch::rewrite_trino_uri(uri, &state.external_address)
            });
            Ok(DispatchOutcome::Async {
                initial_body,
                proxy_next_uri,
            })
        }
        AdapterKind::Sync(sync_adapter) => {
            if already_queued {
                let _ = state.persistence.delete_queued(&query_id).await;
            }

            // Wrap the already-acquired slot so Drop releases it on future cancellation
            // (e.g. the Trino HTTP client times out and axum drops this request future).
            // Without this guard, the running_queries counter for the sync cluster leaks
            // upward on every client timeout until the cluster appears at-capacity and is
            // excluded from round-robin selection.
            let mut slot = ClusterSlotGuard::new(
                cluster_manager.clone(),
                group.clone(),
                cluster_name.clone(),
                state.metrics.clone(),
            );

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
                    queue_duration_ms: 0,
                    execution_ms: elapsed_ms,
                    rows,
                    error,
                    routing_trace: routing_trace.clone(),
                    engine_stats: None,
                    guard_actions: all_guard_actions,
                    was_guard_blocked: false,
                },
            );

            slot.release().await;

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
// CancellationGuard — records a Cancelled query when a sync future is dropped
// ---------------------------------------------------------------------------

/// RAII guard that records a `Cancelled` query when the sync execution future is
/// dropped (e.g. client disconnects). On the normal path the caller calls `disarm()`
/// after `record_query` so the guard becomes a no-op on drop.
struct CancellationGuard {
    state: Arc<AppState>,
    ctx: Option<QueryContext>,
    start: Instant,
}

impl CancellationGuard {
    fn new(state: Arc<AppState>, ctx: QueryContext, start: Instant) -> Self {
        Self {
            state,
            ctx: Some(ctx),
            start,
        }
    }

    fn disarm(&mut self) {
        self.ctx = None;
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            let state = self.state.clone();
            let elapsed = self.start.elapsed().as_millis() as u64;
            tokio::spawn(async move {
                state.record_query(
                    &ctx,
                    QueryOutcome {
                        backend_query_id: None,
                        status: QueryStatus::Cancelled,
                        queue_duration_ms: 0,
                        execution_ms: elapsed,
                        rows: None,
                        error: Some("Client disconnected".to_string()),
                        routing_trace: None,
                        engine_stats: None,
                        guard_actions: vec![],
                        was_guard_blocked: false,
                    },
                );
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
    guard_actions: Vec<queryflux_core::query::GuardAction>,
    /// Time spent waiting for a cluster slot (wire-protocol queueing).
    queue_duration_ms: u64,
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
            queue_duration_ms: 0,
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

    let glc = read_group_live_config(state, &group.0).await;
    let effective_tags: QueryTags = merge_tags(&glc.group_default_tags, &session.tags().clone());

    // Queue loop: wait for a cluster slot with optional timeout.
    let queue_start = Instant::now();
    let timeout = glc.queue_timeout_ms.map(std::time::Duration::from_millis);
    let mut seq: u64 = 0;
    let mut queued_metric_emitted = false;

    let (cluster_name, adapter) = loop {
        match glc.cluster_manager.acquire_cluster(&group).await? {
            Some(name) => match state.adapter(&name.0).await {
                Some(AdapterKind::Sync(a)) => break (name, DispatchAdapter::Sync(a)),
                Some(AdapterKind::Async(a)) => break (name, DispatchAdapter::Async(a)),
                None => {
                    let _ = glc.cluster_manager.release_cluster(&group, &name).await;
                    return Err(QueryFluxError::Engine(format!(
                        "No adapter for {group}/{name}"
                    )));
                }
            },
            None => {
                if !queued_metric_emitted {
                    info!(id = %query_id, group = %group, "Query queued — waiting for cluster slot");
                    queued_metric_emitted = true;
                }

                if let Some(max_wait) = timeout {
                    if queue_start.elapsed() >= max_wait {
                        warn!(
                            id = %query_id, group = %group,
                            waited_ms = queue_start.elapsed().as_millis() as u64,
                            "Queue timeout — no cluster slot available"
                        );
                        return Err(QueryFluxError::Engine(format!(
                            "query timed out waiting for a free cluster slot in group '{}' \
                             (waited {}ms, limit {}ms)",
                            group.0,
                            queue_start.elapsed().as_millis(),
                            max_wait.as_millis()
                        )));
                    }
                }

                queued_backoff_delay(seq).await;
                seq += 1;
            }
        }
    };
    let queue_duration_ms = queue_start.elapsed().as_millis() as u64;
    if queued_metric_emitted {
        info!(
            id = %query_id, group = %group,
            queue_ms = queue_duration_ms,
            "Query dequeued — cluster slot acquired"
        );
    }

    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&glc.cluster_manager, &group, &cluster_name).await;

    state.metrics.on_query_started(&group.0, &cluster_name.0);
    info!(id = %query_id, group = %group, cluster = %cluster_name, "Query executing (sync)");

    let mut slot = ClusterSlotGuard::new(
        glc.cluster_manager.clone(),
        group.clone(),
        cluster_name.clone(),
        state.metrics.clone(),
    );

    let src_dialect = protocol.default_dialect();
    let tgt_dialect = adapter.translation_target_dialect();
    let engine_type = adapter.engine_type();
    let start = Instant::now();

    let (translated, was_translated, effective_params) = match translate_and_prepare(
        state,
        &sql,
        params.clone(),
        &src_dialect,
        &tgt_dialect,
        &glc.group_fixups,
        adapter.supports_native_params(),
    )
    .await
    {
        Ok(r) => r,
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
                    queue_duration_ms: 0,
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

    let cluster_cfg = state.cluster_config_cloned(&cluster_name.0).await;
    let credentials = state
        .identity_resolver
        .resolve(auth_ctx, cluster_cfg.as_ref())
        .await;

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
        queue_duration_ms,
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
    routing_trace: Option<RoutingTrace>,
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

        let mut all_actions: Vec<queryflux_core::query::GuardAction> = Vec::new();

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
                        queue_duration_ms: setup.queue_duration_ms,
                        execution_ms: setup.start.elapsed().as_millis() as u64,
                        rows: None,
                        error: Some(deny_reason.clone()),
                        routing_trace: routing_trace.clone(),
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

    // Cancellation safety: if the future is dropped from here on (client disconnect),
    // the guard records the query as Cancelled so it appears in query history.
    // Disarmed below after the normal-path record_query call.
    let mut cancel_guard = CancellationGuard::new(state.clone(), setup.ctx.clone(), setup.start);

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
    final_outcome.routing_trace = routing_trace;
    final_outcome.queue_duration_ms = setup.queue_duration_ms;
    // Prepend guard actions (allow/warn) collected before execution.
    if !setup.guard_actions.is_empty() {
        setup.guard_actions.extend(final_outcome.guard_actions);
        final_outcome.guard_actions = setup.guard_actions;
    }
    state.record_query(&setup.ctx, final_outcome);
    cancel_guard.disarm();

    sink_result
}
