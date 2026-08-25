use std::sync::Arc;
use std::time::{Duration, Instant};

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
        ClusterGroupName, ClusterName, ExecutingQuery, FrontendProtocol, ProxyQueryId,
        QueryEngineStats, QueryExecution, QueryStats, QueryStatus, QueuedQuery, SqlDialect,
        StoredWireAuth,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::{
    wire_auth::{enrich_session_for_passthrough, resolve_stored_wire_auth},
    AdapterKind, AsyncAdapter, BackendQueryIdSlot, ConnectionFormat, SyncAdapter,
};
use queryflux_guardrails::{GuardChain, GuardContext, GuardLayer};
use queryflux_metrics::MetricsStore;
use queryflux_translation::SchemaContext;

use tracing::{debug, info, warn};

use crate::state::{AppState, QueryContext, QueryOutcome};

/// Resolve the source dialect to record for this query.
///
/// An explicit `session.extra["dialect"]` override — set by the MCP frontend when the
/// caller declares a dialect via the `dialect` tool parameter — always wins. Otherwise
/// falls back to the protocol's wire-implied default (`SqlDialect::Generic` for MCP,
/// same as `FlightSql` — see `FrontendProtocol::default_dialect`). This value is purely
/// descriptive (what gets persisted on the query record); it does **not** by itself
/// determine whether translation actually runs — see `should_attempt_translation`. We
/// deliberately do not infer "the SQL is probably already in the target engine's
/// dialect" here: that's a guess, and recording a guessed dialect as if the caller
/// declared it would make the audit trail wrong, not just the translation.
fn resolve_src_dialect(session: &SessionContext, protocol: &FrontendProtocol) -> SqlDialect {
    if let Some(name) = session.extra.get("dialect") {
        return SqlDialect::Sqlglot(name.clone());
    }
    protocol.default_dialect()
}

/// Whether translation (and therefore sqlglot) should be invoked at all for this query.
///
/// Every protocol except MCP has a wire-implied dialect, so translation always at least
/// attempts to run for them (`TranslationService::maybe_translate` itself no-ops when
/// `src`/`tgt` turn out compatible and no fixup scripts are configured). MCP is the one
/// case where, absent an explicit `dialect` override, we genuinely don't know the source
/// dialect — and calling sqlglot with a guessed dialect risks mis-parsing SQL that was
/// already correct for the target engine. Note that simply recording `Generic` here does
/// *not* make `maybe_translate` skip on its own: `SqlDialect::Generic.is_compatible_with`
/// a real target dialect is false, so it would still call sqlglot with an empty `read`
/// dialect — the exact problem we're avoiding. So when MCP has no override, we skip
/// calling `maybe_translate` entirely: no dialect is passed to sqlglot, and no configured
/// fixup scripts run either, since those need a real dialect to parse under too. The SQL
/// passes through completely unmodified.
fn should_attempt_translation(session: &SessionContext, protocol: &FrontendProtocol) -> bool {
    !matches!(protocol, FrontendProtocol::Mcp) || session.extra.contains_key("dialect")
}

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

    /// Called once, after translation, with the exact SQL sent to the adapter.
    ///
    /// Sinks that derive per-statement framing from the SQL text (e.g. Postgres
    /// wire's CommandComplete tag) must classify from *this*, not the pre-translation
    /// client SQL — translation can rewrite the leading verb (e.g. MySQL `REPLACE
    /// INTO` → target-dialect `INSERT ... ON CONFLICT`). Default is a no-op for
    /// sinks that don't need it.
    async fn on_translated_sql(&mut self, _sql: &str) -> Result<()> {
        Ok(())
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
/// `poll_base_url` and re-joining it with the path extracted from the client request.
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
    mut session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    already_queued: bool,
    // When this query was first enqueued (`None` for a query that has never
    // been queued). Drives the admission fairness gate: older waiters win.
    queued_since: Option<chrono::DateTime<Utc>>,
    sequence: u64,
    auth_ctx: &AuthContext,
) -> Result<DispatchOutcome> {
    // Snapshot all live config fields in one lock acquisition. The guard is
    // dropped before any await point so no lock is held during I/O.
    let (
        authorization,
        cluster_manager,
        group_fixups,
        group_default_tags,
        guard_chain,
        group_guard_chain,
        cluster_cfg,
        adapters,
        max_queued_queries,
    ) = {
        let live = state.live.read().await;
        (
            live.authorization.clone(),
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
            // cluster_cfg resolved after cluster selection below; captured here
            // so credential resolution uses the same config generation.
            live.cluster_configs.clone(),
            // adapters snapshotted from the same read guard as cluster_cfg — a config
            // update landing between credential resolution and adapter selection could
            // otherwise resolve credentials against one config generation and submit
            // through an adapter built from a newer one (e.g. HTTP auth removed between
            // the two reads would make credential resolution and the Trino adapter's own
            // internal derivation disagree on whether to forward client Authorization).
            live.adapters.clone(),
            live.group_max_queued_queries
                .get(&group.0)
                .copied()
                .flatten(),
        )
    };

    if !authorization.check(auth_ctx, &group.0).await {
        return Err(QueryFluxError::Unauthorized(format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        )));
    }

    let effective_tags = merge_tags(&group_default_tags, &session.tags().clone());

    // Admission fairness: don't take a slot that an older, actively-polling
    // queued query is waiting for. Only binds when capacity is scarce — with
    // free slots to spare the gate is a cheap local check and admits.
    if should_yield_to_older_queued(state, &cluster_manager, &group, queued_since).await {
        let uri = persist_queued_query(
            state,
            query_id,
            sql,
            session,
            protocol,
            group,
            already_queued,
            sequence,
            max_queued_queries,
            auth_ctx,
        )
        .await?;
        return Ok(DispatchOutcome::Queued {
            queued_next_uri: uri,
        });
    }

    let cluster_name = match cluster_manager.acquire_cluster(&group).await? {
        Some(c) => {
            match acquire_global_capacity(state, &cluster_manager, &group, &c, &query_id.0).await {
                CapacityGrant::Denied => {
                    // Global capacity full — release local slot and queue.
                    let _ = cluster_manager.release_cluster(&group, &c).await;
                    let uri = persist_queued_query(
                        state,
                        query_id,
                        sql,
                        session,
                        protocol,
                        group,
                        already_queued,
                        sequence,
                        max_queued_queries,
                        auth_ctx,
                    )
                    .await?;
                    return Ok(DispatchOutcome::Queued {
                        queued_next_uri: uri,
                    });
                }
                CapacityGrant::Granted => c,
            }
        }
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
                max_queued_queries,
                auth_ctx,
            )
            .await?;
            return Ok(DispatchOutcome::Queued {
                queued_next_uri: uri,
            });
        }
    };

    // RAII guard: from here on the local slot and global lease are released on
    // every exit — including the future being dropped when the client
    // disconnects mid-dispatch, which previously leaked the lease permanently
    // (the owning replica keeps heartbeating, so expiry never reclaims it).
    let mut slot = ClusterSlotGuard::new(
        cluster_manager.clone(),
        group.clone(),
        cluster_name.clone(),
        state.metrics.clone(),
        state.capacity_store.clone(),
        query_id.0.clone(),
    );

    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&cluster_manager, &group, &cluster_name).await;

    state.metrics.on_query_started(&group.0, &cluster_name.0);

    let this_cluster_cfg = cluster_cfg.get(&cluster_name.0).cloned();
    let credentials = match state
        .identity_resolver
        .resolve(auth_ctx, this_cluster_cfg.as_ref())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            slot.release().await;
            return Err(e);
        }
    };

    // Passthrough: forward the client's own Authorization if the frontend already
    // captured one; otherwise inject a Bearer built from the OIDC raw_token so the
    // backend still receives a per-user credential. The adapter fails closed if
    // neither is available — this is best-effort enrichment, not the fail-closed check.
    enrich_session_for_passthrough(&mut session, &credentials, auth_ctx);

    // Resolved once here (mirroring what the Trino adapter derives internally for the
    // POST itself) so the exact same wire auth can be persisted on `ExecutingQuery` and
    // reused by poll/cancel, which never see the original `SessionContext` again.
    let cluster_sets_http_auth = this_cluster_cfg
        .as_ref()
        .and_then(|c| c.auth.as_ref())
        .is_some_and(|a| a.sets_http_authorization());
    let wire_auth: Option<StoredWireAuth> =
        resolve_stored_wire_auth(&credentials, &session, cluster_sets_http_auth);

    let adapter_kind = match adapters.get(&cluster_name.0).cloned() {
        Some(a) => a,
        None => {
            slot.release().await;
            return Err(QueryFluxError::Engine(format!(
                "No adapter for {group}/{cluster_name}"
            )));
        }
    };

    let tgt_dialect = adapter_kind.translation_target_dialect();
    let src_dialect = resolve_src_dialect(&session, &protocol);
    let engine_type = adapter_kind.engine_type();
    let original_sql = sql.clone();
    let sql = if should_attempt_translation(&session, &protocol) {
        match state
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
                slot.release().await;
                return Err(e);
            }
        }
    } else {
        sql
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
    let sql_parse =
        queryflux_core::sql_classify::SqlParseCache::new(sql.clone(), tgt_dialect.clone());

    let guard_ctx = GuardContext {
        sql: &original_sql,
        translated_sql: &sql,
        engine_type: &engine_type,
        cluster_group: &group,
        user: session.user(),
        agent_context: resolved_agent_ctx.as_ref(),
        query_tags: &effective_tags,
        sql_parse: Some(&sql_parse),
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
                    queue_duration_ms: 0,
                    cache_hit: false,
                },
            );
            slot.release().await;
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
    // Treat serialization failure as fatal — silently omitting guard actions from the
    // audit record would produce incomplete compliance logs.
    let submitted_guard_actions: Vec<serde_json::Value> = all_guard_actions
        .iter()
        .map(|a| {
            serde_json::to_value(a).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "Failed to serialize guard action '{}': {e}",
                    a.guard
                ))
            })
        })
        .collect::<queryflux_core::error::Result<Vec<_>>>()?;

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
                    slot.release().await;
                    warn!(id = %query_id, "Submit error: {e}");
                    return Err(e);
                }
            };

            if already_queued {
                let _ = state.persistence.delete_queued(&query_id).await;
            }
            let queue_duration_ms = queued_since
                .map(|t| (Utc::now() - t).num_milliseconds().max(0) as u64)
                .unwrap_or(0);
            if queue_duration_ms > 0 {
                debug!(id = %query_id, queue_ms = queue_duration_ms, "Queued query dispatched");
            }

            // Extract backend_query_id first so we can build ExecutingQuery before branching.
            let backend_query_id = match &execution {
                QueryExecution::Running {
                    backend_query_id, ..
                } => backend_query_id.clone(),
                QueryExecution::Completed {
                    backend_query_id, ..
                } => backend_query_id.clone(),
            };
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
                poll_base_url: Some(adapter.base_url().to_string()),
                creation_time: now,
                last_accessed: now,
                query_tags: effective_tags,
                agent_context: resolved_agent_ctx,
                submitted_guard_actions,
                was_guard_blocked: false,
                submitted_by: auth_ctx.user.clone(),
                wire_auth: wire_auth.clone(),
            };

            match execution {
                QueryExecution::Running {
                    poll_token,
                    initial_response,
                    ..
                } => {
                    // Slot ownership transfers to the executing record: poll, cancel,
                    // and zombie-eviction paths release it from here on. If the record
                    // can't be persisted, cancel the engine-side query best-effort so
                    // it doesn't burn cluster resources invisibly, then release the slot.
                    if let Err(e) = state.persistence.upsert(executing.clone()).await {
                        warn!(id = %query_id, "Failed to persist executing query: {e}");
                        let cancel_adapter = adapter.clone();
                        let cancel_id = backend_query_id.clone();
                        let cancel_wire_auth = wire_auth.clone();
                        tokio::spawn(async move {
                            if let Err(ce) = cancel_adapter
                                .cancel_query(&cancel_id, cancel_wire_auth.as_ref())
                                .await
                            {
                                warn!(backend = %cancel_id, "Best-effort cancel after persistence failure: {ce}");
                            }
                        });
                        slot.release().await;
                        return Err(QueryFluxError::Persistence(format!(
                            "persist executing query: {e}"
                        )));
                    }
                    slot.disarm();
                    // TODO: persist queue_duration_ms so the poll handler can include it
                    // in the final QueryOutcome. Either add a field to ExecutingQuery or
                    // store it in a side-channel (e.g. a metadata column).
                    info!(id = %query_id, backend = %backend_query_id, cluster = %cluster_name, queue_ms = queue_duration_ms, "Query submitted (async)");

                    let proxy_next_uri = poll_token
                        .as_deref()
                        .map(|uri| rewrite_trino_uri(uri, &state.external_address));
                    Ok(DispatchOutcome::Async {
                        initial_body: initial_response,
                        proxy_next_uri,
                    })
                }
                QueryExecution::Completed {
                    status,
                    error,
                    engine_stats,
                    initial_response,
                    ..
                } => {
                    // Query finished on the initial submit — no poll handler will be called.
                    // Disarm the RAII guard; finalize will call release_query_slot explicitly.
                    slot.disarm();
                    info!(id = %query_id, backend = %backend_query_id, cluster = %cluster_name, "Query completed on submit");
                    let was_translated = executing.translated_sql.is_some();
                    let src_dialect = resolve_src_dialect(&session, &protocol);
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
                    finalize_async_terminal_on_submit(
                        state,
                        &executing,
                        ctx,
                        status,
                        error,
                        engine_stats,
                        queue_duration_ms,
                    )
                    .await;
                    Ok(DispatchOutcome::Async {
                        initial_body: initial_response,
                        proxy_next_uri: None,
                    })
                }
            }
        }
        AdapterKind::Sync(_) => {
            // dispatch_query is the async path only. A sync cluster selected by
            // round-robin in a mixed group signals the caller to retry via
            // execute_to_sink, which will drive its own slot acquisition loop.
            slot.release().await;
            Err(QueryFluxError::SyncEngineRequired(cluster_name.0.clone()))
        }
    }
}

/// Called when `submit_query` returns `QueryExecution::Completed` — the adapter
/// signalled the query is done on the initial POST (fast queries, immediate errors).
/// Handles the protocol-neutral record/release/cleanup that the poll handler would
/// otherwise perform, since no poll request will ever arrive for this query.
async fn finalize_async_terminal_on_submit(
    state: &Arc<AppState>,
    executing: &ExecutingQuery,
    ctx: QueryContext,
    status: QueryStatus,
    error: Option<String>,
    engine_stats: Option<QueryEngineStats>,
    queue_duration_ms: u64,
) {
    let elapsed_ms = (Utc::now() - executing.creation_time)
        .num_milliseconds()
        .max(0) as u64;

    let stored_actions: Vec<queryflux_persistence::GuardAction> = match serde_json::from_value(
        serde_json::Value::Array(executing.submitted_guard_actions.clone()),
    ) {
        Ok(actions) => actions,
        Err(e) => {
            warn!(id = %executing.id, "Failed to deserialize stored guard actions: {e}");
            vec![]
        }
    };

    let mut outcome = QueryOutcome {
        backend_query_id: Some(executing.backend_query_id.0.clone()),
        status,
        execution_ms: elapsed_ms,
        rows: None,
        error,
        routing_trace: None,
        engine_stats,
        guard_actions: vec![],
        was_guard_blocked: false,
        queue_duration_ms,
        cache_hit: false,
    };
    if !stored_actions.is_empty() {
        outcome.guard_actions = stored_actions;
        outcome.was_guard_blocked = executing.was_guard_blocked;
    }

    state.record_query(&ctx, outcome);
    state
        .release_query_slot(
            &executing.cluster_group,
            &executing.cluster_name,
            &executing.id.0,
        )
        .await;
    if let Err(e) = state.persistence.delete(&executing.backend_query_id).await {
        warn!(id = %executing.id, "Failed to delete executing record on terminal submit: {e}");
    }
}

/// Effective `max_running_queries` for a cluster, as resolved in the hot-reloaded
/// local config (cluster override or inherited group limit) — this is what the
/// global capacity check enforces. Falls back to unlimited if the snapshot is
/// unavailable, consistent with the fail-open posture of distributed coordination.
async fn effective_max_running(
    cluster_manager: &Arc<dyn ClusterGroupManager>,
    group: &ClusterGroupName,
    cluster: &ClusterName,
) -> u64 {
    cluster_manager
        .cluster_state(group, cluster)
        .await
        .ok()
        .flatten()
        .map(|s| s.max_running_queries)
        .unwrap_or(u64::MAX)
}

/// How recently a queued query must have been polled to count as an active
/// waiter in the fairness gate. Trino clients poll about once a second, so a
/// client gone for this long has almost certainly disconnected — excluding it
/// keeps a dead client from blocking admission (head-of-line) until the
/// stale-queue cleanup removes its row minutes later.
const QUEUE_ACTIVE_WINDOW_SECS: i64 = 15;

/// Admission fairness gate: should this query yield instead of taking a slot?
///
/// True only when both hold:
/// 1. the group's free capacity (local snapshot) does not exceed the number of
///    older, actively-polling queued queries — i.e. every remaining slot is
///    spoken for by someone who was here first, and
/// 2. such waiters exist at all.
///
/// `queued_since` is the caller's own enqueue time (`None` = never queued, so
/// every active waiter outranks it). The free-slot check runs first and is
/// in-memory, so under healthy load the gate never touches the backend.
/// Backend errors fail open — fairness degrades to poll-order rather than
/// blocking admission.
///
/// **Best-effort ordering**: there is an inherent race between this check and
/// the actual slot acquisition that follows. Under distributed load an older
/// waiter may be dispatched by a different replica in that window, making the
/// yield decision stale. The gate provides FIFO *on average* — it is not a
/// strict ordering guarantee.
async fn should_yield_to_older_queued(
    state: &Arc<AppState>,
    cluster_manager: &Arc<dyn ClusterGroupManager>,
    group: &ClusterGroupName,
    queued_since: Option<chrono::DateTime<Utc>>,
) -> bool {
    let free = match cluster_manager.all_cluster_states().await {
        Ok(snaps) => snaps
            .iter()
            .filter(|s| s.group_name.0 == group.0 && s.enabled && s.is_healthy)
            .map(|s| s.max_running_queries.saturating_sub(s.running_queries))
            .sum::<u64>(),
        // Can't tell — don't block admission on a read failure.
        Err(_) => return false,
    };
    if free == 0 {
        // Nothing to take; acquire_cluster will queue this query anyway.
        return false;
    }
    let active_after = Utc::now() - chrono::Duration::seconds(QUEUE_ACTIVE_WINDOW_SECS);
    match state
        .persistence
        .count_active_queued_before(&group.0, queued_since, active_after)
        .await
    {
        Ok(older_waiters) => older_waiters >= free,
        Err(e) => {
            warn!("Fairness gate query failed; admitting without ordering: {e}");
            false
        }
    }
}

/// Result of a global capacity acquisition attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityGrant {
    /// Capacity confirmed available by the coordination backend.
    Granted,
    /// Global capacity is full or coordination backend unreachable (fail-closed) —
    /// caller must queue or back off.
    Denied,
}

/// In distributed mode, take a global capacity lease for a cluster slot that
/// was just acquired locally. Coordination failures fail closed (query is queued
/// rather than admitted without global coordination) and are counted in
/// `queryflux_coordination_failures_total`. Always `Granted` outside distributed mode.
async fn acquire_global_capacity(
    state: &Arc<AppState>,
    cluster_manager: &Arc<dyn ClusterGroupManager>,
    group: &ClusterGroupName,
    cluster: &ClusterName,
    query_id: &str,
) -> CapacityGrant {
    let Some(cap) = &state.capacity_store else {
        return CapacityGrant::Granted;
    };
    let max_rq = effective_max_running(cluster_manager, group, cluster).await;
    match cap
        .try_acquire(&cluster.0, max_rq, &state.instance_id, query_id)
        .await
    {
        Ok(true) => CapacityGrant::Granted,
        Ok(false) => CapacityGrant::Denied,
        Err(e) => {
            state.metrics.on_coordination_failure("capacity_acquire");
            tracing::warn!(
                "CapacityStore try_acquire failed, rejecting to queue (fail-closed): {e}"
            );
            CapacityGrant::Denied
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
    already_stored: bool,
    sequence: u64,
    max_queued_queries: Option<u64>,
    auth_ctx: &AuthContext,
) -> Result<String> {
    // Enforce queue depth limit before admitting to the queue.
    // Skip when re-persisting an already-queued query so a waiter cannot
    // self-reject by counting itself against the limit.
    if !already_stored {
        if let Some(limit) = max_queued_queries {
            if limit > 0 {
                let active_after = Utc::now() - chrono::Duration::seconds(QUEUE_ACTIVE_WINDOW_SECS);
                let count = state
                    .persistence
                    .count_active_queued_before(&group.0, None, active_after)
                    .await
                    .unwrap_or(0);
                if count >= limit {
                    state.metrics.on_queue_full(&group.0);
                    return Err(QueryFluxError::QueueFull {
                        group: group.0.clone(),
                        count,
                        limit,
                    });
                }
            }
        }
    }

    let now = Utc::now();
    let queued = QueuedQuery {
        id: query_id.clone(),
        sql,
        session: session.without_auth_headers(),
        frontend_protocol: protocol,
        cluster_group: group,
        creation_time: now,
        last_accessed: now,
        sequence,
        submitted_by: auth_ctx.user.clone(),
    };
    state.persistence.upsert_queued(queued).await?;
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
    capacity_store: Option<Arc<dyn queryflux_persistence::CapacityStore>>,
    query_id: String,
    released: bool,
}

impl ClusterSlotGuard {
    fn new(
        cluster_manager: Arc<dyn ClusterGroupManager>,
        group: ClusterGroupName,
        cluster: ClusterName,
        metrics: Arc<dyn MetricsStore>,
        capacity_store: Option<Arc<dyn queryflux_persistence::CapacityStore>>,
        query_id: String,
    ) -> Self {
        Self {
            cluster_manager,
            group,
            cluster,
            metrics,
            capacity_store,
            query_id,
            released: false,
        }
    }

    /// Transfer slot ownership out of this guard without releasing — used when
    /// an async query has been durably persisted as executing and the terminal
    /// paths (poll, cancel, zombie eviction) become responsible for the release.
    fn disarm(&mut self) {
        self.released = true;
    }

    /// Release the slot on the normal path. Idempotent — safe to call twice.
    async fn release(&mut self) {
        if !self.released {
            self.released = true;
            let _ = self
                .cluster_manager
                .release_cluster(&self.group, &self.cluster)
                .await;
            if let Some(cap) = &self.capacity_store {
                if let Err(e) = cap.release(&self.cluster.0, &self.query_id).await {
                    self.metrics.on_coordination_failure("capacity_release");
                    tracing::warn!(
                        "CapacityStore release failed for query {}: {e}",
                        self.query_id
                    );
                }
            }
            self.metrics
                .on_query_finished(&self.group.0, &self.cluster.0);
        }
    }
}

impl Drop for ClusterSlotGuard {
    fn drop(&mut self) {
        if !self.released {
            // Fallback path: the guard was dropped without an explicit `release()` call
            // (e.g. a future was cancelled mid-dispatch). We spawn a best-effort task to
            // clean up the slot.
            //
            // Bounding note: this path is only reached on unclean drops (panics, task
            // cancellations). The upstream `max_running_queries` gate constrains how many
            // guards can be alive simultaneously, so the total number of concurrent
            // best-effort tasks is bounded by the per-cluster concurrency limit.
            let mgr = self.cluster_manager.clone();
            let group = self.group.clone();
            let cluster = self.cluster.clone();
            let metrics = self.metrics.clone();
            let cap = self.capacity_store.clone();
            let qid = self.query_id.clone();
            tokio::spawn(async move {
                let _ = mgr.release_cluster(&group, &cluster).await;
                if let Some(cap) = cap {
                    if let Err(e) = cap.release(&cluster.0, &qid).await {
                        metrics.on_coordination_failure("capacity_release");
                        tracing::warn!("CapacityStore release failed for query {qid}: {e}");
                    }
                }
                metrics.on_query_finished(&group.0, &cluster.0);
            });
        }
    }
}

// ---------------------------------------------------------------------------
// SyncCancelGuard — kill the engine query if the client drops mid-flight
// ---------------------------------------------------------------------------

/// Issues `adapter.cancel_query` when dropped without [`SyncCancelGuard::disarm`].
///
/// Adapters publish the engine-side id into `id_slot` before the blocking wait.
/// On HTTP handler cancellation the future is dropped, this guard fires, and
/// the backend query is stopped (ClickHouse `KILL QUERY`, StarRocks `KILL QUERY`,
/// DuckDB interrupt, Athena `StopQueryExecution`, Trino `DELETE /v1/query`).
struct SyncCancelGuard {
    adapter: DispatchAdapter,
    id_slot: BackendQueryIdSlot,
    /// The wire auth resolved at submit time — reused so cancellation on disconnect uses
    /// the same identity the query was submitted under, not cluster auth.
    wire_auth: Option<StoredWireAuth>,
    state: Option<Arc<AppState>>,
    ctx: Option<QueryContext>,
    start: Instant,
    disarmed: bool,
}

impl SyncCancelGuard {
    fn new(
        adapter: DispatchAdapter,
        id_slot: BackendQueryIdSlot,
        wire_auth: Option<StoredWireAuth>,
        state: Arc<AppState>,
        ctx: QueryContext,
        start: Instant,
    ) -> Self {
        Self {
            adapter,
            id_slot,
            wire_auth,
            state: Some(state),
            ctx: Some(ctx),
            start,
            disarmed: false,
        }
    }

    /// Do not cancel — the query finished (success or engine error).
    fn disarm(&mut self) {
        self.disarmed = true;
        self.state = None;
        self.ctx = None;
    }

    /// Spawn a best-effort cancel now. Idempotent with [`Drop`].
    fn fire(&mut self) {
        if self.disarmed {
            return;
        }
        self.disarmed = true;
        self.state = None;
        self.ctx = None;
        spawn_sync_cancel(
            self.adapter.clone(),
            self.id_slot.clone(),
            self.wire_auth.clone(),
        );
    }
}

impl Drop for SyncCancelGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        self.disarmed = true;
        spawn_sync_cancel(
            self.adapter.clone(),
            self.id_slot.clone(),
            self.wire_auth.clone(),
        );
        if let (Some(state), Some(ctx)) = (self.state.take(), self.ctx.take()) {
            let backend_query_id = self.id_slot.get().map(|id| id.0);
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id,
                    status: QueryStatus::Cancelled,
                    execution_ms: self.start.elapsed().as_millis() as u64,
                    rows: None,
                    error: Some("client disconnected".to_string()),
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: vec![],
                    was_guard_blocked: false,
                    queue_duration_ms: 0,
                    cache_hit: false,
                },
            );
        }
    }
}

/// Outer deadline for detached `cancel_query` tasks. Matches adapter control-plane
/// timeouts (ClickHouse / StarRocks pool checkout) so a hung backend cannot leak tasks.
const SYNC_CANCEL_TIMEOUT: Duration = Duration::from_secs(10);

fn spawn_sync_cancel(
    adapter: DispatchAdapter,
    id_slot: BackendQueryIdSlot,
    wire_auth: Option<StoredWireAuth>,
) {
    let Some(id) = id_slot.get() else {
        return;
    };
    tokio::spawn(async move {
        match tokio::time::timeout(
            SYNC_CANCEL_TIMEOUT,
            adapter.cancel_query(&id, wire_auth.as_ref()),
        )
        .await
        {
            Err(_) => {
                warn!(
                    backend = %id,
                    timeout_secs = SYNC_CANCEL_TIMEOUT.as_secs(),
                    "Sync cancel timed out"
                );
            }
            Ok(Err(e)) => {
                warn!(backend = %id, "Best-effort sync cancel on client disconnect: {e}");
            }
            Ok(Ok(())) => {
                debug!(backend = %id, "Issued backend cancel after client disconnect");
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Sync execution path — shared by MySQL wire, Postgres wire, Flight SQL
// ---------------------------------------------------------------------------

/// Holds either a native sync adapter or an async adapter that bridges to the sync path.
///
/// Async engines (Trino) implement `execute_as_arrow` internally by driving their own
/// submit+poll loop — allowing MySQL/Postgres clients to query them without needing a
/// separate execution path in dispatch.
#[derive(Clone)]
enum DispatchAdapter {
    Sync(Arc<dyn SyncAdapter>),
    Async(Arc<dyn AsyncAdapter>),
}

impl DispatchAdapter {
    #[allow(clippy::too_many_arguments)]
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        credentials: &QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
        params: &QueryParams,
        hints: queryflux_core::sql_classify::ExecutionHints,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<queryflux_engine_adapters::SyncExecution> {
        match self {
            Self::Sync(a) => {
                a.execute_as_arrow(sql, session, credentials, tags, params, hints, id_slot)
                    .await
            }
            Self::Async(a) => {
                a.execute_as_arrow(sql, session, credentials, tags, params, hints, id_slot)
                    .await
            }
        }
    }

    async fn cancel_query(
        &self,
        backend_id: &queryflux_core::query::BackendQueryId,
        wire_auth: Option<&StoredWireAuth>,
    ) -> Result<()> {
        match self {
            Self::Sync(a) => a.cancel_query(backend_id).await,
            Self::Async(a) => a.cancel_query(backend_id, wire_auth).await,
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
    /// Shared SQL parse cache — used by guardrails and execution hints.
    sql_parse: queryflux_core::sql_classify::SqlParseCache,
    /// The wire auth resolved at submit time — reused by `SyncCancelGuard` so a client
    /// disconnect cancels with the same identity the query was submitted under.
    wire_auth: Option<StoredWireAuth>,
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
            queue_duration_ms: 0,
            cache_hit: false,
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
    mut session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    auth_ctx: &AuthContext,
) -> Result<SyncQuerySetup> {
    let query_id = ProxyQueryId::new();

    let (cluster_manager, group_fixups, group_default_tags, wait_timeout_secs) = {
        let live = state.live.read().await;
        let wait_timeout_secs = live
            .group_capacity_wait_timeout_secs
            .get(&group.0)
            .copied()
            .unwrap_or(queryflux_core::config::DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS);
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
            wait_timeout_secs,
        )
    };
    let effective_tags: QueryTags = merge_tags(&group_default_tags, &session.tags().clone());

    // Queue loop: spin until a cluster slot is available (both local and global).
    // `wait_start` is this query's place in line for the fairness gate: queued
    // queries enqueued before it (and still polling) get freed slots first.
    // Bound by `capacityWaitTimeoutSecs` so clients cannot wait forever.
    let wait_start = Utc::now();
    let deadline = wait_start + chrono::Duration::seconds(wait_timeout_secs as i64);
    let mut seq: u64 = 0;
    let (cluster_name, adapter, this_cluster_cfg) = loop {
        if Utc::now() >= deadline {
            return Err(QueryFluxError::CapacityWaitTimeout {
                group: group.0.clone(),
                timeout_secs: wait_timeout_secs,
            });
        }
        if should_yield_to_older_queued(state, &cluster_manager, &group, Some(wait_start)).await {
            queued_backoff_delay(seq).await;
            seq += 1;
            continue;
        }
        match cluster_manager.acquire_cluster(&group).await? {
            Some(name) => {
                match acquire_global_capacity(state, &cluster_manager, &group, &name, &query_id.0)
                    .await
                {
                    CapacityGrant::Denied => {
                        // Global capacity full or coordination unavailable (fail-closed) —
                        // release local slot and retry with backoff.
                        let _ = cluster_manager.release_cluster(&group, &name).await;
                        queued_backoff_delay(seq).await;
                        seq += 1;
                        continue;
                    }
                    CapacityGrant::Granted => {}
                }
                // Read the adapter and its cluster config from the same lock acquisition —
                // this loop can spin for a while waiting on capacity, so a config update
                // landing mid-wait must not let a fresh adapter get paired with a stale
                // (or vice versa) cluster config for credential resolution below.
                let (adapter_for_cluster, cluster_cfg_for_cluster) = {
                    let live = state.live.read().await;
                    (
                        live.adapters.get(&name.0).cloned(),
                        live.cluster_configs.get(&name.0).cloned(),
                    )
                };
                match adapter_for_cluster {
                    Some(AdapterKind::Sync(a)) => {
                        break (name, DispatchAdapter::Sync(a), cluster_cfg_for_cluster)
                    }
                    Some(AdapterKind::Async(a)) => {
                        break (name, DispatchAdapter::Async(a), cluster_cfg_for_cluster)
                    }
                    None => {
                        let _ = cluster_manager.release_cluster(&group, &name).await;
                        if let Some(cap) = &state.capacity_store {
                            let _ = cap.release(&name.0, &query_id.0).await;
                        }
                        return Err(QueryFluxError::Engine(format!(
                            "No adapter for {group}/{name}"
                        )));
                    }
                }
            }
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
        state.capacity_store.clone(),
        query_id.0.clone(),
    );

    let tgt_dialect = adapter.translation_target_dialect();
    let src_dialect = resolve_src_dialect(&session, &protocol);
    let engine_type = adapter.engine_type();
    let start = Instant::now();

    // Translate SQL. On failure: record the query, release the slot, propagate the error.
    // The caller (execute_to_sink) will notify the sink via on_error. Skipped entirely
    // (sqlglot never invoked) when should_attempt_translation is false — see its doc
    // comment for why MCP without a declared dialect takes this path.
    let translated = if should_attempt_translation(&session, &protocol) {
        match state
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
                        queue_duration_ms: 0,
                        cache_hit: false,
                    },
                );
                slot.release().await;
                return Err(e);
            }
        }
    } else {
        sql.clone()
    };

    let was_translated = translated != sql;

    let credentials = match state
        .identity_resolver
        .resolve(auth_ctx, this_cluster_cfg.as_ref())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            slot.release().await;
            return Err(e);
        }
    };

    // Same enrichment as the async dispatch path: Flight SQL already forwards a client
    // `Authorization` gRPC metadata header into `session.extra` verbatim, so this is only
    // a no-op fallback for `passthrough` clusters when that header is missing but the
    // caller authenticated via OIDC. `execute_as_arrow` (Trino reached via this sync
    // bridge) resolves its own wire auth internally from this same session.
    enrich_session_for_passthrough(&mut session, &credentials, auth_ctx);

    // Resolved the same way as the async dispatch path (after enrichment, so passthrough
    // benefits from it too), so a disconnect mid-query cancels with the identity the query
    // actually submitted under — not cluster auth — for passthrough/impersonate/
    // tokenExchange queries reached via the sync bridge (Postgres/MySQL/Flight wire, or
    // Trino's own `execute_as_arrow`).
    let cluster_sets_http_auth = this_cluster_cfg
        .as_ref()
        .and_then(|c| c.auth.as_ref())
        .is_some_and(|a| a.sets_http_authorization());
    let wire_auth: Option<StoredWireAuth> =
        resolve_stored_wire_auth(&credentials, &session, cluster_sets_http_auth);

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
        tgt_dialect: tgt_dialect.clone(),
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
        translated: translated.clone(),
        sql_parse: queryflux_core::sql_classify::SqlParseCache::new(translated, tgt_dialect),
        start,
        slot,
        ctx,
        credentials,
        params: effective_params,
        guard_actions: vec![],
        wire_auth,
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
    id_slot: &BackendQueryIdSlot,
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
            queryflux_core::sql_classify::ExecutionHints {
                is_read_like: Some(setup.sql_parse.is_read_like_async().await),
            },
            id_slot,
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
    let affected_rows = execution.affected_rows;

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

    // Do NOT synthesize on_schema(Schema::empty()) for empty streams: DDL/DML
    // statements produce no batches and sinks treat the absence of on_schema as a
    // signal to emit an OK/CommandComplete instead of a result-set sequence.

    let stats = QueryStats {
        execution_duration_ms: elapsed_ms,
        rows_returned,
        affected_rows,
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
    id_slot: &BackendQueryIdSlot,
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
            id_slot,
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

async fn run_plan_guards(
    guard_chain: &Option<Arc<GuardChain>>,
    group_guard_chain: &Option<Arc<GuardChain>>,
    sql: &str,
    group: &ClusterGroupName,
    session: &SessionContext,
    effective_tags: &queryflux_core::tags::QueryTags,
) -> std::result::Result<Vec<queryflux_persistence::GuardAction>, String> {
    let engine_type = queryflux_core::query::EngineType::Cache;
    let resolved_agent_ctx = session.resolved_agent_context();
    let sql_parse = queryflux_core::sql_classify::SqlParseCache::new(
        sql.to_string(),
        queryflux_core::query::SqlDialect::Generic,
    );
    let guard_ctx = GuardContext {
        sql,
        translated_sql: sql,
        engine_type: &engine_type,
        cluster_group: group,
        user: session.user(),
        agent_context: resolved_agent_ctx.as_ref(),
        query_tags: effective_tags,
        sql_parse: Some(&sql_parse),
    };

    let mut all_actions = Vec::new();
    for chain in [guard_chain.as_ref(), group_guard_chain.as_ref()]
        .into_iter()
        .flatten()
    {
        let (actions, was_blocked) = chain.run(&guard_ctx, GuardLayer::Plan).await;
        all_actions.extend(actions);
        if was_blocked {
            return Err(all_actions
                .iter()
                .find(|a| a.action == "deny")
                .and_then(|a| a.reason.clone())
                .unwrap_or_else(|| "query blocked by guardrail".to_string()));
        }
    }
    Ok(all_actions)
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
    let (authorization, guard_chain, group_guard_chain, cache_cfg) = {
        let live = state.live.read().await;
        (
            live.authorization.clone(),
            live.guard_chain.clone(),
            live.group_guard_chains.get(&group.0).cloned(),
            live.group_cache_settings.get(&group.0).cloned(),
        )
    };

    if !authorization.check(auth_ctx, &group.0).await {
        let msg = format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        );
        return sink.on_error(&msg).await;
    }

    // --- Query result cache: check for hit before acquiring a cluster slot ---
    let cache_hint = queryflux_cache::extract_cache_hint(&sql, &session);
    let effective_cache = cache_cfg.or_else(|| cache_hint.as_ref().map(|h| h.to_group_config()));
    let cache_key = effective_cache
        .as_ref()
        .filter(|_| {
            queryflux_cache::is_deterministic(
                &sql,
                &queryflux_fingerprint::polyglot_dialect(&protocol.default_dialect()),
            )
        })
        .map(|_| queryflux_cache::CacheKey::new(&sql, &group.0, &session, &auth_ctx.user, &params));

    if let Some(ref key) = cache_key {
        let effective_tags = {
            let live = state.live.read().await;
            let group_defaults = live
                .group_default_tags
                .get(&group.0)
                .cloned()
                .unwrap_or_default();
            merge_tags(&group_defaults, &session.tags().clone())
        };
        let guard_actions = match run_plan_guards(
            &guard_chain,
            &group_guard_chain,
            &sql,
            &group,
            &session,
            &effective_tags,
        )
        .await
        {
            Ok(actions) => actions,
            Err(deny_reason) => return sink.on_error(&deny_reason).await,
        };

        let mut cache_sink_adapter = SinkCacheAdapter(sink);
        match state
            .result_cache
            .try_stream_cached(key, &mut cache_sink_adapter)
            .await
        {
            Ok(Some(_stats)) => {
                info!(cache_key = %key, rows = _stats.row_count, "Cache hit — serving from cache");
                state.metrics.on_cache_hit(&group.0);

                let ctx = QueryContext {
                    query_id: ProxyQueryId::new(),
                    sql: sql.chars().take(500).collect(),
                    session: session.clone(),
                    protocol: protocol.clone(),
                    group: group.clone(),
                    cluster: ClusterName("(cache)".to_string()),
                    cluster_group_config_id: None,
                    cluster_config_id: None,
                    engine_type: queryflux_core::query::EngineType::Cache,
                    src_dialect: queryflux_core::query::SqlDialect::Generic,
                    tgt_dialect: queryflux_core::query::SqlDialect::Generic,
                    was_translated: false,
                    translated_sql: None,
                    query_tags: effective_tags,
                    query_params: vec![],
                    agent_context: session.resolved_agent_context(),
                };
                state.record_query(
                    &ctx,
                    QueryOutcome {
                        backend_query_id: None,
                        status: QueryStatus::Success,
                        execution_ms: 0,
                        rows: Some(_stats.row_count),
                        error: None,
                        routing_trace: None,
                        engine_stats: None,
                        guard_actions,
                        was_guard_blocked: false,
                        queue_duration_ms: 0,
                        cache_hit: true,
                    },
                );

                let stats = queryflux_core::query::QueryStats {
                    rows_returned: _stats.row_count,
                    bytes_returned: Some(_stats.size_bytes),
                    queue_duration_ms: 0,
                    execution_duration_ms: 0,
                    affected_rows: None,
                };
                return sink.on_complete(&stats).await;
            }
            Ok(None) => {
                info!(cache_key = %key, "Cache miss");
                state.metrics.on_cache_miss(&group.0);
            }
            Err(e) => {
                warn!(cache_key = %key, error = %e, "Cache lookup failed; proceeding without cache");
            }
        }
    }

    // --- Cache miss path: wrap sink in TeeResultSink if caching is applicable ---
    let cache_writer = if let (Some(ref key), Some(ref cfg)) = (&cache_key, &effective_cache) {
        match state.result_cache.writer(key, cfg.ttl_secs).await {
            Ok(w) => Some(w),
            Err(e) => {
                warn!(error = %e, "Failed to create cache writer; proceeding without caching");
                None
            }
        }
    } else {
        None
    };

    if let Some(writer) = cache_writer {
        let max_bytes = effective_cache
            .as_ref()
            .and_then(|c| c.max_entry_size_mb)
            .map(|mb| mb * 1024 * 1024);
        let group_name = group.0.clone();
        let mut tee = crate::tee_sink::TeeResultSink::new(sink, writer, max_bytes);
        let result = execute_to_sink_inner(
            state,
            sql,
            params,
            session,
            protocol,
            group,
            auth_ctx,
            &mut tee,
            guard_chain,
            group_guard_chain,
        )
        .await;
        tee.finalize_cache(result.is_ok()).await;
        if result.is_ok() && tee.cache_committed() {
            state.metrics.on_cache_write(&group_name);
        }
        result
    } else {
        execute_to_sink_inner(
            state,
            sql,
            params,
            session,
            protocol,
            group,
            auth_ctx,
            sink,
            guard_chain,
            group_guard_chain,
        )
        .await
    }
}

/// Adapter to use a `ResultSink` as a `CacheSink` during cache replay.
struct SinkCacheAdapter<'a, S: ResultSink + ?Sized>(&'a mut S);

#[async_trait]
impl<S: ResultSink + ?Sized> queryflux_cache::CacheSink for SinkCacheAdapter<'_, S> {
    async fn on_schema(&mut self, schema: &arrow::datatypes::Schema) -> anyhow::Result<()> {
        self.0
            .on_schema(schema)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
    async fn on_batch(&mut self, batch: &arrow::record_batch::RecordBatch) -> anyhow::Result<()> {
        self.0
            .on_batch(batch)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

/// Inner function that does the actual sync query execution (slot acquisition,
/// guard chain, engine submission). Extracted so `execute_to_sink` can wrap it
/// with `TeeResultSink` on cache-miss without duplicating logic.
#[allow(clippy::too_many_arguments)]
async fn execute_to_sink_inner(
    state: &Arc<AppState>,
    sql: String,
    params: QueryParams,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    auth_ctx: &AuthContext,
    sink: &mut impl ResultSink,
    guard_chain: Option<Arc<GuardChain>>,
    group_guard_chain: Option<Arc<GuardChain>>,
) -> Result<()> {
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
            sql_parse: Some(&setup.sql_parse),
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
                        queue_duration_ms: 0,
                        cache_hit: false,
                    },
                );
                return sink.on_error(&deny_reason).await;
            }
        }

        // Attach non-blocking guard actions (allow/warn) to the setup context so they
        // flow into record_query at the normal exit point below.
        setup.guard_actions = all_actions;
    }

    if let Err(e) = sink.on_translated_sql(&setup.translated).await {
        return sink.on_error(&e.to_string()).await;
    }

    // Native path: skip Arrow when backend connection format matches frontend protocol.
    // All other guarantees (slot release, record_query) are upheld by this function's
    // outer structure — only the inner execution subroutine is swapped.
    let id_slot = BackendQueryIdSlot::new();
    let mut cancel = SyncCancelGuard::new(
        setup.adapter.clone(),
        id_slot.clone(),
        setup.wire_auth.clone(),
        state.clone(),
        setup.ctx.clone(),
        setup.start,
    );
    let (mut outcome, sink_result) = if setup
        .adapter
        .connection_format()
        .matches_frontend(&protocol)
    {
        execute_native_to_sink(&setup, &protocol, sink, &id_slot).await
    } else {
        execute_stream(&setup, sink, &id_slot).await
    };
    if sink_result.is_err() {
        // Client gone mid-stream (or during schema send): stop the engine query.
        cancel.fire();
        // Only reclassify when the engine itself did not already fail — otherwise
        // an engine error delivered to a departed client is logged as a cancel.
        if outcome.status == QueryStatus::Success {
            outcome.status = QueryStatus::Cancelled;
        }
        if outcome.error.is_none() {
            outcome.error = Some("client disconnected".to_string());
        }
    }
    // Successful completion and engine errors: the query is already finished.
    cancel.disarm();

    // Guaranteed single exit: release slot, then record.
    // slot.release() is idempotent and sets released=true so Drop is a no-op.
    setup.slot.release().await;
    let mut final_outcome: QueryOutcome = outcome.into();
    final_outcome.backend_query_id = id_slot.get().map(|id| id.0);
    // Prepend guard actions (allow/warn) collected before execution.
    if !setup.guard_actions.is_empty() {
        setup.guard_actions.extend(final_outcome.guard_actions);
        final_outcome.guard_actions = setup.guard_actions;
    }
    state.record_query(&setup.ctx, final_outcome);

    sink_result
}

#[cfg(test)]
mod queue_limit_tests {
    use super::persist_queued_query;
    use crate::state::test_fixtures::app_state;
    use queryflux_auth::AuthContext;
    use queryflux_core::error::QueryFluxError;
    use queryflux_core::query::{ClusterGroupName, FrontendProtocol, ProxyQueryId};
    use queryflux_core::session::SessionContext;
    use std::collections::HashMap;

    #[tokio::test]
    async fn persist_queued_rejects_when_at_limit() {
        let state = app_state(false);
        let auth = AuthContext {
            user: "alice".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        };
        let group = ClusterGroupName("default".into());

        persist_queued_query(
            &state,
            ProxyQueryId::new(),
            "SELECT 1".into(),
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group.clone(),
            false,
            0,
            Some(1),
            &auth,
        )
        .await
        .expect("first enqueue");

        let err = persist_queued_query(
            &state,
            ProxyQueryId::new(),
            "SELECT 2".into(),
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group.clone(),
            false,
            0,
            Some(1),
            &auth,
        )
        .await
        .expect_err("second enqueue must fail");
        match err {
            QueryFluxError::QueueFull {
                group: g,
                count,
                limit,
            } => {
                assert_eq!(g, "default");
                assert_eq!(count, 1);
                assert_eq!(limit, 1);
            }
            other => panic!("expected QueueFull, got {other}"),
        }
    }

    #[tokio::test]
    async fn already_stored_skips_limit_check() {
        let state = app_state(false);
        let auth = AuthContext {
            user: "alice".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        };
        let group = ClusterGroupName("default".into());
        let id = ProxyQueryId::new();

        persist_queued_query(
            &state,
            id.clone(),
            "SELECT 1".into(),
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group.clone(),
            false,
            0,
            Some(1),
            &auth,
        )
        .await
        .expect("first enqueue");

        persist_queued_query(
            &state,
            id,
            "SELECT 1".into(),
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group,
            true,
            1,
            Some(1),
            &auth,
        )
        .await
        .expect("already_stored must bypass limit");
    }

    #[tokio::test]
    async fn dispatch_reads_limit_from_live_config() {
        use super::dispatch_query;
        use queryflux_cluster_manager::cluster_state::ClusterState;
        use queryflux_cluster_manager::simple::SimpleClusterGroupManager;
        use queryflux_core::query::{ClusterName, EngineType};
        use std::sync::Arc;

        let state = app_state(false);
        {
            let mut live = state.live.write().await;
            live.group_max_queued_queries
                .insert("default".into(), Some(1));
            let group = ClusterGroupName("default".into());
            let cluster = ClusterName("trino".into());
            let cluster_state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::Trino,
                Some("http://trino.test:8080".into()),
                0,
                true,
            ));
            let mut groups = HashMap::new();
            groups.insert(
                group.clone(),
                (
                    vec![cluster_state],
                    Arc::new(queryflux_cluster_manager::strategy::RoundRobinStrategy::new())
                        as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
                ),
            );
            live.cluster_manager = Arc::new(SimpleClusterGroupManager::new(groups));
        }

        let auth = AuthContext {
            user: "alice".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        };
        let group = ClusterGroupName("default".into());

        dispatch_query(
            &state,
            ProxyQueryId::new(),
            "SELECT 1".into(),
            vec![],
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group.clone(),
            false,
            None,
            0,
            &auth,
        )
        .await
        .expect("first should queue");

        let err = match dispatch_query(
            &state,
            ProxyQueryId::new(),
            "SELECT 2".into(),
            vec![],
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            group,
            false,
            None,
            0,
            &auth,
        )
        .await
        {
            Ok(_) => panic!("second should hit QueueFull"),
            Err(e) => e,
        };
        assert!(
            matches!(err, QueryFluxError::QueueFull { limit: 1, .. }),
            "got {err}"
        );
    }
}

#[cfg(test)]
mod capacity_wait_tests {
    use super::setup_sync_query;
    use crate::state::test_fixtures::app_state;
    use queryflux_auth::AuthContext;
    use queryflux_cluster_manager::cluster_state::ClusterState;
    use queryflux_cluster_manager::simple::SimpleClusterGroupManager;
    use queryflux_core::error::QueryFluxError;
    use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType, FrontendProtocol};
    use queryflux_core::session::SessionContext;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    #[tokio::test]
    async fn sync_setup_times_out_when_no_capacity() {
        let state = app_state(false);
        {
            let mut live = state.live.write().await;
            live.group_capacity_wait_timeout_secs
                .insert("default".into(), 1);
            let group = ClusterGroupName("default".into());
            let cluster = ClusterName("trino".into());
            // max_running_queries = 0 → acquire_cluster always returns None.
            let cluster_state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::Trino,
                Some("http://trino.test:8080".into()),
                0,
                true,
            ));
            let mut groups = HashMap::new();
            groups.insert(
                group.clone(),
                (
                    vec![cluster_state],
                    Arc::new(queryflux_cluster_manager::strategy::RoundRobinStrategy::new())
                        as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
                ),
            );
            live.cluster_manager = Arc::new(SimpleClusterGroupManager::new(groups));
        }

        let auth = AuthContext {
            user: "alice".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        };
        let started = Instant::now();
        let result = setup_sync_query(
            &state,
            "SELECT 1".into(),
            vec![],
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            ClusterGroupName("default".into()),
            &auth,
        )
        .await;
        assert!(
            started.elapsed().as_secs() < 10,
            "timeout should fire near 1s, got {:?}",
            started.elapsed()
        );
        let err = match result {
            Ok(_) => panic!("must time out waiting for capacity"),
            Err(e) => e,
        };
        match err {
            QueryFluxError::CapacityWaitTimeout {
                group,
                timeout_secs,
            } => {
                assert_eq!(group, "default");
                assert_eq!(timeout_secs, 1);
            }
            other => panic!("expected CapacityWaitTimeout, got {other}"),
        }
    }
}

#[cfg(test)]
mod resolve_src_dialect_tests {
    use super::resolve_src_dialect;
    use queryflux_core::query::{FrontendProtocol, SqlDialect};
    use queryflux_core::session::SessionContext;

    #[test]
    fn mcp_without_override_records_generic_not_a_guess() {
        // No inference here — Generic is the honest "we don't know" value, same as
        // FlightSql's own default_dialect(). It is not the target engine's dialect.
        let session = SessionContext::default();
        let resolved = resolve_src_dialect(&session, &FrontendProtocol::Mcp);
        assert_eq!(resolved, SqlDialect::Generic);
    }

    #[test]
    fn non_mcp_protocol_uses_its_own_wire_implied_default() {
        let session = SessionContext::default();
        let resolved = resolve_src_dialect(&session, &FrontendProtocol::PostgresWire);
        assert_eq!(resolved, SqlDialect::Postgres);
    }

    #[test]
    fn explicit_session_override_wins_for_mcp() {
        let mut session = SessionContext::default();
        session
            .extra
            .insert("dialect".to_string(), "bigquery".to_string());
        let resolved = resolve_src_dialect(&session, &FrontendProtocol::Mcp);
        assert_eq!(resolved, SqlDialect::Sqlglot("bigquery".to_string()));
    }

    #[test]
    fn explicit_session_override_wins_for_other_protocols_too() {
        let mut session = SessionContext::default();
        session
            .extra
            .insert("dialect".to_string(), "snowflake".to_string());
        let resolved = resolve_src_dialect(&session, &FrontendProtocol::TrinoHttp);
        assert_eq!(resolved, SqlDialect::Sqlglot("snowflake".to_string()));
    }
}

#[cfg(test)]
mod should_attempt_translation_tests {
    use super::should_attempt_translation;
    use queryflux_core::query::FrontendProtocol;
    use queryflux_core::session::SessionContext;

    #[test]
    fn mcp_without_override_skips_translation() {
        let session = SessionContext::default();
        assert!(!should_attempt_translation(
            &session,
            &FrontendProtocol::Mcp
        ));
    }

    #[test]
    fn mcp_with_explicit_override_attempts_translation() {
        let mut session = SessionContext::default();
        session
            .extra
            .insert("dialect".to_string(), "postgres".to_string());
        assert!(should_attempt_translation(&session, &FrontendProtocol::Mcp));
    }

    #[test]
    fn every_other_protocol_always_attempts_translation() {
        let session = SessionContext::default();
        for protocol in [
            FrontendProtocol::TrinoHttp,
            FrontendProtocol::PostgresWire,
            FrontendProtocol::MySqlWire,
            FrontendProtocol::ClickHouseHttp,
            FrontendProtocol::FlightSql,
            FrontendProtocol::SnowflakeHttp,
            FrontendProtocol::SnowflakeSqlApi,
        ] {
            assert!(
                should_attempt_translation(&session, &protocol),
                "{protocol:?} should always attempt translation (maybe_translate no-ops on its own when compatible)"
            );
        }
    }
}
