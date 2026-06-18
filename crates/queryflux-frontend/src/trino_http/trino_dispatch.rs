use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::query::{ExecutingQuery, FrontendProtocol, QueryEngineStats, QueryStatus};
use queryflux_core::session::SessionContext;
use queryflux_engine_adapters::trino::api::TrinoResponse;
use queryflux_engine_adapters::AsyncAdapter;
use tracing::warn;

use crate::state::{AppState, QueryContext, QueryOutcome};

/// Rewrite a Trino-origin URL to point to QueryFlux instead, keeping the full path.
/// `http://trino:8080/v1/statement/executing/{id}/{token}` →
/// `http://queryflux:9000/v1/statement/executing/{id}/{token}`
///
/// Any instance can then reconstruct the Trino URL by looking up the stored
/// `trino_endpoint` and re-joining it with the path.
pub(crate) fn rewrite_trino_uri(trino_uri: &str, external_address: &str) -> String {
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

/// Determine the terminal `QueryOutcome` from a Trino submit response body.
///
/// Parses the body to determine success vs failure. `engine_stats` is passed in
/// from `adapter.terminal_stats_from_body()` — Trino-specific stats parsing lives
/// in the adapter, not here.
///
/// Returns `(outcome, Option<warn_log_message>)`.
pub(crate) fn trino_submit_terminal_outcome(
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
                    queue_duration_ms: 0,
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
                queue_duration_ms: 0,
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
                queue_duration_ms: 0,
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
                queue_duration_ms: 0,
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
pub(crate) async fn finalize_trino_async_terminal_on_submit(
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

    let stored_actions: Vec<queryflux_core::query::GuardAction> = serde_json::from_value(
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
