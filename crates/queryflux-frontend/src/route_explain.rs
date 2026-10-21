//! "Where would this query run, and would guards let it?" without running it.
//! Shared by the admin route-explain endpoint and the MCP `explain_route` tool.

use queryflux_auth::AuthContext;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, EngineType, FrontendProtocol},
    session::SessionContext,
};
use queryflux_guardrails::{run_chains, GuardChainOutcome, GuardContext, GuardLayer};
use queryflux_persistence::GuardAction;
use queryflux_routing::{chain::RoutingTrace, ChainRouteResult};
use serde::Serialize;

use crate::state::LiveConfig;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteExplanation {
    /// Cluster group the query would run on; `None` when routing denied it.
    pub group: Option<String>,
    /// What decided: the matching router's type, `fallback`, or `denied`.
    pub rule: String,
    /// Clusters in the group and their engines.
    pub clusters: Vec<ClusterEngine>,
    pub trace: RoutingTrace,
    /// Guard verdict for the group (global + group chains); `None` when routing denied it.
    pub guards: Option<GuardPreview>,
}

#[derive(Debug, Serialize)]
pub struct ClusterEngine {
    pub cluster: String,
    pub engine: Option<EngineType>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardPreview {
    /// `allow`, `rewrite` or `deny`.
    pub outcome: &'static str,
    /// Client-facing error when denied (same text a real query gets).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rewritten_sql: Option<String>,
    pub actions: Vec<GuardAction>,
}

/// Routing rule for response headers and explanations.
pub fn route_rule(trace: &RoutingTrace) -> String {
    if trace.denied.is_some() {
        return "denied".to_string();
    }
    if trace.used_fallback {
        return "fallback".to_string();
    }
    trace
        .decisions
        .iter()
        .find(|d| d.matched)
        .map(|d| d.router_type.to_string())
        .unwrap_or_else(|| "fallback".to_string())
}

pub async fn explain_route(
    live: &LiveConfig,
    sql: &str,
    session: &SessionContext,
    protocol: &FrontendProtocol,
    auth_ctx: &AuthContext,
) -> Result<RouteExplanation> {
    let (result, mut trace) = live
        .router_chain
        .route_with_trace(sql, session, protocol, Some(auth_ctx))
        .await?;
    let group = match result {
        ChainRouteResult::Denied { .. } => {
            return Ok(RouteExplanation {
                group: None,
                rule: route_rule(&trace),
                clusters: vec![],
                trace,
                guards: None,
            })
        }
        ChainRouteResult::Routed(g) => live.resolve_routed_group(g, &mut trace, auth_ctx).await?,
    };

    let clusters = live
        .group_members
        .get(&group.0)
        .into_iter()
        .flatten()
        .map(|name| ClusterEngine {
            cluster: name.clone(),
            engine: live.adapters.get(name).map(|a| a.engine_type()),
        })
        .collect();

    Ok(RouteExplanation {
        group: Some(group.0.clone()),
        rule: route_rule(&trace),
        clusters,
        trace,
        guards: Some(preview_guards(live, sql, session, protocol, auth_ctx, &group).await),
    })
}

// ponytail: guards see the client SQL in the protocol's dialect, not the translated engine
// SQL; the verdict can differ for queries whose translation changes what guards inspect.
async fn preview_guards(
    live: &LiveConfig,
    sql: &str,
    session: &SessionContext,
    protocol: &FrontendProtocol,
    auth_ctx: &AuthContext,
    group: &ClusterGroupName,
) -> GuardPreview {
    let dialect = protocol.default_dialect();
    let engine_type = live
        .group_members
        .get(&group.0)
        .and_then(|m| m.first())
        .and_then(|c| live.adapters.get(c))
        .map(|a| a.engine_type())
        .unwrap_or(EngineType::Cache);
    let tags = queryflux_core::tags::QueryTags::new();
    let agent_context = session.resolved_agent_context();
    let ctx = GuardContext {
        sql,
        original_sql: None,
        dialect: &dialect,
        engine_type: &engine_type,
        cluster_group: group,
        user: Some(auth_ctx.user.as_str()),
        groups: &auth_ctx.groups,
        roles: &auth_ctx.roles,
        attributes: &auth_ctx.attributes,
        agent_context: agent_context.as_ref(),
        query_tags: &tags,
        session_extra: &session.extra,
        schema: None,
        sql_parse: None,
    };
    let chains = [
        live.guard_chain.as_deref(),
        live.group_guard_chains.get(&group.0).map(|c| c.as_ref()),
    ]
    .into_iter()
    .flatten();
    let (actions, outcome) = run_chains(chains, &ctx, GuardLayer::Plan).await;
    match outcome {
        GuardChainOutcome::Blocked { .. } => GuardPreview {
            outcome: "deny",
            message: Some(queryflux_guardrails::deny_message(&actions)),
            rewritten_sql: None,
            actions,
        },
        GuardChainOutcome::Rewritten { sql } => GuardPreview {
            outcome: "rewrite",
            message: None,
            rewritten_sql: Some(sql),
            actions,
        },
        GuardChainOutcome::Proceed => GuardPreview {
            outcome: "allow",
            message: None,
            rewritten_sql: None,
            actions,
        },
    }
}
