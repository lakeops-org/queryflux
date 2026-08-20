use queryflux_auth::AuthContext;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use serde::Serialize;

use crate::{ChainRouteResult, RouterTrait, RoutingDecision};

/// One router's evaluation result recorded in the trace.
#[derive(Debug, Clone, Serialize)]
pub struct RouterDecision {
    pub router_type: &'static str,
    pub matched: bool,
    /// The group selected by this router, if it routed.
    pub result: Option<String>,
    /// Present when this router denied the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny_message: Option<String>,
}

/// Full record of how a query was routed: every router that was evaluated,
/// which one matched, and whether the fallback was used.
#[derive(Debug, Clone, Serialize)]
pub struct RoutingTrace {
    pub decisions: Vec<RouterDecision>,
    pub final_group: String,
    pub used_fallback: bool,
    /// Present when routing ended in a deny (final_group may be empty).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied: Option<String>,
}

/// Evaluates a list of routers in order; first Route or Deny wins.
/// Falls back to the configured default group if all routers return NoMatch.
pub struct RouterChain {
    routers: Vec<Box<dyn RouterTrait>>,
    fallback: ClusterGroupName,
}

impl RouterChain {
    pub fn new(routers: Vec<Box<dyn RouterTrait>>, fallback: ClusterGroupName) -> Self {
        Self { routers, fallback }
    }

    /// Route and return only the chain result (no trace overhead).
    pub async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<ChainRouteResult> {
        for router in &self.routers {
            match router
                .route(sql, session, frontend_protocol, auth_ctx)
                .await?
            {
                RoutingDecision::Route(group) => return Ok(ChainRouteResult::Routed(group)),
                RoutingDecision::Deny { message } => {
                    return Ok(ChainRouteResult::Denied { message });
                }
                RoutingDecision::NoMatch => {}
            }
        }
        Ok(ChainRouteResult::Routed(self.fallback.clone()))
    }

    /// Route and return both the chain result and a full routing trace for metrics/UI.
    pub async fn route_with_trace(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<(ChainRouteResult, RoutingTrace)> {
        let mut decisions = Vec::with_capacity(self.routers.len());

        for router in &self.routers {
            match router
                .route(sql, session, frontend_protocol, auth_ctx)
                .await?
            {
                RoutingDecision::Route(group) => {
                    decisions.push(RouterDecision {
                        router_type: router.type_name(),
                        matched: true,
                        result: Some(group.0.clone()),
                        deny_message: None,
                    });
                    let trace = RoutingTrace {
                        decisions,
                        final_group: group.0.clone(),
                        used_fallback: false,
                        denied: None,
                    };
                    return Ok((ChainRouteResult::Routed(group), trace));
                }
                RoutingDecision::Deny { message } => {
                    decisions.push(RouterDecision {
                        router_type: router.type_name(),
                        matched: true,
                        result: None,
                        deny_message: Some(message.clone()),
                    });
                    let trace = RoutingTrace {
                        decisions,
                        final_group: String::new(),
                        used_fallback: false,
                        denied: Some(message.clone()),
                    };
                    return Ok((ChainRouteResult::Denied { message }, trace));
                }
                RoutingDecision::NoMatch => {
                    decisions.push(RouterDecision {
                        router_type: router.type_name(),
                        matched: false,
                        result: None,
                        deny_message: None,
                    });
                }
            }
        }

        let trace = RoutingTrace {
            decisions,
            final_group: self.fallback.0.clone(),
            used_fallback: true,
            denied: None,
        };
        Ok((ChainRouteResult::Routed(self.fallback.clone()), trace))
    }
}
