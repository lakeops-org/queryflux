pub mod chain;
pub mod implementations;

use async_trait::async_trait;
use queryflux_auth::AuthContext;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

/// Outcome of a single router's evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Router did not match — try the next router in the chain.
    NoMatch,
    /// Route the query to this cluster group.
    Route(ClusterGroupName),
    /// Reject the query before dispatch with a client-visible message.
    Deny { message: String },
}

impl From<Option<ClusterGroupName>> for RoutingDecision {
    fn from(value: Option<ClusterGroupName>) -> Self {
        match value {
            Some(group) => Self::Route(group),
            None => Self::NoMatch,
        }
    }
}

/// Final outcome of evaluating the full router chain (after fallback).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainRouteResult {
    Routed(ClusterGroupName),
    Denied { message: String },
}

/// A router inspects an incoming query and returns a routing decision.
///
/// Routers are evaluated in order (a chain). The first `Route` or `Deny` wins.
/// If all routers return `NoMatch`, the routing fallback group from config is used.
#[async_trait]
pub trait RouterTrait: Send + Sync {
    /// Short name used in routing traces (e.g. `"Header"`, `"QueryRegex"`).
    fn type_name(&self) -> &'static str;

    /// Route an incoming query.
    ///
    /// `auth_ctx` is `None` during Phase 1 (NoneAuthProvider, no threading yet) and
    /// `Some` once authentication is wired into all frontends (Phase 2+).
    /// Identity-aware routers (e.g. `UserGroup`) should prefer `auth_ctx.user` when
    /// available, falling back to `session.user()` only when `auth_ctx` is `None`.
    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision>;
}
