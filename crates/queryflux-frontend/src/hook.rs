//! Query lifecycle hooks — an embedder-registered observation/interception point that
//! runs on every query, in-process, alongside (not instead of) the guard chain.
//!
//! Hooks are static on [`AppState`](crate::state::AppState), not part of `LiveConfig`:
//! unlike guards/routers, they aren't re-registered on every hot reload. An empty
//! [`HookBus`] costs one `Arc` clone on the dispatch path — no allocation, no branch
//! beyond the empty-`Vec` iteration.

use std::sync::Arc;

use async_trait::async_trait;
use queryflux_auth::AuthContext;
use queryflux_core::{
    error::QueryFluxError,
    query::{ClusterGroupName, ClusterName, EngineType, FrontendProtocol},
    session::SessionContext,
    tags::QueryTags,
};

/// Borrowed view of an in-flight query, passed to every [`QueryHook`] call site.
///
/// `group` / `cluster` / `engine_type` are `None` before routing/adapter-selection has
/// happened yet (e.g. in `before_route`) and `Some` from `after_route` onward. `sql` is
/// owned rather than borrowed so `before_route` / `before_translate` can rewrite it —
/// the caller reads `ctx.sql` back after the hook call to pick up any change. There is
/// deliberately no `query_id`: routing runs before every frontend mints one.
///
/// `rows` / `execution_ms` are `None` everywhere except where a result is actually
/// known: `after_execute` and `on_error` on the sync (MySQL/Postgres/FlightSQL) and
/// cache-hit paths. The async (Trino submit/poll) path's `after_execute` fires at
/// submission-accepted, before the engine has produced a result, so both stay `None`
/// there — polling for the final result happens in a separate code path this hook
/// system doesn't observe yet.
pub struct HookContext<'a> {
    pub sql: String,
    pub session: &'a SessionContext,
    pub protocol: &'a FrontendProtocol,
    pub group: Option<&'a ClusterGroupName>,
    pub cluster: Option<&'a ClusterName>,
    pub engine_type: Option<&'a EngineType>,
    pub query_tags: &'a QueryTags,
    pub auth: Option<&'a AuthContext>,
    pub rows: Option<u64>,
    pub execution_ms: Option<u64>,
}

/// What a [`QueryHook`] callback wants dispatch to do next.
#[derive(Debug, Clone)]
pub enum HookOutcome {
    /// Proceed normally.
    Continue,
    /// Stop the query here. Maps to [`QueryFluxError::Denied`].
    Deny { message: String },
}

impl HookOutcome {
    pub fn is_deny(&self) -> bool {
        matches!(self, HookOutcome::Deny { .. })
    }
}

/// Observes or intercepts the query lifecycle. All methods default to no-ops /
/// `Continue`, so an embedder implements only the points it cares about.
///
/// Hooks do **not** pick the cluster group — that stays [`RouterTrait`](crate::state::AppState)'s
/// job via `route_query`; `before_route` / `after_route` can only observe or deny.
#[async_trait]
pub trait QueryHook: Send + Sync {
    async fn before_route(&self, ctx: &mut HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn after_route(&self, ctx: &HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn before_translate(&self, ctx: &mut HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn after_translate(&self, ctx: &HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn before_guard(&self, ctx: &HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn after_guard(&self, ctx: &HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn before_execute(&self, ctx: &HookContext<'_>) -> HookOutcome {
        let _ = ctx;
        HookOutcome::Continue
    }
    async fn after_execute(&self, ctx: &HookContext<'_>) {
        let _ = ctx;
    }
    async fn on_error(&self, ctx: &HookContext<'_>, err: &QueryFluxError) {
        let (_, _) = (ctx, err);
    }
    async fn on_cancel(&self, ctx: &HookContext<'_>) {
        let _ = ctx;
    }
}

/// Ordered list of registered [`QueryHook`]s, fired in registration order. The first
/// `Deny` from a `before_*` hook short-circuits the remaining hooks at that call site.
#[derive(Default)]
pub struct HookBus {
    hooks: Vec<Arc<dyn QueryHook>>,
}

impl HookBus {
    pub fn new(hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self { hooks }
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    /// Maps a `Deny` outcome straight to [`QueryFluxError::Denied`] — the shape every
    /// `before_*` call site wires into its own early-return.
    pub fn deny_err(outcome: &HookOutcome) -> Option<QueryFluxError> {
        match outcome {
            HookOutcome::Continue => None,
            HookOutcome::Deny { message } => Some(QueryFluxError::Denied(message.clone())),
        }
    }

    pub async fn before_route(&self, ctx: &mut HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.before_route(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn after_route(&self, ctx: &HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.after_route(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn before_translate(&self, ctx: &mut HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.before_translate(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn after_translate(&self, ctx: &HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.after_translate(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn before_guard(&self, ctx: &HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.before_guard(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn after_guard(&self, ctx: &HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.after_guard(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn before_execute(&self, ctx: &HookContext<'_>) -> HookOutcome {
        for h in &self.hooks {
            let outcome = h.before_execute(ctx).await;
            if outcome.is_deny() {
                return outcome;
            }
        }
        HookOutcome::Continue
    }

    pub async fn after_execute(&self, ctx: &HookContext<'_>) {
        for h in &self.hooks {
            h.after_execute(ctx).await;
        }
    }

    pub async fn on_error(&self, ctx: &HookContext<'_>, err: &QueryFluxError) {
        for h in &self.hooks {
            h.on_error(ctx, err).await;
        }
    }

    pub async fn on_cancel(&self, ctx: &HookContext<'_>) {
        for h in &self.hooks {
            h.on_cancel(ctx).await;
        }
    }
}
