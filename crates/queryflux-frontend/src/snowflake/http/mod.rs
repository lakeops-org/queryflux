//! Snowflake HTTP wire v1 protocol support — session-based `/session/v1/*` and
//! `/queries/v1/*` endpoints.
//!
//! `SnowflakeWireState` bundles the shared `AppState` with the per-frontend
//! `SnowflakeSessionStore`. Axum's `FromRef` lets SQL API v2 handlers (which only
//! need `Arc<AppState>`) continue to work transparently when the combined state is used.

use std::sync::Arc;

use axum::extract::FromRef;
use axum::routing::{delete, get, post};
use axum::Router;

use crate::snowflake::http::session_store::SnowflakeSessionStore;
use crate::snowflake::in_flight::SnowflakeInFlightRegistry;
use crate::state::AppState;

pub mod format;
pub mod handlers;
pub mod session_store;

// ---------------------------------------------------------------------------
// Combined frontend state
// ---------------------------------------------------------------------------

/// State type used by the Snowflake HTTP wire v1 routes.
///
/// Bundles the shared `AppState` (used by routing, dispatch, auth) with the
/// process-local session store (owned exclusively by this frontend). Nothing
/// outside the `snowflake` module ever references `SnowflakeSessionStore`.
#[derive(Clone)]
pub struct SnowflakeWireState {
    pub app: Arc<AppState>,
    pub sessions: Arc<SnowflakeSessionStore>,
    pub in_flight: Arc<SnowflakeInFlightRegistry>,
}

/// Allow Axum handlers that only need `Arc<AppState>` to extract it from
/// `SnowflakeWireState` via `State<Arc<AppState>>`. Used by the SQL API v2
/// handlers which are merged onto the same port.
impl FromRef<SnowflakeWireState> for Arc<AppState> {
    fn from_ref(s: &SnowflakeWireState) -> Self {
        s.app.clone()
    }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// HTTP wire v1 route table. All handlers extract `State<SnowflakeWireState>`.
pub fn routes() -> Router<SnowflakeWireState> {
    Router::new()
        .route(
            "/session/v1/login-request",
            post(handlers::session::login_request),
        )
        .route("/session", delete(handlers::session::logout))
        .route("/session/heartbeat", get(handlers::session::heartbeat))
        .route(
            "/session/token-request",
            post(handlers::token::token_request),
        )
        .route(
            "/queries/v1/query-request",
            post(handlers::query::query_request),
        )
        .route(
            "/queries/v1/query-monitoring-request",
            get(handlers::query::query_monitoring_request),
        )
        .route(
            "/queries/v1/{query_id}",
            delete(handlers::query::cancel_query),
        )
}
