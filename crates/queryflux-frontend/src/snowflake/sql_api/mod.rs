//! Snowflake SQL REST API v2 frontend (Form 2) — Design B: protocol bridge.
//!
//! Exposes `routes()` — a stateless `Router<SnowflakeWireState>`.

use axum::{
    routing::{get, post},
    Router,
};

use crate::snowflake::http::SnowflakeWireState;

pub mod handlers;

pub fn routes() -> Router<SnowflakeWireState> {
    Router::new()
        .route("/api/v2/statements", post(handlers::submit_statement))
        .route(
            "/api/v2/statements/{handle}",
            get(handlers::get_statement).delete(handlers::cancel_statement),
        )
}
