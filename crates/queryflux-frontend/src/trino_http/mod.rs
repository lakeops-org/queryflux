pub mod handlers;
pub mod result_sink;
pub mod state;
pub mod trino_dispatch;

use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};
use tokio::net::TcpListener;
use tracing::info;

use crate::FrontendListenerTrait;
use handlers::*;
use queryflux_core::error::Result;
use state::AppState;

pub struct TrinoHttpFrontend {
    pub state: Arc<AppState>,
    pub port: u16,
}

impl TrinoHttpFrontend {
    pub fn new(state: Arc<AppState>, port: u16) -> Self {
        Self { state, port }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/statement", post(post_statement))
            .route(
                "/v1/statement/qf/queued/{id}/{seq}",
                get(get_queued_statement),
            )
            // Catch all Trino statement poll URLs: /v1/statement/queued/{id}/...
            // and /v1/statement/executing/{id}/... — both use the same handler.
            // axum gives /v1/statement/qf/... (static "qf") higher priority than this wildcard.
            .route("/v1/statement/{*trino_path}", get(get_executing_statement))
            .route(
                "/v1/statement/{*trino_path}",
                delete(delete_executing_statement),
            )
            .with_state(self.state.clone())
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for TrinoHttpFrontend {
    async fn listen(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("Trino HTTP frontend listening on {addr}");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Other(e.into()))?;
        axum::serve(listener, self.router())
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Other(e.into()))?;
        Ok(())
    }
}
