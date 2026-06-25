pub mod handlers;
pub mod result_sink;
pub mod state;

use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};
use tokio::net::TcpListener;
use tracing::info;

use crate::{FrontendListenerTrait, ShutdownRx};
use handlers::*;
use queryflux_core::error::Result;
use state::AppState;

pub struct TrinoHttpFrontend {
    pub state: Arc<AppState>,
    pub port: u16,
    pub max_connections: Option<usize>,
}

impl TrinoHttpFrontend {
    pub fn new(state: Arc<AppState>, port: u16, max_connections: Option<usize>) -> Self {
        Self {
            state,
            port,
            max_connections,
        }
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
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("Trino HTTP frontend listening on {addr}");
        if let Some(limit) = self.max_connections.filter(|&l| l > 0) {
            info!(
                max_connections = limit,
                "Concurrent request limit enabled (idle keep-alive clients do not count)"
            );
        }
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Other(e.into()))?;
        let router = if let Some(limit) = self.max_connections.filter(|&l| l > 0) {
            self.router()
                .layer(tower::limit::ConcurrencyLimitLayer::new(limit))
        } else {
            self.router()
        };
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Other(e.into()))?;
        Ok(())
    }
}
