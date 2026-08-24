//! MCP (Model Context Protocol) frontend — streamable HTTP tool calls for AI agents.
//!
//! Exposes `execute_query`, `list_schemas`, `describe_table`, `explain_query`,
//! `get_query_status`, and `cancel_query` as MCP tools. Every tool authenticates via
//! `Authorization: Bearer <token>` (same `AuthProvider` as every other HTTP frontend)
//! and dispatches through the standard `execute_to_sink` pipeline, so routing,
//! translation, guardrails, and query-history persistence are identical to every other
//! frontend — nothing MCP-specific is layered on top.

pub mod sink;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use queryflux_core::error::{QueryFluxError, Result};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, tower::StreamableHttpService, StreamableHttpServerConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::state::AppState;
use crate::{FrontendListenerTrait, ShutdownRx};
use tools::QueryFluxMcpServer;

pub struct McpFrontend {
    state: Arc<AppState>,
    port: u16,
    max_connections: Option<usize>,
}

impl McpFrontend {
    pub fn new(state: Arc<AppState>, port: u16, max_connections: Option<usize>) -> Self {
        Self {
            state,
            port,
            max_connections,
        }
    }
}

#[async_trait]
impl FrontendListenerTrait for McpFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| QueryFluxError::Other(e.into()))?;

        let ct = CancellationToken::new();
        let config = StreamableHttpServerConfig::default().with_cancellation_token(ct.clone());

        let server_state = self.state.clone();
        let service: StreamableHttpService<QueryFluxMcpServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(QueryFluxMcpServer::new(server_state.clone())),
                Arc::new(LocalSessionManager::default()),
                config,
            );

        let mut router = Router::new().nest_service("/mcp", service);
        if let Some(limit) = self.max_connections.filter(|&l| l > 0) {
            router = router.layer(tower::limit::ConcurrencyLimitLayer::new(limit));
        }

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;
        info!("MCP frontend listening on {addr}");

        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
                ct.cancel();
            })
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))
    }
}
