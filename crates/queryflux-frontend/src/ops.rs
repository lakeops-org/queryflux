//! The ops-only listener a worker runs instead of the admin API (#255): health,
//! readiness, Prometheus metrics and the worker's config-sync status. No admin routes,
//! no config writes.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use queryflux_core::error::Result;
use queryflux_metrics::prometheus_store::PrometheusMetrics;
use tokio::net::TcpListener;
use tracing::info;

use crate::{state::LiveConfig, FrontendListenerTrait, ShutdownRx};

/// Returns the worker's config-sync status as JSON for `/worker/status`.
pub type StatusFn = Arc<dyn Fn() -> serde_json::Value + Send + Sync>;

pub struct OpsFrontend {
    prometheus: Arc<PrometheusMetrics>,
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    port: u16,
    status: StatusFn,
}

#[derive(Clone)]
struct OpsState {
    prometheus: Arc<PrometheusMetrics>,
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    status: StatusFn,
}

impl OpsFrontend {
    pub fn new(
        prometheus: Arc<PrometheusMetrics>,
        live: Arc<tokio::sync::RwLock<LiveConfig>>,
        port: u16,
        status: StatusFn,
    ) -> Self {
        Self {
            prometheus,
            live,
            port,
            status,
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(|| async { (StatusCode::OK, "ok") }))
            .route(
                "/readyz",
                get(|State(s): State<OpsState>| async move { crate::admin::readiness(&s.live).await }),
            )
            .route(
                "/metrics",
                get(|State(s): State<OpsState>| async move {
                    (
                        StatusCode::OK,
                        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                        s.prometheus.gather_text(),
                    )
                }),
            )
            .route(
                "/worker/status",
                get(|State(s): State<OpsState>| async move { axum::Json((s.status)()).into_response() }),
            )
            .with_state(OpsState {
                prometheus: self.prometheus.clone(),
                live: self.live.clone(),
                status: self.status.clone(),
            })
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for OpsFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!("Ops listener on {addr} — /health /readyz /metrics /worker/status");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Engine(e.to_string()))?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Engine(e.to_string()))?;
        Ok(())
    }
}
