//! The ops-only listener a worker runs instead of the admin API (#255): health,
//! readiness, Prometheus metrics and the worker's config-sync status. No admin routes,
//! no config writes. With peer bootstrap on, it also serves the worker's active config to
//! other workers (`/worker/peer-config`, behind the fleet's shared secret).

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use queryflux_core::error::Result;
use queryflux_metrics::prometheus_store::PrometheusMetrics;
use tokio::net::TcpListener;
use tracing::info;

use crate::{state::LiveConfig, FrontendListenerTrait, ShutdownRx};

/// Returns the worker's config-sync status as JSON for `/worker/status`.
pub type StatusFn = Arc<dyn Fn() -> serde_json::Value + Send + Sync>;

/// Returns the worker's active config for a peer, or `None` while it has none.
pub type PeerConfigFn = Arc<dyn Fn() -> Option<serde_json::Value> + Send + Sync>;

/// `/worker/peer-config`: the secret peers present, and the config to serve.
#[derive(Clone)]
pub struct PeerConfigEndpoint {
    pub shared_secret: String,
    pub config: PeerConfigFn,
}

pub struct OpsFrontend {
    prometheus: Arc<PrometheusMetrics>,
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    port: u16,
    status: StatusFn,
    peer: Option<PeerConfigEndpoint>,
}

#[derive(Clone)]
struct OpsState {
    prometheus: Arc<PrometheusMetrics>,
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    status: StatusFn,
    peer: Option<PeerConfigEndpoint>,
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
            peer: None,
        }
    }

    /// Serve this worker's active config to peers starting without a control plane.
    pub fn with_peer_config(mut self, peer: PeerConfigEndpoint) -> Self {
        self.peer = Some(peer);
        self
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
            .route("/worker/peer-config", get(peer_config))
            .with_state(OpsState {
                prometheus: self.prometheus.clone(),
                live: self.live.clone(),
                status: self.status.clone(),
                peer: self.peer.clone(),
            })
    }
}

async fn peer_config(
    State(s): State<OpsState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let Some(peer) = &s.peer else {
        return (StatusCode::NOT_FOUND, "peer bootstrap is not enabled").into_response();
    };
    if !crate::worker_api::authorized(&headers, &peer.shared_secret) {
        return (
            StatusCode::UNAUTHORIZED,
            "invalid or missing worker credential",
        )
            .into_response();
    }
    match (peer.config)() {
        Some(config) => axum::Json(config).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "this worker has no config yet",
        )
            .into_response(),
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
