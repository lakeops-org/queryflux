pub mod http;
pub mod sql_api;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use queryflux_core::{
    config::SnowflakeHttpFrontendConfig,
    error::{QueryFluxError, Result},
};
use tracing::{info, warn};

use crate::state::AppState;
use crate::{FrontendListenerTrait, ShutdownRx};
use http::{
    session_store::{SnowflakeHttpSessionPolicy, SnowflakeSessionStore},
    SnowflakeWireState,
};

/// Snowflake frontend — serves both the HTTP wire protocol v1
/// (`/session/v1/*`, `/queries/v1/*`) and the SQL API v2 (`/api/v2/statements`)
/// on the same port.
///
/// The session store is process-local (`DashMap`). When running multiple replicas,
/// the load balancer must pin each client to the same instance (sticky affinity).
pub struct SnowflakeFrontend {
    state: Arc<AppState>,
    cfg: SnowflakeHttpFrontendConfig,
    sessions: Arc<SnowflakeSessionStore>,
}

impl SnowflakeFrontend {
    pub fn new(state: Arc<AppState>, cfg: SnowflakeHttpFrontendConfig) -> Self {
        if !cfg.session_affinity_acknowledged {
            warn!(
                "Snowflake HTTP wire frontend is enabled but `sessionAffinityAcknowledged` is \
                 false. If you run multiple QueryFlux replicas, configure your load balancer for \
                 sticky session affinity — clients will break if requests are routed to a \
                 different replica mid-session."
            );
        }
        let policy = SnowflakeHttpSessionPolicy {
            max_session_age: if cfg.session_max_age_secs == 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(cfg.session_max_age_secs)
            },
            idle_timeout: if cfg.session_idle_timeout_secs == 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(cfg.session_idle_timeout_secs)
            },
        };
        let sessions = Arc::new(SnowflakeSessionStore::new(policy));
        Self {
            state,
            cfg,
            sessions,
        }
    }

    pub fn router(&self) -> Router {
        let wire_state = SnowflakeWireState {
            app: self.state.clone(),
            sessions: self.sessions.clone(),
        };
        // Both sub-routers are resolved to Router<()> via .with_state() before merging.
        // SQL API v2 handlers extract State<Arc<AppState>>; wire v1 handlers extract
        // State<SnowflakeWireState>. FromRef<SnowflakeWireState> for Arc<AppState> lets
        // the SQL API handlers work with the SnowflakeWireState directly.
        let sql_api = sql_api::routes().with_state(wire_state.app.clone());
        let wire = http::routes().with_state(wire_state);
        sql_api.merge(wire)
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for SnowflakeFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{}", self.cfg.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| QueryFluxError::Other(e.into()))?;

        info!("Snowflake frontend (wire v1 + SQL API v2) listening on {addr}");
        if let Some(limit) = self.cfg.max_connections.filter(|&l| l > 0) {
            info!(
                max_connections = limit,
                "Concurrent request limit enabled (idle keep-alive clients do not count)"
            );
        }

        // Start background GC for expired sessions.
        self.sessions.spawn_gc();

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;
        let router = if let Some(limit) = self.cfg.max_connections.filter(|&l| l > 0) {
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
            .map_err(|e| QueryFluxError::Other(e.into()))
    }
}
