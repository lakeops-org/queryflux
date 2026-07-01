pub mod admin;
pub mod dispatch;
pub mod flight_sql;
pub mod mysql_wire;
pub mod postgres_wire;
pub mod snowflake;
pub mod state;
pub mod tee_sink;
pub mod trino_http;

use async_trait::async_trait;
use queryflux_core::error::Result;

/// Receiver half of the graceful-shutdown broadcast channel.
/// When the sender writes `true`, frontends should stop accepting new connections
/// and drain in-flight work.
pub type ShutdownRx = tokio::sync::watch::Receiver<bool>;

/// Implemented by each frontend protocol server (Trino HTTP, PG wire, MySQL wire, etc.).
///
/// Each listener binds to a port, accepts connections in its native protocol,
/// translates requests into `IncomingQuery`, submits them to the `QueryDispatcher`,
/// and encodes results back into its native wire format.
#[async_trait]
pub trait FrontendListenerTrait: Send + Sync {
    /// Start the listener. Returns when the shutdown signal fires and in-flight
    /// work has drained (or the server framework finishes its own drain).
    async fn listen(&self, shutdown: ShutdownRx) -> Result<()>;
}
