pub mod abort;
pub mod admin;
pub mod dispatch;
pub mod flight_sql;
pub mod mcp;
pub mod mysql_wire;
pub mod postgres_wire;
pub mod routing_resolve;
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

/// Max accepted inbound frontend message / packet body size (MySQL + Postgres wire).
/// Rejects before allocating so clients cannot force multi‑GiB (Postgres) or
/// multi‑MiB DoS buffers. Kept below MySQL's 24-bit packet max (16 MiB − 1).
pub const MAX_FRONTEND_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Extract the token from an `Authorization: Bearer <token>` header value. Matches the
/// `Bearer` scheme name case-insensitively per RFC 6750 §2.1 (e.g. `bearer <token>` is a
/// valid header), unlike a plain `str::strip_prefix("Bearer ")`.
pub fn strip_bearer_prefix(value: &str) -> Option<&str> {
    let bytes = value.as_bytes();
    if bytes.len() >= 7 && bytes[..7].eq_ignore_ascii_case(b"bearer ") {
        Some(&value[7..])
    } else {
        None
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_bearer_prefix_matches_canonical_case() {
        assert_eq!(strip_bearer_prefix("Bearer abc123"), Some("abc123"));
    }

    #[test]
    fn strip_bearer_prefix_matches_lowercase_scheme() {
        assert_eq!(strip_bearer_prefix("bearer abc123"), Some("abc123"));
    }

    #[test]
    fn strip_bearer_prefix_matches_mixed_case_scheme() {
        assert_eq!(strip_bearer_prefix("BeArEr abc123"), Some("abc123"));
    }

    #[test]
    fn strip_bearer_prefix_rejects_other_schemes() {
        assert_eq!(strip_bearer_prefix("Basic abc123"), None);
    }

    #[test]
    fn strip_bearer_prefix_rejects_short_input() {
        assert_eq!(strip_bearer_prefix("Bear"), None);
        assert_eq!(strip_bearer_prefix(""), None);
    }

    #[test]
    fn strip_bearer_prefix_allows_empty_token() {
        assert_eq!(strip_bearer_prefix("Bearer "), Some(""));
    }
}
