use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryFluxError {
    #[error("Engine error: {0}")]
    Engine(String),

    /// Backend availability failure, distinguished from a server-reported SQL error.
    #[error("Engine error: {0}")]
    BackendFailure(String),

    /// Backend request or execution deadline exceeded.
    #[error("Engine error: {0}")]
    BackendTimeout(String),

    #[error("Translation error: {0}")]
    Translation(String),

    #[error("Routing error: {0}")]
    Routing(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Persistence error: {0}")]
    Persistence(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Rejected by a routing deny rule before dispatch. Message is safe to show to clients.
    #[error("{0}")]
    Denied(String),

    #[error("Query not found: {0}")]
    QueryNotFound(String),

    #[error("Cluster not found: {0}")]
    ClusterNotFound(String),

    #[error("No cluster group available: {0}")]
    NoClusterGroupAvailable(String),

    /// Returned by `dispatch_query` when the acquired cluster only supports Arrow (sync)
    /// execution. The caller should retry via `execute_to_sink` instead.
    #[error("Cluster {0} requires Arrow execution path")]
    SyncEngineRequired(String),

    /// The cluster group's `maxQueuedQueries` limit has been reached.
    #[error("Queue full for group '{group}': {count}/{limit} queued queries")]
    QueueFull {
        group: String,
        count: u64,
        limit: u64,
    },

    /// No cluster capacity within `capacityWaitTimeoutSecs`.
    #[error(
        "Capacity wait timed out for group '{group}' after {timeout_secs}s — no slot available"
    )]
    CapacityWaitTimeout { group: String, timeout_secs: u64 },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, QueryFluxError>;

impl QueryFluxError {
    pub fn backend_failure(message: impl Into<String>) -> Self {
        Self::BackendFailure(message.into())
    }

    pub fn backend_timeout(message: impl Into<String>) -> Self {
        Self::BackendTimeout(message.into())
    }

    pub fn backend_transport(message: String, timed_out: bool) -> Self {
        if timed_out {
            Self::BackendTimeout(message)
        } else {
            Self::BackendFailure(message)
        }
    }

    /// Classify an adapter HTTP response by its status code, never by the
    /// response body (which may contain a user-controlled SQL error).
    pub fn backend_http_status(status: u16, message: String) -> Self {
        match status {
            408 | 504 => Self::BackendTimeout(message),
            429 | 502 | 503 => Self::BackendFailure(message),
            _ => Self::Engine(message),
        }
    }

    /// Returns `true` if the error is likely transient and the operation may
    /// succeed on retry (e.g. connection refused, pool timeout, serialization
    /// conflict). Returns `false` for permanent failures like constraint
    /// violations, auth errors, or bad input.
    pub fn is_transient(&self) -> bool {
        match self {
            QueryFluxError::BackendFailure(_) | QueryFluxError::BackendTimeout(_) => true,
            // Persistence errors: inspect the message for sqlx error kinds.
            // Connection-level and pool errors are transient; constraint
            // violations and type errors are permanent.
            QueryFluxError::Persistence(msg) => {
                let m = msg.to_lowercase();
                m.contains("connection refused")
                    || m.contains("connection reset")
                    || m.contains("broken pipe")
                    || m.contains("pool timed out")
                    || m.contains("timed out")
                    || m.contains("could not connect")
                    // Postgres serialization failure (40001) — safe to retry
                    || m.contains("40001")
                    || m.contains("serialization failure")
                    || m.contains("deadlock detected")
            }
            // Engine errors may be transient (backend temporarily unavailable).
            QueryFluxError::Engine(msg) => {
                let m = msg.to_lowercase();
                m.contains("connection refused")
                    || m.contains("timed out")
                    || m.contains("unavailable")
                    || m.contains("503")
                    || m.contains("429")
            }
            // Everything else is permanent: auth failures, bad input, routing
            // misses, config errors, not-found, etc.
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryFluxError;

    #[test]
    fn queue_full_display_includes_counts() {
        let err = QueryFluxError::QueueFull {
            group: "analytics".into(),
            count: 5,
            limit: 5,
        };
        let msg = err.to_string();
        assert!(msg.contains("analytics"), "{msg}");
        assert!(msg.contains("5/5"), "{msg}");
        assert!(!err.is_transient());
    }

    #[test]
    fn capacity_wait_timeout_display() {
        let err = QueryFluxError::CapacityWaitTimeout {
            group: "analytics".into(),
            timeout_secs: 300,
        };
        let msg = err.to_string();
        assert!(msg.contains("analytics"), "{msg}");
        assert!(msg.contains("300"), "{msg}");
        assert!(!err.is_transient());
    }

    #[test]
    fn backend_http_status_is_based_on_code_not_body() {
        assert!(matches!(
            QueryFluxError::backend_http_status(503, "syntax error".into()),
            QueryFluxError::BackendFailure(_)
        ));
        assert!(matches!(
            QueryFluxError::backend_http_status(504, "unrelated".into()),
            QueryFluxError::BackendTimeout(_)
        ));
        assert!(matches!(
            QueryFluxError::backend_http_status(400, "connection refused".into()),
            QueryFluxError::Engine(_)
        ));
        assert!(matches!(
            QueryFluxError::backend_http_status(500, "query failed".into()),
            QueryFluxError::Engine(_)
        ));
    }
}
