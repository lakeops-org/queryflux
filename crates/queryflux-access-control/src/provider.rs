use async_trait::async_trait;
use queryflux_core::access_model::{AccessDecision, AccessRequest};

/// A provider-level failure — the policy engine could not be reached or its response could
/// not be understood. A well-formed *deny* is `Ok(AccessDecision)`, never an `Err`.
/// [`crate::AccessController`] decides fail-open vs fail-closed from this.
#[derive(Debug)]
pub enum PolicyError {
    Timeout,
    Transport(String),
    /// Non-2xx status, plus the response body (best-effort, truncated) — a policy engine's
    /// error body is often the only clue for what was actually wrong with the request (e.g.
    /// a field-level validation error), so it must not be discarded.
    Status(u16, String),
    Body(String),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::Timeout => write!(f, "policy provider timed out"),
            PolicyError::Transport(e) => write!(f, "policy provider transport error: {e}"),
            PolicyError::Status(s, body) if body.is_empty() => {
                write!(f, "policy provider returned HTTP {s}")
            }
            PolicyError::Status(s, body) => write!(f, "policy provider returned HTTP {s}: {body}"),
            PolicyError::Body(e) => write!(f, "policy provider response unparseable: {e}"),
        }
    }
}

impl std::error::Error for PolicyError {}

/// A pluggable policy decision engine (OPA today; Cerbos/Cedar/etc. later).
#[async_trait]
pub trait PolicyDecisionProvider: Send + Sync {
    /// Map `req` to the provider's wire format, call it, and map the response back.
    /// `Err` only for provider-level failure (see [`PolicyError`]).
    async fn evaluate(&self, req: &AccessRequest) -> Result<AccessDecision, PolicyError>;

    fn name(&self) -> &'static str;
}
