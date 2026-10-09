use queryflux_core::error::{QueryFluxError, Result};
use serde::{Deserialize, Serialize};

use crate::authorization::is_query_owner;

/// Raw credential material extracted from the frontend protocol before any verification.
///
/// Populated by the frontend handler from protocol-specific sources:
/// - `TrinoHttp`:     `Authorization` header → Basic → `username`/`password`, Bearer → `bearer_token`
/// - `PostgresWire`:  startup message `user` field → `username`
/// - `MySqlWire`:     handshake `user` field → `username`
/// - `FlightSQL`:     gRPC metadata `Authorization` Bearer → `bearer_token`
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<String>,
    /// Raw JWT or opaque token from `Authorization: Bearer <token>`.
    /// Preserved as `raw_token` in `AuthContext` for `tokenExchange` backend mode.
    pub bearer_token: Option<String>,
}

/// Verified identity produced by `AuthProvider::authenticate`.
///
/// This is the canonical subject for all downstream decisions:
/// routing (identity-aware routers), authorization (OpenFGA / allow-lists),
/// audit logs, and backend credential resolution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthContext {
    /// Canonical username. Never empty — `NoneAuthProvider` falls back to `"anonymous"`.
    pub user: String,
    /// Group memberships extracted from the IdP token or LDAP DN.
    #[serde(default)]
    pub groups: Vec<String>,
    /// Roles extracted from the IdP token (e.g. `realm_access.roles` in Keycloak).
    #[serde(default)]
    pub roles: Vec<String>,
    /// The original JWT, kept for `tokenExchange` backend mode.
    /// `None` when using `NoneAuthProvider` or `StaticAuthProvider`.
    pub raw_token: Option<String>,
    /// The verified plaintext password, kept only for `passthrough` on MySQL-wire backends
    /// (StarRocks LDAP `COM_CHANGE_USER` — see `mysql_native::apply_passthrough_identity`).
    ///
    /// **Only ever set by a provider that just verified this password against the same
    /// identity backend a `passthrough` cluster would also authenticate against** —
    /// currently `LdapAuthProvider` only. `StaticAuthProvider` deliberately leaves this
    /// `None` even though it also sees a password: its password map is QueryFlux's own
    /// local config, not necessarily valid against any backend, so treating it as
    /// forwardable would be a real vulnerability, not just a missed optimization.
    #[serde(default, skip_serializing)]
    pub raw_password: Option<String>,
    /// Verified ABAC attributes for data-level policy (e.g. `department`, `region`,
    /// `data_classification`). Populated **only** from a verified credential — for OIDC,
    /// from the JWT claim paths in `auth.oidc.attributeClaims`. Never from a backend
    /// connection credential or client-declared session parameters. Empty for
    /// `None`/`Static`/`Ldap` providers unless a future provider fills it.
    #[serde(default)]
    pub attributes: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Reject poll/cancel/dequeue when the caller is not the query owner.
///
/// - Empty `submitted_by` (legacy in-flight rows) is allowed so a rolling
///   deploy does not brick queries persisted before this field existed.
/// - Two anonymous identities cannot be distinguished and are allowed
///   (network-trust / `auth.provider: none` with no username).
/// - Otherwise the authenticated user must equal `submitted_by`.
pub fn require_query_owner(auth: &AuthContext, submitted_by: &str) -> Result<()> {
    if is_query_owner(auth, submitted_by) {
        Ok(())
    } else {
        Err(QueryFluxError::Unauthorized(
            "query belongs to a different user".to_string(),
        ))
    }
}

/// Resolved wire credentials for a specific backend query execution.
///
/// Produced by `BackendIdentityResolver` from `(AuthContext, queryAuth config)`.
/// Passed alongside `SessionContext` to adapter methods so adapters know how to
/// authenticate the outgoing request to the backend engine.
#[derive(Debug, Clone)]
pub enum QueryCredentials {
    /// Use the cluster's own service account (Type 1 credentials from `ClusterConfig.auth`).
    ///
    /// The adapter applies `cluster.auth` directly.
    /// For the Trino adapter, `SessionContext::TrinoHttp` headers (including the client's
    /// `Authorization`) are still forwarded unchanged (implicit Trino HTTP client-header passthrough)
    /// when the cluster does not itself set HTTP auth — a deprecated-but-supported carryover
    /// from before `passthrough` existed as an explicit mode.
    ServiceAccount,

    /// Forward the client's own credential to the backend unchanged.
    ///
    /// Carries no payload: the actual value lives in `SessionContext.extra["authorization"]`
    /// (either the client's original header, or a `Bearer {raw_token}` injected by dispatch
    /// when the header is missing but an OIDC `raw_token` is available). Keeping the token
    /// out of this enum means it never has to round-trip through `AuthContext` at the
    /// adapter boundary — see Phase 0 critical bug #3.
    ///
    /// Adapters fail closed (return an auth error) if no forwardable credential is found —
    /// this never silently degrades to `ServiceAccount`.
    Passthrough,

    /// Service account authenticates to the backend; user identity injected via engine header.
    ///
    /// Trino adapter behavior:
    ///   1. Remove client's `Authorization` header from the outgoing request
    ///   2. Apply `cluster.auth` (Type 1) as backend authentication
    ///   3. Set `X-Trino-User: {user}` header
    ///
    /// Only valid for Trino. Startup validation rejects `impersonate` for other engines.
    Impersonate { user: String },

    /// Use a pre-resolved Bearer token (e.g. from OAuth token exchange).
    ///
    /// The adapter sets `Authorization: Bearer <token>` on the outgoing request.
    /// Used by `tokenExchange` mode.
    Bearer { token: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(user: &str) -> AuthContext {
        AuthContext {
            user: user.to_string(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        }
    }

    #[test]
    fn owner_matches() {
        assert!(require_query_owner(&ctx("alice"), "alice").is_ok());
    }

    #[test]
    fn different_user_denied() {
        let err = require_query_owner(&ctx("bob"), "alice").unwrap_err();
        assert!(matches!(
            err,
            queryflux_core::error::QueryFluxError::Unauthorized(_)
        ));
    }

    #[test]
    fn anonymous_cannot_access_named_owner() {
        assert!(require_query_owner(&ctx("anonymous"), "alice").is_err());
    }

    #[test]
    fn named_user_cannot_access_anonymous_query() {
        assert!(require_query_owner(&ctx("alice"), "anonymous").is_err());
    }

    #[test]
    fn both_anonymous_allowed() {
        assert!(require_query_owner(&ctx("anonymous"), "anonymous").is_ok());
    }

    #[test]
    fn legacy_empty_owner_allowed() {
        assert!(require_query_owner(&ctx("alice"), "").is_ok());
        assert!(require_query_owner(&ctx("anonymous"), "").is_ok());
    }
}

impl AuthContext {
    /// Stable identity for gateway-owned resources across refreshed JWTs. Only
    /// call on an AuthContext returned by the configured authentication provider.
    pub fn session_owner(&self) -> String {
        use base64::Engine;
        let claims = self
            .raw_token
            .as_deref()
            .and_then(|t| t.split('.').nth(1))
            .and_then(|p| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .ok()
            })
            .and_then(|p| serde_json::from_slice::<serde_json::Value>(&p).ok());
        let mut binding = serde_json::Map::new();
        binding.insert("user".into(), serde_json::Value::String(self.user.clone()));
        if let Some(claims) = claims {
            for key in [
                "iss",
                "aud",
                "sub",
                "tenant_id",
                "cluster_id",
                "cluster_ref",
                "token_type",
                "actor_id",
                "actor_email",
                "actor",
                "policy_test",
                "context",
                "agent_manifest_id",
                "agent_session_id",
                "capability_attachment_id",
                "agent_id",
                "conversation_id",
            ] {
                binding.insert(key.into(), claims[key].clone());
            }
        }
        serde_json::Value::Object(binding).to_string()
    }
}

#[cfg(test)]
mod session_owner_tests {
    use super::*;
    use base64::Engine;
    fn context(cluster: &str, role: &str, nonce: u64) -> AuthContext {
        let claims = serde_json::json!({"iss":"issuer", "sub":"same-user", "tenant_id":"tenant", "cluster_id":cluster, "role":role, "jti":nonce});
        AuthContext {
            user: "same-user".into(),
            raw_token: Some(format!(
                "test.{}.test",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
            )),
            ..Default::default()
        }
    }
    #[test]
    fn renewed_roles_keep_owner_but_another_cluster_does_not() {
        assert_eq!(
            context("cluster1", "reader", 1).session_owner(),
            context("cluster1", "writer", 2).session_owner()
        );
        assert_ne!(
            context("cluster1", "reader", 1).session_owner(),
            context("cluster2", "reader", 1).session_owner()
        );
    }
}
