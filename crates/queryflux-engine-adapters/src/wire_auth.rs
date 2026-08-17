//! Shared derivation of [`StoredWireAuth`] from resolved [`QueryCredentials`].
//!
//! One function, reused everywhere a `QueryCredentials` needs to become the concrete
//! wire-level value an adapter actually sends (and, for async adapters, persists so
//! poll/cancel can re-apply the same credential). Kept out of any single adapter so the
//! precedence rules stay identical across call sites instead of drifting.

use queryflux_auth::QueryCredentials;
use queryflux_core::query::StoredWireAuth;
use queryflux_core::session::SessionContext;

/// Resolve the concrete wire auth for a request, given the credential mode selected by
/// `BackendIdentityResolver` and the session's forwarded headers.
///
/// `cluster_sets_http_authorization` should be true when the cluster's own `Type 1` auth
/// (`ClusterAuth::Basic`/`Bearer`) already sets the HTTP `Authorization` header — see
/// [`queryflux_core::config::ClusterAuth::sets_http_authorization`].
///
/// Returns `None` when the adapter should fall back to applying cluster auth alone
/// (`ServiceAccount` with no forwardable client header, or a cluster that sets its own
/// HTTP auth). Returns `Some` for every mode that needs something applied on top of —
/// or instead of — cluster auth.
///
/// Note: `Passthrough` returning `None` is a real, distinct outcome from `ServiceAccount`
/// returning `None` — the caller (adapter) is responsible for treating a `None` result
/// under `QueryCredentials::Passthrough` as a fail-closed error, not as "use cluster auth."
/// This function only derives the value; it does not decide what an absent value means.
pub fn resolve_stored_wire_auth(
    credentials: &QueryCredentials,
    session: &SessionContext,
    cluster_sets_http_authorization: bool,
) -> Option<StoredWireAuth> {
    match credentials {
        QueryCredentials::ServiceAccount => {
            if cluster_sets_http_authorization {
                None
            } else {
                // Deprecated implicit passthrough, kept for backward compat: a cluster with
                // no HTTP auth of its own still forwards whatever Authorization the client sent.
                session
                    .extra
                    .get("authorization")
                    .cloned()
                    .map(StoredWireAuth::Authorization)
            }
        }
        QueryCredentials::Passthrough => session
            .extra
            .get("authorization")
            .cloned()
            .map(StoredWireAuth::Authorization),
        QueryCredentials::Impersonate { user } => {
            Some(StoredWireAuth::ImpersonateUser(user.clone()))
        }
        QueryCredentials::Bearer { token } => {
            Some(StoredWireAuth::Authorization(format!("Bearer {token}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn session_with_auth(value: Option<&str>) -> SessionContext {
        let mut extra = HashMap::new();
        if let Some(v) = value {
            extra.insert("authorization".to_string(), v.to_string());
        }
        SessionContext {
            extra,
            ..Default::default()
        }
    }

    #[test]
    fn service_account_with_cluster_http_auth_ignores_client_header() {
        let session = session_with_auth(Some("Bearer client-token"));
        let resolved = resolve_stored_wire_auth(&QueryCredentials::ServiceAccount, &session, true);
        assert!(resolved.is_none());
    }

    #[test]
    fn service_account_without_cluster_http_auth_forwards_client_header() {
        let session = session_with_auth(Some("Bearer client-token"));
        let resolved = resolve_stored_wire_auth(&QueryCredentials::ServiceAccount, &session, false);
        assert!(matches!(
            resolved,
            Some(StoredWireAuth::Authorization(v)) if v == "Bearer client-token"
        ));
    }

    #[test]
    fn passthrough_forwards_client_header_regardless_of_cluster_auth() {
        let session = session_with_auth(Some("Basic dXNlcjpwYXNz"));
        let resolved = resolve_stored_wire_auth(&QueryCredentials::Passthrough, &session, true);
        assert!(matches!(
            resolved,
            Some(StoredWireAuth::Authorization(v)) if v == "Basic dXNlcjpwYXNz"
        ));
    }

    #[test]
    fn passthrough_with_no_header_resolves_to_none() {
        let session = session_with_auth(None);
        let resolved = resolve_stored_wire_auth(&QueryCredentials::Passthrough, &session, false);
        assert!(resolved.is_none());
    }

    #[test]
    fn impersonate_carries_user_regardless_of_session() {
        let session = session_with_auth(None);
        let resolved = resolve_stored_wire_auth(
            &QueryCredentials::Impersonate {
                user: "alice".to_string(),
            },
            &session,
            true,
        );
        assert!(matches!(
            resolved,
            Some(StoredWireAuth::ImpersonateUser(u)) if u == "alice"
        ));
    }

    #[test]
    fn bearer_formats_authorization_header() {
        let session = session_with_auth(None);
        let resolved = resolve_stored_wire_auth(
            &QueryCredentials::Bearer {
                token: "exchanged".to_string(),
            },
            &session,
            false,
        );
        assert!(matches!(
            resolved,
            Some(StoredWireAuth::Authorization(v)) if v == "Bearer exchanged"
        ));
    }
}
