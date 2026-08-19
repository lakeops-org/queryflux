//! Shared derivation of [`StoredWireAuth`] from resolved [`QueryCredentials`].
//!
//! One function, reused everywhere a `QueryCredentials` needs to become the concrete
//! wire-level value an adapter actually sends (and, for async adapters, persists so
//! poll/cancel can re-apply the same credential). Kept out of any single adapter so the
//! precedence rules stay identical across call sites instead of drifting.

use queryflux_auth::{AuthContext, QueryCredentials};
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

/// Best-effort passthrough enrichment: if `credentials` is `Passthrough` and the session
/// doesn't already carry a forwardable `authorization` value (the frontend's own header,
/// or one restored from a persisted queued session), inject `Bearer {raw_token}` when the
/// caller authenticated via OIDC. No-op for every other credential mode, and a no-op when
/// no `raw_token` is available — the adapter is responsible for failing closed on that.
///
/// Must run after `BackendIdentityResolver::resolve` (needs `credentials`) and before the
/// adapter call (`session` must still be mutable at the call site). Shared by every
/// dispatch path — the async Trino-HTTP path and the sync-bridge path used by Postgres/
/// MySQL/Flight-wire frontends — so passthrough works the same way regardless of which
/// frontend the query arrived on, as long as that frontend populates `raw_token`.
pub fn enrich_session_for_passthrough(
    session: &mut SessionContext,
    credentials: &QueryCredentials,
    auth_ctx: &AuthContext,
) {
    if !matches!(credentials, QueryCredentials::Passthrough) {
        return;
    }
    // HTTP-shaped (Trino): forward the client's own Authorization, or synthesize one from
    // an OIDC raw_token if the frontend didn't already capture a header.
    if !session.extra.contains_key("authorization") {
        if let Some(token) = &auth_ctx.raw_token {
            session
                .extra
                .insert("authorization".to_string(), format!("Bearer {token}"));
        }
    }
    // MySQL-wire-shaped (StarRocks and any future mysql_native consumer): username +
    // password for COM_CHANGE_USER. Only ever populated when `raw_password` came from a
    // provider that verified it against the same identity backend the target cluster
    // authenticates against (LdapAuthProvider) — see `AuthContext::raw_password`.
    if let Some(password) = &auth_ctx.raw_password {
        session
            .extra
            .entry("passthrough_username".to_string())
            .or_insert_with(|| auth_ctx.user.clone());
        session
            .extra
            .entry("passthrough_password".to_string())
            .or_insert_with(|| password.clone());
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

    fn auth_ctx(raw_token: Option<&str>) -> AuthContext {
        AuthContext {
            user: "alice".to_string(),
            groups: vec![],
            roles: vec![],
            raw_token: raw_token.map(str::to_string),
            ..Default::default()
        }
    }

    fn auth_ctx_with_password(raw_password: Option<&str>) -> AuthContext {
        AuthContext {
            user: "alice".to_string(),
            groups: vec![],
            roles: vec![],
            raw_password: raw_password.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn enrich_injects_bearer_when_passthrough_and_no_header_and_raw_token_present() {
        let mut session = session_with_auth(None);
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx(Some("oidc-jwt")),
        );
        assert_eq!(
            session.extra.get("authorization").map(String::as_str),
            Some("Bearer oidc-jwt")
        );
    }

    #[test]
    fn enrich_does_not_overwrite_an_existing_authorization_header() {
        let mut session = session_with_auth(Some("Bearer client-sent-token"));
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx(Some("oidc-jwt")),
        );
        assert_eq!(
            session.extra.get("authorization").map(String::as_str),
            Some("Bearer client-sent-token")
        );
    }

    #[test]
    fn enrich_is_a_noop_without_a_raw_token() {
        let mut session = session_with_auth(None);
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx(None),
        );
        assert!(!session.extra.contains_key("authorization"));
    }

    #[test]
    fn enrich_is_a_noop_for_non_passthrough_modes() {
        let mut session = session_with_auth(None);
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::ServiceAccount,
            &auth_ctx(Some("oidc-jwt")),
        );
        assert!(!session.extra.contains_key("authorization"));
    }

    #[test]
    fn enrich_injects_mysql_username_and_password_when_raw_password_present() {
        let mut session = session_with_auth(None);
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx_with_password(Some("ldap-verified-pw")),
        );
        assert_eq!(
            session
                .extra
                .get("passthrough_username")
                .map(String::as_str),
            Some("alice")
        );
        assert_eq!(
            session
                .extra
                .get("passthrough_password")
                .map(String::as_str),
            Some("ldap-verified-pw")
        );
    }

    #[test]
    fn enrich_does_not_inject_mysql_credentials_without_raw_password() {
        let mut session = session_with_auth(None);
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx_with_password(None),
        );
        assert!(!session.extra.contains_key("passthrough_username"));
        assert!(!session.extra.contains_key("passthrough_password"));
    }

    #[test]
    fn enrich_does_not_overwrite_existing_mysql_passthrough_credentials() {
        let mut session = session_with_auth(None);
        session.extra.insert(
            "passthrough_username".to_string(),
            "already-set".to_string(),
        );
        session.extra.insert(
            "passthrough_password".to_string(),
            "already-set-pw".to_string(),
        );
        enrich_session_for_passthrough(
            &mut session,
            &QueryCredentials::Passthrough,
            &auth_ctx_with_password(Some("new-pw")),
        );
        assert_eq!(
            session
                .extra
                .get("passthrough_username")
                .map(String::as_str),
            Some("already-set")
        );
        assert_eq!(
            session
                .extra
                .get("passthrough_password")
                .map(String::as_str),
            Some("already-set-pw")
        );
    }
}
