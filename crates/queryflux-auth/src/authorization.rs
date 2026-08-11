use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::config::{ClusterGroupAuthorizationConfig, OpenFgaConfig, OpenFgaCredentials};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::credentials::AuthContext;

/// Action on an existing (or about-to-run) query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryAction {
    /// Read in-flight results (poll). Owner only — this is result data.
    Poll,
    /// Take a queued query and dispatch it. Owner only — otherwise B runs A's SQL.
    Dequeue,
    /// Stop an in-flight or queued query. Owner or operator.
    Cancel,
}

/// Query identity used for action checks.
#[derive(Debug, Clone)]
pub struct QueryAuthz {
    pub submitted_by: String,
    pub group: String,
}

/// IdP roles/groups that may act as operators (cancel any query).
#[derive(Debug, Clone, Default)]
pub struct OperatorPolicy {
    pub roles: Vec<String>,
    pub groups: Vec<String>,
}

impl OperatorPolicy {
    pub fn from_lists(roles: Vec<String>, groups: Vec<String>) -> Self {
        Self { roles, groups }
    }

    pub fn is_operator(&self, auth: &AuthContext) -> bool {
        if self.roles.iter().any(|r| auth.roles.iter().any(|a| a == r)) {
            return true;
        }
        self.groups
            .iter()
            .any(|g| auth.groups.iter().any(|a| a == g))
    }
}

fn is_anonymous(user: &str) -> bool {
    user.is_empty() || user == "anonymous"
}

/// Owner match used by poll/dequeue/cancel.
///
/// Empty `submitted_by` (legacy rows) is allowed. Two anonymous identities
/// cannot be distinguished and are allowed.
pub fn is_query_owner(auth: &AuthContext, submitted_by: &str) -> bool {
    if submitted_by.is_empty() {
        return true;
    }
    if is_anonymous(&auth.user) && is_anonymous(submitted_by) {
        return true;
    }
    auth.user == submitted_by
}

/// Shared rules: poll/dequeue → owner; cancel → owner or operator.
pub fn allow_query_action(
    auth: &AuthContext,
    action: QueryAction,
    query: &QueryAuthz,
    operators: &OperatorPolicy,
) -> bool {
    if is_query_owner(auth, &query.submitted_by) {
        return true;
    }
    matches!(action, QueryAction::Cancel) && operators.is_operator(auth)
}

/// Checks whether an authenticated subject may execute queries against a cluster group,
/// and whether they may poll / dequeue / cancel an existing query.
///
/// Implementations:
/// - `AllowAllAuthorization`      — default; permits run-on-any-group (Phase 1 / no config)
/// - `SimpleAuthorizationPolicy`  — reads `allowGroups`/`allowUsers` from config (Phase 3)
/// - `OpenFgaAuthorizationClient` — Zanzibar-style fine-grained authz (Phase 3)
#[async_trait]
pub trait AuthorizationChecker: Send + Sync {
    /// Returns `true` if `auth_ctx.user` (and/or their groups) may run queries on `group`.
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool;

    /// Returns `true` if `auth_ctx` may perform `action` on `query`.
    ///
    /// Default: owner for all actions; operators may cancel.
    async fn check_query(
        &self,
        auth_ctx: &AuthContext,
        action: QueryAction,
        query: &QueryAuthz,
    ) -> bool;
}

// ---------------------------------------------------------------------------
// AllowAllAuthorization
// ---------------------------------------------------------------------------

/// Permits all run-on-group requests. Used when no `authorization` block is configured.
///
/// Query actions still enforce owner (and optional operators for cancel).
#[derive(Default)]
pub struct AllowAllAuthorization {
    operators: OperatorPolicy,
}

impl AllowAllAuthorization {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_operators(operators: OperatorPolicy) -> Self {
        Self { operators }
    }
}

#[async_trait]
impl AuthorizationChecker for AllowAllAuthorization {
    async fn check(&self, _auth_ctx: &AuthContext, _group: &str) -> bool {
        true
    }

    async fn check_query(
        &self,
        auth_ctx: &AuthContext,
        action: QueryAction,
        query: &QueryAuthz,
    ) -> bool {
        allow_query_action(auth_ctx, action, query, &self.operators)
    }
}

// ---------------------------------------------------------------------------
// SimpleAuthorizationPolicy
// ---------------------------------------------------------------------------

/// Allow-list authorization backed by `allowGroups`/`allowUsers` per cluster group.
///
/// Used when `authorization.provider: none` (no OpenFGA dependency).
///
/// Rules per cluster group:
/// - If both `allowGroups` and `allowUsers` are empty → allow all (open group).
/// - Otherwise: allow if `auth_ctx.user` is in `allowUsers` OR any of `auth_ctx.groups`
///   intersects `allowGroups`.
///
/// Unknown groups (not in the policy map) default to allow-all — this preserves
/// backward compatibility when groups are added to config without an authorization block.
pub struct SimpleAuthorizationPolicy {
    /// cluster_group_name → authorization config
    policies: HashMap<String, ClusterGroupAuthorizationConfig>,
    operators: OperatorPolicy,
}

impl SimpleAuthorizationPolicy {
    pub fn new(policies: HashMap<String, ClusterGroupAuthorizationConfig>) -> Self {
        Self {
            policies,
            operators: OperatorPolicy::default(),
        }
    }

    pub fn with_operators(mut self, operators: OperatorPolicy) -> Self {
        self.operators = operators;
        self
    }
}

#[async_trait]
impl AuthorizationChecker for SimpleAuthorizationPolicy {
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool {
        let Some(policy) = self.policies.get(group) else {
            // Group not in policy map — allow-all (backward compat).
            return true;
        };

        // Both lists empty → open group.
        if policy.allow_groups.is_empty() && policy.allow_users.is_empty() {
            return true;
        }

        // Username match.
        if policy.allow_users.contains(&auth_ctx.user) {
            debug!(user = %auth_ctx.user, group, "SimplePolicy: user allowed by allowUsers");
            return true;
        }

        // Group membership match.
        for g in &auth_ctx.groups {
            if policy.allow_groups.contains(g) {
                debug!(user = %auth_ctx.user, group, matched_group = %g, "SimplePolicy: user allowed by allowGroups");
                return true;
            }
        }

        warn!(user = %auth_ctx.user, group, "SimplePolicy: access denied");
        false
    }

    async fn check_query(
        &self,
        auth_ctx: &AuthContext,
        action: QueryAction,
        query: &QueryAuthz,
    ) -> bool {
        allow_query_action(auth_ctx, action, query, &self.operators)
    }
}

// ---------------------------------------------------------------------------
// OpenFgaAuthorizationClient
// ---------------------------------------------------------------------------

/// OpenFGA Zanzibar-style fine-grained authorization.
///
/// Issues a `/stores/{store_id}/check` request for every `check()` call with:
///   user:    `user:<auth_ctx.user>`
///   relation: `reader`
///   object:  `cluster_group:<group>`
///
/// Credentials:
/// - `api_key`: adds `Authorization: Bearer <key>` header
/// - `client_credentials`: exchanges client_id/secret for an OAuth access token,
///   then uses it as Bearer (token cached until expiry - 30s)
///
/// On any HTTP error or unreachable OpenFGA, **denies** access and logs a warning.
/// Operators should ensure OpenFGA is highly available; a sidecar pattern is recommended.
/// Refresh OAuth token this long before `expires_at` so we do not send requests with a token
/// about to expire.
const OPENFGA_TOKEN_REFRESH_BUFFER: std::time::Duration = std::time::Duration::from_secs(30);

pub struct OpenFgaAuthorizationClient {
    config: OpenFgaConfig,
    http_client: reqwest::Client,
    /// Cached OAuth token for client_credentials flow: (token, expires_at).
    token_cache: tokio::sync::Mutex<Option<(String, std::time::Instant)>>,
    operators: OperatorPolicy,
}

impl OpenFgaAuthorizationClient {
    pub fn new(config: OpenFgaConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("build OpenFGA http client"),
            token_cache: tokio::sync::Mutex::new(None),
            operators: OperatorPolicy::default(),
        }
    }

    pub fn with_operators(mut self, operators: OperatorPolicy) -> Self {
        self.operators = operators;
        self
    }

    async fn bearer_token(&self) -> Option<String> {
        match &self.config.credentials {
            None => None,
            Some(OpenFgaCredentials::ApiKey { api_key }) => Some(api_key.clone()),
            Some(OpenFgaCredentials::ClientCredentials {
                client_id,
                client_secret,
                token_endpoint,
            }) => {
                // Reuse cache only while `now + buffer < expires_at` (future `expires_at` must
                // not use `elapsed()`, which subtracts the wrong way and can panic).
                {
                    let guard = self.token_cache.lock().await;
                    if let Some((token, expires_at)) = guard.as_ref() {
                        let now = std::time::Instant::now();
                        if now + OPENFGA_TOKEN_REFRESH_BUFFER < *expires_at {
                            return Some(token.clone());
                        }
                    }
                }

                // Exchange client credentials for a token.
                let resp = self
                    .http_client
                    .post(token_endpoint)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", client_id),
                        ("client_secret", client_secret),
                    ])
                    .send()
                    .await
                    .ok()?;

                let body: serde_json::Value = resp.json().await.ok()?;
                let token = body.get("access_token")?.as_str()?.to_string();
                let expires_in = body
                    .get("expires_in")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(300);

                let expires_at =
                    std::time::Instant::now() + std::time::Duration::from_secs(expires_in);
                *self.token_cache.lock().await = Some((token.clone(), expires_at));
                Some(token)
            }
        }
    }

    async fn check_relation(&self, auth_ctx: &AuthContext, relation: &str, group: &str) -> bool {
        let url = format!(
            "{}/stores/{}/check",
            self.config.url.trim_end_matches('/'),
            self.config.store_id,
        );

        let body = CheckRequest {
            tuple_key: TupleKey {
                user: format!("user:{}", auth_ctx.user),
                relation: relation.to_string(),
                object: format!("cluster_group:{group}"),
            },
        };

        let mut req = self.http_client.post(&url).json(&body);
        if let Some(token) = self.bearer_token().await {
            req = req.bearer_auth(token);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<CheckResponse>().await {
                Ok(r) => {
                    if !r.allowed {
                        warn!(user = %auth_ctx.user, group, relation, "OpenFGA: access denied");
                    }
                    r.allowed
                }
                Err(e) => {
                    warn!(error = %e, "OpenFGA: failed to parse check response — denying");
                    false
                }
            },
            Ok(resp) => {
                warn!(status = %resp.status(), user = %auth_ctx.user, group, relation, "OpenFGA: check returned error status — denying");
                false
            }
            Err(e) => {
                warn!(error = %e, "OpenFGA: check request failed — denying");
                false
            }
        }
    }
}

/// OpenFGA check request body.
#[derive(Serialize)]
struct CheckRequest {
    tuple_key: TupleKey,
}

#[derive(Serialize)]
struct TupleKey {
    user: String,
    relation: String,
    object: String,
}

/// OpenFGA check response body.
#[derive(Deserialize)]
struct CheckResponse {
    allowed: bool,
}

#[async_trait]
impl AuthorizationChecker for OpenFgaAuthorizationClient {
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool {
        self.check_relation(auth_ctx, "reader", group).await
    }

    async fn check_query(
        &self,
        auth_ctx: &AuthContext,
        action: QueryAction,
        query: &QueryAuthz,
    ) -> bool {
        if allow_query_action(auth_ctx, action, query, &self.operators) {
            return true;
        }
        if matches!(action, QueryAction::Cancel) && !query.group.is_empty() {
            return self
                .check_relation(auth_ctx, "can_cancel", &query.group)
                .await;
        }
        false
    }
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
        }
    }

    fn ctx_ops(user: &str, roles: &[&str], groups: &[&str]) -> AuthContext {
        AuthContext {
            user: user.to_string(),
            groups: groups.iter().map(|s| s.to_string()).collect(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
            raw_token: None,
        }
    }

    fn q(owner: &str) -> QueryAuthz {
        QueryAuthz {
            submitted_by: owner.to_string(),
            group: "g1".to_string(),
        }
    }

    #[tokio::test]
    async fn owner_can_poll_and_cancel() {
        let authz = AllowAllAuthorization::default();
        let alice = ctx("alice");
        assert!(
            authz
                .check_query(&alice, QueryAction::Poll, &q("alice"))
                .await
        );
        assert!(
            authz
                .check_query(&alice, QueryAction::Cancel, &q("alice"))
                .await
        );
        assert!(
            authz
                .check_query(&alice, QueryAction::Dequeue, &q("alice"))
                .await
        );
    }

    #[tokio::test]
    async fn other_user_cannot_poll_or_cancel() {
        let authz = AllowAllAuthorization::default();
        let bob = ctx("bob");
        assert!(
            !authz
                .check_query(&bob, QueryAction::Poll, &q("alice"))
                .await
        );
        assert!(
            !authz
                .check_query(&bob, QueryAction::Dequeue, &q("alice"))
                .await
        );
        assert!(
            !authz
                .check_query(&bob, QueryAction::Cancel, &q("alice"))
                .await
        );
    }

    #[tokio::test]
    async fn operator_role_can_cancel_but_not_poll() {
        let authz = AllowAllAuthorization::with_operators(OperatorPolicy::from_lists(
            vec!["queryflux-operator".into()],
            vec![],
        ));
        let ops = ctx_ops("oncall", &["queryflux-operator"], &[]);
        assert!(
            authz
                .check_query(&ops, QueryAction::Cancel, &q("alice"))
                .await
        );
        assert!(
            !authz
                .check_query(&ops, QueryAction::Poll, &q("alice"))
                .await
        );
        assert!(
            !authz
                .check_query(&ops, QueryAction::Dequeue, &q("alice"))
                .await
        );
    }

    #[tokio::test]
    async fn operator_group_can_cancel() {
        let authz = AllowAllAuthorization::with_operators(OperatorPolicy::from_lists(
            vec![],
            vec!["platform-ops".into()],
        ));
        let ops = ctx_ops("oncall", &[], &["platform-ops"]);
        assert!(
            authz
                .check_query(&ops, QueryAction::Cancel, &q("alice"))
                .await
        );
    }

    #[test]
    fn legacy_empty_owner_is_owner() {
        assert!(is_query_owner(&ctx("alice"), ""));
        assert!(is_query_owner(&ctx("anonymous"), "anonymous"));
        assert!(!is_query_owner(&ctx("bob"), "alice"));
    }
}
