//! `BackendIdentityResolver` — maps `(AuthContext, ClusterConfig)` → `QueryCredentials`.
//!
//! Called once per query, after cluster selection, before the adapter submits the query.
//!
//! Resolution rules:
//! - `auth_ctx.user == "anonymous"` or no `queryAuth` config → `ServiceAccount`
//! - `queryAuth: serviceAccount`   → `ServiceAccount`
//! - `queryAuth: passthrough`      → `Passthrough` (no-op at resolver — the adapter/dispatch
//!   layer resolves the actual forwarded credential from `SessionContext`/`raw_token`)
//! - `queryAuth: impersonate`      → `Impersonate { user }`
//! - `queryAuth: tokenExchange`    → RFC 8693 token exchange → `Bearer { token }`
//!   - **Fails closed** when `raw_token` is absent or the exchange fails: returns `Err`,
//!     never silently substitutes `ServiceAccount` (that would submit the query under the
//!     wrong principal).
//!   - Exchanged tokens are cached per (user, token_endpoint) until expiry − 30 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use queryflux_core::config::{ClusterConfig, QueryAuthConfig, TokenExchangeConfig};
use queryflux_core::error::{QueryFluxError, Result};
use tracing::{debug, error};

use crate::credentials::{AuthContext, QueryCredentials};

/// Drop cached exchanged tokens this long before `expires_at` (same idea as OpenFGA client cache).
const TOKEN_EXCHANGE_CACHE_BUFFER: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// BackendIdentityResolver
// ---------------------------------------------------------------------------

pub struct BackendIdentityResolver {
    http_client: reqwest::Client,
    /// Cache key: (username, token_endpoint) → (exchanged_token, expires_at).
    token_cache: Arc<DashMap<(String, String), (String, Instant)>>,
}

impl BackendIdentityResolver {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("build token-exchange http client"),
            token_cache: Arc::new(DashMap::new()),
        }
    }

    /// Resolve the `QueryCredentials` to use for this query.
    ///
    /// `cluster_cfg` is `None` when the cluster name is not in the config map
    /// (e.g. dynamically registered clusters) — falls back to `ServiceAccount`.
    ///
    /// Returns `Err` when `tokenExchange` is configured but the exchange fails. The
    /// caller must surface this as an auth error rather than silently substituting the
    /// service-account identity (which would send the query with the wrong principal).
    pub async fn resolve(
        &self,
        auth_ctx: &AuthContext,
        cluster_cfg: Option<&ClusterConfig>,
    ) -> Result<QueryCredentials> {
        // Anonymous identity → always service account regardless of cluster config.
        if auth_ctx.user == "anonymous" {
            return Ok(QueryCredentials::ServiceAccount);
        }

        let creds = match cluster_cfg.and_then(|c| c.query_auth.as_ref()) {
            None | Some(QueryAuthConfig::ServiceAccount) => QueryCredentials::ServiceAccount,

            Some(QueryAuthConfig::Passthrough) => QueryCredentials::Passthrough,

            Some(QueryAuthConfig::Impersonate) => QueryCredentials::Impersonate {
                user: auth_ctx.user.clone(),
            },

            Some(QueryAuthConfig::TokenExchange(cfg)) => {
                match self.exchange_token(auth_ctx, cfg).await {
                    Ok(token) => QueryCredentials::Bearer { token },
                    Err(e) => {
                        // Do NOT fall back to ServiceAccount — that would submit the query
                        // under the wrong identity. Propagate the error so the caller
                        // returns a proper auth failure to the client.
                        error!(
                            user = %auth_ctx.user,
                            error = %e,
                            "tokenExchange failed — rejecting query"
                        );
                        return Err(e);
                    }
                }
            }
        };
        Ok(creds)
    }

    async fn exchange_token(
        &self,
        auth_ctx: &AuthContext,
        cfg: &TokenExchangeConfig,
    ) -> Result<String> {
        let cache_key = (auth_ctx.user.clone(), cfg.token_endpoint.clone());

        // Fast path: return cached token if still more than the buffer before expiry.
        // `expires_at` is in the future — do not call `expires_at.elapsed()` (`now - expires_at` panics).
        if let Some(entry) = self.token_cache.get(&cache_key) {
            let (token, expires_at) = entry.value();
            if Instant::now() + TOKEN_EXCHANGE_CACHE_BUFFER < *expires_at {
                debug!(user = %auth_ctx.user, "tokenExchange: using cached token");
                return Ok(token.clone());
            }
        }

        let raw_token = auth_ctx.raw_token.as_deref().ok_or_else(|| {
            QueryFluxError::Auth(
                "tokenExchange requires a bearer token (use OidcAuthProvider on the frontend)"
                    .into(),
            )
        })?;

        debug!(user = %auth_ctx.user, endpoint = %cfg.token_endpoint, "tokenExchange: exchanging token");

        // RFC 8693 token exchange request.
        let mut params = vec![
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token", raw_token),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
        ];
        // Borrow from Option<String> so the lifetime is tied to cfg.
        if let Some(aud) = &cfg.target_audience {
            params.push(("audience", aud.as_str()));
        }
        if let Some(scope) = &cfg.scope {
            params.push(("scope", scope.as_str()));
        }

        let resp = self
            .http_client
            .post(&cfg.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| QueryFluxError::Auth(format!("tokenExchange HTTP error: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Auth(format!(
                "tokenExchange: server returned {status}: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            QueryFluxError::Auth(format!("tokenExchange: failed to parse response: {e}"))
        })?;

        let token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Auth("tokenExchange: response missing 'access_token'".into())
            })?
            .to_string();

        let expires_in = body
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        let expires_at = Instant::now() + Duration::from_secs(expires_in);

        self.token_cache
            .insert(cache_key, (token.clone(), expires_at));

        // Sweep expired entries on each write to keep the map bounded. The map
        // holds at most one entry per (user, token_endpoint) pair, so this is
        // cheap in practice and avoids a separate background task.
        let now = Instant::now();
        self.token_cache.retain(|_, (_, exp)| *exp > now);

        Ok(token)
    }
}

impl Default for BackendIdentityResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn ctx(user: &str, raw_token: Option<&str>) -> AuthContext {
        AuthContext {
            user: user.to_string(),
            groups: vec![],
            roles: vec![],
            raw_token: raw_token.map(str::to_string),
        }
    }

    fn cluster_with(query_auth: Option<QueryAuthConfig>) -> ClusterConfig {
        ClusterConfig {
            query_auth,
            ..Default::default()
        }
    }

    /// Spawn a raw-socket mock HTTP endpoint that answers every accepted connection with
    /// `body` once, then increments `hits`. Mirrors the pattern already used for HTTP-guard
    /// tests in `queryflux-guardrails` — no mocking crate dependency needed.
    async fn spawn_mock_endpoint(
        status_line: &'static str,
        body: String,
    ) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let response = format!(
                    "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                hits_clone.fetch_add(1, Ordering::SeqCst);
            }
        });
        (format!("http://{addr}/token"), hits)
    }

    fn token_exchange_cfg(endpoint: String) -> TokenExchangeConfig {
        TokenExchangeConfig {
            token_endpoint: endpoint,
            client_id: "queryflux".to_string(),
            client_secret: "secret".to_string(),
            target_audience: None,
            scope: None,
        }
    }

    #[tokio::test]
    async fn no_cluster_config_is_service_account() {
        let resolver = BackendIdentityResolver::new();
        let creds = resolver.resolve(&ctx("alice", None), None).await.unwrap();
        assert!(matches!(creds, QueryCredentials::ServiceAccount));
    }

    #[tokio::test]
    async fn explicit_service_account() {
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::ServiceAccount));
        let creds = resolver
            .resolve(&ctx("alice", None), Some(&cluster))
            .await
            .unwrap();
        assert!(matches!(creds, QueryCredentials::ServiceAccount));
    }

    #[tokio::test]
    async fn passthrough_resolves_to_passthrough_variant() {
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::Passthrough));
        let creds = resolver
            .resolve(&ctx("alice", None), Some(&cluster))
            .await
            .unwrap();
        assert!(matches!(creds, QueryCredentials::Passthrough));
    }

    #[tokio::test]
    async fn impersonate_carries_the_authenticated_user() {
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::Impersonate));
        let creds = resolver
            .resolve(&ctx("alice", None), Some(&cluster))
            .await
            .unwrap();
        assert!(matches!(creds, QueryCredentials::Impersonate { user } if user == "alice"));
    }

    #[tokio::test]
    async fn anonymous_is_always_service_account_regardless_of_query_auth() {
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::Impersonate));
        let creds = resolver
            .resolve(&ctx("anonymous", None), Some(&cluster))
            .await
            .unwrap();
        assert!(matches!(creds, QueryCredentials::ServiceAccount));
    }

    #[tokio::test]
    async fn token_exchange_success_returns_bearer() {
        let (endpoint, hits) = spawn_mock_endpoint(
            "HTTP/1.1 200 OK",
            r#"{"access_token":"exchanged-token","expires_in":3600}"#.to_string(),
        )
        .await;
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::TokenExchange(token_exchange_cfg(
            endpoint,
        ))));
        let creds = resolver
            .resolve(&ctx("alice", Some("client-jwt")), Some(&cluster))
            .await
            .unwrap();
        assert!(matches!(creds, QueryCredentials::Bearer { token } if token == "exchanged-token"));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn token_exchange_missing_raw_token_fails_closed() {
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::TokenExchange(token_exchange_cfg(
            "http://127.0.0.1:1/token".to_string(), // never reached
        ))));
        let err = resolver
            .resolve(&ctx("alice", None), Some(&cluster))
            .await
            .unwrap_err();
        assert!(matches!(err, QueryFluxError::Auth(_)));
    }

    #[tokio::test]
    async fn token_exchange_server_error_fails_closed_not_service_account() {
        let (endpoint, _hits) =
            spawn_mock_endpoint("HTTP/1.1 500 Internal Server Error", "{}".to_string()).await;
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::TokenExchange(token_exchange_cfg(
            endpoint,
        ))));
        let err = resolver
            .resolve(&ctx("alice", Some("client-jwt")), Some(&cluster))
            .await
            .unwrap_err();
        assert!(matches!(err, QueryFluxError::Auth(_)));
    }

    #[tokio::test]
    async fn token_exchange_caches_token_across_resolves() {
        let (endpoint, hits) = spawn_mock_endpoint(
            "HTTP/1.1 200 OK",
            r#"{"access_token":"cached-token","expires_in":3600}"#.to_string(),
        )
        .await;
        let resolver = BackendIdentityResolver::new();
        let cluster = cluster_with(Some(QueryAuthConfig::TokenExchange(token_exchange_cfg(
            endpoint,
        ))));
        let auth_ctx = ctx("alice", Some("client-jwt"));

        let first = resolver.resolve(&auth_ctx, Some(&cluster)).await.unwrap();
        let second = resolver.resolve(&auth_ctx, Some(&cluster)).await.unwrap();
        assert!(matches!(first, QueryCredentials::Bearer { token } if token == "cached-token"));
        assert!(matches!(second, QueryCredentials::Bearer { token } if token == "cached-token"));
        // Second resolve must hit the cache, not the token endpoint again.
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
