mod wire;

use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;

use queryflux_core::access_model::{AccessDecision, AccessRequest};

use crate::config::{ClientCredentials, OpaProviderConfig};
use crate::provider::{PolicyDecisionProvider, PolicyError};

/// OPA (Open Policy Agent) policy provider.
///
/// Serializes the neutral request to `{"input": {...}}`, POSTs it to `{url}{decision_path}`,
/// and maps `{"result": {...}}` back. Optional bearer auth via a static token or an OAuth2
/// client-credentials exchange (token cached until expiry). Token-cache shape mirrors
/// `queryflux_auth::OpenFgaAuthorizationClient`.
pub struct OpaProvider {
    endpoint: String,
    timeout: Duration,
    http: reqwest::Client,
    bearer_token: Option<String>,
    client_credentials: Option<ClientCredentials>,
    token_cache: Mutex<Option<TokenCacheEntry>>,
}

const TOKEN_REFRESH_BUFFER: Duration = Duration::from_secs(30);
/// How long a failed token fetch is remembered before the next caller is allowed to retry
/// it. Without this, every caller queued behind the refresh lock during an IdP outage would
/// each attempt (and wait out) its own failing request in turn, so total latency for the
/// Nth waiter grows with N instead of staying bounded by one request's timeout.
const TOKEN_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
/// `PolicyError::Status`'s body is client-visible (forwarded as the denial reason), so it's
/// bounded the same way request/response logging elsewhere is.
const ERROR_BODY_LIMIT: usize = 4096;

enum TokenCacheEntry {
    Valid(String, Instant),
    /// The last fetch failed; retry after this instant instead of before it.
    FailedUntil(Instant),
}

impl OpaProvider {
    pub fn new(cfg: &OpaProviderConfig) -> Result<Self, String> {
        let base = cfg.url.trim_end_matches('/');
        let endpoint = format!("{base}{}", cfg.decision_path);
        Ok(Self {
            endpoint,
            timeout: Duration::from_millis(cfg.timeout_ms.max(1)),
            http: reqwest::Client::builder()
                .timeout(Duration::from_millis(cfg.timeout_ms.max(1)) + Duration::from_secs(5))
                .build()
                .map_err(|e| format!("build OPA http client: {e}"))?,
            bearer_token: cfg.bearer_token.clone(),
            client_credentials: cfg.client_credentials.clone(),
            token_cache: Mutex::new(None),
        })
    }

    async fn bearer(&self) -> Option<String> {
        if let Some(t) = &self.bearer_token {
            return Some(t.clone());
        }
        let cc = self.client_credentials.as_ref()?;

        // Held across the refresh itself (not just the check), including the `.await` on
        // the token-endpoint POST — `tokio::sync::Mutex` is designed to be held over an
        // await point. This serializes concurrent refreshes instead of every in-flight
        // query independently firing its own token request when the cache is near expiry:
        // the first caller in refreshes, everyone else blocks briefly and then observes
        // the freshly cached token instead of duplicating the HTTP call.
        let mut guard = self.token_cache.lock().await;
        let now = Instant::now();
        match guard.as_ref() {
            Some(TokenCacheEntry::Valid(tok, exp)) if now + TOKEN_REFRESH_BUFFER < *exp => {
                return Some(tok.clone());
            }
            // A prior fetch failed recently: fail fast instead of repeating the same
            // request every waiter had to queue behind — see `TOKEN_FAILURE_COOLDOWN`.
            Some(TokenCacheEntry::FailedUntil(until)) if now < *until => return None,
            _ => {}
        }
        // Bounded the same as the policy request itself (plus reqwest's own connect
        // timeout inside that), so a slow or hung token endpoint can't make `evaluate()`'s
        // total latency an unbounded multiple of `self.timeout` before fail-open engages.
        let fetch = async {
            let resp = self
                .http
                .post(&cc.token_endpoint)
                .timeout(self.timeout)
                .form(&[
                    ("grant_type", "client_credentials"),
                    ("client_id", &cc.client_id),
                    ("client_secret", &cc.client_secret),
                ])
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?;
            let body: serde_json::Value = resp.json().await.ok()?;
            let token = body.get("access_token")?.as_str()?.to_string();
            let expires_in = body
                .get("expires_in")
                .and_then(|v| v.as_u64())
                .unwrap_or(300);
            Some((token, Duration::from_secs(expires_in)))
        };
        match fetch.await {
            Some((token, ttl)) => {
                *guard = Some(TokenCacheEntry::Valid(token.clone(), now + ttl));
                Some(token)
            }
            None => {
                // A fresh `Instant`, not `now` from before the fetch: the fetch itself can
                // take up to `self.timeout`, so a cooldown measured from `now` could already
                // be expired (or nearly so) the moment it's stored — especially once
                // `self.timeout` approaches `TOKEN_FAILURE_COOLDOWN` — defeating the point of
                // caching the failure at all.
                *guard = Some(TokenCacheEntry::FailedUntil(
                    Instant::now() + TOKEN_FAILURE_COOLDOWN,
                ));
                None
            }
        }
    }
}

#[async_trait]
impl PolicyDecisionProvider for OpaProvider {
    async fn evaluate(&self, req: &AccessRequest) -> Result<AccessDecision, PolicyError> {
        let body = wire::to_request(req);
        let mut request = self
            .http
            .post(&self.endpoint)
            .timeout(self.timeout)
            .json(&body);
        if let Some(token) = self.bearer().await {
            request = request.bearer_auth(token);
        }

        let resp = request.send().await.map_err(|e| {
            if e.is_timeout() {
                PolicyError::Timeout
            } else {
                PolicyError::Transport(e.to_string())
            }
        })?;

        let status = resp.status();
        if !status.is_success() {
            let body = truncate_error_body(resp.text().await.unwrap_or_default());
            return Err(PolicyError::Status(status.as_u16(), body));
        }

        let parsed: wire::OpaResponse = resp
            .json()
            .await
            .map_err(|e| PolicyError::Body(e.to_string()))?;

        Ok(wire::from_response(parsed, req))
    }

    fn name(&self) -> &'static str {
        "opa"
    }
}

/// Bounds a provider error body before it becomes a client-visible denial reason.
/// Truncates on a UTF-8 char boundary — `String::truncate` panics otherwise, and a
/// provider's error body is untrusted, arbitrary text.
fn truncate_error_body(mut body: String) -> String {
    if body.len() <= ERROR_BODY_LIMIT {
        return body;
    }
    let mut cut = ERROR_BODY_LIMIT;
    while !body.is_char_boundary(cut) {
        cut -= 1;
    }
    body.truncate(cut);
    body.push_str("... (truncated)");
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_error_body_leaves_short_bodies_untouched() {
        let short = "policy denied: missing role".to_string();
        assert_eq!(truncate_error_body(short.clone()), short);
    }

    #[test]
    fn truncate_error_body_bounds_long_bodies() {
        let long = "x".repeat(ERROR_BODY_LIMIT + 500);
        let out = truncate_error_body(long);
        assert!(out.len() <= ERROR_BODY_LIMIT + "... (truncated)".len());
        assert!(out.ends_with("... (truncated)"));
    }

    /// A multi-byte character must never land the cut mid-codepoint (`String::truncate`
    /// panics on that), even when the limit itself falls inside one.
    #[test]
    fn truncate_error_body_respects_utf8_boundaries() {
        // A one-byte ASCII prefix before the repeated 2-byte "é" shifts every character's
        // start to an odd byte offset — since `ERROR_BODY_LIMIT` is even, byte
        // `ERROR_BODY_LIMIT` then falls strictly inside a character rather than between
        // two, which `"é".repeat(ERROR_BODY_LIMIT)` alone would not: with no prefix, every
        // character starts at an even offset, so the cut always lands cleanly between
        // characters and the boundary-seeking loop never actually runs.
        let long = format!("x{}", "é".repeat(ERROR_BODY_LIMIT));
        assert!(
            !long.is_char_boundary(ERROR_BODY_LIMIT),
            "test setup must land mid-character at the limit, or this test proves nothing"
        );
        let out = truncate_error_body(long);
        assert!(out.ends_with("... (truncated)"));
    }
}
