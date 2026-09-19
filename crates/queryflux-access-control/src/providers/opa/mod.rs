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
    token_cache: Mutex<Option<(String, Instant)>>,
}

const TOKEN_REFRESH_BUFFER: Duration = Duration::from_secs(30);

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
        {
            let guard = self.token_cache.lock().await;
            if let Some((tok, exp)) = guard.as_ref() {
                if Instant::now() + TOKEN_REFRESH_BUFFER < *exp {
                    return Some(tok.clone());
                }
            }
        }
        let resp = self
            .http
            .post(&cc.token_endpoint)
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
        let exp = Instant::now() + Duration::from_secs(expires_in);
        *self.token_cache.lock().await = Some((token.clone(), exp));
        Some(token)
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
            return Err(PolicyError::Status(status.as_u16()));
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
