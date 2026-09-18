mod wire;

use std::time::Duration;

use async_trait::async_trait;

use queryflux_core::access_model::{AccessDecision, AccessRequest};

use crate::config::CerbosProviderConfig;
use crate::provider::{PolicyDecisionProvider, PolicyError};

/// Cerbos PDP policy provider.
///
/// Serializes the neutral request to a `CheckResources` call (`{"principal", "resources"}`),
/// POSTs it to `{url}{checkResourcesPath}`, and maps the per-resource `actions`/`outputs`
/// back to a neutral [`AccessDecision`] — see [`wire`] for the exact contract, including how
/// row filters/column masks are carried through Cerbos's generic `outputs` mechanism (Cerbos
/// has no native concept of either). Optional static bearer/API token; self-hosted Cerbos
/// typically needs none (secured by network policy instead).
///
/// Cerbos hard-requires `principal.roles` to be non-empty (a validation error, not a policy
/// decision, for an empty array) — an identity with no roles resolved is short-circuited to
/// a local deny in [`Self::evaluate`] rather than sent to Cerbos at all.
pub struct CerbosProvider {
    endpoint: String,
    timeout: Duration,
    http: reqwest::Client,
    bearer_token: Option<String>,
}

impl CerbosProvider {
    pub fn new(cfg: &CerbosProviderConfig) -> Result<Self, String> {
        let base = cfg.url.trim_end_matches('/');
        let endpoint = format!("{base}{}", cfg.check_resources_path);
        Ok(Self {
            endpoint,
            timeout: Duration::from_millis(cfg.timeout_ms.max(1)),
            http: reqwest::Client::builder()
                .timeout(Duration::from_millis(cfg.timeout_ms.max(1)) + Duration::from_secs(5))
                .build()
                .map_err(|e| format!("build Cerbos http client: {e}"))?,
            bearer_token: cfg.bearer_token.clone(),
        })
    }
}

#[async_trait]
impl PolicyDecisionProvider for CerbosProvider {
    async fn evaluate(&self, req: &AccessRequest) -> Result<AccessDecision, PolicyError> {
        // Cerbos's `CheckResources` hard-requires a non-empty `principal.roles` — a request
        // with none is rejected as a 400 validation error, not evaluated as "no role
        // matches". An identity with no roles resolved (no `staticUsers`/IdP role mapping,
        // or a purely group-based identity) can never match a Cerbos rule by definition, so
        // short-circuit to the same deny-all outcome Cerbos would reach anyway — instead of
        // spending a network round trip only to have it misclassified as a provider error
        // (which affects fail-open behavior and metrics, not just the error message).
        if req.identity.roles.is_empty() {
            return Ok(AccessDecision::deny_all(
                "cerbos requires at least one principal role; none were resolved for this identity",
            ));
        }

        let body = wire::to_request(&req.context.query_id, req);
        let mut request = self
            .http
            .post(&self.endpoint)
            .timeout(self.timeout)
            .json(&body);
        if let Some(token) = &self.bearer_token {
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
            let body = resp.text().await.unwrap_or_default();
            return Err(PolicyError::Status(status.as_u16(), body));
        }

        let parsed: wire::CheckResourcesResponse = resp
            .json()
            .await
            .map_err(|e| PolicyError::Body(e.to_string()))?;

        Ok(wire::from_response(parsed, req))
    }

    fn name(&self) -> &'static str {
        "cerbos"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    use queryflux_core::access_model::{
        AccessResource, Columns, Identity, Operation, RequestContext,
    };
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn req(table: &str) -> AccessRequest {
        AccessRequest {
            identity: Identity {
                user: "alice".to_string(),
                roles: vec!["engineer".to_string()],
                ..Identity::default()
            },
            operation: Operation::table_select(),
            resources: vec![AccessResource {
                catalog: None,
                schema: None,
                table: table.to_string(),
                columns: Columns::All,
            }],
            context: RequestContext::default(),
        }
    }

    async fn start_stub<F>(handler_body: F) -> String
    where
        F: Fn(HeaderMap) -> serde_json::Value + Send + Sync + 'static,
    {
        let handler_body = Arc::new(handler_body);
        async fn handler(
            State(f): State<Arc<dyn Fn(HeaderMap) -> serde_json::Value + Send + Sync>>,
            headers: HeaderMap,
            Json(_body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            Json(f(headers))
        }
        let app = Router::new()
            .route("/api/check/resources", post(handler))
            .with_state(handler_body as Arc<dyn Fn(HeaderMap) -> serde_json::Value + Send + Sync>);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    fn allow_response() -> serde_json::Value {
        serde_json::json!({"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []}]})
    }

    fn cfg(url: &str, bearer_token: Option<&str>) -> CerbosProviderConfig {
        CerbosProviderConfig {
            url: url.to_string(),
            check_resources_path: "/api/check/resources".to_string(),
            timeout_ms: 2_000,
            bearer_token: bearer_token.map(str::to_string),
        }
    }

    /// Regression: Cerbos's `CheckResources` hard-requires a non-empty `principal.roles`
    /// (validated against a real Cerbos PDP — an empty array is rejected as HTTP 400
    /// "principal.roles: value is required", not evaluated as "no rule matches"). An
    /// identity with no roles must short-circuit to a clean deny *without* making the
    /// network call at all — proven here by pointing at a port nothing listens on: if the
    /// provider attempted the HTTP call, this would fail with a transport error instead of
    /// returning `Ok`.
    #[tokio::test]
    async fn identity_with_no_roles_denies_without_calling_cerbos() {
        let provider =
            CerbosProvider::new(&cfg("http://127.0.0.1:1", None)).expect("build provider");
        let mut request = req("orders");
        request.identity.roles = vec![];
        let decision = provider.evaluate(&request).await.expect("must not error");
        assert!(!decision.is_allowed());
    }

    #[tokio::test]
    async fn evaluate_round_trips_through_a_real_http_call() {
        let url = start_stub(|_| allow_response()).await;
        let provider = CerbosProvider::new(&cfg(&url, None)).expect("build provider");
        let decision = provider.evaluate(&req("orders")).await.expect("evaluate");
        assert!(decision.is_allowed());
        assert_eq!(provider.name(), "cerbos");
    }

    /// A configured static bearer token must actually reach the wire as
    /// `Authorization: Bearer <token>` — not just be accepted by config parsing.
    #[tokio::test]
    async fn bearer_token_is_sent_as_authorization_header() {
        let seen = Arc::new(Mutex::new(None));
        let seen_clone = seen.clone();
        let url = start_stub(move |headers| {
            *seen_clone.lock().unwrap() = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            allow_response()
        })
        .await;
        let provider = CerbosProvider::new(&cfg(&url, Some("s3cr3t"))).expect("build provider");
        provider.evaluate(&req("orders")).await.expect("evaluate");
        assert_eq!(seen.lock().unwrap().as_deref(), Some("Bearer s3cr3t"));
    }

    /// No bearer token configured: no `Authorization` header at all — self-hosted Cerbos
    /// behind network policy must not receive a spurious/empty auth header.
    #[tokio::test]
    async fn no_bearer_token_configured_sends_no_authorization_header() {
        let seen = Arc::new(Mutex::new(None));
        let seen_clone = seen.clone();
        let url = start_stub(move |headers| {
            *seen_clone.lock().unwrap() = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            allow_response()
        })
        .await;
        let provider = CerbosProvider::new(&cfg(&url, None)).expect("build provider");
        provider.evaluate(&req("orders")).await.expect("evaluate");
        assert_eq!(seen.lock().unwrap().as_deref(), None);
    }

    #[tokio::test]
    async fn non_success_status_maps_to_policy_error_status() {
        async fn handler() -> axum::http::StatusCode {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
        let app = Router::new().route("/api/check/resources", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let provider =
            CerbosProvider::new(&cfg(&format!("http://{addr}"), None)).expect("build provider");
        match provider.evaluate(&req("orders")).await {
            Err(PolicyError::Status(500, _)) => {}
            other => panic!("expected PolicyError::Status(500, _), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_body_maps_to_policy_error_body() {
        async fn handler() -> &'static str {
            "not json at all"
        }
        let app = Router::new().route("/api/check/resources", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let provider =
            CerbosProvider::new(&cfg(&format!("http://{addr}"), None)).expect("build provider");
        match provider.evaluate(&req("orders")).await {
            Err(PolicyError::Body(_)) => {}
            other => panic!("expected PolicyError::Body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn slow_server_maps_to_policy_error_timeout() {
        async fn handler() -> Json<serde_json::Value> {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Json(allow_response())
        }
        let app = Router::new().route("/api/check/resources", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let mut c = cfg(&format!("http://{addr}"), None);
        c.timeout_ms = 50;
        let provider = CerbosProvider::new(&c).expect("build provider");
        match provider.evaluate(&req("orders")).await {
            Err(PolicyError::Timeout) => {}
            other => panic!("expected PolicyError::Timeout, got {other:?}"),
        }
    }
}
