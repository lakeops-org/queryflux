//! The control plane's API for workers (#255).
//!
//! `GET /worker/config` returns the current [`WorkerSnapshotV1`]. Workers authenticate
//! with `Authorization: Bearer <workerApi.sharedSecret>`, separate from the admin API's
//! Basic auth. The snapshot's revision is the `ETag`; a request whose `If-None-Match`
//! matches gets `304 Not Modified`, so polling an unchanged config is cheap.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use queryflux_core::worker_snapshot::{WorkerSnapshotV1, WORKER_SNAPSHOT_MEDIA_TYPE};

/// Builds the current snapshot. `Err` carries a message for the worker (for example
/// "config cannot be published: …").
pub type WorkerSnapshotFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<WorkerSnapshotV1, String>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct WorkerApi {
    pub shared_secret: String,
    pub snapshot: WorkerSnapshotFn,
}

pub fn router(api: WorkerApi) -> Router {
    Router::new()
        .route("/worker/config", get(get_config))
        .with_state(Arc::new(api))
}

/// Constant-time comparison, so response timing does not leak how much of the
/// secret matched.
fn secret_matches(given: &str, expected: &str) -> bool {
    let (a, b) = (given.as_bytes(), expected.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn authorized(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(crate::strip_bearer_prefix)
        .is_some_and(|token| secret_matches(token, secret))
}

fn etag(revision: &str) -> String {
    format!("\"{revision}\"")
}

async fn get_config(State(api): State<Arc<WorkerApi>>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &api.shared_secret) {
        return (
            StatusCode::UNAUTHORIZED,
            "invalid or missing worker credential",
        )
            .into_response();
    }
    let snapshot = match (api.snapshot)().await {
        Ok(s) => s,
        Err(message) => {
            tracing::warn!("Worker config unavailable: {message}");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({ "error": message })),
            )
                .into_response();
        }
    };
    let tag = etag(&snapshot.revision);
    let tag_header = HeaderValue::from_str(&tag).unwrap_or(HeaderValue::from_static("\"\""));
    let unchanged = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim() == tag));
    if unchanged {
        return (StatusCode::NOT_MODIFIED, [(header::ETAG, tag_header)]).into_response();
    }
    let body = match serde_json::to_vec(&snapshot) {
        Ok(b) => b,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(WORKER_SNAPSHOT_MEDIA_TYPE),
            ),
            (header::ETAG, tag_header),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use queryflux_core::worker_snapshot::{
        CredentialMode, SnapshotSettings, WORKER_SNAPSHOT_SCHEMA_VERSION,
    };
    use tower::ServiceExt;

    fn snapshot() -> WorkerSnapshotV1 {
        WorkerSnapshotV1 {
            schema_version: WORKER_SNAPSHOT_SCHEMA_VERSION,
            revision: String::new(),
            credentials: CredentialMode::Embedded,
            clusters: vec![],
            groups: Default::default(),
            routers: vec![],
            routing_fallback: "g".into(),
            group_translation_scripts: Default::default(),
            settings: SnapshotSettings::default(),
        }
        .seal()
    }

    fn app(result: Result<WorkerSnapshotV1, String>) -> Router {
        router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(move || {
                let r = result.clone();
                Box::pin(async move { r })
            }),
        })
    }

    async fn get(app: Router, auth: Option<&str>, if_none_match: Option<&str>) -> Response {
        let mut req = Request::get("/worker/config");
        if let Some(a) = auth {
            req = req.header(header::AUTHORIZATION, a);
        }
        if let Some(t) = if_none_match {
            req = req.header(header::IF_NONE_MATCH, t);
        }
        app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap()
    }

    #[tokio::test]
    async fn rejects_a_missing_or_wrong_secret() {
        let s = snapshot();
        assert_eq!(
            get(app(Ok(s.clone())), None, None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(app(Ok(s)), Some("Bearer nope"), None).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn serves_the_snapshot_with_its_revision_as_etag() {
        let s = snapshot();
        let resp = get(app(Ok(s.clone())), Some("Bearer s3cret"), None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()[header::CONTENT_TYPE],
            WORKER_SNAPSHOT_MEDIA_TYPE
        );
        assert_eq!(resp.headers()[header::ETAG], format!("\"{}\"", s.revision));
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let back: WorkerSnapshotV1 = serde_json::from_slice(&body).unwrap();
        assert!(back.is_sealed());
        assert_eq!(back.revision, s.revision);
    }

    #[tokio::test]
    async fn an_unchanged_revision_is_not_modified() {
        let s = snapshot();
        let tag = format!("\"{}\"", s.revision);
        let resp = get(app(Ok(s)), Some("Bearer s3cret"), Some(&tag)).await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.headers()[header::ETAG], tag);
    }

    #[tokio::test]
    async fn an_unpublishable_config_is_service_unavailable() {
        let resp = get(
            app(Err("config cannot be published: dangling fallback".into())),
            Some("Bearer s3cret"),
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
