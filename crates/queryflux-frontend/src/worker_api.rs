//! The control plane's API for workers (#255).
//!
//! - `GET /worker/config` returns the current [`WorkerSnapshotV1`]. The snapshot's revision
//!   is the `ETag`; a matching `If-None-Match` gets `304 Not Modified`.
//! - `GET /worker/config/wait?since=REV&timeoutSecs=N` (long-poll) holds the request until
//!   the published revision differs from `since`, or answers 304 after the timeout. It
//!   re-checks whenever this process reloads its config.
//! - `POST /worker/register` and `POST /worker/heartbeat` record what each worker reports
//!   (name, version, labels, coordination mode, revisions, last error).
//!
//! Workers authenticate with `Authorization: Bearer <workerApi.sharedSecret>`, separate
//! from the admin API's Basic auth.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use queryflux_core::worker_snapshot::{WorkerSnapshotV1, WORKER_SNAPSHOT_MEDIA_TYPE};
use queryflux_persistence::{WorkerRecord, WorkerRegistryStore};

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
    /// Where registrations and heartbeats are recorded; `None` accepts and drops them.
    pub registry: Option<Arc<dyn WorkerRegistryStore>>,
    /// Bumped whenever this process reloads its config; wakes long-polls.
    pub changes: Option<tokio::sync::watch::Receiver<u64>>,
}

/// A long-poll never holds a request longer than this.
const MAX_WAIT_SECS: u64 = 60;
/// Workers seen within this window count as live for fleet checks.
const LIVE_WINDOW_SECS: i64 = 120;

pub fn router(api: WorkerApi) -> Router {
    Router::new()
        .route("/worker/config", get(get_config))
        .route("/worker/config/wait", get(wait_config))
        .route("/worker/register", post(report))
        .route("/worker/heartbeat", post(report))
        .with_state(Arc::new(WorkerApiState {
            api,
            last_fleet_warning: std::sync::atomic::AtomicI64::new(0),
        }))
}

struct WorkerApiState {
    api: WorkerApi,
    /// Unix seconds of the last shared-capacity warning (rate limit).
    last_fleet_warning: std::sync::atomic::AtomicI64,
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

fn snapshot_response(snapshot: &WorkerSnapshotV1, if_none_match: Option<&str>) -> Response {
    let tag = etag(&snapshot.revision);
    let tag_header = HeaderValue::from_str(&tag).unwrap_or(HeaderValue::from_static("\"\""));
    if if_none_match.is_some_and(|v| v.split(',').any(|t| t.trim() == tag)) {
        return (StatusCode::NOT_MODIFIED, [(header::ETAG, tag_header)]).into_response();
    }
    match serde_json::to_vec(snapshot) {
        Ok(body) => (
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
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn unavailable(message: String) -> Response {
    tracing::warn!("Worker config unavailable: {message}");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        "invalid or missing worker credential",
    )
        .into_response()
}

async fn get_config(State(st): State<Arc<WorkerApiState>>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &st.api.shared_secret) {
        return unauthorized();
    }
    match (st.api.snapshot)().await {
        Ok(snapshot) => snapshot_response(
            &snapshot,
            headers
                .get(header::IF_NONE_MATCH)
                .and_then(|v| v.to_str().ok()),
        ),
        Err(message) => unavailable(message),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WaitParams {
    since: Option<String>,
    timeout_secs: Option<u64>,
}

async fn wait_config(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Query(params): Query<WaitParams>,
) -> Response {
    if !authorized(&headers, &st.api.shared_secret) {
        return unauthorized();
    }
    let timeout =
        std::time::Duration::from_secs(params.timeout_secs.unwrap_or(30).min(MAX_WAIT_SECS));
    let deadline = tokio::time::Instant::now() + timeout;
    let mut changes = st.api.changes.clone();
    loop {
        let snapshot = match (st.api.snapshot)().await {
            Ok(s) => s,
            Err(message) => return unavailable(message),
        };
        if params.since.as_deref() != Some(snapshot.revision.as_str()) {
            return snapshot_response(&snapshot, None);
        }
        let changed = async {
            match changes.as_mut() {
                Some(rx) => {
                    if rx.changed().await.is_err() {
                        std::future::pending::<()>().await
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = changed => continue,
            _ = tokio::time::sleep_until(deadline) => {
                return snapshot_response(&snapshot, Some(&etag(&snapshot.revision)));
            }
        }
    }
}

async fn report(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(worker): Json<WorkerRecord>,
) -> Response {
    if !authorized(&headers, &st.api.shared_secret) {
        return unauthorized();
    }
    if worker.name.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "worker name is required").into_response();
    }
    if let Some(registry) = &st.api.registry {
        if let Err(e) = registry.upsert_worker(&worker).await {
            tracing::warn!(worker = %worker.name, "Failed to record worker report: {e}");
            return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response();
        }
        warn_on_unshared_capacity(&st, registry.as_ref()).await;
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Several live workers with any of them on local-only limits means their caps are not
/// shared: each worker admits up to the full limit on its own.
async fn warn_on_unshared_capacity(st: &WorkerApiState, registry: &dyn WorkerRegistryStore) {
    let now = chrono::Utc::now();
    let last = st
        .last_fleet_warning
        .load(std::sync::atomic::Ordering::Relaxed);
    if now.timestamp() - last < 60 {
        return;
    }
    let Ok(workers) = registry
        .list_workers(now - chrono::Duration::seconds(LIVE_WINDOW_SECS))
        .await
    else {
        return;
    };
    let local: Vec<&str> = workers
        .iter()
        .filter(|w| w.coordination_mode == "local")
        .map(|w| w.name.as_str())
        .collect();
    if workers.len() >= 2 && !local.is_empty() {
        st.last_fleet_warning
            .store(now.timestamp(), std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            workers = workers.len(),
            local = ?local,
            "Several workers are live and some use coordination.mode: local — their limits \
             are not shared, so a group can run up to its limit on each of those workers"
        );
    }
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
            registry: None,
            changes: None,
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

    fn changing_app(
        current: Arc<std::sync::Mutex<WorkerSnapshotV1>>,
        changes: tokio::sync::watch::Receiver<u64>,
        registry: Option<Arc<dyn WorkerRegistryStore>>,
    ) -> Router {
        router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(move || {
                let s = current.lock().unwrap().clone();
                Box::pin(async move { Ok(s) })
            }),
            registry,
            changes: Some(changes),
        })
    }

    fn with_fallback(fallback: &str) -> WorkerSnapshotV1 {
        let mut s = snapshot();
        s.routing_fallback = fallback.into();
        s.seal()
    }

    #[tokio::test]
    async fn long_poll_returns_as_soon_as_the_revision_changes() {
        let current = Arc::new(std::sync::Mutex::new(with_fallback("a")));
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        let app = changing_app(current.clone(), rx, None);
        let since = current.lock().unwrap().revision.clone();

        let req = Request::get(format!("/worker/config/wait?since={since}&timeoutSecs=10"))
            .header(header::AUTHORIZATION, "Bearer s3cret")
            .body(Body::empty())
            .unwrap();
        let waiting = tokio::spawn(app.oneshot(req));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!waiting.is_finished(), "unchanged revision must wait");

        *current.lock().unwrap() = with_fallback("b");
        tx.send_modify(|v| *v += 1);
        let resp = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
            .await
            .expect("woken by the change")
            .unwrap()
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let back: WorkerSnapshotV1 = serde_json::from_slice(&body).unwrap();
        assert_eq!(back.routing_fallback, "b");
    }

    #[tokio::test]
    async fn long_poll_times_out_with_not_modified() {
        let current = Arc::new(std::sync::Mutex::new(with_fallback("a")));
        let (_tx, rx) = tokio::sync::watch::channel(0u64);
        let since = current.lock().unwrap().revision.clone();
        let req = Request::get(format!("/worker/config/wait?since={since}&timeoutSecs=1"))
            .header(header::AUTHORIZATION, "Bearer s3cret")
            .body(Body::empty())
            .unwrap();
        let resp = changing_app(current, rx, None).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn heartbeats_are_recorded() {
        let registry = Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let (_tx, rx) = tokio::sync::watch::channel(0u64);
        let app = changing_app(
            Arc::new(std::sync::Mutex::new(snapshot())),
            rx,
            Some(registry.clone()),
        );
        let report = serde_json::json!({
            "name": "worker-a",
            "version": "0.4.0",
            "labels": { "app": "checkout" },
            "coordinationMode": "local",
            "activeRevision": "abc"
        });
        let req = Request::post("/worker/heartbeat")
            .header(header::AUTHORIZATION, "Bearer s3cret")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(report.to_string()))
            .unwrap();
        assert_eq!(
            app.oneshot(req).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        let workers = registry
            .list_workers(chrono::Utc::now() - chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].active_revision.as_deref(), Some("abc"));
        assert_eq!(workers[0].labels["app"], "checkout");
    }
}
