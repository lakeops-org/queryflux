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
//! - `/worker/capacity/*` and `/worker/sweep-lock/*` give workers with
//!   `coordination.mode: controlPlane` the same capacity leases, sweep locks and published
//!   engine counts this process uses, so one `maxRunningQueries` holds across it and every
//!   worker. Available only when this process has a coordination store (Postgres).
//!
//! - `/worker/query-records` stores the query history workers send (a
//!   [`QueryRecordBatchV1`]) in this process's history store, stamped with the worker.
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
use queryflux_persistence::{
    CoordinationStore, MetricsStore, QueryRecordBatchV1, SweepGuard, WorkerRecord,
    WorkerRegistryStore, QUERY_RECORDS_SCHEMA_VERSION,
};

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
    /// This process's coordination store, shared with workers; `None` when it has none.
    pub coordination: Option<Arc<dyn CoordinationStore>>,
    /// Where worker query records are stored (history only, not Prometheus: each worker
    /// exports its own metrics). `None` refuses them.
    pub history: Option<Arc<dyn MetricsStore>>,
}

/// Largest query-record batch accepted. Workers send at most a few hundred records.
const MAX_RECORD_BATCH_BYTES: usize = 32 * 1024 * 1024;

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
        .route("/worker/capacity/acquire", post(capacity_acquire))
        .route("/worker/capacity/release", post(capacity_release))
        .route("/worker/capacity/heartbeat", post(capacity_heartbeat))
        .route("/worker/capacity/release-all", post(capacity_release_all))
        .route(
            "/worker/capacity/running",
            get(running_get).post(running_publish),
        )
        .route("/worker/capacity/leases", get(lease_count))
        .route(
            "/worker/query-records",
            post(ingest_records)
                .layer(axum::extract::DefaultBodyLimit::max(MAX_RECORD_BATCH_BYTES)),
        )
        .route("/worker/sweep-lock/{name}", post(sweep_lock_acquire))
        .route(
            "/worker/sweep-lock/{name}/{token}",
            axum::routing::delete(sweep_lock_release),
        )
        .with_state(Arc::new(WorkerApiState {
            api,
            last_fleet_warning: std::sync::atomic::AtomicI64::new(0),
            sweep_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }))
}

/// A sweep lock this process holds on a worker's behalf, until the worker releases it or
/// it is older than [`SWEEP_LOCK_TTL`] (a worker that died mid-sweep).
type HeldSweepLocks = tokio::sync::Mutex<
    std::collections::HashMap<String, (Box<dyn SweepGuard>, std::time::Instant)>,
>;
const SWEEP_LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(300);

struct WorkerApiState {
    api: WorkerApi,
    /// Unix seconds of the last shared-capacity warning (rate limit).
    last_fleet_warning: std::sync::atomic::AtomicI64,
    sweep_locks: HeldSweepLocks,
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

// -- Query history ----------------------------------------------------------

async fn ingest_records(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(batch): Json<QueryRecordBatchV1>,
) -> Response {
    if !authorized(&headers, &st.api.shared_secret) {
        return unauthorized();
    }
    let Some(store) = st.api.history.clone() else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "this control plane does not store worker query history",
        )
            .into_response();
    };
    if batch.schema_version != QUERY_RECORDS_SCHEMA_VERSION {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported query-records schemaVersion {} (this control plane accepts {})",
                batch.schema_version, QUERY_RECORDS_SCHEMA_VERSION
            ),
        )
            .into_response();
    }
    let accepted = batch.records.len();
    for mut record in batch.records {
        record.worker = Some(batch.worker.clone());
        // Retried batches repeat records; the store keeps the first per proxy query id.
        if let Err(e) = store.record_query(record).await {
            return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response();
        }
    }
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "accepted": accepted })),
    )
        .into_response()
}

// -- Shared capacity --------------------------------------------------------

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquireRequest {
    pub cluster: String,
    pub max_running_queries: u64,
    pub instance_id: String,
    pub query_id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseRequest {
    pub cluster: String,
    pub query_id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceRequest {
    pub instance_id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterQuery {
    pub cluster: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishRunning {
    pub cluster: String,
    pub count: u64,
}

/// Authorize the worker and return the coordination store, or the response to send.
#[allow(clippy::result_large_err)]
fn coordination(
    st: &WorkerApiState,
    headers: &HeaderMap,
) -> Result<Arc<dyn CoordinationStore>, Response> {
    if !authorized(headers, &st.api.shared_secret) {
        return Err(unauthorized());
    }
    st.api.coordination.clone().ok_or_else(|| {
        (
            StatusCode::NOT_IMPLEMENTED,
            "this control plane has no coordination store; use coordination.mode: local",
        )
            .into_response()
    })
}

fn store_error(e: queryflux_core::error::QueryFluxError) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
}

async fn capacity_acquire(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<AcquireRequest>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store
        .try_acquire(
            &req.cluster,
            req.max_running_queries,
            &req.instance_id,
            &req.query_id,
        )
        .await
    {
        Ok(granted) => Json(serde_json::json!({ "granted": granted })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn capacity_release(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<ReleaseRequest>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.release(&req.cluster, &req.query_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e),
    }
}

async fn capacity_heartbeat(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<InstanceRequest>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.heartbeat(&req.instance_id).await {
        Ok(n) => Json(serde_json::json!({ "renewed": n })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn capacity_release_all(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<InstanceRequest>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.release_all_for_instance(&req.instance_id).await {
        Ok(n) => Json(serde_json::json!({ "released": n })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn running_get(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Query(q): Query<ClusterQuery>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.active_count(&q.cluster).await {
        Ok(n) => Json(serde_json::json!({ "count": n })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn running_publish(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<PublishRunning>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.publish_running_count(&req.cluster, req.count).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e),
    }
}

async fn lease_count(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Query(q): Query<ClusterQuery>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.active_lease_count(&q.cluster).await {
        Ok(n) => Json(serde_json::json!({ "count": n })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn sweep_lock_acquire(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let mut held = st.sweep_locks.lock().await;
    // Release locks a worker took and never returned (it died mid-sweep).
    let expired: Vec<String> = held
        .iter()
        .filter(|(_, (_, at))| at.elapsed() > SWEEP_LOCK_TTL)
        .map(|(k, _)| k.clone())
        .collect();
    for token in expired {
        if let Some((guard, _)) = held.remove(&token) {
            guard.release().await;
        }
    }
    match store.try_sweep_lock(&name).await {
        Ok(Some(guard)) => {
            let token = uuid::Uuid::new_v4().to_string();
            held.insert(token.clone(), (guard, std::time::Instant::now()));
            Json(serde_json::json!({ "token": token })).into_response()
        }
        Ok(None) => Json(serde_json::json!({ "token": null })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn sweep_lock_release(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    axum::extract::Path((_name, token)): axum::extract::Path<(String, String)>,
) -> Response {
    if let Err(r) = coordination(&st, &headers) {
        return r;
    }
    let guard = st.sweep_locks.lock().await.remove(&token);
    if let Some((guard, _)) = guard {
        guard.release().await;
    }
    StatusCode::NO_CONTENT.into_response()
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
            history: None,
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
            coordination: None,
            history: None,
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
            coordination: None,
            history: None,
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

    #[tokio::test]
    async fn capacity_is_shared_through_the_control_plane() {
        let store = Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let app = router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
            registry: None,
            changes: None,
            coordination: Some(store),
            history: None,
        });
        let post = |path: &str, body: serde_json::Value| {
            Request::post(path.to_string())
                .header(header::AUTHORIZATION, "Bearer s3cret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let resp = app
            .clone()
            .oneshot(post(
                "/worker/capacity/acquire",
                serde_json::json!({
                    "cluster": "c", "maxRunningQueries": 1, "instanceId": "w1", "queryId": "q1"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v["granted"].is_boolean());

        let lock = app
            .clone()
            .oneshot(
                Request::post("/worker/sweep-lock/reconcile")
                    .header(header::AUTHORIZATION, "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lock.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn capacity_needs_a_coordination_store() {
        let resp = app(Ok(snapshot()))
            .oneshot(
                Request::get("/worker/capacity/running?cluster=c")
                    .header(header::AUTHORIZATION, "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn worker_records_are_stored_with_the_worker_name() {
        let store = Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let app = router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
            registry: None,
            changes: None,
            coordination: None,
            history: Some(store.clone()),
        });
        let record = serde_json::json!({
            "proxyQueryId": "q-1",
            "clusterGroup": "g",
            "clusterName": "c",
            "engineType": "duckDb",
            "frontendProtocol": "trinoHttp",
            "sourceDialect": "trino",
            "targetDialect": "duckDb",
            "wasRewritten": false,
            "wasTranslated": false,
            "sqlPreview": "SELECT 1",
            "status": "success",
            "queueDurationMs": 0,
            "executionDurationMs": 3,
            "createdAt": "2026-10-14T00:00:00Z",
            "worker": "spoofed"
        });
        let post = |body: serde_json::Value| {
            Request::post("/worker/query-records")
                .header(header::AUTHORIZATION, "Bearer s3cret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let batch = serde_json::json!({
            "schemaVersion": 1, "worker": "worker-a", "records": [record.clone(), record]
        });
        let resp = app.clone().oneshot(post(batch)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        use queryflux_persistence::QueryHistoryStore;
        let page = store
            .list_queries(&queryflux_persistence::query_history::QueryFilters {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.len(), 1, "a repeated record is stored once");
        assert_eq!(page[0].worker.as_deref(), Some("worker-a"));

        let future = serde_json::json!({ "schemaVersion": 2, "worker": "w", "records": [] });
        let resp = app.oneshot(post(future)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
