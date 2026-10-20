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
//!   [`QueryRecordBatchV1`]) in this process's history store, stamped with the worker,
//!   as one batch write. At most [`MAX_CONCURRENT_RECORD_BATCHES`] batches are written at
//!   once; a batch that cannot start within [`RECORD_BATCH_QUEUE_WAIT`] gets
//!   `503 Service Unavailable` with `Retry-After`, and the worker keeps it spooled.
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
    /// Capacity quotas for workers (`capacity.mode: quotas`); `None` answers 404, and
    /// workers keep using per-query leases.
    pub quotas: Option<Arc<dyn queryflux_persistence::quota::QuotaStore>>,
}

/// Largest query-record batch accepted. Workers send at most a few hundred records.
const MAX_RECORD_BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Query-record batches written at once. Each holds one connection of the admin pool
/// (2 by default) for one short transaction; more would only queue on the pool and delay
/// the admin API sharing it.
pub const MAX_CONCURRENT_RECORD_BATCHES: usize = 2;
/// How long a batch may wait for a write slot before it is refused with 503.
pub const RECORD_BATCH_QUEUE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
/// `Retry-After` (seconds) on a refused or failed query-record batch.
const RECORD_BATCH_RETRY_AFTER_SECS: u64 = 2;

/// A long-poll never holds a request longer than this.
const MAX_WAIT_SECS: u64 = 60;
/// Workers seen within this window count as live for fleet checks.
const LIVE_WINDOW_SECS: i64 = 120;

pub fn router(api: WorkerApi) -> Router {
    router_with_ingest_limit(api, MAX_CONCURRENT_RECORD_BATCHES, RECORD_BATCH_QUEUE_WAIT)
}

/// [`router`] with a different history-ingest limit: `max_batches` written at once, each
/// waiting at most `queue_wait` to start.
pub fn router_with_ingest_limit(
    api: WorkerApi,
    max_batches: usize,
    queue_wait: std::time::Duration,
) -> Router {
    Router::new()
        .route("/worker/config", get(get_config))
        .route("/worker/config/wait", get(wait_config))
        .route("/worker/register", post(report))
        .route("/worker/heartbeat", post(report))
        .route("/worker/capacity/acquire", post(capacity_acquire))
        .route("/worker/capacity/release", post(capacity_release))
        .route(
            "/worker/capacity/release-batch",
            post(capacity_release_batch),
        )
        .route("/worker/capacity/quota", post(capacity_quota))
        .route(
            "/worker/capacity/quota/release",
            post(capacity_quota_release),
        )
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
            ingest_slots: tokio::sync::Semaphore::new(max_batches.max(1)),
            ingest_queue_wait: queue_wait,
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
    /// Bounds concurrent query-record batch writes (backpressure for history ingest).
    ingest_slots: tokio::sync::Semaphore,
    ingest_queue_wait: std::time::Duration,
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

pub(crate) fn authorized(headers: &HeaderMap, secret: &str) -> bool {
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
    // Backpressure: a few batches are written at once and others wait briefly. When
    // ingest falls behind, refuse with 503; the worker keeps the batch spooled and retries,
    // so its queries never wait on this.
    let _slot = match tokio::time::timeout(st.ingest_queue_wait, st.ingest_slots.acquire()).await {
        Ok(Ok(permit)) => permit,
        _ => {
            tracing::warn!(
                worker = %batch.worker,
                records = batch.records.len(),
                "Query history ingest is behind; asking the worker to retry"
            );
            return retry_later("query history ingest is busy, retry later".to_string());
        }
    };
    let accepted = batch.records.len();
    let worker = batch.worker;
    let records: Vec<_> = batch
        .records
        .into_iter()
        .map(|mut record| {
            record.worker = Some(worker.clone());
            record
        })
        .collect();
    // One write for the whole batch. Retried batches repeat records; the store keeps the
    // first per proxy query id and counts each once in digest stats.
    if let Err(e) = store.record_queries(records).await {
        return retry_later(e.to_string());
    }
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "accepted": accepted })),
    )
        .into_response()
}

/// `503` with `Retry-After`: the worker keeps the batch and sends it again.
fn retry_later(message: String) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(
            header::RETRY_AFTER,
            HeaderValue::from(RECORD_BATCH_RETRY_AFTER_SECS),
        )],
        message,
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
pub struct ReleaseBatchRequest {
    pub leases: Vec<ReleaseRequest>,
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

/// Authorize the worker and return the quota store, or 404 when this control plane has
/// no quotas (`capacity.mode: leases`): the worker then keeps per-query leases.
#[allow(clippy::result_large_err)]
fn quotas(
    st: &WorkerApiState,
    headers: &HeaderMap,
) -> Result<Arc<dyn queryflux_persistence::quota::QuotaStore>, Response> {
    if !authorized(headers, &st.api.shared_secret) {
        return Err(unauthorized());
    }
    st.api.quotas.clone().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "capacity quotas are off on this control plane (capacity.mode: leases)",
        )
            .into_response()
    })
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaReportRequest {
    pub instance_id: String,
    #[serde(default)]
    pub clusters: Vec<queryflux_persistence::quota::DemandReport>,
}

/// A worker's demand report; the response carries its current quotas (capacity quotas).
async fn capacity_quota(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<QuotaReportRequest>,
) -> Response {
    let store = match quotas(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.report_demand(&req.instance_id, &req.clusters).await {
        Ok(Some(resp)) => Json(resp).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "capacity quotas are off").into_response(),
        Err(e) => store_error(e),
    }
}

async fn capacity_quota_release(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<InstanceRequest>,
) -> Response {
    let store = match quotas(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.release_holder(&req.instance_id).await {
        Ok(n) => Json(serde_json::json!({ "released": n })).into_response(),
        Err(e) => store_error(e),
    }
}

/// Releases from one worker, sent together (`BatchedReleaseStore` on the worker).
async fn capacity_release_batch(
    State(st): State<Arc<WorkerApiState>>,
    headers: HeaderMap,
    Json(req): Json<ReleaseBatchRequest>,
) -> Response {
    let store = match coordination(&st, &headers) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let leases: Vec<(String, String)> = req
        .leases
        .into_iter()
        .map(|l| (l.cluster, l.query_id))
        .collect();
    match store.release_many(&leases).await {
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
            translation: None,
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
            quotas: None,
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
            quotas: None,
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
            quotas: None,
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
            quotas: None,
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

    /// A history store whose batch writes wait until the test opens the gate.
    struct GatedHistory {
        entered: tokio::sync::Notify,
        gate: tokio::sync::Semaphore,
        batches: std::sync::Mutex<Vec<usize>>,
    }

    #[async_trait::async_trait]
    impl MetricsStore for GatedHistory {
        async fn record_query(
            &self,
            _: queryflux_persistence::QueryRecord,
        ) -> queryflux_core::error::Result<()> {
            unreachable!("ingest writes whole batches")
        }
        async fn record_queries(
            &self,
            records: Vec<queryflux_persistence::QueryRecord>,
        ) -> queryflux_core::error::Result<()> {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
            self.batches.lock().unwrap().push(records.len());
            Ok(())
        }
        async fn record_cluster_snapshot(
            &self,
            _: queryflux_persistence::ClusterSnapshot,
        ) -> queryflux_core::error::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn busy_history_ingest_answers_503_with_retry_after() {
        let store = Arc::new(GatedHistory {
            entered: tokio::sync::Notify::new(),
            gate: tokio::sync::Semaphore::new(0),
            batches: Default::default(),
        });
        let app = router_with_ingest_limit(
            WorkerApi {
                shared_secret: "s3cret".into(),
                snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
                registry: None,
                changes: None,
                coordination: None,
                history: Some(store.clone()),
                quotas: None,
            },
            1,
            std::time::Duration::from_millis(50),
        );
        let record = |id: &str| {
            serde_json::json!({
                "proxyQueryId": id, "clusterGroup": "g", "clusterName": "c",
                "engineType": "duckDb", "frontendProtocol": "trinoHttp",
                "sourceDialect": "trino", "targetDialect": "duckDb",
                "wasRewritten": false, "wasTranslated": false, "status": "success",
                "queueDurationMs": 0, "executionDurationMs": 1,
                "createdAt": "2026-10-14T00:00:00Z"
            })
        };
        let post = |ids: &[&str]| {
            let batch = serde_json::json!({
                "schemaVersion": 1, "worker": "w",
                "records": ids.iter().map(|id| record(id)).collect::<Vec<_>>()
            });
            Request::post("/worker/query-records")
                .header(header::AUTHORIZATION, "Bearer s3cret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(batch.to_string()))
                .unwrap()
        };

        // The first batch takes the only write slot and holds it.
        let entered = store.entered.notified();
        let first = tokio::spawn(app.clone().oneshot(post(&["a", "b", "c"])));
        entered.await;

        let busy = app.clone().oneshot(post(&["d"])).await.unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            busy.headers().get(header::RETRY_AFTER).unwrap(),
            &RECORD_BATCH_RETRY_AFTER_SECS.to_string()
        );

        store.gate.add_permits(2);
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::ACCEPTED);
        let retried = app.oneshot(post(&["d"])).await.unwrap();
        assert_eq!(retried.status(), StatusCode::ACCEPTED);
        assert_eq!(
            *store.batches.lock().unwrap(),
            vec![3, 1],
            "each batch is one write"
        );
    }
}
