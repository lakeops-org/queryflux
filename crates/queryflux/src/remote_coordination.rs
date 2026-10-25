//! Coordination for a worker with `coordination.mode: controlPlane` (#255): capacity leases,
//! sweep locks and engine running counts go to the control plane's `/worker/capacity/*` and
//! `/worker/sweep-lock/*` endpoints, so one `maxRunningQueries` holds across the control
//! plane and every worker. The worker never talks to the database.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::config::{OnCoordinationUnavailable, UpstreamConfig};
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_persistence::{BackendCapabilities, CapacityStore, SweepCoordinator, SweepGuard};
use serde::Deserialize;
use serde_json::json;

/// Requests sit on the admission path, so they get a short timeout: a slow control plane
/// is treated as an unavailable one.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Longest capacity wait held on the control plane in one request.
const MAX_REMOTE_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

pub(crate) struct RemoteCoordinationStore {
    client: reqwest::Client,
    base: String,
    bearer: crate::bearer::Bearer,
    on_unavailable: OnCoordinationUnavailable,
    /// Cleared when the control plane answers 404 to a batch release (a version before
    /// `/worker/capacity/release-batch`); releases are then sent one by one.
    batch_release: std::sync::atomic::AtomicBool,
    /// Cleared when the control plane answers 404 to `/worker/capacity/wait` (a version
    /// before it); waits then fall back to sleeping.
    wait_endpoint: std::sync::atomic::AtomicBool,
    /// Put before every holder (`instanceId`) sent upstream. A hub sets
    /// `<site>/`, so `worker-0` at two sites holds separate leases and quotas.
    holder_prefix: String,
}

impl RemoteCoordinationStore {
    pub(crate) fn new(
        control_plane: &UpstreamConfig,
        bearer: crate::bearer::Bearer,
        on_unavailable: OnCoordinationUnavailable,
    ) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("reqwest client"),
            base: control_plane.url.trim_end_matches('/').to_string(),
            bearer,
            on_unavailable,
            batch_release: std::sync::atomic::AtomicBool::new(true),
            wait_endpoint: std::sync::atomic::AtomicBool::new(true),
            holder_prefix: String::new(),
        }
    }

    pub(crate) fn with_holder_prefix(mut self, prefix: String) -> Self {
        self.holder_prefix = prefix;
        self
    }

    fn holder(&self, instance_id: &str) -> String {
        format!("{}{instance_id}", self.holder_prefix)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let resp = req
            .bearer_auth(self.bearer.get())
            .send()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("control plane unreachable: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(QueryFluxError::Persistence(format!(
            "control plane coordination returned {status}: {body}"
        )))
    }

    async fn post<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<T> {
        let resp = self
            .send(self.client.post(format!("{}{path}", self.base)).json(&body))
            .await?;
        resp.json()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("control plane response: {e}")))
    }

    async fn post_empty(&self, path: &str, body: serde_json::Value) -> Result<()> {
        self.send(self.client.post(format!("{}{path}", self.base)).json(&body))
            .await
            .map(|_| ())
    }

    async fn get_count(&self, path: &str, cluster: &str) -> Result<u64> {
        let resp = self
            .send(
                self.client
                    .get(format!("{}{path}", self.base))
                    .query(&[("cluster", cluster)]),
            )
            .await?;
        let v: Count = resp
            .json()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("control plane response: {e}")))?;
        Ok(v.count)
    }
}

#[derive(Deserialize)]
struct Count {
    count: u64,
}

#[derive(Deserialize)]
struct Granted {
    granted: bool,
}

#[derive(Deserialize)]
struct Renewed {
    renewed: u64,
}

#[derive(Deserialize)]
struct Released {
    released: u64,
}

#[derive(Deserialize)]
struct LockToken {
    token: Option<String>,
}

#[async_trait]
impl CapacityStore for RemoteCoordinationStore {
    async fn try_acquire(
        &self,
        cluster_name: &str,
        max_running_queries: u64,
        instance_id: &str,
        query_id: &str,
    ) -> Result<bool> {
        let res: Result<Granted> = self
            .post(
                "/worker/capacity/acquire",
                json!({
                    "cluster": cluster_name,
                    "maxRunningQueries": max_running_queries,
                    "instanceId": self.holder(instance_id),
                    "queryId": query_id,
                }),
            )
            .await;
        match (res, self.on_unavailable) {
            (Ok(g), _) => Ok(g.granted),
            // The local slot was already taken, so this worker's own limit still applies.
            (Err(e), OnCoordinationUnavailable::LocalLimits) => {
                tracing::warn!(
                    cluster = %cluster_name,
                    "Control plane coordination unavailable, admitting on this worker's \
                     limit (onUnavailable: localLimits): {e}"
                );
                Ok(true)
            }
            (Err(e), OnCoordinationUnavailable::FailClosed) => Err(e),
        }
    }

    async fn release(&self, cluster_name: &str, query_id: &str) -> Result<()> {
        self.post_empty(
            "/worker/capacity/release",
            json!({ "cluster": cluster_name, "queryId": query_id }),
        )
        .await
    }

    /// Long-poll the control plane until it hands this query a slot, for at most
    /// [`MAX_REMOTE_WAIT`] so the request stays well inside its timeout.
    async fn wait_for_grant(
        &self,
        cluster_name: &str,
        query_id: &str,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        use std::sync::atomic::Ordering;
        let timeout = timeout.min(MAX_REMOTE_WAIT);
        if !self.wait_endpoint.load(Ordering::Relaxed) {
            tokio::time::sleep(timeout).await;
            return Ok(false);
        }
        let resp = self
            .client
            .post(format!("{}/worker/capacity/wait", self.base))
            .json(&json!({
                "cluster": cluster_name,
                "queryId": query_id,
                "waitMs": timeout.as_millis() as u64,
            }))
            .bearer_auth(self.bearer.get())
            .send()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("control plane unreachable: {e}")))?;
        match resp.status() {
            s if s.is_success() => resp
                .json::<Granted>()
                .await
                .map(|g| g.granted)
                .map_err(|e| QueryFluxError::Persistence(format!("control plane response: {e}"))),
            reqwest::StatusCode::NOT_FOUND => {
                tracing::info!("Control plane has no capacity wait endpoint; retrying on a timer");
                self.wait_endpoint.store(false, Ordering::Relaxed);
                tokio::time::sleep(timeout).await;
                Ok(false)
            }
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(QueryFluxError::Persistence(format!(
                    "control plane capacity wait returned {s}: {body}"
                )))
            }
        }
    }

    async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
        use std::sync::atomic::Ordering;
        if leases.is_empty() {
            return Ok(());
        }
        if self.batch_release.load(Ordering::Relaxed) {
            let body = json!({
                "leases": leases
                    .iter()
                    .map(|(cluster, query_id)| json!({ "cluster": cluster, "queryId": query_id }))
                    .collect::<Vec<_>>(),
            });
            let resp = self
                .client
                .post(format!("{}/worker/capacity/release-batch", self.base))
                .json(&body)
                .bearer_auth(self.bearer.get())
                .send()
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("control plane unreachable: {e}"))
                })?;
            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }
            if status != reqwest::StatusCode::NOT_FOUND {
                let body = resp.text().await.unwrap_or_default();
                return Err(QueryFluxError::Persistence(format!(
                    "control plane coordination returned {status}: {body}"
                )));
            }
            tracing::info!("Control plane has no batch release endpoint; releasing one by one");
            self.batch_release.store(false, Ordering::Relaxed);
        }
        for (cluster, query_id) in leases {
            self.release(cluster, query_id).await?;
        }
        Ok(())
    }

    async fn heartbeat(&self, instance_id: &str) -> Result<u64> {
        let r: Renewed = self
            .post(
                "/worker/capacity/heartbeat",
                json!({ "instanceId": self.holder(instance_id) }),
            )
            .await?;
        Ok(r.renewed)
    }

    /// The control plane expires stale leases, including those of workers that died.
    async fn expire_stale(&self, _cutoff: DateTime<Utc>) -> Result<u64> {
        Ok(0)
    }

    async fn active_count(&self, cluster_name: &str) -> Result<u64> {
        self.get_count("/worker/capacity/running", cluster_name)
            .await
    }

    async fn publish_running_count(&self, cluster_name: &str, count: u64) -> Result<()> {
        self.post_empty(
            "/worker/capacity/running",
            json!({ "cluster": cluster_name, "count": count }),
        )
        .await
    }

    async fn active_lease_count(&self, cluster_name: &str) -> Result<u64> {
        self.get_count("/worker/capacity/leases", cluster_name)
            .await
    }

    async fn release_all_for_instance(&self, instance_id: &str) -> Result<u64> {
        let r: Released = self
            .post(
                "/worker/capacity/release-all",
                json!({ "instanceId": self.holder(instance_id) }),
            )
            .await?;
        Ok(r.released)
    }
}

/// Quotas through the control plane's `POST /worker/capacity/quota`. A 404 means it has
/// none (an older version, or `capacity.mode: leases`), and the worker keeps using
/// per-query leases.
#[async_trait]
impl queryflux_persistence::quota::QuotaStore for RemoteCoordinationStore {
    async fn report_demand(
        &self,
        instance_id: &str,
        reports: &[queryflux_persistence::quota::DemandReport],
    ) -> Result<Option<queryflux_persistence::quota::QuotaResponse>> {
        let resp = self
            .client
            .post(format!("{}/worker/capacity/quota", self.base))
            .json(&json!({ "instanceId": self.holder(instance_id), "clusters": reports }))
            .bearer_auth(self.bearer.get())
            .send()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("control plane unreachable: {e}")))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Persistence(format!(
                "control plane quota report returned {status}: {body}"
            )));
        }
        resp.json()
            .await
            .map(Some)
            .map_err(|e| QueryFluxError::Persistence(format!("control plane response: {e}")))
    }

    async fn release_holder(&self, instance_id: &str) -> Result<u64> {
        let r: Released = self
            .post(
                "/worker/capacity/quota/release",
                json!({ "instanceId": self.holder(instance_id) }),
            )
            .await?;
        Ok(r.released)
    }
}

#[async_trait]
impl SweepCoordinator for RemoteCoordinationStore {
    async fn try_sweep_lock(&self, name: &str) -> Result<Option<Box<dyn SweepGuard>>> {
        let path = format!("/worker/sweep-lock/{name}");
        let t: LockToken = self.post(&path, json!({})).await?;
        Ok(t.token.map(|token| {
            Box::new(RemoteSweepGuard {
                client: self.client.clone(),
                url: format!("{}{path}/{token}", self.base),
                bearer: self.bearer.clone(),
            }) as Box<dyn SweepGuard>
        }))
    }
}

impl BackendCapabilities for RemoteCoordinationStore {
    fn supports_distributed_coordination(&self) -> bool {
        true
    }
}

/// A sweep lock the control plane holds for this worker. If the release is lost, the
/// control plane drops the lock after a timeout.
struct RemoteSweepGuard {
    client: reqwest::Client,
    url: String,
    bearer: crate::bearer::Bearer,
}

#[async_trait]
impl SweepGuard for RemoteSweepGuard {
    async fn release(self: Box<Self>) {
        if let Err(e) = self
            .client
            .delete(&self.url)
            .bearer_auth(self.bearer.get())
            .send()
            .await
        {
            tracing::debug!("Sweep lock release failed (control plane drops it later): {e}");
        }
    }
}

/// The store as the shared handle `main` passes around.
pub(crate) fn remote_coordination(
    control_plane: &UpstreamConfig,
    bearer: crate::bearer::Bearer,
    on_unavailable: OnCoordinationUnavailable,
) -> Arc<RemoteCoordinationStore> {
    Arc::new(RemoteCoordinationStore::new(
        control_plane,
        bearer,
        on_unavailable,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_frontend::worker_api::{router, WorkerApi};

    /// A lease table like Postgres's, enough to show the limit is shared. (The in-memory
    /// store grants every lease: a lone process limits with its own counters.)
    #[derive(Default)]
    struct Leases {
        held: std::sync::Mutex<Vec<(String, String, String)>>,
        locked: Arc<std::sync::Mutex<bool>>,
    }

    #[async_trait]
    impl CapacityStore for Leases {
        async fn try_acquire(&self, c: &str, max: u64, i: &str, q: &str) -> Result<bool> {
            let mut held = self.held.lock().unwrap();
            if held.iter().filter(|(hc, _, _)| hc == c).count() as u64 >= max {
                return Ok(false);
            }
            held.push((c.into(), i.into(), q.into()));
            Ok(true)
        }
        async fn release(&self, c: &str, q: &str) -> Result<()> {
            self.held
                .lock()
                .unwrap()
                .retain(|(hc, _, hq)| !(hc == c && hq == q));
            Ok(())
        }
        async fn heartbeat(&self, _: &str) -> Result<u64> {
            Ok(0)
        }
        async fn expire_stale(&self, _: DateTime<Utc>) -> Result<u64> {
            Ok(0)
        }
        async fn active_count(&self, _: &str) -> Result<u64> {
            Ok(0)
        }
        async fn publish_running_count(&self, _: &str, _: u64) -> Result<()> {
            Ok(())
        }
        async fn active_lease_count(&self, c: &str) -> Result<u64> {
            let held = self.held.lock().unwrap();
            Ok(held.iter().filter(|(hc, _, _)| hc == c).count() as u64)
        }
        async fn release_all_for_instance(&self, i: &str) -> Result<u64> {
            let mut held = self.held.lock().unwrap();
            let before = held.len();
            held.retain(|(_, hi, _)| hi != i);
            Ok((before - held.len()) as u64)
        }
    }

    struct Unlock(Arc<std::sync::Mutex<bool>>);

    #[async_trait]
    impl SweepGuard for Unlock {
        async fn release(self: Box<Self>) {
            *self.0.lock().unwrap() = false;
        }
    }

    #[async_trait]
    impl SweepCoordinator for Leases {
        async fn try_sweep_lock(&self, _: &str) -> Result<Option<Box<dyn SweepGuard>>> {
            let mut locked = self.locked.lock().unwrap();
            if *locked {
                return Ok(None);
            }
            *locked = true;
            Ok(Some(Box::new(Unlock(self.locked.clone()))))
        }
    }

    impl BackendCapabilities for Leases {
        fn supports_distributed_coordination(&self) -> bool {
            true
        }
    }

    fn leases() -> Option<Arc<dyn queryflux_persistence::CoordinationStore>> {
        Some(Arc::new(Leases::default()))
    }

    async fn control_plane(
        coordination: Option<Arc<dyn queryflux_persistence::CoordinationStore>>,
    ) -> String {
        let app = router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
            registry: None,
            changes: None,
            coordination,
            history: None,
            quotas: None,
            sites: Vec::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn client(url: &str, on_unavailable: OnCoordinationUnavailable) -> RemoteCoordinationStore {
        let cp: UpstreamConfig = serde_yaml::from_str(&format!("url: {url}\n")).unwrap();
        RemoteCoordinationStore::new(&cp, crate::bearer::Bearer::secret("s3cret"), on_unavailable)
    }

    #[tokio::test]
    async fn two_workers_share_one_limit() {
        let url = control_plane(leases()).await;
        let a = client(&url, OnCoordinationUnavailable::FailClosed);
        let b = client(&url, OnCoordinationUnavailable::FailClosed);

        assert!(a.try_acquire("c", 1, "wa", "q1").await.unwrap());
        assert!(!b.try_acquire("c", 1, "wb", "q2").await.unwrap());
        assert_eq!(b.active_lease_count("c").await.unwrap(), 1);

        a.release("c", "q1").await.unwrap();
        assert!(b.try_acquire("c", 1, "wb", "q2").await.unwrap());
        assert_eq!(b.release_all_for_instance("wb").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn same_worker_name_at_two_sites_holds_separate_leases() {
        let url = control_plane(leases()).await;
        let a =
            client(&url, OnCoordinationUnavailable::FailClosed).with_holder_prefix("dc1/".into());
        let b =
            client(&url, OnCoordinationUnavailable::FailClosed).with_holder_prefix("dc2/".into());

        assert!(a.try_acquire("c", 2, "worker-0", "q1").await.unwrap());
        assert!(b.try_acquire("c", 2, "worker-0", "q2").await.unwrap());
        // dc1's worker-0 shutting down frees only its own lease.
        assert_eq!(a.release_all_for_instance("worker-0").await.unwrap(), 1);
        assert_eq!(b.active_lease_count("c").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn sweep_lock_is_exclusive_until_released() {
        let url = control_plane(leases()).await;
        let a = client(&url, OnCoordinationUnavailable::FailClosed);
        let b = client(&url, OnCoordinationUnavailable::FailClosed);

        let held = a.try_sweep_lock("engine-reconcile").await.unwrap();
        assert!(held.is_some());
        assert!(b
            .try_sweep_lock("engine-reconcile")
            .await
            .unwrap()
            .is_none());
        held.unwrap().release().await;
        assert!(b
            .try_sweep_lock("engine-reconcile")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn unavailable_control_plane_follows_on_unavailable() {
        // Nothing listens on this port.
        let url = "http://127.0.0.1:9";
        let closed = client(url, OnCoordinationUnavailable::FailClosed);
        assert!(closed.try_acquire("c", 1, "w", "q").await.is_err());
        let local = client(url, OnCoordinationUnavailable::LocalLimits);
        assert!(local.try_acquire("c", 1, "w", "q").await.unwrap());
    }

    #[tokio::test]
    async fn releases_go_in_one_batch_request() {
        let store = Arc::new(Leases::default());
        let url = control_plane(Some(store.clone())).await;
        let a = client(&url, OnCoordinationUnavailable::FailClosed);
        for q in ["q1", "q2", "q3"] {
            assert!(a.try_acquire("c", 3, "wa", q).await.unwrap());
        }
        a.release_many(&[("c".into(), "q1".into()), ("c".into(), "q3".into())])
            .await
            .unwrap();
        assert_eq!(a.active_lease_count("c").await.unwrap(), 1);
        assert!(a.batch_release.load(std::sync::atomic::Ordering::Relaxed));
    }

    /// A control plane from before `/worker/capacity/release-batch` answers 404: the
    /// worker falls back to one release per lease, and stops trying the batch endpoint.
    #[tokio::test]
    async fn batch_release_falls_back_on_an_older_control_plane() {
        use axum::{routing::post, Json, Router};
        let released: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let app = Router::new().route(
            "/worker/capacity/release",
            post({
                let released = released.clone();
                move |Json(v): Json<serde_json::Value>| {
                    let released = released.clone();
                    async move {
                        released
                            .lock()
                            .unwrap()
                            .push(v["queryId"].as_str().unwrap().to_string());
                        axum::http::StatusCode::NO_CONTENT
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let a = client(
            &format!("http://{addr}"),
            OnCoordinationUnavailable::FailClosed,
        );
        a.release_many(&[("c".into(), "q1".into()), ("c".into(), "q2".into())])
            .await
            .unwrap();
        a.release_many(&[("c".into(), "q3".into())]).await.unwrap();
        assert_eq!(*released.lock().unwrap(), vec!["q1", "q2", "q3"]);
        assert!(!a.batch_release.load(std::sync::atomic::Ordering::Relaxed));
    }

    /// Grants each reported cluster a fixed quota.
    struct FixedQuotas;

    #[async_trait]
    impl queryflux_persistence::quota::QuotaStore for FixedQuotas {
        async fn report_demand(
            &self,
            _: &str,
            reports: &[queryflux_persistence::quota::DemandReport],
        ) -> Result<Option<queryflux_persistence::quota::QuotaResponse>> {
            Ok(Some(queryflux_persistence::quota::QuotaResponse {
                grants: reports
                    .iter()
                    .map(|r| queryflux_persistence::quota::QuotaGrant {
                        cluster: r.cluster.clone(),
                        quota: 4,
                        seq: 7,
                    })
                    .collect(),
                lease_secs: 15,
                report_interval_ms: 750,
            }))
        }
        async fn release_holder(&self, _: &str) -> Result<u64> {
            Ok(2)
        }
    }

    async fn control_plane_with_quotas(
        quotas: Option<Arc<dyn queryflux_persistence::quota::QuotaStore>>,
    ) -> String {
        let app = router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
            registry: None,
            changes: None,
            coordination: leases(),
            history: None,
            quotas,
            sites: Vec::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn quota_reports_return_grants_when_the_control_plane_has_quotas() {
        use queryflux_persistence::quota::{DemandReport, QuotaStore};
        let url = control_plane_with_quotas(Some(Arc::new(FixedQuotas))).await;
        let w = client(&url, OnCoordinationUnavailable::FailClosed);
        let resp = w
            .report_demand(
                "w1",
                &[DemandReport {
                    cluster: "c".into(),
                    running: 1,
                    waiting: 2,
                    seen_seq: 0,
                    max_running_queries: 10,
                }],
            )
            .await
            .unwrap()
            .expect("quotas on");
        assert_eq!(resp.grants[0].quota, 4);
        assert_eq!(resp.grants[0].seq, 7);
        assert_eq!(resp.report_interval_ms, 750);
        assert_eq!(w.release_holder("w1").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn quota_reports_are_declined_when_quotas_are_off() {
        use queryflux_persistence::quota::QuotaStore;
        let url = control_plane_with_quotas(None).await;
        let w = client(&url, OnCoordinationUnavailable::FailClosed);
        assert!(w.report_demand("w1", &[]).await.unwrap().is_none());
    }

    /// A control plane on Postgres with quotas on, and two workers: each turns quotas on
    /// by itself, gets a share after an allocator pass, admits within it without leases,
    /// and the two together never exceed the limit. Needs `DATABASE_URL`.
    #[tokio::test]
    #[ignore]
    async fn quotas_end_to_end_with_postgres() {
        use queryflux_persistence::quota::{AllocatorPolicy, QuotaAllocator};
        use queryflux_persistence::quota_admission::QuotaAdmission;
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let pg = Arc::new(
            queryflux_persistence::postgres::PostgresStore::connect(&url)
                .await
                .unwrap(),
        );
        pg.migrate().await.unwrap();
        let cluster = format!("e2e-{}", uuid::Uuid::new_v4());
        let app = router(WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
            registry: None,
            changes: None,
            coordination: Some(pg.clone()),
            history: None,
            quotas: Some(pg.clone()),
            sites: Vec::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cp = format!("http://{addr}");

        let metrics: Arc<dyn queryflux_persistence::MetricsStore> =
            Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let workers: Vec<Arc<QuotaAdmission>> = (0..2)
            .map(|i| {
                let remote = Arc::new(client(&cp, OnCoordinationUnavailable::FailClosed));
                let qa = QuotaAdmission::new(
                    remote.clone(),
                    remote,
                    format!("{cluster}-w{i}"),
                    metrics.clone(),
                );
                qa.follow_source();
                qa
            })
            .collect();
        // Each worker probes, finds quotas on, and starts reporting.
        for _ in 0..50 {
            if workers.iter().all(|w| w.enabled()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(workers.iter().all(|w| w.enabled()));

        const MAX: u64 = 40;
        // Demand: 30 queries each, denied at first (no grant yet, the reserve is 6).
        let mut held = [Vec::new(), Vec::new()];
        for (i, w) in workers.iter().enumerate() {
            for q in 0..30 {
                let id = format!("{cluster}-w{i}-q{q}");
                if w.try_acquire(&cluster, MAX, &format!("{cluster}-w{i}"), &id)
                    .await
                    .unwrap()
                {
                    held[i].push(id);
                }
            }
        }
        // Before any grant, per-query leases may use the whole limit: 40 run as leases,
        // 20 wait. Those queries finish; the waiters stay as demand.
        assert_eq!(pg.active_lease_count(&cluster).await.unwrap(), MAX);
        let mut released = [Vec::new(), Vec::new()];
        for (i, w) in workers.iter().enumerate() {
            for id in held[i].drain(..) {
                w.release(&cluster, &id).await.unwrap();
                released[i].push(id);
            }
        }
        let policy = AllocatorPolicy::default();
        for _ in 0..3 {
            for w in &workers {
                w.report_now().await;
            }
            pg.run_allocation(&policy, metrics.as_ref()).await.unwrap();
        }
        for w in &workers {
            w.report_now().await;
        }
        // The queries denied in the first round retry first (the releases above handed
        // them slots, first come first served), then a new round fills what is left:
        // grants first, then leases.
        let retry: Vec<Vec<String>> = (0..2)
            .map(|i| {
                (0..30)
                    .map(|q| format!("{cluster}-w{i}-q{q}"))
                    .filter(|id| !released[i].contains(id))
                    .collect()
            })
            .collect();
        let mut pending: Vec<Vec<String>> = (0..2)
            .map(|i| {
                retry[i]
                    .iter()
                    .cloned()
                    .chain((0..30).map(|q| format!("{cluster}-w{i}-r{q}")))
                    .collect()
            })
            .collect();
        // Waiting queries keep retrying, as dispatch does; a few passes let slots handed
        // to waiters be claimed and queued releases reach Postgres.
        for _ in 0..10 {
            for (i, w) in workers.iter().enumerate() {
                let mut still = Vec::new();
                for id in pending[i].drain(..) {
                    if w.try_acquire(&cluster, MAX, &format!("{cluster}-w{i}"), &id)
                        .await
                        .unwrap()
                    {
                        held[i].push(id);
                    } else {
                        still.push(id);
                    }
                }
                pending[i] = still;
            }
            let total = (held[0].len() + held[1].len()) as u64;
            assert!(total <= MAX, "admitted {total} > limit {MAX}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // Stop waiting: the rest give up and leave the line.
        for (i, w) in workers.iter().enumerate() {
            for id in pending[i].drain(..) {
                w.release(&cluster, &id).await.unwrap();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let leases = pg.active_lease_count(&cluster).await.unwrap();
        let on_quota: u64 = workers
            .iter()
            .map(|w| w.snapshot(&cluster).unwrap().1)
            .sum();
        let total = (held[0].len() + held[1].len()) as u64;
        assert_eq!(total, on_quota + leases);
        assert!(on_quota > 0, "admissions ran on quota");
        assert!(total <= MAX, "admitted {total} > limit {MAX}");
        assert_eq!(
            total, MAX,
            "demand (60) exceeds the limit, so all of it is used"
        );

        for (i, w) in workers.iter().enumerate() {
            for id in &held[i] {
                w.release(&cluster, id).await.unwrap();
            }
            w.release_all_for_instance(&format!("{cluster}-w{i}"))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn control_plane_without_coordination_is_an_error() {
        let url = control_plane(None).await;
        let a = client(&url, OnCoordinationUnavailable::FailClosed);
        let err = a.try_acquire("c", 1, "w", "q").await.unwrap_err();
        assert!(err.to_string().contains("501"), "{err}");
    }
}
