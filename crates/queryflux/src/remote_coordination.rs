//! Coordination for a worker with `coordination.mode: controlPlane` (#255): capacity leases,
//! sweep locks and engine running counts go to the control plane's `/worker/capacity/*` and
//! `/worker/sweep-lock/*` endpoints, so one `maxRunningQueries` holds across the control
//! plane and every worker. The worker never talks to the database.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::config::{ControlPlaneClientConfig, OnCoordinationUnavailable};
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_persistence::{BackendCapabilities, CapacityStore, SweepCoordinator, SweepGuard};
use serde::Deserialize;
use serde_json::json;

/// Requests sit on the admission path, so they get a short timeout: a slow control plane
/// is treated as an unavailable one.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) struct RemoteCoordinationStore {
    client: reqwest::Client,
    base: String,
    bearer: String,
    on_unavailable: OnCoordinationUnavailable,
}

impl RemoteCoordinationStore {
    pub(crate) fn new(
        control_plane: &ControlPlaneClientConfig,
        on_unavailable: OnCoordinationUnavailable,
    ) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("reqwest client"),
            base: control_plane.url.trim_end_matches('/').to_string(),
            bearer: control_plane.auth.bearer().to_string(),
            on_unavailable,
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let resp =
            req.bearer_auth(&self.bearer).send().await.map_err(|e| {
                QueryFluxError::Persistence(format!("control plane unreachable: {e}"))
            })?;
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
                    "instanceId": instance_id,
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

    async fn heartbeat(&self, instance_id: &str) -> Result<u64> {
        let r: Renewed = self
            .post(
                "/worker/capacity/heartbeat",
                json!({ "instanceId": instance_id }),
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
                json!({ "instanceId": instance_id }),
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
    bearer: String,
}

#[async_trait]
impl SweepGuard for RemoteSweepGuard {
    async fn release(self: Box<Self>) {
        if let Err(e) = self
            .client
            .delete(&self.url)
            .bearer_auth(&self.bearer)
            .send()
            .await
        {
            tracing::debug!("Sweep lock release failed (control plane drops it later): {e}");
        }
    }
}

/// The store as the shared handle `main` passes around.
pub(crate) fn remote_coordination(
    control_plane: &ControlPlaneClientConfig,
    on_unavailable: OnCoordinationUnavailable,
) -> Arc<dyn queryflux_persistence::CoordinationStore> {
    Arc::new(RemoteCoordinationStore::new(control_plane, on_unavailable))
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
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn client(url: &str, on_unavailable: OnCoordinationUnavailable) -> RemoteCoordinationStore {
        let cp: ControlPlaneClientConfig = serde_yaml::from_str(&format!(
            "url: {url}\nauth:\n  mode: sharedSecret\n  sharedSecret: s3cret\n"
        ))
        .unwrap();
        RemoteCoordinationStore::new(&cp, on_unavailable)
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
    async fn control_plane_without_coordination_is_an_error() {
        let url = control_plane(None).await;
        let a = client(&url, OnCoordinationUnavailable::FailClosed);
        let err = a.try_acquire("c", 1, "w", "q").await.unwrap_err();
        assert!(err.to_string().contains("501"), "{err}");
    }
}
