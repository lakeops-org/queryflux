//! Admission transports: how a simulated worker asks for and gives back a slot.
//!
//! The harness only talks to [`AdmissionBackend`] and [`WorkerAdmission`], so a new
//! admission design (quota assignment, a shared reserve, a counter-row ledger) plugs in as
//! one more implementation and is measured by the same scenarios and the same
//! cap-violation checker.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use queryflux_core::config::PostgresPersistenceConfig;
use queryflux_frontend::worker_api::{self, WorkerApi, WorkerSnapshotFn};
use queryflux_persistence::postgres::PostgresStore;
use queryflux_persistence::{CapacityStore, CoordinationStore};
use serde::Deserialize;
use serde_json::json;

/// A way of admitting queries against shared per-cluster caps.
#[async_trait]
pub trait AdmissionBackend: Send + Sync {
    /// Short name for reports (`store`, `http`, ...).
    fn name(&self) -> &'static str;

    /// The client one simulated worker uses. Called once per worker before the run;
    /// transports that keep state per worker (a local quota, a reserve token) keep it in
    /// the returned handle.
    async fn worker(&self, instance_id: &str) -> anyhow::Result<Arc<dyn WorkerAdmission>>;

    /// Leases currently held on `cluster`, as the shared store sees them. Read through a
    /// separate connection pool so sampling does not compete with admissions. `None` when
    /// the transport has no such notion.
    async fn active_leases(&self, cluster: &str) -> anyhow::Result<Option<u64>>;

    /// Drop whatever the given workers still hold (end of a scenario).
    async fn cleanup(&self, instance_ids: &[String]) -> anyhow::Result<()>;

    /// Stop servers and background tasks the backend started.
    async fn shutdown(&self) {}
}

/// One simulated worker's admission client.
#[async_trait]
pub trait WorkerAdmission: Send + Sync {
    /// Ask for a slot on `cluster` under `cap`. `Ok(false)` means "full, try again".
    /// A transport that can wait for a slot (long-poll) may block here and return `true`.
    async fn try_acquire(&self, cluster: &str, cap: u64, query_id: &str) -> anyhow::Result<bool>;

    /// Give the slot back.
    async fn release(&self, cluster: &str, query_id: &str) -> anyhow::Result<()>;
}

/// Connect a `PostgresStore` the way a control plane would, with `coordination_pool`
/// connections for capacity (`coordinationPoolSize`) and minimal other pools.
pub async fn connect_store(
    database_url: &str,
    coordination_pool: u32,
) -> anyhow::Result<PostgresStore> {
    let conn = PostgresPersistenceConfig {
        query_pool_size: Some(1),
        coordination_pool_size: Some(coordination_pool.max(1)),
        admin_pool_size: Some(1),
        acquire_timeout_secs: Some(30),
        ..Default::default()
    };
    PostgresStore::connect_from_config(database_url, &conn)
        .await
        .map_err(|e| anyhow!("{e}"))
}

async fn observer_leases(observer: &PostgresStore, cluster: &str) -> anyhow::Result<Option<u64>> {
    observer
        .active_lease_count(cluster)
        .await
        .map(Some)
        .map_err(|e| anyhow!("{e}"))
}

async fn observer_cleanup(observer: &PostgresStore, instance_ids: &[String]) -> anyhow::Result<()> {
    for id in instance_ids {
        observer
            .release_all_for_instance(id)
            .await
            .map_err(|e| anyhow!("{e}"))?;
    }
    Ok(())
}

// -- store: CapacityStore on PostgresStore, in process -----------------------------------

/// Calls `CapacityStore` on one shared `PostgresStore`: what the control plane itself does
/// for each worker request, without HTTP.
pub struct StoreBackend {
    store: Arc<PostgresStore>,
    observer: Arc<PostgresStore>,
}

impl StoreBackend {
    pub async fn connect(database_url: &str, coordination_pool: u32) -> anyhow::Result<Self> {
        Ok(Self {
            store: Arc::new(connect_store(database_url, coordination_pool).await?),
            observer: Arc::new(connect_store(database_url, 2).await?),
        })
    }
}

struct StoreWorker {
    store: Arc<PostgresStore>,
    instance_id: String,
}

#[async_trait]
impl AdmissionBackend for StoreBackend {
    fn name(&self) -> &'static str {
        "store"
    }

    async fn worker(&self, instance_id: &str) -> anyhow::Result<Arc<dyn WorkerAdmission>> {
        Ok(Arc::new(StoreWorker {
            store: self.store.clone(),
            instance_id: instance_id.to_string(),
        }))
    }

    async fn active_leases(&self, cluster: &str) -> anyhow::Result<Option<u64>> {
        observer_leases(&self.observer, cluster).await
    }

    async fn cleanup(&self, instance_ids: &[String]) -> anyhow::Result<()> {
        observer_cleanup(&self.observer, instance_ids).await
    }
}

#[async_trait]
impl WorkerAdmission for StoreWorker {
    async fn try_acquire(&self, cluster: &str, cap: u64, query_id: &str) -> anyhow::Result<bool> {
        self.store
            .try_acquire(cluster, cap, &self.instance_id, query_id)
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    async fn release(&self, cluster: &str, query_id: &str) -> anyhow::Result<()> {
        self.store
            .release(cluster, query_id)
            .await
            .map_err(|e| anyhow!("{e}"))
    }
}

// -- http: the control plane's /worker/capacity/* API -----------------------------------

/// Same timeout as the worker's `RemoteCoordinationStore`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Drives `/worker/capacity/*` with the same JSON the worker's `RemoteCoordinationStore`
/// sends (that type is private to the `queryflux` binary). By default the API is served in
/// process on an ephemeral port by the real `worker_api::router`, backed by a
/// `PostgresStore`; `external` points it at a running control plane instead.
pub struct HttpBackend {
    base: String,
    secret: String,
    observer: Arc<PostgresStore>,
    server: Option<tokio::task::JoinHandle<()>>,
}

impl HttpBackend {
    /// Serve the worker API in process on 127.0.0.1:0.
    pub async fn serve(database_url: &str, coordination_pool: u32) -> anyhow::Result<Self> {
        let store = Arc::new(connect_store(database_url, coordination_pool).await?);
        let secret = "queryflux-bench".to_string();
        let snapshot: WorkerSnapshotFn = Arc::new(|| {
            Box::pin(async { Err("config is not served by the capacity benchmark".to_string()) })
        });
        let app = worker_api::router(WorkerApi {
            shared_secret: secret.clone(),
            snapshot,
            registry: None,
            changes: None,
            coordination: Some(store as Arc<dyn CoordinationStore>),
            history: None,
            quotas: None,
            runs: None,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("[capacity] worker API server stopped: {e}");
            }
        });
        Ok(Self {
            base: format!("http://{addr}"),
            secret,
            observer: Arc::new(connect_store(database_url, 2).await?),
            server: Some(server),
        })
    }

    /// Use a control plane that is already running. `database_url` must be its database,
    /// for lease sampling and cleanup.
    pub async fn external(url: &str, secret: &str, database_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            base: url.trim_end_matches('/').to_string(),
            secret: secret.to_string(),
            observer: Arc::new(connect_store(database_url, 2).await?),
            server: None,
        })
    }
}

#[async_trait]
impl AdmissionBackend for HttpBackend {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn worker(&self, instance_id: &str) -> anyhow::Result<Arc<dyn WorkerAdmission>> {
        // One client (and connection pool) per worker, as each worker process has its own.
        Ok(Arc::new(HttpWorker {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .context("reqwest client")?,
            base: self.base.clone(),
            secret: self.secret.clone(),
            instance_id: instance_id.to_string(),
        }))
    }

    async fn active_leases(&self, cluster: &str) -> anyhow::Result<Option<u64>> {
        observer_leases(&self.observer, cluster).await
    }

    async fn cleanup(&self, instance_ids: &[String]) -> anyhow::Result<()> {
        observer_cleanup(&self.observer, instance_ids).await
    }

    async fn shutdown(&self) {
        if let Some(s) = &self.server {
            s.abort();
        }
    }
}

struct HttpWorker {
    client: reqwest::Client,
    base: String,
    secret: String,
    instance_id: String,
}

#[derive(Deserialize)]
struct Granted {
    granted: bool,
}

impl HttpWorker {
    async fn post(&self, path: &str, body: serde_json::Value) -> anyhow::Result<reqwest::Response> {
        let resp = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.secret)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("control plane unreachable: {e}"))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(anyhow!(
            "control plane coordination returned {status}: {body}"
        ))
    }
}

#[async_trait]
impl WorkerAdmission for HttpWorker {
    async fn try_acquire(&self, cluster: &str, cap: u64, query_id: &str) -> anyhow::Result<bool> {
        let g: Granted = self
            .post(
                "/worker/capacity/acquire",
                json!({
                    "cluster": cluster,
                    "maxRunningQueries": cap,
                    "instanceId": self.instance_id,
                    "queryId": query_id,
                }),
            )
            .await?
            .json()
            .await
            .context("control plane response")?;
        Ok(g.granted)
    }

    async fn release(&self, cluster: &str, query_id: &str) -> anyhow::Result<()> {
        self.post(
            "/worker/capacity/release",
            json!({ "cluster": cluster, "queryId": query_id }),
        )
        .await
        .map(|_| ())
    }
}

// -- in-memory backends for the harness's own tests --------------------------------------

#[cfg(test)]
pub mod fake {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    type Held = Arc<Mutex<HashMap<String, HashSet<String>>>>;

    /// Leases per cluster keyed by query id, like the lease table. Honors caps, or ignores
    /// them when `broken`.
    pub struct FakeBackend {
        pub held: Held,
        pub broken: bool,
        pub latency: Duration,
    }

    impl FakeBackend {
        pub fn new(broken: bool) -> Self {
            Self {
                held: Arc::default(),
                broken,
                latency: Duration::from_micros(200),
            }
        }
    }

    struct FakeWorker {
        held: Held,
        broken: bool,
        latency: Duration,
    }

    #[async_trait]
    impl AdmissionBackend for FakeBackend {
        fn name(&self) -> &'static str {
            "fake"
        }
        async fn worker(&self, _: &str) -> anyhow::Result<Arc<dyn WorkerAdmission>> {
            Ok(Arc::new(FakeWorker {
                held: self.held.clone(),
                broken: self.broken,
                latency: self.latency,
            }))
        }
        async fn active_leases(&self, cluster: &str) -> anyhow::Result<Option<u64>> {
            let held = self.held.lock().unwrap();
            Ok(Some(held.get(cluster).map_or(0, |q| q.len() as u64)))
        }
        async fn cleanup(&self, _: &[String]) -> anyhow::Result<()> {
            self.held.lock().unwrap().clear();
            Ok(())
        }
    }

    #[async_trait]
    impl WorkerAdmission for FakeWorker {
        async fn try_acquire(&self, cluster: &str, cap: u64, qid: &str) -> anyhow::Result<bool> {
            tokio::time::sleep(self.latency).await;
            let mut held = self.held.lock().unwrap();
            let leases = held.entry(cluster.to_string()).or_default();
            if self.broken || (leases.len() as u64) < cap {
                // Like ON CONFLICT DO NOTHING: an existing lease for the id is not a grant.
                Ok(leases.insert(qid.to_string()))
            } else {
                Ok(false)
            }
        }
        async fn release(&self, cluster: &str, qid: &str) -> anyhow::Result<()> {
            tokio::time::sleep(self.latency).await;
            if let Some(leases) = self.held.lock().unwrap().get_mut(cluster) {
                leases.remove(qid);
            }
            Ok(())
        }
    }
}
