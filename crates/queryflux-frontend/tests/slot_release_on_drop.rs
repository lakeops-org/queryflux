//! Regression for #292: a dispatch future dropped while waiting on the global
//! capacity lease must still release the local cluster slot.
//!
//! Also: when the lease acquire fails outright (timeout, lost connection), the
//! backend may still have committed the lease, so dispatch must release it; a plain
//! "capacity full" denial took no lease and must not send a release.
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_auth::{
    AllowAllAuthorization, AuthContext, AuthProvider, AuthorizationChecker,
    BackendIdentityResolver, NoneAuthProvider,
};
use queryflux_cache::noop::NoopResultCache;
use queryflux_cluster_manager::{cluster_state::ClusterState, simple::SimpleClusterGroupManager};
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, ProxyQueryId, QueryStats,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::{
    duckdb::{DuckDbAdapter, DuckDbConfig, DEFAULT_MAX_RESULT_BUFFER_BYTES},
    AdapterKind,
};
use queryflux_frontend::{
    dispatch::{dispatch_query, execute_to_sink, DispatchOutcome, ResultSink},
    state::{AppState, LiveConfig},
};
use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
use queryflux_persistence::{in_memory::InMemoryPersistence, CapacityStore};
use queryflux_routing::chain::RouterChain;
use queryflux_translation::TranslationService;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, RwLock};

struct Metrics;
#[async_trait]
impl MetricsStore for Metrics {
    /// Discard query history; the test asserts on cluster state only.
    async fn record_query(&self, _: QueryRecord) -> Result<()> {
        Ok(())
    }
    /// Discard cluster snapshots; the test asserts on cluster state only.
    async fn record_cluster_snapshot(&self, _: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}

/// Capacity store whose `try_acquire` never completes, modelling a slow
/// Postgres transaction that is still pending when the client goes away.
#[derive(Default)]
struct HangingCapacity {
    entered: Notify,
    releases: AtomicUsize,
}
#[async_trait]
impl CapacityStore for HangingCapacity {
    /// Signal entry, then block forever so the caller's future can be dropped mid-await.
    async fn try_acquire(&self, _: &str, _: u64, _: &str, _: &str) -> Result<bool> {
        self.entered.notify_one();
        std::future::pending().await
    }
    /// Count releases so the test can assert the lease cleanup also ran.
    async fn release(&self, _: &str, _: &str) -> Result<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
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
    async fn active_lease_count(&self, _: &str) -> Result<u64> {
        Ok(0)
    }
    async fn release_all_for_instance(&self, _: &str) -> Result<u64> {
        Ok(0)
    }
}

struct NullSink;
#[async_trait]
impl ResultSink for NullSink {
    async fn on_schema(&mut self, _: &arrow::datatypes::Schema) -> Result<()> {
        Ok(())
    }
    async fn on_batch(&mut self, _: &arrow::record_batch::RecordBatch) -> Result<()> {
        Ok(())
    }
    async fn on_complete(&mut self, _: &QueryStats) -> Result<()> {
        Ok(())
    }
    async fn on_error(&mut self, _: &str) -> Result<()> {
        Ok(())
    }
}

/// Capacity store that fails the next `fail_acquires` acquires with an error (the
/// lease may or may not have been taken), or denies every acquire when `deny` is set,
/// and records each acquire and release in order.
#[derive(Default)]
struct FlakyCapacity {
    fail_acquires: AtomicUsize,
    deny: AtomicBool,
    events: Mutex<Vec<&'static str>>,
}
impl FlakyCapacity {
    fn failing(n: usize) -> Arc<Self> {
        let c = Self::default();
        c.fail_acquires.store(n, Ordering::SeqCst);
        Arc::new(c)
    }
    fn denying() -> Arc<Self> {
        let c = Self::default();
        c.deny.store(true, Ordering::SeqCst);
        Arc::new(c)
    }
    fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }
}
#[async_trait]
impl CapacityStore for FlakyCapacity {
    /// Fail, deny, or grant per the configured behaviour, recording the outcome.
    async fn try_acquire(&self, _: &str, _: u64, _: &str, _: &str) -> Result<bool> {
        let failing = self
            .fail_acquires
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        let mut events = self.events.lock().unwrap();
        if failing {
            events.push("acquire:error");
            return Err(QueryFluxError::Persistence("connection reset".into()));
        }
        if self.deny.load(Ordering::SeqCst) {
            events.push("acquire:denied");
            return Ok(false);
        }
        events.push("acquire:granted");
        Ok(true)
    }
    /// Record the release.
    async fn release(&self, _: &str, _: &str) -> Result<()> {
        self.events.lock().unwrap().push("release");
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
    async fn active_lease_count(&self, _: &str) -> Result<u64> {
        Ok(0)
    }
    async fn release_all_for_instance(&self, _: &str) -> Result<u64> {
        Ok(0)
    }
}

/// Wire one DuckDB cluster behind the given capacity store.
fn test_state(capacity: Arc<dyn CapacityStore>) -> (Arc<AppState>, Arc<ClusterState>) {
    let group_name = ClusterGroupName("duckdb".into());
    let cluster_name = ClusterName("duckdb-1".into());
    let adapter = AdapterKind::Sync(Arc::new(
        DuckDbAdapter::new(
            cluster_name.clone(),
            group_name.clone(),
            DuckDbConfig {
                database_path: None,
                motherduck_token: None,
                pool_size: 1,
                max_result_buffer_bytes: DEFAULT_MAX_RESULT_BUFFER_BYTES,
            },
        )
        .unwrap(),
    ));
    let cluster_state = Arc::new(ClusterState::new(
        cluster_name.clone(),
        group_name.clone(),
        None,
        None,
        EngineType::DuckDb,
        None,
        10,
        true,
    ));
    let groups = HashMap::from([(
        group_name.clone(),
        (
            vec![cluster_state.clone()],
            Arc::new(queryflux_cluster_manager::strategy::RoundRobinStrategy::new())
                as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        ),
    )]);
    let live = LiveConfig {
        router_chain: RouterChain::new(vec![], group_name.clone()),
        guard_chain: None,
        group_guard_chains: HashMap::new(),
        access_control_guard: None,
        cluster_manager: Arc::new(SimpleClusterGroupManager::new(groups)),
        adapters: HashMap::from([(cluster_name.0.clone(), adapter)]),
        health_check_targets: vec![],
        custom_health_queries: HashMap::new(),
        custom_reconcile_queries: HashMap::new(),
        cluster_configs: HashMap::new(),
        group_members: HashMap::from([(group_name.0.clone(), vec![cluster_name.0.clone()])]),
        group_order: vec![group_name.0.clone()],
        group_translation_scripts: HashMap::new(),
        group_default_tags: HashMap::new(),
        group_max_queued_queries: HashMap::new(),
        group_capacity_wait_timeout_secs: HashMap::new(),
        group_cache_settings: HashMap::new(),
        auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
        authorization: Arc::new(AllowAllAuthorization::default()) as Arc<dyn AuthorizationChecker>,
        catalog: Arc::new(queryflux_core::catalog::NullCatalogProvider),
    };
    let state = Arc::new(AppState {
        external_address: "http://127.0.0.1:8080".into(),
        live: Arc::new(RwLock::new(live)),
        persistence: Arc::new(InMemoryPersistence::new()),
        translation: Arc::new(TranslationService::disabled()),
        metrics: Arc::new(Metrics),
        identity_resolver: Arc::new(BackendIdentityResolver::new()),
        capacity_store: Some(capacity),
        queue_coordinator: None,
        instance_id: "test".into(),
        http_client: reqwest::Client::new(),
        result_cache: Arc::new(NoopResultCache),
    });
    (state, cluster_state)
}

/// Abort `task` once it is parked in `try_acquire`, then require the local slot
/// and the (possibly half-acquired) lease to be released by the guard's drop.
async fn assert_released_after_abort(
    task: tokio::task::JoinHandle<()>,
    capacity: &HangingCapacity,
    cluster: &ClusterState,
) {
    tokio::time::timeout(Duration::from_secs(5), capacity.entered.notified())
        .await
        .expect("dispatch never reached try_acquire");
    assert_eq!(
        cluster.running_queries(),
        1,
        "local slot held while waiting"
    );

    task.abort();
    let _ = task.await;

    // The guard's Drop releases on a spawned task — give it a moment to run.
    tokio::time::timeout(Duration::from_secs(5), async {
        while cluster.running_queries() != 0 || capacity.releases.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("local slot or global lease leaked after dropped dispatch");
}

#[tokio::test]
async fn sync_dispatch_dropped_during_global_acquire_releases_slot() {
    let capacity = Arc::new(HangingCapacity::default());
    let (state, cluster) = test_state(capacity.clone());
    let task = tokio::spawn(async move {
        let live = state.snapshot().await;
        let _ = execute_to_sink(
            &state,
            &live,
            "SELECT 1".into(),
            vec![],
            SessionContext::default(),
            FrontendProtocol::Mcp,
            ClusterGroupName("duckdb".into()),
            &mut NullSink,
            &AuthContext::default(),
        )
        .await;
    });
    assert_released_after_abort(task, &capacity, &cluster).await;
}

#[tokio::test]
async fn async_dispatch_dropped_during_global_acquire_releases_slot() {
    let capacity = Arc::new(HangingCapacity::default());
    let (state, cluster) = test_state(capacity.clone());
    let task = tokio::spawn(async move {
        let live = state.snapshot().await;
        let _ = dispatch_query(
            &state,
            &live,
            ProxyQueryId::new(),
            "SELECT 1".into(),
            vec![],
            SessionContext::default(),
            FrontendProtocol::TrinoHttp,
            ClusterGroupName("duckdb".into()),
            false,
            None,
            0,
            &AuthContext::default(),
        )
        .await;
    });
    assert_released_after_abort(task, &capacity, &cluster).await;
}

/// Run one async (Trino HTTP) dispatch against `capacity` and return its outcome.
async fn dispatch_once(capacity: Arc<FlakyCapacity>) -> (DispatchOutcome, Arc<ClusterState>) {
    let (state, cluster) = test_state(capacity);
    let live = state.snapshot().await;
    let outcome = dispatch_query(
        &state,
        &live,
        ProxyQueryId::new(),
        "SELECT 1".into(),
        vec![],
        SessionContext::default(),
        FrontendProtocol::TrinoHttp,
        ClusterGroupName("duckdb".into()),
        false,
        None,
        0,
        &AuthContext::default(),
    )
    .await
    .expect("dispatch");
    (outcome, cluster)
}

#[tokio::test]
async fn async_dispatch_releases_lease_when_acquire_fails() {
    let capacity = FlakyCapacity::failing(1);
    let (outcome, cluster) = dispatch_once(capacity.clone()).await;
    assert!(
        matches!(outcome, DispatchOutcome::Queued { .. }),
        "fails closed to the queue"
    );
    assert_eq!(cluster.running_queries(), 0, "local slot released");
    assert_eq!(
        capacity.events(),
        ["acquire:error", "release"],
        "a lease the failed acquire may have committed is released"
    );
}

#[tokio::test]
async fn async_dispatch_sends_no_release_when_capacity_is_full() {
    let capacity = FlakyCapacity::denying();
    let (outcome, cluster) = dispatch_once(capacity.clone()).await;
    assert!(matches!(outcome, DispatchOutcome::Queued { .. }));
    assert_eq!(cluster.running_queries(), 0, "local slot released");
    assert_eq!(
        capacity.events(),
        ["acquire:denied"],
        "a denied acquire took no lease, so nothing is released"
    );
}

#[tokio::test]
async fn sync_dispatch_releases_lease_before_retrying_a_failed_acquire() {
    let capacity = FlakyCapacity::failing(1);
    let (state, cluster) = test_state(capacity.clone());
    let live = state.snapshot().await;
    execute_to_sink(
        &state,
        &live,
        "SELECT 1".into(),
        vec![],
        SessionContext::default(),
        FrontendProtocol::Mcp,
        ClusterGroupName("duckdb".into()),
        &mut NullSink,
        &AuthContext::default(),
    )
    .await
    .expect("query runs once the retry is granted");
    assert_eq!(cluster.running_queries(), 0);
    assert_eq!(
        capacity.events(),
        ["acquire:error", "release", "acquire:granted", "release"],
        "the possibly committed lease is released before the retry acquires again"
    );
}
