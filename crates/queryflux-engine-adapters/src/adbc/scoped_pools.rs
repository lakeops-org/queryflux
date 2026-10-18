//! Bounded scoped-pool ownership. Connection destructors run outside the cache lock
//! and on blocking workers: releasing a driver connection can involve network I/O.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use queryflux_metrics::adbc::ScopedPoolMetrics;
use r2d2::{ManageConnection, Pool};

use super::PoolScopeKey;

pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 900;
pub const DEFAULT_MAX_COUNT: usize = 500;

/// Keep single-flight ownership with blocking work even if its caller times out
/// or is cancelled. Returning the guard lets the caller retain it until publish.
pub(super) fn spawn_with_build_guard<T: Send + 'static>(
    guard: tokio::sync::OwnedMutexGuard<()>,
    work: impl FnOnce() -> T + Send + 'static,
) -> tokio::task::JoinHandle<(T, tokio::sync::OwnedMutexGuard<()>)> {
    tokio::task::spawn_blocking(move || {
        let result = work();
        (result, guard)
    })
}

struct Entry<M: ManageConnection> {
    pool: Pool<M>,
    last_used: Instant,
}

struct State<M: ManageConnection> {
    entries: HashMap<PoolScopeKey, Entry<M>>,
    builds: HashMap<PoolScopeKey, Weak<tokio::sync::Mutex<()>>>,
}

pub(super) struct ScopedPools<M: ManageConnection> {
    state: Mutex<State<M>>,
    idle_timeout: Duration,
    max_count: usize,
    metrics: ScopedPoolMetrics,
    reaper: OnceLock<tokio::task::AbortHandle>,
}

impl<M: ManageConnection> ScopedPools<M> {
    pub fn new(idle_timeout: Duration, max_count: usize, group: &str, cluster: &str) -> Self {
        assert!(!idle_timeout.is_zero() && max_count > 0);
        Self {
            state: Mutex::new(State {
                entries: HashMap::new(),
                builds: HashMap::new(),
            }),
            idle_timeout,
            max_count,
            metrics: ScopedPoolMetrics::new(group, cluster),
            reaper: OnceLock::new(),
        }
    }

    /// Starts lazily in the query runtime; adapter construction can be synchronous.
    /// The task holds only a weak reference between sweeps, so it cannot keep an
    /// obsolete adapter's pools (or driver library) alive after a config reload.
    pub fn start_reaper(self: &Arc<Self>) {
        self.reaper.get_or_init(|| {
            let weak = Arc::downgrade(self);
            let interval = self.idle_timeout.min(Duration::from_secs(60));
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let weak = weak.clone();
                    let swept = tokio::task::spawn_blocking(move || {
                        let Some(cache) = weak.upgrade() else {
                            return false;
                        };
                        cache.sweep();
                        true
                    })
                    .await;
                    if !matches!(swept, Ok(true)) {
                        break;
                    }
                }
            })
            .abort_handle()
        });
    }

    pub fn get(&self, key: &PoolScopeKey) -> Option<Pool<M>> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let entry = state.entries.get_mut(key)?;
        // Do not revive expired entries between background sweeps. Their
        // destruction is deferred to insert/sweep on a blocking worker.
        // The cached-pool gauge includes them until that cleanup removes them.
        if now.duration_since(entry.last_used) >= self.idle_timeout {
            return None;
        }
        entry.last_used = now;
        Some(entry.pool.clone())
    }

    pub fn build_lock(&self, key: &PoolScopeKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut state = self.state.lock().unwrap();
        state.builds.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = state.builds.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        state.builds.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }

    fn remove_idle(&self, state: &mut State<M>, now: Instant) -> Vec<Pool<M>> {
        let keys: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.last_used) >= self.idle_timeout)
            .map(|(key, _)| key.clone())
            .collect();
        let removed: Vec<_> = keys
            .iter()
            .filter_map(|key| state.entries.remove(key).map(|entry| entry.pool))
            .collect();
        self.metrics.pools.sub(removed.len() as i64);
        self.metrics.idle_evictions.inc_by(removed.len() as u64);
        state.builds.retain(|_, lock| lock.strong_count() > 0);
        removed
    }

    /// Call on a blocking worker. Removal and insertion share one lock, so
    /// simultaneous builds cannot race past the configured capacity.
    pub fn insert(&self, key: PoolScopeKey, pool: Pool<M>) {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let mut removed = self.remove_idle(&mut state, now);
        if let Some(old) = state.entries.remove(&key) {
            self.metrics.pools.dec();
            removed.push(old.pool);
        }
        if state.entries.len() >= self.max_count {
            let lru = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
                .expect("nonempty cache at capacity");
            removed.push(state.entries.remove(&lru).unwrap().pool);
            self.metrics.pools.dec();
            self.metrics.lru_evictions.inc();
        }
        state.entries.insert(
            key,
            Entry {
                pool,
                last_used: now,
            },
        );
        self.metrics.pools.inc();
        drop(state);
        // r2d2 owns its connections and ManagedConnection/ManagedDatabase call
        // the driver's release callbacks on drop. Checked-out connections keep
        // their pool alive until the query finishes; eviction never closes them
        // underneath an active statement or Arrow reader.
        drop(removed);
    }

    fn sweep(&self) {
        let mut state = self.state.lock().unwrap();
        let removed = self.remove_idle(&mut state, Instant::now());
        drop(state);
        drop(removed);
    }
}

impl<M: ManageConnection> Drop for ScopedPools<M> {
    fn drop(&mut self) {
        if let Some(reaper) = self.reaper.get() {
            reaper.abort();
        }
        self.metrics
            .pools
            .sub(self.state.get_mut().unwrap().entries.len() as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestManager(Arc<AtomicUsize>);
    struct TestConnection(Arc<AtomicUsize>);

    impl Drop for TestConnection {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ManageConnection for TestManager {
        type Connection = TestConnection;
        type Error = std::io::Error;

        fn connect(&self) -> Result<Self::Connection, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(TestConnection(self.0.clone()))
        }
        fn is_valid(&self, _: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }
        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    fn pool() -> (Pool<TestManager>, Arc<AtomicUsize>) {
        let live = Arc::new(AtomicUsize::new(0));
        let pool = Pool::builder()
            .max_size(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .build(TestManager(live.clone()))
            .unwrap();
        (pool, live)
    }

    fn key(name: &str) -> PoolScopeKey {
        PoolScopeKey {
            token: Some(name.into()),
            ..Default::default()
        }
    }

    fn cache(name: &str, max: usize) -> ScopedPools<TestManager> {
        ScopedPools::new(Duration::from_secs(900), max, "scoped-pool-tests", name)
    }

    fn expire(cache: &ScopedPools<TestManager>, name: &str) {
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key(name))
            .unwrap()
            .last_used = Instant::now() - cache.idle_timeout;
    }

    fn await_closed(live: &AtomicUsize) {
        // r2d2's connection creation worker may briefly retain a pool reference
        // after build returns. Wait for that worker to release it too.
        let deadline = Instant::now() + Duration::from_secs(2);
        while live.load(Ordering::SeqCst) != 0 {
            assert!(Instant::now() < deadline, "connection was not released");
            std::thread::yield_now();
        }
    }

    #[test]
    fn lru_uses_last_access_and_closes_evicted_connections() {
        let cache = cache("lru", 2);
        let (a, live_a) = pool();
        let (b, live_b) = pool();
        let (c, live_c) = pool();
        cache.insert(key("a"), a);
        cache.insert(key("b"), b);
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key("a"))
            .unwrap()
            .last_used = Instant::now() - Duration::from_secs(10);
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key("b"))
            .unwrap()
            .last_used = Instant::now() - Duration::from_secs(5);
        drop(cache.get(&key("a")).unwrap());
        cache.insert(key("c"), c);
        assert!(cache.get(&key("b")).is_none());
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("c")).is_some());
        assert_eq!(cache.metrics.pools.get(), 2);
        assert_eq!(cache.metrics.lru_evictions.get(), 1);
        await_closed(&live_b);
        drop(cache);
        await_closed(&live_a);
        await_closed(&live_c);
    }

    #[test]
    fn expiry_does_not_revive_pool_or_interrupt_checked_out_connection() {
        let cache = cache("expiry", 2);
        let (pool, live) = pool();
        let connection = pool.get().unwrap();
        cache.insert(key("a"), pool);
        expire(&cache, "a");
        assert!(cache.get(&key("a")).is_none());
        cache.sweep();
        assert_eq!(cache.metrics.pools.get(), 0);
        assert_eq!(cache.metrics.idle_evictions.get(), 1);
        assert_eq!(live.load(Ordering::SeqCst), 1);
        drop(connection);
        await_closed(&live);
    }

    #[test]
    fn concurrent_inserts_stay_within_capacity() {
        let cache = Arc::new(cache("concurrent", 3));
        let (pool, _) = pool();
        let barrier = Arc::new(std::sync::Barrier::new(16));
        std::thread::scope(|scope| {
            for i in 0..16 {
                let cache = cache.clone();
                let pool = pool.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    cache.insert(key(&i.to_string()), pool);
                    assert!(cache.state.lock().unwrap().entries.len() <= 3);
                });
            }
        });
        assert_eq!(cache.metrics.pools.get(), 3);
        assert_eq!(cache.metrics.lru_evictions.get(), 13);
    }

    #[tokio::test]
    async fn reaper_expires_without_requests_and_does_not_retain_cache() {
        let cache = Arc::new(ScopedPools::new(
            Duration::from_millis(20),
            2,
            "scoped-pool-tests",
            "background",
        ));
        let (pool, live) = pool();
        cache.insert(key("a"), pool);
        cache.start_reaper();
        tokio::time::timeout(Duration::from_secs(2), async {
            while live.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cache.metrics.pools.get(), 0);
        assert_eq!(cache.metrics.idle_evictions.get(), 1);
        let weak = Arc::downgrade(&cache);
        drop(cache);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn build_locks_are_shared_until_all_waiters_release_them() {
        let cache = cache("locks", 2);
        let first = cache.build_lock(&key("a"));
        let waiter = cache.build_lock(&key("a"));
        assert!(Arc::ptr_eq(&first, &waiter));
        drop(first);
        assert!(Arc::ptr_eq(&waiter, &cache.build_lock(&key("a"))));
        drop(waiter);
        cache.sweep();
        assert!(cache.state.lock().unwrap().builds.is_empty());
    }

    #[tokio::test]
    async fn timed_out_build_holds_scope_lock_until_worker_finishes() {
        let cache = cache("timed-out-build", 2);
        let guard = cache.build_lock(&key("a")).lock_owned().await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let build = spawn_with_build_guard(guard, move || {
            started_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        started_rx.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(10), build)
            .await
            .is_err());

        // Pruning weak entries must not create a new lock for the running build.
        cache.sweep();
        let same_scope = cache.build_lock(&key("a"));
        assert!(same_scope.try_lock().is_err());
        assert!(cache.build_lock(&key("b")).try_lock().is_ok());
        finish_tx.send(()).unwrap();
        let guard = tokio::time::timeout(Duration::from_secs(2), same_scope.lock_owned())
            .await
            .unwrap();
        drop(guard);
        // The build thread's guard unlocks the mutex before it drops its `Arc` to it, so
        // right after `lock_owned` succeeds that thread may still hold the last other
        // reference, and the entry is not prunable yet. Wait for it rather than racing it.
        // The test cannot await `build` instead: like `AdbcAdapter`, which hands the
        // handle to `tokio::time::timeout`, the caller has dropped it, so the runtime
        // drops the guard on the build thread when the work finishes. That detached path
        // is what this test covers. The window is microseconds; the deadline is a
        // generous bound, not a wait (the loop exits as soon as the entry is pruned).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            cache.sweep();
            if cache.state.lock().unwrap().builds.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "build lock entries were not released after the build finished"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn cancelled_publication_keeps_guard_until_pool_is_cached() {
        let cache = Arc::new(cache("cancelled-publication", 2));
        let guard = cache.build_lock(&key("a")).lock_owned().await;
        let (built_pool, _) = pool();
        let (built_pool, guard) = spawn_with_build_guard(guard, move || built_pool)
            .await
            .unwrap();
        assert!(cache.build_lock(&key("a")).try_lock().is_err());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let cache_for_insert = cache.clone();
        let insertion = spawn_with_build_guard(guard, move || {
            started_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            cache_for_insert.insert(key("a"), built_pool);
        });
        started_rx.await.unwrap();
        drop(insertion); // A cancelled request drops its JoinHandle the same way.
        assert!(cache.build_lock(&key("a")).try_lock().is_err());
        finish_tx.send(()).unwrap();
        let _guard = tokio::time::timeout(
            Duration::from_secs(2),
            cache.build_lock(&key("a")).lock_owned(),
        )
        .await
        .unwrap();
        assert!(cache.get(&key("a")).is_some());
    }

    #[tokio::test]
    async fn failed_or_panicking_build_releases_scope_lock() {
        let cache = cache("failed-build", 2);
        let guard = cache.build_lock(&key("a")).lock_owned().await;
        let (result, guard) = spawn_with_build_guard(guard, || Err::<(), _>("build failed"))
            .await
            .unwrap();
        assert!(result.is_err());
        drop(guard);
        assert!(cache.build_lock(&key("a")).try_lock().is_ok());

        let guard = cache.build_lock(&key("a")).lock_owned().await;
        assert!(spawn_with_build_guard(guard, || panic!("driver panic"))
            .await
            .unwrap_err()
            .is_panic());
        assert!(cache.build_lock(&key("a")).try_lock().is_ok());
    }

    #[test]
    fn metrics_are_exposed_and_survive_overlapping_adapter_generations() {
        let first = cache("metrics", 1);
        let second = cache("metrics", 1);
        let (a, _) = pool();
        let (b, _) = pool();
        first.insert(key("a"), a);
        second.insert(key("b"), b);
        assert_eq!(first.metrics.pools.get(), 2);
        expire(&first, "a");
        first.sweep();
        let text = queryflux_metrics::prometheus_store::PrometheusMetrics::new()
            .unwrap()
            .gather_text();
        assert!(text.contains("queryflux_adbc_scoped_pools{cluster_group=\"scoped-pool-tests\",cluster_name=\"metrics\"} 1"));
        assert!(text.contains("queryflux_adbc_scoped_pool_evictions_total{cluster_group=\"scoped-pool-tests\",cluster_name=\"metrics\",reason=\"idle\"} 1"));
        drop(first);
        assert_eq!(second.metrics.pools.get(), 1);
        let gauge = second.metrics.pools.clone();
        drop(second);
        assert_eq!(gauge.get(), 0);
    }
}
