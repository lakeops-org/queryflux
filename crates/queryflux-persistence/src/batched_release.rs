//! Capacity releases off the query path (high-throughput plan, WS1).
//!
//! Every query ends by releasing its capacity lease: a `DELETE` in Postgres, or an HTTP
//! call to the control plane on a worker. [`BatchedReleaseStore`] wraps a
//! [`CapacityStore`] so that `release` only queues the lease and returns. A background
//! task sends what has queued up as one [`CapacityStore::release_many`] call, so at high
//! query rates many releases share one statement or request, and the end of a query never
//! waits on the network.
//!
//! A failed batch is retried with backoff until it succeeds, so a release lost to a brief
//! outage no longer leaks its slot. If the queue grows past [`MAX_PENDING`] (a long
//! outage), `release` falls back to releasing inline, as before. Everything else passes
//! straight through to the wrapped store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::Result;
use tokio::sync::{mpsc, oneshot};

use crate::{CapacityStore, MetricsStore};

/// Most leases sent in one `release_many` call.
const MAX_BATCH: usize = 500;
/// Queued releases above which `release` stops queueing and releases inline.
const MAX_PENDING: usize = 100_000;
const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(5);

enum Msg {
    Release(String, String),
    Flush(oneshot::Sender<()>),
}

pub struct BatchedReleaseStore {
    inner: Arc<dyn CapacityStore>,
    tx: mpsc::UnboundedSender<Msg>,
    pending: Arc<AtomicUsize>,
    /// Query ids with a release queued and not yet sent, so an acquire for the same id
    /// can wait for it instead of having its new lease removed by it.
    queued_ids: Arc<dashmap::DashMap<String, usize>>,
    metrics: Arc<dyn MetricsStore>,
}

impl BatchedReleaseStore {
    /// Wrap `inner` and start the flusher. Must be called inside a Tokio runtime. The
    /// flusher stops when the store is dropped, after sending what is queued.
    pub fn new(inner: Arc<dyn CapacityStore>, metrics: Arc<dyn MetricsStore>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicUsize::new(0));
        let queued_ids: Arc<dashmap::DashMap<String, usize>> = Arc::default();
        tokio::spawn(flusher(
            inner.clone(),
            rx,
            pending.clone(),
            queued_ids.clone(),
            metrics.clone(),
        ));
        Self {
            inner,
            tx,
            pending,
            queued_ids,
            metrics,
        }
    }

    /// Wait until every release queued before this call has been sent, or `timeout`.
    /// Returns false on timeout (the store is unreachable and still retrying).
    pub async fn flush(&self, timeout: Duration) -> bool {
        let (done, wait) = oneshot::channel();
        if self.tx.send(Msg::Flush(done)).is_err() {
            return true;
        }
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    /// Releases queued and not yet confirmed by the wrapped store.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

/// Releases that failed and wait for their retry. New releases don't wait behind them:
/// each round sends what newly arrived at once, and retries the failed ones on their own
/// backoff. After a few failed retries they are sent one at a time, so a single lease the
/// store keeps rejecting can't hold the others back.
struct Retry {
    leases: Vec<(String, String)>,
    at: tokio::time::Instant,
    delay: Duration,
    attempts: u32,
}

enum Wake {
    Msg(Msg),
    Retry,
}

async fn flusher(
    inner: Arc<dyn CapacityStore>,
    mut rx: mpsc::UnboundedReceiver<Msg>,
    pending: Arc<AtomicUsize>,
    queued_ids: Arc<dashmap::DashMap<String, usize>>,
    metrics: Arc<dyn MetricsStore>,
) {
    let mut retry = Retry {
        leases: Vec::new(),
        at: tokio::time::Instant::now(),
        delay: RETRY_MIN,
        attempts: 0,
    };
    let mut flushes: Vec<oneshot::Sender<()>> = Vec::new();
    let mut closed = false;
    loop {
        let wake = match (retry.leases.is_empty(), closed) {
            (true, true) => return,
            (true, false) => match rx.recv().await {
                Some(m) => Wake::Msg(m),
                None => return,
            },
            (false, true) => {
                tokio::time::sleep_until(retry.at).await;
                Wake::Retry
            }
            (false, false) => tokio::select! {
                m = rx.recv() => match m {
                    Some(m) => Wake::Msg(m),
                    None => {
                        closed = true;
                        continue;
                    }
                },
                _ = tokio::time::sleep_until(retry.at) => Wake::Retry,
            },
        };

        let mut failed = Vec::new();
        match wake {
            Wake::Msg(first) => {
                // Send what queued up while the previous round was busy.
                let mut fresh = Vec::new();
                let mut next = Some(first);
                while let Some(msg) = next.take().or_else(|| rx.try_recv().ok()) {
                    match msg {
                        Msg::Release(cluster, query_id) => fresh.push((cluster, query_id)),
                        Msg::Flush(done) => flushes.push(done),
                    }
                    if fresh.len() >= MAX_BATCH {
                        break;
                    }
                }
                failed = send(inner.as_ref(), fresh, false, &pending, &queued_ids).await;
                if !failed.is_empty() && retry.leases.is_empty() {
                    retry.at = tokio::time::Instant::now() + retry.delay;
                }
            }
            Wake::Retry => {
                let leases = std::mem::take(&mut retry.leases);
                let one_by_one = retry.attempts >= 3;
                let still = send(inner.as_ref(), leases, one_by_one, &pending, &queued_ids).await;
                retry.attempts += 1;
                retry.delay = (retry.delay * 2).min(RETRY_MAX);
                retry.at = tokio::time::Instant::now() + retry.delay;
                retry.leases = still;
            }
        }
        if !failed.is_empty() {
            retry.leases.extend(failed);
        }
        if !retry.leases.is_empty() {
            metrics.on_coordination_failure("capacity_release");
            tracing::warn!(
                leases = retry.leases.len(),
                "Capacity releases failed, retrying in {:?}",
                retry.delay
            );
        } else {
            retry.attempts = 0;
            retry.delay = RETRY_MIN;
            // Everything read so far has been sent, including every release queued before
            // any flush message read so far.
            for done in flushes.drain(..) {
                let _ = done.send(());
            }
        }
    }
}

/// Send `leases` (in one call, or one at a time) and return the ones that failed.
async fn send(
    inner: &dyn CapacityStore,
    leases: Vec<(String, String)>,
    one_by_one: bool,
    pending: &AtomicUsize,
    queued_ids: &dashmap::DashMap<String, usize>,
) -> Vec<(String, String)> {
    let sent = |done: &[(String, String)]| {
        pending.fetch_sub(done.len(), Ordering::Relaxed);
        for (_, query_id) in done {
            queued_ids.remove_if_mut(query_id, |_, n| {
                *n -= 1;
                *n == 0
            });
        }
    };
    if leases.is_empty() {
        return leases;
    }
    if !one_by_one || leases.len() == 1 {
        return match inner.release_many(&leases).await {
            Ok(()) => {
                sent(&leases);
                Vec::new()
            }
            Err(e) => {
                tracing::debug!(leases = leases.len(), "Capacity release batch failed: {e}");
                leases
            }
        };
    }
    let mut failed = Vec::new();
    for lease in leases {
        match inner.release(&lease.0, &lease.1).await {
            Ok(()) => sent(std::slice::from_ref(&lease)),
            Err(e) => {
                tracing::debug!(query_id = %lease.1, "Capacity release failed: {e}");
                failed.push(lease);
            }
        }
    }
    failed
}

#[async_trait]
impl CapacityStore for BatchedReleaseStore {
    async fn try_acquire(
        &self,
        cluster_name: &str,
        max_running_queries: u64,
        instance_id: &str,
        query_id: &str,
    ) -> Result<bool> {
        // A release for this id is still queued: send it first, so it can't remove the
        // lease this acquire is about to take. Callers don't normally release an id and
        // acquire it again, so this is rare.
        if self.queued_ids.contains_key(query_id) && !self.flush(Duration::from_secs(5)).await {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "a release for query {query_id} is still queued; retry"
            )));
        }
        self.inner
            .try_acquire(cluster_name, max_running_queries, instance_id, query_id)
            .await
    }

    async fn release(&self, cluster_name: &str, query_id: &str) -> Result<()> {
        if self.pending.load(Ordering::Relaxed) < MAX_PENDING {
            self.pending.fetch_add(1, Ordering::Relaxed);
            *self.queued_ids.entry(query_id.to_string()).or_insert(0) += 1;
            if self
                .tx
                .send(Msg::Release(cluster_name.to_string(), query_id.to_string()))
                .is_ok()
            {
                return Ok(());
            }
            self.pending.fetch_sub(1, Ordering::Relaxed);
            self.queued_ids.remove_if_mut(query_id, |_, n| {
                *n -= 1;
                *n == 0
            });
        } else {
            self.metrics
                .on_coordination_failure("capacity_release_backlog");
        }
        self.inner.release(cluster_name, query_id).await
    }

    async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
        self.inner.release_many(leases).await
    }

    async fn heartbeat(&self, instance_id: &str) -> Result<u64> {
        self.inner.heartbeat(instance_id).await
    }

    async fn expire_stale(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        self.inner.expire_stale(cutoff).await
    }

    async fn active_count(&self, cluster_name: &str) -> Result<u64> {
        self.inner.active_count(cluster_name).await
    }

    async fn publish_running_count(&self, cluster_name: &str, count: u64) -> Result<()> {
        self.inner.publish_running_count(cluster_name, count).await
    }

    async fn active_lease_count(&self, cluster_name: &str) -> Result<u64> {
        self.inner.active_lease_count(cluster_name).await
    }

    async fn release_all_for_instance(&self, instance_id: &str) -> Result<u64> {
        self.inner.release_all_for_instance(instance_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use queryflux_core::error::QueryFluxError;

    /// Records every `release_many` batch; fails the first `fail` calls.
    #[derive(Default)]
    struct Recorder {
        batches: Mutex<Vec<Vec<(String, String)>>>,
        fail: AtomicUsize,
    }

    #[async_trait]
    impl CapacityStore for Recorder {
        async fn try_acquire(&self, _: &str, _: u64, _: &str, _: &str) -> Result<bool> {
            Ok(true)
        }
        async fn release(&self, cluster: &str, query_id: &str) -> Result<()> {
            self.release_many(&[(cluster.to_string(), query_id.to_string())])
                .await
        }
        async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
            if self
                .fail
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(QueryFluxError::Persistence("down".into()));
            }
            self.batches.lock().unwrap().push(leases.to_vec());
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

    fn no_metrics() -> Arc<dyn MetricsStore> {
        Arc::new(crate::in_memory::InMemoryPersistence::new())
    }

    fn released(r: &Recorder) -> Vec<String> {
        let mut ids: Vec<String> = r
            .batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .map(|(_, q)| q.clone())
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn release_returns_at_once_and_flush_sends_everything() {
        let rec = Arc::new(Recorder::default());
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        for i in 0..1200 {
            store.release("c", &format!("q{i:04}")).await.unwrap();
        }
        assert!(store.flush(Duration::from_secs(5)).await);
        assert_eq!(released(&rec).len(), 1200);
        assert_eq!(store.pending(), 0);
        assert!(
            rec.batches
                .lock()
                .unwrap()
                .iter()
                .all(|b| b.len() <= MAX_BATCH),
            "batches are capped"
        );
    }

    #[tokio::test]
    async fn failed_batches_are_retried_until_they_succeed() {
        let rec = Arc::new(Recorder {
            fail: AtomicUsize::new(2),
            ..Default::default()
        });
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        store.release("c", "q1").await.unwrap();
        store.release("c", "q2").await.unwrap();
        assert!(store.flush(Duration::from_secs(5)).await);
        assert_eq!(released(&rec), vec!["q1", "q2"]);
        assert_eq!(store.pending(), 0);
    }

    /// Rejects one lease forever; releases everything else.
    #[derive(Default)]
    struct Poisoned {
        released: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CapacityStore for Poisoned {
        async fn try_acquire(&self, _: &str, _: u64, _: &str, _: &str) -> Result<bool> {
            Ok(true)
        }
        async fn release(&self, _: &str, q: &str) -> Result<()> {
            if q == "bad" {
                return Err(QueryFluxError::Persistence("rejected".into()));
            }
            self.released.lock().unwrap().push(q.to_string());
            Ok(())
        }
        async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
            if leases.iter().any(|(_, q)| q == "bad") {
                return Err(QueryFluxError::Persistence("rejected".into()));
            }
            for (c, q) in leases {
                self.release(c, q).await?;
            }
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

    #[tokio::test(start_paused = true)]
    async fn a_lease_the_store_keeps_rejecting_does_not_hold_up_the_rest() {
        let store_ = Arc::new(Poisoned::default());
        let store = BatchedReleaseStore::new(store_.clone(), no_metrics());
        store.release("c", "bad").await.unwrap();
        store.release("c", "q1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Released later, while the first batch is failing: sent on arrival.
        store.release("c", "q2").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(*store_.released.lock().unwrap(), vec!["q2"]);
        // After a few retries the failed batch is split, and q1 goes through.
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(*store_.released.lock().unwrap(), vec!["q2", "q1"]);
        assert_eq!(store.pending(), 1, "only the rejected one is left");
    }

    #[tokio::test]
    async fn flush_times_out_while_the_store_is_down() {
        let rec = Arc::new(Recorder {
            fail: AtomicUsize::new(usize::MAX),
            ..Default::default()
        });
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        store.release("c", "q1").await.unwrap();
        assert!(!store.flush(Duration::from_millis(200)).await);
        assert_eq!(store.pending(), 1);
    }

    /// Release then acquire the same id: the release reaches the store first.
    #[tokio::test]
    async fn an_acquire_waits_for_a_queued_release_of_the_same_id() {
        let rec = Arc::new(Recorder {
            fail: AtomicUsize::new(2),
            ..Default::default()
        });
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        store.release("c", "q1").await.unwrap();
        assert!(store.try_acquire("c", 10, "i", "q1").await.unwrap());
        assert_eq!(
            released(&rec),
            vec!["q1"],
            "sent before the acquire returned"
        );
        assert!(store.queued_ids.is_empty());
    }

    #[tokio::test]
    async fn a_full_queue_releases_inline() {
        let rec = Arc::new(Recorder::default());
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        store.pending.store(MAX_PENDING, Ordering::Relaxed);
        store.release("c", "inline").await.unwrap();
        assert_eq!(released(&rec), vec!["inline"], "sent without the flusher");
    }
}
