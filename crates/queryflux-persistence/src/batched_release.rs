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
    metrics: Arc<dyn MetricsStore>,
}

impl BatchedReleaseStore {
    /// Wrap `inner` and start the flusher. Must be called inside a Tokio runtime. The
    /// flusher stops when the store is dropped, after sending what is queued.
    pub fn new(inner: Arc<dyn CapacityStore>, metrics: Arc<dyn MetricsStore>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicUsize::new(0));
        tokio::spawn(flusher(inner.clone(), rx, pending.clone(), metrics.clone()));
        Self {
            inner,
            tx,
            pending,
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

async fn flusher(
    inner: Arc<dyn CapacityStore>,
    mut rx: mpsc::UnboundedReceiver<Msg>,
    pending: Arc<AtomicUsize>,
    metrics: Arc<dyn MetricsStore>,
) {
    let mut batch: Vec<(String, String)> = Vec::new();
    let mut flushes: Vec<oneshot::Sender<()>> = Vec::new();
    while let Some(first) = rx.recv().await {
        let mut next = Some(first);
        // Take whatever queued up while the previous batch was being sent.
        while let Some(msg) = next.take().or_else(|| rx.try_recv().ok()) {
            match msg {
                Msg::Release(cluster, query_id) => batch.push((cluster, query_id)),
                Msg::Flush(done) => flushes.push(done),
            }
            if batch.len() >= MAX_BATCH {
                break;
            }
        }
        let mut delay = RETRY_MIN;
        while !batch.is_empty() {
            match inner.release_many(&batch).await {
                Ok(()) => {
                    pending.fetch_sub(batch.len(), Ordering::Relaxed);
                    batch.clear();
                }
                Err(e) => {
                    metrics.on_coordination_failure("capacity_release");
                    tracing::warn!(
                        leases = batch.len(),
                        "Capacity release failed, retrying in {delay:?}: {e}"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(RETRY_MAX);
                }
            }
        }
        // Messages are read in order, so every release queued before a flush message read
        // in this round was in this batch or an earlier one: all sent now.
        for done in flushes.drain(..) {
            let _ = done.send(());
        }
    }
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
        self.inner
            .try_acquire(cluster_name, max_running_queries, instance_id, query_id)
            .await
    }

    async fn release(&self, cluster_name: &str, query_id: &str) -> Result<()> {
        if self.pending.load(Ordering::Relaxed) < MAX_PENDING {
            self.pending.fetch_add(1, Ordering::Relaxed);
            if self
                .tx
                .send(Msg::Release(cluster_name.to_string(), query_id.to_string()))
                .is_ok()
            {
                return Ok(());
            }
            self.pending.fetch_sub(1, Ordering::Relaxed);
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

    #[tokio::test]
    async fn a_full_queue_releases_inline() {
        let rec = Arc::new(Recorder::default());
        let store = BatchedReleaseStore::new(rec.clone(), no_metrics());
        store.pending.store(MAX_PENDING, Ordering::Relaxed);
        store.release("c", "inline").await.unwrap();
        assert_eq!(released(&rec), vec!["inline"], "sent without the flusher");
    }
}
