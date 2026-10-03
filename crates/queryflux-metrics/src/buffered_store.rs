use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;

use queryflux_core::error::Result;

use crate::{ClusterSnapshot, MetricsStore, QueryRecord};

#[allow(clippy::large_enum_variant)]
enum MetricsEvent {
    Query(QueryRecord),
    Snapshot(ClusterSnapshot),
}

/// Metrics store that buffers writes and flushes them to an inner store in the background.
///
/// `record_query` and `record_cluster_snapshot` return immediately — records are sent
/// to a bounded channel and written to the inner store by a background task in batches.
/// If the channel is full, records are silently dropped to avoid blocking query execution.
///
/// Construct via `BufferedMetricsStore::new`; the background flush task is spawned
/// automatically on the current tokio runtime.
pub struct BufferedMetricsStore {
    tx: mpsc::Sender<MetricsEvent>,
    inner: Arc<dyn MetricsStore>,
}

impl BufferedMetricsStore {
    /// Creates a buffered store wrapping `inner`.
    ///
    /// - `batch_size`: flush when this many query records have accumulated.
    /// - `flush_interval`: flush at least this often regardless of batch size.
    pub fn new(inner: Arc<dyn MetricsStore>, batch_size: usize, flush_interval: Duration) -> Self {
        let (tx, rx) = mpsc::channel(10_000);
        tokio::spawn(flush_loop(rx, inner.clone(), batch_size, flush_interval));
        Self { tx, inner }
    }
}

#[async_trait]
impl MetricsStore for BufferedMetricsStore {
    fn on_translation(&self, outcome: queryflux_core::query::TranslationOutcome) {
        self.inner.on_translation(outcome);
    }

    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        let _ = self.tx.try_send(MetricsEvent::Query(record));
        Ok(())
    }

    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()> {
        let _ = self.tx.try_send(MetricsEvent::Snapshot(snapshot));
        Ok(())
    }
}

async fn flush_loop(
    mut rx: mpsc::Receiver<MetricsEvent>,
    inner: Arc<dyn MetricsStore>,
    batch_size: usize,
    flush_interval: Duration,
) {
    let mut queries: Vec<QueryRecord> = Vec::with_capacity(batch_size);
    let mut snapshots: Vec<ClusterSnapshot> = Vec::new();
    let mut ticker = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    None => {
                        // Channel closed — flush remaining records and exit.
                        flush(&inner, &mut queries, &mut snapshots).await;
                        return;
                    }
                    Some(MetricsEvent::Query(r)) => {
                        queries.push(r);
                        if queries.len() >= batch_size {
                            flush(&inner, &mut queries, &mut snapshots).await;
                        }
                    }
                    Some(MetricsEvent::Snapshot(s)) => {
                        snapshots.push(s);
                    }
                }
            }
            _ = ticker.tick() => {
                flush(&inner, &mut queries, &mut snapshots).await;
            }
        }
    }
}

async fn flush(
    inner: &Arc<dyn MetricsStore>,
    queries: &mut Vec<QueryRecord>,
    snapshots: &mut Vec<ClusterSnapshot>,
) {
    for record in queries.drain(..) {
        if let Err(e) = inner.record_query(record).await {
            warn!("Failed to flush query record to metrics store: {e}");
        }
    }
    for snap in snapshots.drain(..) {
        if let Err(e) = inner.record_cluster_snapshot(snap).await {
            warn!("Failed to flush cluster snapshot to metrics store: {e}");
        }
    }
}
