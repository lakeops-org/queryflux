pub mod buffered_store;
pub mod prometheus_store;

use std::sync::Arc;

use async_trait::async_trait;

use queryflux_core::error::Result;

// `MetricsStore`, `QueryRecord`, and `ClusterSnapshot` now live in
// `queryflux-persistence` so that a complete persistence backend only needs
// to depend on one crate.  Re-export them here so all existing call sites
// (`use queryflux_metrics::{MetricsStore, QueryRecord, ...}`) continue to
// compile without any changes.
pub use queryflux_core::query::GuardAction;
pub use queryflux_persistence::{ClusterSnapshot, MetricsStore, QueryRecord};

/// Fans out to multiple stores. Useful for combining Prometheus (real-time)
/// with Postgres (historical) without changing callers.
pub struct MultiMetricsStore {
    stores: Vec<Arc<dyn MetricsStore>>,
}

impl MultiMetricsStore {
    pub fn new(stores: Vec<Arc<dyn MetricsStore>>) -> Self {
        Self { stores }
    }
}

#[async_trait]
impl MetricsStore for MultiMetricsStore {
    fn on_query_started(&self, group: &str, cluster: &str) {
        for s in &self.stores {
            s.on_query_started(group, cluster);
        }
    }

    fn on_query_finished(&self, group: &str, cluster: &str) {
        for s in &self.stores {
            s.on_query_finished(group, cluster);
        }
    }

    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        for s in &self.stores {
            if let Err(e) = s.record_query(record.clone()).await {
                tracing::warn!("MetricsStore::record_query error: {e}");
            }
        }
        Ok(())
    }

    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()> {
        for s in &self.stores {
            if let Err(e) = s.record_cluster_snapshot(snapshot.clone()).await {
                tracing::warn!("MetricsStore::record_cluster_snapshot error: {e}");
            }
        }
        Ok(())
    }
}

/// Discards all metrics — default for deployments that don't need the UI.
pub struct NoopMetricsStore;

#[async_trait]
impl MetricsStore for NoopMetricsStore {
    async fn record_query(&self, _record: QueryRecord) -> Result<()> {
        Ok(())
    }
    async fn record_cluster_snapshot(&self, _snapshot: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}
