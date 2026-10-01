pub mod circuit_breaker;
pub mod cluster_state;
pub mod simple;
pub mod strategy;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, ClusterName},
};

use circuit_breaker::BackendOutcome;
use cluster_state::ClusterStateSnapshot;

/// Manages all cluster groups: picks the best cluster for a new query,
/// tracks running/queued counts, and exposes live state for the admin API.
#[async_trait]
pub trait ClusterGroupManager: Send + Sync {
    /// Pick the least-loaded healthy cluster in a group.
    /// Returns `None` if the group is at capacity (triggers queueing).
    async fn acquire_cluster(&self, group: &ClusterGroupName) -> Result<Option<ClusterName>>;

    /// Signal that a query has finished on a cluster (success, failure, or cancel).
    async fn release_cluster(&self, group: &ClusterGroupName, cluster: &ClusterName) -> Result<()>;

    /// Report a completed backend request. Caller-side errors must not be sent here.
    /// Managers without a circuit breaker may keep the default no-op implementation.
    fn record_backend_outcome(
        &self,
        _group: &ClusterGroupName,
        _cluster: &ClusterName,
        _outcome: BackendOutcome,
    ) {
    }

    /// Get a snapshot of live state for a specific cluster.
    async fn cluster_state(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
    ) -> Result<Option<ClusterStateSnapshot>>;

    /// Get state for all clusters across all groups.
    async fn all_cluster_states(&self) -> Result<Vec<ClusterStateSnapshot>>;

    /// Update mutable configuration for a specific cluster at runtime.
    /// Returns `true` if the cluster was found and updated, `false` if not found.
    async fn update_cluster(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
        enabled: Option<bool>,
        max_running_queries: Option<u64>,
    ) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ManagerWithoutBreaker;

    #[async_trait]
    impl ClusterGroupManager for ManagerWithoutBreaker {
        async fn acquire_cluster(&self, _group: &ClusterGroupName) -> Result<Option<ClusterName>> {
            Ok(None)
        }

        async fn release_cluster(
            &self,
            _group: &ClusterGroupName,
            _cluster: &ClusterName,
        ) -> Result<()> {
            Ok(())
        }

        async fn cluster_state(
            &self,
            _group: &ClusterGroupName,
            _cluster: &ClusterName,
        ) -> Result<Option<ClusterStateSnapshot>> {
            Ok(None)
        }

        async fn all_cluster_states(&self) -> Result<Vec<ClusterStateSnapshot>> {
            Ok(vec![])
        }

        async fn update_cluster(
            &self,
            _group: &ClusterGroupName,
            _cluster: &ClusterName,
            _enabled: Option<bool>,
            _max_running_queries: Option<u64>,
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[test]
    fn manager_without_breaker_uses_default_backend_outcome_handler() {
        let manager: &dyn ClusterGroupManager = &ManagerWithoutBreaker;
        manager.record_backend_outcome(
            &ClusterGroupName("group".into()),
            &ClusterName("cluster".into()),
            BackendOutcome::Failure,
        );
    }
}
