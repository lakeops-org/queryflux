pub mod cluster_state;
pub mod simple;
pub mod strategy;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, ClusterName},
};

use cluster_state::ClusterStateSnapshot;

/// Manages all cluster groups: picks the best cluster for a new query,
/// tracks running/queued counts, and exposes live state for the admin API.
#[async_trait]
pub trait ClusterGroupManager: Send + Sync {
    /// Pick the least-loaded healthy cluster in a group.
    /// Returns `None` if the group is at capacity (triggers queueing).
    async fn acquire_cluster(&self, group: &ClusterGroupName) -> Result<Option<ClusterName>>;

    /// Acquire capacity on the exact cluster owning a transaction; never reroute.
    async fn acquire_specific_cluster(
        &self,
        _group: &ClusterGroupName,
        _cluster: &ClusterName,
    ) -> Result<Option<ClusterName>> {
        Err(queryflux_core::error::QueryFluxError::Routing(
            "manager does not support pinned transactions".into(),
        ))
    }

    /// Signal that a query has finished on a cluster (success, failure, or cancel).
    async fn release_cluster(&self, group: &ClusterGroupName, cluster: &ClusterName) -> Result<()>;

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
