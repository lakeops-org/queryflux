use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName},
};

use crate::{
    circuit_breaker::{BackendOutcome, CircuitState},
    cluster_state::{ClusterState, ClusterStateSnapshot},
    strategy::{ClusterCandidate, ClusterSelectionStrategy},
    ClusterGroupManager,
};

type ManagedGroup = (Vec<Arc<ClusterState>>, Arc<dyn ClusterSelectionStrategy>);

pub struct SimpleClusterGroupManager {
    /// group → (ordered cluster states, selection strategy)
    groups: HashMap<ClusterGroupName, ManagedGroup>,
}

impl SimpleClusterGroupManager {
    pub fn new(groups: HashMap<ClusterGroupName, ManagedGroup>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl ClusterGroupManager for SimpleClusterGroupManager {
    async fn acquire_cluster(&self, group: &ClusterGroupName) -> Result<Option<ClusterName>> {
        let (clusters, strategy) = self
            .groups
            .get(group)
            .ok_or_else(|| QueryFluxError::NoClusterGroupAvailable(group.0.clone()))?;

        // Build the eligible candidate list (healthy + enabled + under capacity).
        let eligible: Vec<(usize, &Arc<ClusterState>)> = clusters
            .iter()
            .enumerate()
            .filter(|(_, c)| c.can_accept_query() && !c.is_at_capacity())
            .collect();

        if eligible.is_empty() {
            return Ok(None);
        }

        let candidates: Vec<ClusterCandidate<'_>> = eligible
            .iter()
            .map(|(_, c)| ClusterCandidate {
                name: c.cluster_name.0.as_str(),
                engine_type: c.engine_type.clone(),
                running_queries: c.running_queries(),
                max_running_queries: c.max_running_queries(),
            })
            .collect();

        let picked_local_idx = if strategy.requires_blocking_dispatch() {
            // Strategies like the Python script one may block the thread (e.g. holding
            // the GIL) — run them off the async runtime rather than calling inline.
            let owned: Vec<(String, queryflux_core::query::EngineType, u64, u64)> = candidates
                .iter()
                .map(|c| {
                    (
                        c.name.to_string(),
                        c.engine_type.clone(),
                        c.running_queries,
                        c.max_running_queries,
                    )
                })
                .collect();
            let strategy = Arc::clone(strategy);
            tokio::task::spawn_blocking(move || {
                let candidates: Vec<ClusterCandidate<'_>> = owned
                    .iter()
                    .map(|(name, engine_type, running, max)| ClusterCandidate {
                        name: name.as_str(),
                        engine_type: engine_type.clone(),
                        running_queries: *running,
                        max_running_queries: *max,
                    })
                    .collect();
                strategy.pick(&candidates)
            })
            .await
            .map_err(|e| QueryFluxError::Routing(format!("spawn_blocking error: {e}")))?
            .unwrap_or(0)
        } else {
            strategy.pick(&candidates).unwrap_or(0)
        };
        let (_, chosen) = eligible[picked_local_idx];
        // A blocking-dispatch strategy yields at the `.await` above; another task can
        // fill `chosen` to capacity or a health check can mark it unhealthy in that
        // window. Re-validate before admitting the query, falling back to any other
        // still-eligible member rather than trusting a snapshot that may be stale.
        if chosen.can_accept_query() && chosen.try_increment_running() {
            if !chosen.can_accept_query() {
                chosen.decrement_running();
            } else {
                return Ok(Some(chosen.cluster_name.clone()));
            }
        }

        for (_, candidate) in &eligible {
            if candidate.can_accept_query() && candidate.try_increment_running() {
                if candidate.can_accept_query() {
                    return Ok(Some(candidate.cluster_name.clone()));
                }
                candidate.decrement_running();
            }
        }

        Ok(None)
    }

    async fn release_cluster(&self, group: &ClusterGroupName, cluster: &ClusterName) -> Result<()> {
        if let Some((clusters, _)) = self.groups.get(group) {
            if let Some(state) = clusters.iter().find(|c| &c.cluster_name == cluster) {
                state.decrement_running();
            }
        }
        Ok(())
    }

    fn record_backend_outcome(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
        outcome: BackendOutcome,
    ) {
        if let Some((clusters, _)) = self.groups.get(group) {
            if let Some(state) = clusters.iter().find(|c| &c.cluster_name == cluster) {
                if state.record_backend_outcome(outcome) == Some(CircuitState::Open) {
                    tracing::warn!(group = %group, cluster = %cluster, "Cluster circuit breaker opened");
                }
            }
        }
    }

    async fn cluster_state(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
    ) -> Result<Option<ClusterStateSnapshot>> {
        Ok(self
            .groups
            .get(group)
            .and_then(|(cs, _)| cs.iter().find(|c| &c.cluster_name == cluster))
            .map(|c| c.snapshot()))
    }

    async fn all_cluster_states(&self) -> Result<Vec<ClusterStateSnapshot>> {
        Ok(self
            .groups
            .values()
            .flat_map(|(cs, _)| cs.iter().map(|c| c.snapshot()))
            .collect())
    }

    async fn update_cluster(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
        enabled: Option<bool>,
        max_running_queries: Option<u64>,
    ) -> Result<bool> {
        let Some((clusters, _)) = self.groups.get(group) else {
            return Ok(false);
        };
        let Some(state) = clusters.iter().find(|c| &c.cluster_name == cluster) else {
            return Ok(false);
        };
        if let Some(v) = enabled {
            state.set_enabled(v);
        }
        if let Some(v) = max_running_queries {
            state.set_max_running_queries(v);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod circuit_breaker_tests {
    use super::*;
    use crate::strategy::FailoverStrategy;
    use queryflux_core::{config::CircuitBreakerConfig, query::EngineType};

    #[tokio::test]
    async fn open_member_is_skipped_by_failover_selection() {
        let group = ClusterGroupName("analytics".into());
        let broken = Arc::new(
            ClusterState::new(
                ClusterName("primary".into()),
                group.clone(),
                None,
                None,
                EngineType::Trino,
                None,
                2,
                true,
            )
            .with_circuit_breaker(
                Some(CircuitBreakerConfig {
                    min_requests: 1,
                    initial_backoff_secs: 1,
                    ..CircuitBreakerConfig::default()
                }),
                None,
            ),
        );
        let fallback = Arc::new(ClusterState::new(
            ClusterName("secondary".into()),
            group.clone(),
            None,
            None,
            EngineType::Trino,
            None,
            2,
            true,
        ));
        let manager = SimpleClusterGroupManager::new(HashMap::from([(
            group.clone(),
            (
                vec![broken.clone(), fallback],
                Arc::new(FailoverStrategy) as Arc<dyn ClusterSelectionStrategy>,
            ),
        )]));

        assert_eq!(
            manager.acquire_cluster(&group).await.unwrap(),
            Some(ClusterName("primary".into()))
        );
        manager
            .release_cluster(&group, &ClusterName("primary".into()))
            .await
            .unwrap();
        manager.record_backend_outcome(
            &group,
            &ClusterName("primary".into()),
            BackendOutcome::Failure,
        );
        assert_eq!(broken.breaker_state(), Some(CircuitState::Open));
        assert_eq!(
            manager.acquire_cluster(&group).await.unwrap(),
            Some(ClusterName("secondary".into()))
        );
        manager
            .release_cluster(&group, &ClusterName("secondary".into()))
            .await
            .unwrap();
        assert!(!broken.can_accept_query());
    }
}
