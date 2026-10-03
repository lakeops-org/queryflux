use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use queryflux_core::config::CircuitBreakerConfig;
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use serde::{Deserialize, Serialize};

use crate::circuit_breaker::{BackendOutcome, CircuitBreaker, CircuitState};

/// Live mutable state for a single cluster instance.
/// Shared across threads via `Arc`; counters are atomic.
#[derive(Debug)]
pub struct ClusterState {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    /// Postgres `cluster_configs.id` when config is DB-backed; `None` for YAML-only deployments.
    pub cluster_config_id: Option<i64>,
    /// Postgres `cluster_group_configs.id` for this group membership row.
    pub cluster_group_config_id: Option<i64>,
    pub engine_type: EngineType,
    pub endpoint: Option<String>,
    max_running_queries: Arc<AtomicU64>,
    /// Whether this cluster is administratively enabled.
    /// Disabled clusters are excluded from `acquire_cluster`.
    enabled: Arc<AtomicBool>,
    running_queries: Arc<AtomicU64>,
    queued_queries: Arc<AtomicU64>,
    /// Set to `false` by the background health-check loop when the cluster
    /// fails its health check. Starts as `true` (optimistic).
    is_healthy: Arc<AtomicBool>,
    breaker: Option<Arc<CircuitBreaker>>,
}

impl ClusterState {
    /// Constructor mirrors persisted cluster row fields; arity tracks the struct.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        cluster_config_id: Option<i64>,
        cluster_group_config_id: Option<i64>,
        engine_type: EngineType,
        endpoint: Option<String>,
        max_running_queries: u64,
        enabled: bool,
    ) -> Self {
        Self {
            cluster_name,
            group_name,
            cluster_config_id,
            cluster_group_config_id,
            engine_type,
            endpoint,
            max_running_queries: Arc::new(AtomicU64::new(max_running_queries)),
            enabled: Arc::new(AtomicBool::new(enabled)),
            running_queries: Arc::new(AtomicU64::new(0)),
            queued_queries: Arc::new(AtomicU64::new(0)),
            is_healthy: Arc::new(AtomicBool::new(true)),
            breaker: None,
        }
    }

    /// Preserve the breaker through a config reload only while its policy is unchanged.
    pub fn with_circuit_breaker(
        mut self,
        config: Option<CircuitBreakerConfig>,
        previous: Option<&ClusterState>,
    ) -> Self {
        self.breaker = config.map(|config| {
            previous
                .filter(|state| state.group_name == self.group_name)
                .and_then(|state| state.breaker.as_ref())
                .filter(|breaker| breaker.config() == &config)
                .cloned()
                .unwrap_or_else(|| Arc::new(CircuitBreaker::new(config)))
        });
        self
    }

    pub fn breaker_state(&self) -> Option<CircuitState> {
        self.breaker.as_ref().map(|breaker| breaker.state())
    }

    /// True only when both the periodic health check and query breaker allow admission.
    pub fn can_accept_query(&self) -> bool {
        self.is_enabled()
            && self.is_healthy()
            && self
                .breaker
                .as_ref()
                .is_none_or(|breaker| breaker.allows_queries())
    }

    pub fn record_backend_outcome(&self, outcome: BackendOutcome) -> Option<CircuitState> {
        self.breaker
            .as_ref()
            .and_then(|breaker| breaker.record(outcome))
    }

    pub fn begin_breaker_probe(&self) -> bool {
        self.breaker
            .as_ref()
            .is_some_and(|breaker| breaker.begin_probe())
    }

    pub fn finish_breaker_probe(&self, healthy: bool) -> Option<CircuitState> {
        self.breaker
            .as_ref()
            .map(|breaker| breaker.finish_probe(healthy))
    }

    pub fn max_running_queries(&self) -> u64 {
        self.max_running_queries.load(Ordering::Relaxed)
    }

    pub fn set_max_running_queries(&self, value: u64) {
        self.max_running_queries.store(value, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn running_queries(&self) -> u64 {
        self.running_queries.load(Ordering::Relaxed)
    }

    pub fn queued_queries(&self) -> u64 {
        self.queued_queries.load(Ordering::Relaxed)
    }

    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }

    /// Called by the background health-check loop.
    pub fn set_healthy(&self, healthy: bool) {
        self.is_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Overwrite the running query counter with a ground-truth value from the engine.
    /// Called by the background reconciler. Clamped to max_running_queries to stay sane.
    pub fn set_running_queries(&self, count: u64) {
        let clamped = count.min(self.max_running_queries());
        self.running_queries.store(clamped, Ordering::Relaxed);
    }

    pub fn set_queued_queries(&self, count: u64) {
        self.queued_queries.store(count, Ordering::Relaxed);
    }

    pub fn try_increment_running(&self) -> bool {
        let mut current = self.running_queries.load(Ordering::Relaxed);

        loop {
            let max = self.max_running_queries.load(Ordering::Relaxed);

            if current >= max {
                return false;
            }

            match self.running_queries.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Never wraps: extra releases (e.g. after restart when persistence still has executing rows
    /// but in-memory counters were reset) must not underflow `u64`.
    pub fn decrement_running(&self) {
        let mut current = self.running_queries.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return;
            }
            match self.running_queries.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(c) => current = c,
            }
        }
    }

    pub fn increment_queued(&self) {
        self.queued_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_queued(&self) {
        let mut current = self.queued_queries.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return;
            }
            match self.queued_queries.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(c) => current = c,
            }
        }
    }

    pub fn is_at_capacity(&self) -> bool {
        self.running_queries() >= self.max_running_queries()
    }

    pub fn snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot {
            cluster_name: self.cluster_name.clone(),
            group_name: self.group_name.clone(),
            cluster_config_id: self.cluster_config_id,
            cluster_group_config_id: self.cluster_group_config_id,
            engine_type: self.engine_type.clone(),
            endpoint: self.endpoint.clone(),
            running_queries: self.running_queries(),
            queued_queries: self.queued_queries(),
            max_running_queries: self.max_running_queries(),
            is_healthy: self.is_healthy(),
            enabled: self.is_enabled(),
            breaker_state: self.breaker_state(),
            can_accept_query: self.can_accept_query(),
        }
    }
}

/// A point-in-time read of cluster state, safe to serialize and send over the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub cluster_config_id: Option<i64>,
    pub cluster_group_config_id: Option<i64>,
    pub engine_type: EngineType,
    /// The HTTP endpoint of the cluster (e.g. `http://trino-1:8080`).
    pub endpoint: Option<String>,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    /// Whether the most recent health check passed.
    pub is_healthy: bool,
    /// Whether this cluster is administratively enabled.
    pub enabled: bool,
    /// `None` when this group's breaker is disabled.
    #[serde(default)]
    pub breaker_state: Option<CircuitState>,
    /// Whether selection can admit a query before checking capacity.
    pub can_accept_query: bool,
}

#[cfg(test)]
mod capacity_race_probe {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn concurrent_capacity_reservations_do_not_oversubscribe() {
        const WORKERS: usize = 32;

        let state = Arc::new(ClusterState::new(
            ClusterName("test-cluster".to_string()),
            ClusterGroupName("test-group".to_string()),
            None,
            None,
            EngineType::Trino,
            None,
            1,
            true,
        ));

        let barrier = Arc::new(Barrier::new(WORKERS));
        let mut handles = Vec::new();

        for _ in 0..WORKERS {
            let state = Arc::clone(&state);
            let barrier = Arc::clone(&barrier);

            handles.push(std::thread::spawn(move || {
                barrier.wait();
                state.try_increment_running()
            }));
        }

        let successful = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|success| *success)
            .count();

        assert_eq!(successful, 1);
        assert_eq!(state.running_queries(), 1);
    }
}

#[cfg(test)]
mod circuit_breaker_tests {
    use super::*;

    #[test]
    fn reload_preserves_state_only_within_the_same_group_and_policy() {
        fn state(group: &str) -> ClusterState {
            ClusterState::new(
                ClusterName("shared".into()),
                ClusterGroupName(group.into()),
                None,
                None,
                EngineType::Trino,
                None,
                1,
                true,
            )
        }
        let config = CircuitBreakerConfig {
            min_requests: 1,
            ..CircuitBreakerConfig::default()
        };
        let previous = state("first").with_circuit_breaker(Some(config.clone()), None);
        previous.record_backend_outcome(BackendOutcome::Failure);
        assert_eq!(previous.breaker_state(), Some(CircuitState::Open));
        assert_eq!(
            state("first")
                .with_circuit_breaker(Some(config.clone()), Some(&previous))
                .breaker_state(),
            Some(CircuitState::Open)
        );
        assert_eq!(
            state("second")
                .with_circuit_breaker(Some(config), Some(&previous))
                .breaker_state(),
            Some(CircuitState::Closed)
        );
    }
}
