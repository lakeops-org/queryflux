use std::collections::HashMap;
use std::sync::Arc;

use queryflux_cluster_manager::{
    cluster_state::ClusterState,
    simple::SimpleClusterGroupManager,
    strategy::{ClusterSelectionStrategy, PythonScriptStrategy},
    ClusterGroupManager,
};
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};

/// End-to-end check that a Python-scripted strategy runs through
/// `SimpleClusterGroupManager::acquire_cluster` (async, off the runtime via
/// `spawn_blocking`) and that its pick is honored.
#[tokio::test]
async fn acquire_cluster_dispatches_python_strategy_off_the_runtime() {
    let group = ClusterGroupName("g".to_string());

    let a = Arc::new(ClusterState::new(
        ClusterName("cluster-a".to_string()),
        group.clone(),
        None,
        None,
        EngineType::Trino,
        None,
        10,
        true,
    ));
    let b = Arc::new(ClusterState::new(
        ClusterName("cluster-b".to_string()),
        group.clone(),
        None,
        None,
        EngineType::Trino,
        None,
        10,
        true,
    ));

    let script = r#"
def select_cluster(candidates):
    for c in candidates:
        if c["name"] == "cluster-b":
            return c["name"]
    return None
"#;
    let strategy: Arc<dyn ClusterSelectionStrategy> =
        Arc::new(PythonScriptStrategy::new(script.to_string()));

    let mut groups = HashMap::new();
    groups.insert(group.clone(), (vec![a, b], strategy));
    let manager = SimpleClusterGroupManager::new(groups);

    let picked = manager.acquire_cluster(&group).await.unwrap();
    assert_eq!(picked, Some(ClusterName("cluster-b".to_string())));
}

/// A blocking-dispatch strategy that always picks the first candidate, but signals
/// `started` before it "runs" (simulated with a short sleep) so a concurrent task can
/// change cluster state mid-pick — exercising the check-to-use window between building
/// `eligible` and re-validating the chosen cluster after the `spawn_blocking` await.
struct SlowFirstCandidateStrategy {
    started: Arc<tokio::sync::Notify>,
}

impl ClusterSelectionStrategy for SlowFirstCandidateStrategy {
    fn requires_blocking_dispatch(&self) -> bool {
        true
    }

    fn pick(
        &self,
        _candidates: &[queryflux_cluster_manager::strategy::ClusterCandidate<'_>],
    ) -> Option<usize> {
        self.started.notify_one();
        std::thread::sleep(std::time::Duration::from_millis(150));
        Some(0)
    }
}

/// If a cluster is disabled while a blocking-dispatch strategy is still running,
/// `acquire_cluster` must not admit the query to that now-ineligible cluster — it should
/// fall back to another still-eligible member instead of trusting the stale snapshot.
#[tokio::test]
async fn acquire_cluster_revalidates_pick_after_blocking_dispatch() {
    let group = ClusterGroupName("g".to_string());

    let a = Arc::new(ClusterState::new(
        ClusterName("cluster-a".to_string()),
        group.clone(),
        None,
        None,
        EngineType::Trino,
        None,
        10,
        true,
    ));
    let b = Arc::new(ClusterState::new(
        ClusterName("cluster-b".to_string()),
        group.clone(),
        None,
        None,
        EngineType::Trino,
        None,
        10,
        true,
    ));

    let started = Arc::new(tokio::sync::Notify::new());
    let strategy: Arc<dyn ClusterSelectionStrategy> = Arc::new(SlowFirstCandidateStrategy {
        started: started.clone(),
    });

    let mut groups = HashMap::new();
    groups.insert(group.clone(), (vec![a.clone(), b.clone()], strategy));
    let manager = Arc::new(SimpleClusterGroupManager::new(groups));

    let acquire_task = {
        let manager = manager.clone();
        let group = group.clone();
        tokio::spawn(async move { manager.acquire_cluster(&group).await })
    };

    // Wait until pick() has started (candidates already snapshotted), then disable
    // cluster-a — the strategy's chosen candidate — while pick() is still "running".
    started.notified().await;
    a.set_enabled(false);

    let picked = acquire_task.await.unwrap().unwrap();
    assert_eq!(
        picked,
        Some(ClusterName("cluster-b".to_string())),
        "must not admit to cluster-a after it became ineligible mid-pick"
    );
}
