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
