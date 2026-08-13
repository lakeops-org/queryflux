//! Seed Postgres cluster/group rows from YAML only when the name is missing.

use std::collections::HashMap;

use queryflux_core::{
    config::{ClusterConfig, ClusterGroupConfig},
    error::{QueryFluxError, Result},
};

use crate::cluster_config::{UpsertClusterConfig, UpsertClusterGroupConfig};
use crate::ClusterConfigStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YamlSeedReport {
    pub seeded: usize,
    pub existing_before: usize,
}

pub async fn seed_clusters_from_yaml_if_missing(
    pg: &dyn ClusterConfigStore,
    clusters: &HashMap<String, ClusterConfig>,
) -> Result<YamlSeedReport> {
    let existing_before = pg.list_cluster_configs().await?.len();
    let mut seeded = 0usize;
    for (name, cfg) in clusters {
        match UpsertClusterConfig::from_core(cfg) {
            Ok(Some(upsert)) => {
                if pg.insert_cluster_config_if_missing(name, &upsert).await? {
                    seeded += 1;
                }
            }
            Ok(None) => {}
            Err(e) => {
                return Err(QueryFluxError::Persistence(format!(
                    "cluster '{name}': serializing for YAML seed: {e}"
                )));
            }
        }
    }
    Ok(YamlSeedReport {
        seeded,
        existing_before,
    })
}

pub async fn seed_groups_from_yaml_if_missing(
    pg: &dyn ClusterConfigStore,
    groups: &HashMap<String, ClusterGroupConfig>,
) -> Result<YamlSeedReport> {
    let existing_before = pg.list_group_configs().await?.len();
    let mut seeded = 0usize;
    for (name, cfg) in groups {
        if pg
            .insert_group_config_if_missing(name, &UpsertClusterGroupConfig::from_core(cfg))
            .await?
        {
            seeded += 1;
        }
    }
    Ok(YamlSeedReport {
        seeded,
        existing_before,
    })
}

#[cfg(test)]
mod tests {
    use queryflux_core::config::{ClusterConfig, ClusterGroupConfig};

    use crate::cluster_config::UpsertClusterGroupConfig;
    use crate::in_memory::InMemoryPersistence;
    use crate::ClusterConfigStore;

    use super::*;

    fn trino_cluster(endpoint: &str, pool_size: u64) -> ClusterConfig {
        serde_json::from_value(serde_json::json!({
            "engine": "trino",
            "endpoint": endpoint,
            "poolSize": pool_size,
        }))
        .unwrap()
    }

    fn studio_trino_upsert(pool_size: u64) -> UpsertClusterConfig {
        UpsertClusterConfig {
            engine_key: "trino".into(),
            enabled: true,
            max_running_queries: None,
            config: serde_json::json!({
                "endpoint": "http://studio-trino:8080",
                "poolSize": pool_size,
            }),
        }
    }

    fn yaml_group(max_running: u64, members: &[&str]) -> ClusterGroupConfig {
        serde_json::from_value(serde_json::json!({
            "members": members,
            "maxRunningQueries": max_running,
        }))
        .unwrap()
    }

    fn studio_group_upsert(max_running: i64) -> UpsertClusterGroupConfig {
        UpsertClusterGroupConfig {
            enabled: true,
            members: vec!["trino".into()],
            max_running_queries: max_running,
            max_queued_queries: None,
            strategy: None,
            allow_groups: vec![],
            allow_users: vec![],
            translation_script_ids: vec![],
            default_tags: serde_json::json!({}),
            cache: None,
        }
    }

    #[tokio::test]
    async fn seeds_missing_cluster_on_empty_store() {
        let store = InMemoryPersistence::new();
        let mut yaml = HashMap::new();
        yaml.insert("trino".into(), trino_cluster("http://yaml-trino:8080", 4));

        let report = seed_clusters_from_yaml_if_missing(&store, &yaml)
            .await
            .unwrap();
        assert_eq!(report.seeded, 1);
        assert_eq!(report.existing_before, 0);

        let rows = store.list_cluster_configs().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].config.get("poolSize"), Some(&serde_json::json!(4)));
    }

    #[tokio::test]
    async fn yaml_seed_skips_existing_cluster_names() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(8))
            .await
            .unwrap();

        let mut yaml = HashMap::new();
        yaml.insert("trino".into(), trino_cluster("http://yaml-trino:8080", 4));
        yaml.insert(
            "analytics".into(),
            trino_cluster("http://analytics:8080", 2),
        );

        let report = seed_clusters_from_yaml_if_missing(&store, &yaml)
            .await
            .unwrap();
        assert_eq!(report.seeded, 1);
        assert_eq!(report.existing_before, 1);

        let rows = store.list_cluster_configs().await.unwrap();
        assert_eq!(rows.len(), 2);
        let trino = rows.iter().find(|r| r.name == "trino").unwrap();
        assert_eq!(trino.config.get("poolSize"), Some(&serde_json::json!(8)));
    }

    #[tokio::test]
    async fn insert_cluster_config_if_missing_does_not_overwrite() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(8))
            .await
            .unwrap();

        let yaml_upsert = UpsertClusterConfig::from_core(&trino_cluster("http://yaml:8080", 4))
            .unwrap()
            .unwrap();
        assert!(!store
            .insert_cluster_config_if_missing("trino", &yaml_upsert)
            .await
            .unwrap());

        let row = store.get_cluster_config("trino").await.unwrap().unwrap();
        assert_eq!(row.config.get("poolSize"), Some(&serde_json::json!(8)));
    }

    #[tokio::test]
    async fn seeds_missing_group_on_empty_store() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(4))
            .await
            .unwrap();

        let mut yaml = HashMap::new();
        yaml.insert("default".into(), yaml_group(10, &["trino"]));

        let report = seed_groups_from_yaml_if_missing(&store, &yaml)
            .await
            .unwrap();
        assert_eq!(report.seeded, 1);
        assert_eq!(report.existing_before, 0);

        let rows = store.list_group_configs().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].max_running_queries, 10);
    }

    #[tokio::test]
    async fn yaml_seed_skips_existing_group_names() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(4))
            .await
            .unwrap();
        store
            .upsert_group_config("default", &studio_group_upsert(20))
            .await
            .unwrap();

        let mut yaml = HashMap::new();
        yaml.insert("default".into(), yaml_group(10, &["trino"]));
        yaml.insert("analytics".into(), yaml_group(5, &["trino"]));

        let report = seed_groups_from_yaml_if_missing(&store, &yaml)
            .await
            .unwrap();
        assert_eq!(report.seeded, 1);
        assert_eq!(report.existing_before, 1);

        let rows = store.list_group_configs().await.unwrap();
        assert_eq!(rows.len(), 2);
        let default = rows.iter().find(|r| r.name == "default").unwrap();
        assert_eq!(default.max_running_queries, 20);
    }

    #[tokio::test]
    async fn insert_group_config_if_missing_does_not_overwrite() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(4))
            .await
            .unwrap();
        store
            .upsert_group_config("default", &studio_group_upsert(20))
            .await
            .unwrap();

        let yaml_upsert = UpsertClusterGroupConfig::from_core(&yaml_group(10, &["trino"]));
        assert!(!store
            .insert_group_config_if_missing("default", &yaml_upsert)
            .await
            .unwrap());

        let row = store.get_group_config("default").await.unwrap().unwrap();
        assert_eq!(row.max_running_queries, 20);
    }

    #[tokio::test]
    async fn insert_group_config_if_missing_skips_stale_yaml_when_group_exists() {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("trino", &studio_trino_upsert(4))
            .await
            .unwrap();
        store
            .upsert_group_config("default", &studio_group_upsert(20))
            .await
            .unwrap();

        let stale_yaml = UpsertClusterGroupConfig {
            enabled: true,
            members: vec!["removed-cluster".into()],
            max_running_queries: 10,
            max_queued_queries: None,
            strategy: None,
            allow_groups: vec![],
            allow_users: vec![],
            translation_script_ids: vec![],
            default_tags: serde_json::json!({}),
            cache: None,
        };
        assert!(!store
            .insert_group_config_if_missing("default", &stale_yaml)
            .await
            .unwrap());
    }
}
