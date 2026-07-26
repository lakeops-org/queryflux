//! Single place to register backend adapters for the `queryflux` binary:
//! descriptors for validation / admin API and dispatch to per-adapter factories.

use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_core::config::{ClusterConfig, EngineConfig};
use queryflux_core::engine_registry::EngineDescriptor;
use queryflux_core::error::QueryFluxError;
use queryflux_core::query::{ClusterGroupName, ClusterName};
use queryflux_engine_adapters::adbc::AdbcFactory;
use queryflux_engine_adapters::athena::{AthenaAdapter, AthenaConfig, AthenaFactory};
use queryflux_engine_adapters::clickhouse::{
    ClickHouseAdapter, ClickHouseConfig, ClickHouseFactory,
};
use queryflux_engine_adapters::duckdb::http::{
    DuckDbHttpAdapter, DuckDbHttpConfig, DuckDbHttpFactory,
};
use queryflux_engine_adapters::duckdb::{DuckDbAdapter, DuckDbConfig, DuckDbFactory};
use queryflux_engine_adapters::starrocks::{StarRocksAdapter, StarRocksConfig, StarRocksFactory};
use queryflux_engine_adapters::trino::{TrinoAdapter, TrinoConfig, TrinoFactory};
use queryflux_engine_adapters::{AdapterKind, EngineAdapterFactory, EngineConfigParseable};

/// All registered engine adapter factories.
///
/// Adding a new engine means adding its factory here — the rest is driven by
/// the [`EngineAdapterFactory`] trait.
pub fn all_factories() -> Vec<Box<dyn EngineAdapterFactory>> {
    vec![
        Box::new(TrinoFactory),
        Box::new(DuckDbFactory),
        Box::new(DuckDbHttpFactory),
        Box::new(StarRocksFactory),
        Box::new(ClickHouseFactory),
        Box::new(AthenaFactory),
        Box::new(AdbcFactory),
    ]
}

/// All engine descriptors for [`queryflux_core::engine_registry::EngineRegistry`].
pub fn all_descriptors() -> Vec<EngineDescriptor> {
    all_factories().iter().map(|f| f.descriptor()).collect()
}

fn map_qf_err(e: QueryFluxError) -> anyhow::Error {
    anyhow::Error::new(e)
}

/// Build an adapter directly from a DB record's engine key + config JSON blob.
///
/// This is the DB load path: `JSONB -> adapter`, bypassing the `ClusterConfig` god struct.
/// Looks up the matching [`EngineAdapterFactory`] by `engine_key`.
pub async fn build_adapter_from_record(
    cluster_name: ClusterName,
    group: ClusterGroupName,
    engine_key: &str,
    config_json: &serde_json::Value,
) -> Result<AdapterKind> {
    let factories = all_factories();
    let factory = factories
        .iter()
        .find(|f| f.engine_key() == engine_key)
        .ok_or_else(|| anyhow::anyhow!("Unknown engine key: '{engine_key}'"))?;

    factory
        .build_from_config_json(cluster_name, group, config_json)
        .await
        .map_err(map_qf_err)
}

/// Build an adapter for `cluster_cfg`. `cluster_name_str` is used only in error context messages.
///
/// This is the YAML load path: `ClusterConfig -> adapter`. Kept for backward compatibility.
pub async fn build_adapter(
    cluster_name: ClusterName,
    placeholder_group: ClusterGroupName,
    cluster_cfg: &ClusterConfig,
    cluster_name_str: &str,
) -> Result<AdapterKind> {
    let engine = cluster_cfg.engine.as_ref().context(format!(
        "cluster '{cluster_name_str}' missing required 'engine' field"
    ))?;

    let adapter: AdapterKind = match engine {
        EngineConfig::Trino => {
            let config = TrinoConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Async(Arc::new(TrinoAdapter::new(
                cluster_name,
                placeholder_group,
                config,
            )))
        }
        EngineConfig::DuckDb => {
            let config = DuckDbConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Sync(Arc::new(
                DuckDbAdapter::new(cluster_name, placeholder_group, config).map_err(map_qf_err)?,
            ))
        }
        EngineConfig::DuckDbHttp => {
            let config = DuckDbHttpConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Sync(Arc::new(
                DuckDbHttpAdapter::new(cluster_name, placeholder_group, config)
                    .map_err(map_qf_err)?,
            ))
        }
        EngineConfig::StarRocks => {
            let config = StarRocksConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Sync(Arc::new(
                StarRocksAdapter::new(cluster_name, placeholder_group, config)
                    .map_err(map_qf_err)?,
            ))
        }
        EngineConfig::Athena => {
            let config = AthenaConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Async(Arc::new(
                AthenaAdapter::new(cluster_name, placeholder_group, config)
                    .await
                    .map_err(map_qf_err)?,
            ))
        }
        EngineConfig::ClickHouse => {
            let config = ClickHouseConfig::from_cluster_config(cluster_cfg, cluster_name_str)
                .map_err(map_qf_err)?;
            AdapterKind::Sync(Arc::new(
                ClickHouseAdapter::new(cluster_name, placeholder_group, config)
                    .map_err(map_qf_err)?,
            ))
        }
        EngineConfig::Adbc => {
            anyhow::bail!("ADBC clusters must be created via the admin API, not YAML config")
        }
    };

    Ok(adapter)
}
