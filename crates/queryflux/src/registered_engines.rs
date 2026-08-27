//! Single place to register backend adapters for the `queryflux` binary:
//! descriptors for validation / admin API and dispatch to per-adapter factories.

use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_core::config::ClusterConfig;
use queryflux_core::engine_registry::EngineDescriptor;
use queryflux_core::error::QueryFluxError;
use queryflux_core::query::{ClusterGroupName, ClusterName};
use queryflux_engine_adapters::adbc::AdbcFactory;
use queryflux_engine_adapters::athena::AthenaFactory;
use queryflux_engine_adapters::clickhouse::ClickHouseFactory;
use queryflux_engine_adapters::duckdb::http::DuckDbHttpFactory;
use queryflux_engine_adapters::duckdb::DuckDbFactory;
use queryflux_engine_adapters::starrocks::StarRocksFactory;
use queryflux_engine_adapters::trino::TrinoFactory;
use queryflux_engine_adapters::{AdapterKind, EngineAdapterFactory};

/// The built-in engine adapter factories the shipped `queryflux` binary ships with.
/// Merged into a [`crate::PluginRegistry`] by `.with_builtin_plugins()`; embedders that
/// want a custom set of engines can skip this and register their own factories instead.
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

/// Descriptors for every factory in `factories`, for [`queryflux_core::engine_registry::EngineRegistry`].
pub fn all_descriptors(factories: &[Arc<dyn EngineAdapterFactory>]) -> Vec<EngineDescriptor> {
    factories.iter().map(|f| f.descriptor()).collect()
}

fn map_qf_err(e: QueryFluxError) -> anyhow::Error {
    anyhow::Error::new(e)
}

fn find_factory<'a>(
    factories: &'a [Arc<dyn EngineAdapterFactory>],
    engine_key: &str,
) -> Result<&'a Arc<dyn EngineAdapterFactory>> {
    factories
        .iter()
        .find(|f| f.engine_key() == engine_key)
        .ok_or_else(|| anyhow::anyhow!("Unknown engine key: '{engine_key}'"))
}

/// Build an adapter directly from a DB record's engine key + config JSON blob.
///
/// This is the DB load path: `JSONB -> adapter`, bypassing the `ClusterConfig` god struct.
/// Looks up the matching [`EngineAdapterFactory`] in `factories` by `engine_key` — the
/// built-in list, an embedder's registered extras, or both, depending on the caller.
pub async fn build_adapter_from_record(
    factories: &[Arc<dyn EngineAdapterFactory>],
    cluster_name: ClusterName,
    group: ClusterGroupName,
    engine_key: &str,
    config_json: &serde_json::Value,
) -> Result<AdapterKind> {
    let factory = find_factory(factories, engine_key)?;
    factory
        .build_from_config_json(cluster_name, group, config_json)
        .await
        .map_err(map_qf_err)
}

/// Build an adapter for `cluster_cfg`. `cluster_name_str` is used only in error context messages.
///
/// This is the YAML load path: `ClusterConfig -> adapter`, dispatched purely through the
/// matching [`EngineAdapterFactory`] (by the same `engine_key` the DB path uses) — no
/// hardcoded per-engine match here, so built-in and embedder-registered engines alike
/// go through one construction path.
pub async fn build_adapter(
    factories: &[Arc<dyn EngineAdapterFactory>],
    cluster_name: ClusterName,
    placeholder_group: ClusterGroupName,
    cluster_cfg: &ClusterConfig,
    cluster_name_str: &str,
) -> Result<AdapterKind> {
    let engine = cluster_cfg.engine.as_ref().context(format!(
        "cluster '{cluster_name_str}' missing required 'engine' field"
    ))?;
    let engine_key = queryflux_core::engine_registry::engine_key(engine);
    let factory = find_factory(factories, engine_key)?;
    factory
        .build_from_cluster_config(
            cluster_name,
            placeholder_group,
            cluster_cfg,
            cluster_name_str,
        )
        .await
        .map_err(map_qf_err)
}
