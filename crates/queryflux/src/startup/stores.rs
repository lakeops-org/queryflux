//! Building the stores a process runs on: persistence, metrics, and (with Postgres)
//! the durable backend and replica coordination.

use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_core::config::ProxyConfig;
use queryflux_metrics::{
    buffered_store::BufferedMetricsStore, prometheus_store::PrometheusMetrics, MetricsStore,
    MultiMetricsStore,
};
use queryflux_persistence::{
    in_memory::InMemoryPersistence, postgres::PostgresStore, BackendStore, CoordinationStore,
    DistributedBackendStore,
};
use tracing::info;

pub(crate) struct Stores {
    pub(crate) prometheus: Arc<PrometheusMetrics>,
    pub(crate) persistence: Arc<dyn queryflux_persistence::Persistence>,
    pub(crate) metrics: Arc<dyn MetricsStore>,
    /// The in-memory store, in in-memory mode (it also backs the admin API there).
    pub(crate) mem_store: Option<Arc<InMemoryPersistence>>,
    /// The durable backend; `None` in in-memory mode.
    pub(crate) backend: Option<Arc<dyn BackendStore>>,
    pub(crate) distributed_backend: Option<Arc<dyn DistributedBackendStore>>,
    pub(crate) coordination: Option<Arc<dyn CoordinationStore>>,
}

/// When Postgres is configured we seed cluster/group config on first run and read from
/// the DB on subsequent starts, so persistence must be ready before config is compiled.
pub(crate) async fn build_stores(config: &ProxyConfig) -> Result<Stores> {
    // When Postgres is configured we seed cluster/group config on first run and read
    // from the DB on subsequent starts, so persistence must be ready before the
    // two-pass cluster/adapter construction below.
    let prometheus = Arc::new(
        PrometheusMetrics::new_with_deny_list(config.queryflux.metrics.tags_deny_list.clone())
            .context("Failed to init Prometheus metrics")?,
    );
    let mut pg_store: Option<Arc<PostgresStore>> = None;
    let mut mem_store: Option<Arc<InMemoryPersistence>> = None;

    let (persistence, metrics): (
        Arc<dyn queryflux_persistence::Persistence>,
        Arc<dyn MetricsStore>,
    ) = match &config.queryflux.persistence {
        queryflux_core::config::PersistenceConfig::Postgres { conn } => {
            let url = conn
                .connection_url()
                .map_err(|m| anyhow::anyhow!("Invalid postgres persistence config: {m}"))?;
            let pg = Arc::new(
                PostgresStore::connect_from_config(&url, conn)
                    .await
                    .context("Failed to connect to Postgres")?,
            );
            if conn.auto_migrate {
                pg.migrate().await.context("Migration failed")?;
            } else {
                info!(
                    "persistence.autoMigrate=false — skipping startup migrations \
                     (apply with `queryflux migrate` or a Job)"
                );
            }
            let buffered = Arc::new(BufferedMetricsStore::new(
                pg.clone() as Arc<dyn MetricsStore>,
                100,
                std::time::Duration::from_secs(5),
            ));
            let metrics = Arc::new(MultiMetricsStore::new(vec![
                prometheus.clone() as Arc<dyn MetricsStore>,
                buffered as Arc<dyn MetricsStore>,
            ]));
            pg_store = Some(pg.clone());
            (
                pg as Arc<dyn queryflux_persistence::Persistence>,
                metrics as Arc<dyn MetricsStore>,
            )
        }
        queryflux_core::config::PersistenceConfig::Redis { url } => {
            anyhow::bail!(
                "persistence.type = redis is not implemented (configured url: {url}). \
                 Use type: postgres or type: inMemory."
            );
        }
        queryflux_core::config::PersistenceConfig::InMemory => {
            let mem = Arc::new(InMemoryPersistence::new());
            mem_store = Some(mem.clone());
            (
                mem.clone() as Arc<dyn queryflux_persistence::Persistence>,
                in_memory_metrics(prometheus.clone(), mem),
            )
        }
    };

    // The durable backend behind the proxy, type-erased so that everything south
    // of this point is wired against traits. Redis is intentionally rejected above
    // until a real `BackendStore` implementation exists (no silent in-memory fallback).
    // `None` in in-memory mode, which intentionally has no durable config source.
    let backend: Option<Arc<dyn BackendStore>> = pg_store.clone().map(|pg| pg as _);
    // Multi-replica coordination is optional: only backends that also implement
    // `DistributedBackendStore` (Postgres today) are stored here.
    let distributed_backend: Option<Arc<dyn DistributedBackendStore>> =
        pg_store.clone().map(|pg| pg as _);
    // The narrower view admission and the lease/reconcile/sweep tasks use: leases, sweep
    // locks and published engine counts, without config or history storage.
    let coordination: Option<Arc<dyn CoordinationStore>> = pg_store.map(|pg| pg as _);

    Ok(Stores {
        prometheus,
        persistence,
        metrics,
        mem_store,
        backend,
        distributed_backend,
        coordination,
    })
}

/// Metrics chain for in-memory persistence: Prometheus plus the in-memory store
/// itself. The in-memory store must be part of the chain — the admin API serves
/// `/admin/queries` and `/admin/stats` from it, so recording only to Prometheus
/// leaves query history and the Studio dashboard permanently empty.
pub(crate) fn in_memory_metrics(
    prometheus: Arc<PrometheusMetrics>,
    mem: Arc<InMemoryPersistence>,
) -> Arc<dyn MetricsStore> {
    Arc::new(MultiMetricsStore::new(vec![
        prometheus as Arc<dyn MetricsStore>,
        mem as Arc<dyn MetricsStore>,
    ]))
}
