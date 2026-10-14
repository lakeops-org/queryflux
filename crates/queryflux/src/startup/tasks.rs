//! Background tasks a QueryFlux process runs, one function per task. Each spawns its
//! task and returns; every task stops when `shutdown_rx` flips to `true`.

use std::collections::HashMap;
use std::sync::Arc;

use queryflux_cluster_manager::cluster_state::ClusterState;
use queryflux_core::config::ProxyConfig;
use queryflux_frontend::state::AppState;
use queryflux_metrics::{prometheus_store::PrometheusMetrics, MetricsStore};
use queryflux_persistence::{BackendStore, CoordinationStore};
use tokio::sync::watch;

/// Push cluster utilization snapshots to metrics (and, on the sweep-lock holder, to history) every 5s.
pub(crate) fn spawn_cluster_snapshots(
    app_state: &Arc<AppState>,
    prometheus: &Arc<PrometheusMetrics>,
    backend: &Option<Arc<dyn BackendStore>>,
    coordination: &Option<Arc<dyn CoordinationStore>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    // Background task: push cluster utilization snapshots every 5s.
    // In distributed mode, overlay the last published engine reconcile counts so
    // every replica's metrics reflect the same backend ground truth.
    //
    // Every replica refreshes its *local* Prometheus gauges (each replica's
    // /metrics is scraped independently), but only the replica holding the
    // sweep lock persists history rows to the backend — otherwise R replicas
    // write R duplicate rows per cluster per tick and Studio's history tables
    // grow R times faster.
    tokio::spawn({
        let state = app_state.clone();
        let prometheus = prometheus.clone();
        let backend = backend.clone();
        let coordination = coordination.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let cluster_manager = state.live.read().await.cluster_manager.clone();
                let Ok(snapshots) = cluster_manager.all_cluster_states().await else {
                    continue;
                };
                let breaker_states: Vec<_> = snapshots
                    .iter()
                    .map(|snap| {
                        let state = snap.breaker_state.map(|state| match state {
                            queryflux_cluster_manager::circuit_breaker::CircuitState::Closed => 0,
                            queryflux_cluster_manager::circuit_breaker::CircuitState::Open => 1,
                            queryflux_cluster_manager::circuit_breaker::CircuitState::HalfOpen => 2,
                        });
                        (
                            snap.group_name.0.clone(),
                            snap.cluster_name.0.clone(),
                            state,
                        )
                    })
                    .collect();
                prometheus.sync_circuit_breaker_states(&breaker_states);
                let mut records = Vec::with_capacity(snapshots.len());
                for snap in snapshots {
                    let global_running = if let Some(db) = &coordination {
                        let store = db.clone() as Arc<dyn queryflux_persistence::CapacityStore>;
                        store
                            .active_count(&snap.cluster_name.0)
                            .await
                            .unwrap_or(snap.running_queries)
                    } else {
                        snap.running_queries
                    };
                    records.push(queryflux_metrics::ClusterSnapshot {
                        cluster_name: snap.cluster_name,
                        group_name: snap.group_name,
                        engine_type: snap.engine_type,
                        running_queries: global_running,
                        queued_queries: snap.queued_queries,
                        max_running_queries: snap.max_running_queries,
                        recorded_at: chrono::Utc::now(),
                    });
                }
                for record in &records {
                    let _ = prometheus.record_cluster_snapshot(record.clone()).await;
                }
                // History rows go to the durable backend. When the backend can
                // coordinate, only the sweep-lock owner persists this cycle; a
                // coordination failure fails open (duplicate rows beat no rows).
                // A non-coordinating backend persists unconditionally — it
                // cannot dedup across replicas anyway.
                let lock = match &coordination {
                    Some(db) => match db.try_sweep_lock("cluster-snapshots").await {
                        Ok(Some(lock)) => Some(Some(lock)),
                        Ok(None) => None, // another replica persists this cycle
                        Err(e) => {
                            tracing::debug!("Snapshot sweep lock failed: {e}");
                            Some(None)
                        }
                    },
                    None => Some(None),
                };
                if let Some(lock) = lock {
                    if let Some(backend) = &backend {
                        for record in records {
                            let _ = backend.record_cluster_snapshot(record).await;
                        }
                    }
                    if let Some(lock) = lock {
                        lock.release().await;
                    }
                }
            }
        }
    });
}

/// Renew this replica's capacity leases every 60s.
pub(crate) fn spawn_lease_heartbeat(
    app_state: &Arc<AppState>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    // Background task: renew capacity lease heartbeats for this replica every 60s so that
    // long-running queries on a live instance are never reclaimed by `expire_stale` (cutoff
    // is 300s — five missed beats). Leases of crashed replicas stop heartbeating and expire.
    if let Some(cap) = app_state.capacity_store.clone() {
        let state = app_state.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                if let Err(e) = cap.heartbeat(&state.instance_id).await {
                    state.metrics.on_coordination_failure("capacity_heartbeat");
                    tracing::warn!("Capacity lease heartbeat failed: {e}");
                }
            }
        });
    }
}

/// Release capacity held by executing queries whose client stopped polling.
pub(crate) fn spawn_zombie_sweep(
    app_state: &Arc<AppState>,
    coordination: &Option<Arc<dyn CoordinationStore>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    // Background task: release capacity for zombie executing queries (client disconnected
    // before polling to completion). Runs every 120s; evicts entries not polled for > 5 min.
    //
    // Uses `last_accessed` from persistence — updated by any proxy instance that handles
    // a poll, throttled to at most one write per 120s. Safe across multiple instances.
    // Also expires stale capacity leases from crashed replicas.
    tokio::spawn({
        let state = app_state.clone();
        let coordination = coordination.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300; // matches Trino's query.client.timeout default
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }

                // Single-owner sweep: the eviction and lease expiry below are global
                // (idempotent, but redundant on every replica), so only the replica
                // holding the advisory lock runs them this cycle. A crashed owner's
                // lock is released with its connection, so another replica takes
                // over on its next tick. On lock errors, fail open and sweep anyway.
                let sweep_lock = match &coordination {
                    Some(backend) => match backend.try_sweep_lock("zombie-eviction").await {
                        Ok(Some(lock)) => Some(lock),
                        Ok(None) => continue, // another replica owns this cycle
                        Err(e) => {
                            state.metrics.on_coordination_failure("sweep_lock");
                            tracing::warn!("Sweep lock failed, sweeping anyway: {e}");
                            None
                        }
                    },
                    None => None,
                };

                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);

                // Expire stale capacity leases (crashed replicas).
                if let Some(cap) = &state.capacity_store {
                    match cap.expire_stale(cutoff).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!("Expired {n} stale capacity leases"),
                        Err(e) => {
                            state.metrics.on_coordination_failure("capacity_expire");
                            tracing::warn!("Capacity lease expiry failed: {e}");
                        }
                    }
                }

                let Ok(all) = state.persistence.list_all().await else {
                    continue;
                };
                for q in all {
                    if q.last_accessed < cutoff {
                        tracing::warn!(
                            id = %q.backend_query_id,
                            cluster = %q.cluster_name,
                            group = %q.cluster_group,
                            last_accessed = %q.last_accessed,
                            "Evicting zombie executing query — not polled for >5 min"
                        );

                        // Best-effort cancel on the backend with cluster credentials
                        // (same path as client/admin cancel). The previous unauthenticated
                        // DELETE on `/v1/statement/executing/...` did not stop secured Trino.
                        let backend_id = q.backend_query_id.clone();
                        let wire_auth = q.wire_auth.clone();
                        let cluster = q.cluster_name.0.clone();
                        let (engine_type, tgt_dialect) =
                            if let Some(adapter) = state.adapter(&cluster).await {
                                (adapter.engine_type(), adapter.translation_target_dialect())
                            } else {
                                state.engine_type_for_cluster(&cluster).await
                            };
                        state.record_executing_cancelled(
                            &q,
                            queryflux_core::query::FrontendProtocol::TrinoHttp,
                            engine_type,
                            tgt_dialect,
                            "zombie_evicted: not polled for >5 min",
                        );
                        if let Some(adapter) = state.adapter(&cluster).await {
                            tokio::spawn(async move {
                                if let Err(e) =
                                    adapter.cancel_query(&backend_id, wire_auth.as_ref()).await
                                {
                                    tracing::debug!(
                                        "Zombie cancel request failed (best-effort): {e}"
                                    );
                                }
                            });
                        } else {
                            tracing::debug!(
                                cluster = %cluster,
                                id = %q.backend_query_id,
                                "No adapter available for zombie cancel"
                            );
                        }

                        state
                            .release_query_slot(&q.cluster_group, &q.cluster_name, &q.id.0)
                            .await;
                        let _ = state.persistence.delete(&q.backend_query_id).await;
                    }
                }

                if let Some(lock) = sweep_lock {
                    lock.release().await;
                }
            }
        }
    });
}

/// Remove queued queries whose client stopped polling.
pub(crate) fn spawn_stale_queue_sweep(
    app_state: &Arc<AppState>,
    coordination: &Option<Arc<dyn CoordinationStore>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    // Background task: clean up stale queued queries (client disconnected before getting
    // cluster capacity). Runs every 120s;
    // deletes queued entries not accessed for > 5 minutes.
    tokio::spawn({
        let state = app_state.clone();
        let coordination = coordination.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }

                let sweep_lock = match &coordination {
                    Some(backend) => match backend.try_sweep_lock("stale-queued-eviction").await {
                        Ok(Some(lock)) => Some(lock),
                        Ok(None) => continue,
                        Err(e) => {
                            state.metrics.on_coordination_failure("sweep_lock");
                            tracing::warn!("Stale queued sweep lock failed, sweeping anyway: {e}");
                            None
                        }
                    },
                    None => None,
                };

                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                let Ok(queued) = state.persistence.list_queued().await else {
                    if let Some(lock) = sweep_lock {
                        lock.release().await;
                    }
                    continue;
                };
                let mut cleaned = 0u64;
                for q in queued {
                    if q.last_accessed < cutoff {
                        if let Ok(Some(taken)) = state.persistence.take_queued(&q.id).await {
                            state.record_queued_terminal(
                                &taken,
                                queryflux_core::query::QueryStatus::Failed,
                                "stale_queued_evicted: client disconnected before dispatch",
                            );
                            cleaned += 1;
                        }
                    }
                }
                if cleaned > 0 {
                    tracing::info!("Cleaned up {cleaned} stale queued queries");
                }

                if let Some(lock) = sweep_lock {
                    lock.release().await;
                }
            }
        }
    });
}

/// Delete expired result-cache entries periodically.
pub(crate) fn spawn_cache_cleanup(
    app_state: &Arc<AppState>,
    shutdown_rx: &watch::Receiver<bool>,
    config: &ProxyConfig,
) {
    // Background task: periodically clean up expired cache entries.
    {
        let cache = app_state.result_cache.clone();
        let interval_secs = config
            .queryflux
            .cache_backend
            .as_ref()
            .map(|c| c.cleanup_interval_secs)
            .unwrap_or(300);
        if interval_secs > 0 {
            let mut shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    return;
                }
                loop {
                    if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                        break;
                    }
                    match cache.cleanup_expired().await {
                        Ok(0) => {}
                        Ok(n) => {
                            tracing::info!(deleted = n, "Cache cleanup: removed expired entries")
                        }
                        Err(e) => tracing::warn!(error = %e, "Cache cleanup failed"),
                    }
                }
            });
        }
    }
}

/// Enforce `query_history_retention_days` hourly.
pub(crate) fn spawn_history_retention(
    backend: &Option<Arc<dyn BackendStore>>,
    shutdown_rx: &watch::Receiver<bool>,
    config: &ProxyConfig,
) {
    // Background task: enforce query_history_retention_days — runs hourly and deletes
    // query_records, query_digest_stats, and cluster_snapshots older than the window.
    // Only active when Postgres is configured and retention_days is set.
    if let (Some(backend), Some(retention_days)) = (
        backend.clone(),
        config.queryflux.query_history_retention_days,
    ) {
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                return;
            }
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                match backend.purge_old_query_records(cutoff).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!("Purged {n} history rows older than {retention_days} days")
                    }
                    Err(e) => tracing::warn!("Query history purge failed: {e}"),
                }
            }
        });
    }
}

/// Health-check every cluster every 30s.
pub(crate) fn spawn_health_checks(app_state: &Arc<AppState>, shutdown_rx: &watch::Receiver<bool>) {
    // Background task: health-check each cluster every 30s via its adapter.
    // Clusters with a custom `healthCheckQuery` use that SQL instead of `SELECT 1`.
    // ADBC SaaS backends without a custom query skip health checks (always healthy).
    tokio::spawn({
        let state = app_state.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let (targets, custom_health) = {
                    let live = state.live.read().await;
                    (
                        live.health_check_targets.clone(),
                        live.custom_health_queries.clone(),
                    )
                };
                for (adapter, cstate) in &targets {
                    let cluster_name = &cstate.cluster_name.0;
                    let breaker_probe = BreakerProbeGuard::reserve(cstate);
                    let healthy = if let Some(custom_sql) = custom_health.get(cluster_name) {
                        adapter.execute_custom_health_check(custom_sql).await
                    } else {
                        adapter.health_check().await
                    };
                    if let Some(probe) = breaker_probe {
                        let state = probe.finish(healthy);
                        tracing::info!(
                            cluster = %cluster_name,
                            group = %cstate.group_name.0,
                            breaker_state = ?state,
                            "Circuit breaker health probe completed"
                        );
                    }
                    if !healthy {
                        tracing::warn!(
                            cluster = %cluster_name,
                            group = %cstate.group_name.0,
                            "Health check failed — marking cluster unhealthy"
                        );
                    } else if !cstate.is_healthy() {
                        tracing::info!(
                            cluster = %cluster_name,
                            group = %cstate.group_name.0,
                            "Health check recovered — marking cluster healthy"
                        );
                    }
                    cstate.set_healthy(healthy);
                }
            }
        }
    });
}

/// Reconcile running-query counters with engine ground truth every 30s.
pub(crate) fn spawn_engine_reconcile(
    app_state: &Arc<AppState>,
    coordination: &Option<Arc<dyn CoordinationStore>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    // Background task: reconcile in-memory running_queries counters with ground truth
    // from each engine (engines that implement fetch_running_query_count). Runs every 30s.
    // Corrects drift caused by proxy crashes, client disconnects, or any other leak.
    // In distributed mode, only the replica holding the `engine-reconcile` sweep lock
    // queries backends; it publishes counts to Postgres and other replicas sync locally.
    tokio::spawn({
        let state = app_state.clone();
        let coordination = coordination.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let (targets, custom_reconcile) = {
                    let live = state.live.read().await;
                    (
                        live.health_check_targets.clone(),
                        live.custom_reconcile_queries.clone(),
                    )
                };
                let targets = reconcile_targets_from_health_targets(&targets);

                if let Some(ref db) = coordination {
                    let capacity_store =
                        db.clone() as Arc<dyn queryflux_persistence::CapacityStore>;
                    match db.try_sweep_lock("engine-reconcile").await {
                        Ok(Some(lock)) => {
                            for (adapter, states) in &targets {
                                let cluster_name = &states[0].cluster_name.0;
                                let tracked = observe_running(states);
                                let actual = fetch_engine_running_count(
                                    adapter,
                                    cluster_name,
                                    &custom_reconcile,
                                )
                                .await;
                                if let Some(count) = actual {
                                    if let Err(e) = capacity_store
                                        .publish_running_count(cluster_name, count)
                                        .await
                                    {
                                        state
                                            .metrics
                                            .on_coordination_failure("engine_reconcile_publish");
                                        tracing::warn!(
                                            cluster = %cluster_name,
                                            "Failed to publish engine running count: {e}"
                                        );
                                    }
                                }
                                for (cstate, tracked) in states.iter().zip(tracked) {
                                    apply_reconcile_to_cluster_state(cstate, tracked, actual);
                                }
                            }
                            let _ = lock.release().await;
                        }
                        Ok(None) => {
                            for (_, states) in &targets {
                                let cluster_name = &states[0].cluster_name.0;
                                let tracked = observe_running(states);
                                match capacity_store.active_count(cluster_name).await {
                                    Ok(count) => {
                                        for (cstate, tracked) in states.iter().zip(tracked) {
                                            apply_reconcile_to_cluster_state(
                                                cstate,
                                                tracked,
                                                Some(count),
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        state
                                            .metrics
                                            .on_coordination_failure("engine_reconcile_read");
                                        tracing::warn!(
                                            cluster = %cluster_name,
                                            "Failed to read engine running count: {e}"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            state
                                .metrics
                                .on_coordination_failure("engine_reconcile_sweep_lock");
                            tracing::warn!("Engine reconcile sweep lock failed: {e}");
                        }
                    }
                } else {
                    for (adapter, states) in &targets {
                        let cluster_name = &states[0].cluster_name.0;
                        let tracked = observe_running(states);
                        let actual =
                            fetch_engine_running_count(adapter, cluster_name, &custom_reconcile)
                                .await;
                        for (cstate, tracked) in states.iter().zip(tracked) {
                            apply_reconcile_to_cluster_state(cstate, tracked, actual);
                        }
                    }
                }
            }
        }
    });
}

/// Returns `true` when the interval fired (continue work), `false` on shutdown.
pub(crate) async fn tick_or_shutdown(
    interval: &mut tokio::time::Interval,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = shutdown.wait_for(|v| *v) => false,
        _ = interval.tick() => true,
    }
}

/// A cancelled health-check future must not leave its member HalfOpen forever.
pub(crate) struct BreakerProbeGuard {
    state: Arc<ClusterState>,
    completed: bool,
}

impl BreakerProbeGuard {
    pub(crate) fn reserve(state: &Arc<ClusterState>) -> Option<Self> {
        state.begin_breaker_probe().then(|| Self {
            state: state.clone(),
            completed: false,
        })
    }

    pub(crate) fn finish(
        mut self,
        healthy: bool,
    ) -> Option<queryflux_cluster_manager::circuit_breaker::CircuitState> {
        let result = self.state.finish_breaker_probe(healthy);
        self.completed = true;
        result
    }
}

impl Drop for BreakerProbeGuard {
    fn drop(&mut self) {
        if !self.completed {
            // The probe did not finish (for example, its task was cancelled).
            // Treat it as a failed check and retry after the usual backoff.
            self.state.finish_breaker_probe(false);
        }
    }
}

/// Health probes are per group/member; engine reconciliation is per backend.
/// Keep every member state so the single fetched count reaches all groups.
pub(crate) fn reconcile_targets_from_health_targets<A: Clone>(
    targets: &[(A, Arc<ClusterState>)],
) -> Vec<(A, Vec<Arc<ClusterState>>)> {
    let mut positions: HashMap<String, usize> = HashMap::new();
    let mut grouped: Vec<(_, Vec<Arc<ClusterState>>)> = Vec::new();
    for (adapter, state) in targets {
        let name = &state.cluster_name.0;
        if let Some(&index) = positions.get(name) {
            grouped[index].1.push(state.clone());
        } else {
            positions.insert(name.clone(), grouped.len());
            grouped.push((adapter.clone(), vec![state.clone()]));
        }
    }
    grouped
}

pub(crate) async fn fetch_engine_running_count(
    adapter: &queryflux_engine_adapters::AdapterKind,
    cluster_name: &str,
    custom_reconcile: &HashMap<String, String>,
) -> Option<u64> {
    if let Some(custom_sql) = custom_reconcile.get(cluster_name) {
        adapter.execute_custom_reconcile_query(custom_sql).await
    } else {
        adapter.fetch_running_query_count().await
    }
}

/// Read every state's running counter before the (slow) engine fetch, so the correction can
/// be applied relative to what was observed — see [`apply_reconcile_to_cluster_state`].
pub(crate) fn observe_running(
    states: &[Arc<queryflux_cluster_manager::cluster_state::ClusterState>],
) -> Vec<u64> {
    states.iter().map(|s| s.running_queries()).collect()
}

/// `tracked` is the counter value observed *before* the engine count was fetched. The fix is
/// applied as a relative correction (`ClusterState::reconcile_running`), so queries admitted or
/// released while the fetch was in flight — including through a newer config generation that
/// shares this counter after a reload — are not overwritten by a stale count.
pub(crate) fn apply_reconcile_to_cluster_state(
    cstate: &queryflux_cluster_manager::cluster_state::ClusterState,
    tracked: u64,
    actual: Option<u64>,
) {
    let cluster_name = &cstate.cluster_name.0;
    let max = cstate.max_running_queries();
    if tracked > max {
        let fix = actual.unwrap_or(0);
        tracing::warn!(
            cluster = %cluster_name,
            group = %cstate.group_name.0,
            tracked,
            max,
            fix,
            "running_queries above group capacity; resetting from engine count"
        );
        cstate.reconcile_running(tracked, fix);
        return;
    }
    if let Some(actual) = actual {
        if actual != tracked {
            tracing::info!(
                cluster = %cluster_name,
                group = %cstate.group_name.0,
                tracked,
                actual,
                "Reconciling running_queries counter with engine ground truth"
            );
            cstate.reconcile_running(tracked, actual);
        }
    }
}

/// A worker's name for the control plane: `workerName`, else the hostname.
pub(crate) fn worker_name(config: &ProxyConfig) -> String {
    config
        .queryflux
        .worker_name
        .clone()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| format!("worker-{}", std::process::id()))
}

/// Register with the control plane, then report sync state every heartbeat interval.
/// Best effort: failures are logged and retried on the next tick.
pub(crate) fn spawn_worker_heartbeat(
    config: &ProxyConfig,
    sync_state: &Arc<std::sync::Mutex<crate::config_source::WorkerSyncState>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    let Some(cp) = config.queryflux.control_plane.clone() else {
        return;
    };
    let base = cp.url.trim_end_matches('/').to_string();
    let name = worker_name(config);
    let labels = config.queryflux.worker_labels.clone();
    let coordination_mode = match config.queryflux.coordination.as_ref().map(|c| c.mode) {
        Some(queryflux_core::config::CoordinationMode::Local) => "local",
        _ => "controlPlane",
    };
    let sync_state = sync_state.clone();
    let mut shutdown_rx = shutdown_rx.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let mut registered = false;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            cp.heartbeat_interval_secs.max(1),
        ));
        loop {
            if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                break;
            }
            let st = sync_state.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let report = queryflux_persistence::WorkerRecord {
                name: name.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                labels: labels.clone(),
                coordination_mode: coordination_mode.to_string(),
                downloaded_revision: st.downloaded_revision,
                active_revision: st.active_revision,
                last_error: st.last_error,
                last_seen: chrono::Utc::now(),
            };
            let path = if registered { "heartbeat" } else { "register" };
            match client
                .post(format!("{base}/worker/{path}"))
                .bearer_auth(cp.auth.bearer())
                .json(&report)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    if !registered {
                        tracing::info!(worker = %name, "Registered with the control plane");
                    }
                    registered = true;
                }
                Ok(r) => tracing::debug!(status = %r.status(), "Worker {path} rejected"),
                Err(e) => tracing::debug!("Worker {path} failed: {e}"),
            }
        }
    });
}
