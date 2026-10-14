//! Where `LiveConfig` comes from after startup, and when it is reloaded.
//!
//! A [`ConfigSource`] answers two questions: *when* config may have changed
//! ([`ConfigSource::changed`]) and *how* to bring the live config up to date
//! ([`ConfigSource::reload`]). [`run_reload_loop`] drives any source. Today there are two:
//! [`PostgresConfigSource`] (cluster, group, routing and settings rows) and
//! [`AdminStoreConfigSource`] (YAML mode, where only settings stored through the admin API
//! can change at runtime). Worker mode adds a remote source on the same loop.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use queryflux_frontend::state::LiveConfig;
use queryflux_metrics::MetricsStore;
use queryflux_persistence::{AdminStore, BackendStore};
use tokio::sync::{mpsc, watch, Mutex, Notify, RwLock};

use crate::guards::{
    apply_stored_access_control, build_guard_chains_from_db_value,
    load_guard_script_bodies_from_admin,
};
use crate::live_config::{reload_live_config, AdapterReloadCache, PreservedLive};

/// The shared state every source updates.
pub(crate) struct LiveHandles {
    pub(crate) live: Arc<RwLock<LiveConfig>>,
    pub(crate) cache: Arc<Mutex<AdapterReloadCache>>,
    pub(crate) metrics: Arc<dyn MetricsStore>,
}

#[async_trait]
pub(crate) trait ConfigSource: Send {
    /// Wait until config may have changed. Returns `false` when the process is shutting
    /// down and the loop should stop.
    async fn changed(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool;

    /// Bring the live config up to date. Must leave the current config in place on failure.
    async fn reload(&self, handles: &LiveHandles);
}

/// Run `source` until shutdown: wait for a change, reload, repeat.
pub(crate) async fn run_reload_loop(
    mut source: Box<dyn ConfigSource>,
    handles: LiveHandles,
    mut shutdown: watch::Receiver<bool>,
) {
    while source.changed(&mut shutdown).await {
        source.reload(&handles).await;
    }
}

/// The local triggers both built-in sources share: this replica's admin writes
/// (`notify`), other replicas' writes (`revisions`, e.g. Postgres LISTEN/NOTIFY) and an
/// optional safety-net timer.
pub(crate) struct ReloadTriggers {
    notify: Arc<Notify>,
    revisions: Option<mpsc::Receiver<u64>>,
    interval: Option<tokio::time::Interval>,
}

impl ReloadTriggers {
    pub(crate) async fn new(
        notify: Arc<Notify>,
        revisions: Option<mpsc::Receiver<u64>>,
        periodic_secs: Option<u64>,
    ) -> Self {
        let interval = match periodic_secs {
            Some(secs) => {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(secs));
                interval.tick().await; // skip the first immediate tick — startup already loaded
                Some(interval)
            }
            None => None,
        };
        Self {
            notify,
            revisions,
            interval,
        }
    }

    /// Wait for any trigger, then coalesce a burst of revision notifications into one
    /// reload. Returns `false` on shutdown.
    async fn wait(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool {
        tokio::select! {
            _ = shutdown.wait_for(|v| *v) => return false,
            _ = tick(&mut self.interval) => {}
            _ = self.notify.notified() => {
                tracing::debug!("Config reload triggered by local admin write");
            }
            rev = recv_revision(&mut self.revisions) => {
                tracing::debug!(revision = rev, "Config reload triggered by distributed revision change");
            }
        }
        coalesce_revisions(&mut self.revisions).await;
        true
    }
}

async fn tick(interval: &mut Option<tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

// Coalesce notification bursts: a bulk admin save bumps the revision once per write, and
// each bump is one channel message — without draining, N rapid writes would trigger N
// full reloads (adapter rebuilds included) on every replica. A short settle window lets
// writes a few hundred ms apart collapse into one reload too.
async fn coalesce_revisions(rx: &mut Option<mpsc::Receiver<u64>>) {
    if let Some(rx) = rx.as_mut() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        while rx.try_recv().is_ok() {}
    }
}

// Resolves when the revision receiver gets a message, or pends forever if there is no
// receiver. A closed channel (the LISTEN/NOTIFY task died) drops the receiver so we don't
// spin on an immediately-ready `recv()`; periodic polling remains the safety net.
async fn recv_revision(rx: &mut Option<mpsc::Receiver<u64>>) -> u64 {
    match rx {
        Some(r) => match r.recv().await {
            Some(rev) => rev,
            None => {
                tracing::warn!("Config revision channel closed — falling back to periodic polling");
                *rx = None;
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
}

/// Cluster, group, routing and settings rows in Postgres, compiled leniently.
pub(crate) struct PostgresConfigSource {
    backend: Arc<dyn BackendStore>,
    triggers: ReloadTriggers,
}

impl PostgresConfigSource {
    pub(crate) fn new(backend: Arc<dyn BackendStore>, triggers: ReloadTriggers) -> Self {
        Self { backend, triggers }
    }
}

#[async_trait]
impl ConfigSource for PostgresConfigSource {
    async fn changed(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool {
        self.triggers.wait(shutdown).await
    }

    async fn reload(&self, handles: &LiveHandles) {
        let mut cache_guard = handles.cache.lock().await;
        // Snapshot the pieces a reload must never silently weaken; the read guard is
        // dropped before the write below.
        let prev = {
            let l = handles.live.read().await;
            PreservedLive {
                auth_provider: l.auth_provider.clone(),
                authorization: l.authorization.clone(),
                guard_chain: l.guard_chain.clone(),
                group_guard_chains: l.group_guard_chains.clone(),
                access_control_guard: l.access_control_guard.clone(),
                catalog: l.catalog.clone(),
            }
        };
        // A rejected reload must not change the adapter/state cache that the next
        // attempt uses to preserve the currently live breakers.
        let mut candidate_cache = cache_guard.clone();
        match reload_live_config(&self.backend, &mut candidate_cache, &prev, &handles.metrics).await
        {
            Ok(new_live) => {
                let mut live_guard = handles.live.write().await;
                *cache_guard = candidate_cache;
                *live_guard = new_live;
                tracing::info!("Live config reloaded from backend");
            }
            Err(e) => {
                handles.metrics.on_config_reload_failure("reload");
                tracing::warn!("Config reload failed: {e}");
            }
        }
    }
}

/// YAML mode: routing rules, cluster configs and adapters are fixed at startup from the
/// YAML file and need a restart to change. Only settings stored through the admin API
/// (guard chains, catalog provider, access control) are hot-reloaded.
pub(crate) struct AdminStoreConfigSource {
    admin: Option<Arc<dyn AdminStore>>,
    triggers: ReloadTriggers,
}

impl AdminStoreConfigSource {
    pub(crate) fn new(admin: Option<Arc<dyn AdminStore>>, triggers: ReloadTriggers) -> Self {
        Self { admin, triggers }
    }
}

#[async_trait]
impl ConfigSource for AdminStoreConfigSource {
    async fn changed(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool {
        self.triggers.wait(shutdown).await
    }

    async fn reload(&self, handles: &LiveHandles) {
        let Some(store) = &self.admin else {
            return;
        };
        let live = &handles.live;
        let metrics = &handles.metrics;
        let guard_script_bodies = load_guard_script_bodies_from_admin(store.as_ref()).await;
        match store.get_proxy_setting("guardrails_config").await {
            Ok(Some(v)) => {
                let (global, groups) = build_guard_chains_from_db_value(&v, &guard_script_bodies);
                let mut w = live.write().await;
                w.guard_chain = global;
                w.group_guard_chains = groups;
            }
            Ok(None) => {
                let mut w = live.write().await;
                w.guard_chain = None;
                w.group_guard_chains = HashMap::new();
            }
            Err(e) => {
                metrics.on_config_reload_failure("guard_reload");
                tracing::warn!("Guard chain reload failed: {e}");
            }
        }
        // Catalog provider: same admin-store-only reload contract as guard chains above —
        // a `Null` provider is always a safe fallback (schema-aware translation just
        // degrades to dialect-only), unlike auth/authz.
        match store.get_proxy_setting("catalog_config").await {
            Ok(Some(v)) => {
                match serde_json::from_value::<queryflux_core::config::CatalogProviderConfig>(v) {
                    Ok(cfg) => {
                        let provider = queryflux_catalog::build_catalog_provider(&cfg).await;
                        live.write().await.catalog = provider;
                    }
                    Err(e) => {
                        metrics.on_config_reload_failure("catalog_reload");
                        tracing::warn!("Catalog config parse failed: {e}");
                    }
                }
            }
            Ok(None) => {
                live.write().await.catalog = Arc::new(queryflux_core::catalog::NullCatalogProvider);
            }
            Err(e) => {
                metrics.on_config_reload_failure("catalog_reload");
                tracing::warn!("Catalog config reload failed: {e}");
            }
        }
        match store.get_proxy_setting("access_control_config").await {
            Ok(Some(v)) => match apply_stored_access_control(&v) {
                Ok(guard) => {
                    live.write().await.access_control_guard = guard;
                }
                Err(e) => {
                    metrics.on_config_reload_failure("access_control_reload");
                    tracing::warn!("Access control config parse/build failed: {e}");
                }
            },
            Ok(None) => {}
            Err(e) => {
                metrics.on_config_reload_failure("access_control_reload");
                tracing::warn!("Access control config reload failed: {e}");
            }
        }
    }
}

/// What a worker knows about its config sync, for `/worker/status` and logs.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkerSyncState {
    /// Last revision fetched from the control plane (may have failed to activate).
    pub(crate) downloaded_revision: Option<String>,
    /// Revision currently serving queries.
    pub(crate) active_revision: Option<String>,
    pub(crate) activated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) last_successful_sync: Option<chrono::DateTime<chrono::Utc>>,
    /// Why the last fetch or activation failed; cleared on the next success.
    pub(crate) last_error: Option<String>,
    pub(crate) consecutive_failures: u32,
}

/// A worker's config source: the control plane's `GET /worker/config` (#255).
///
/// Polls with `If-None-Match` set to the last downloaded revision, so an unchanged
/// config costs a 304, and a revision that failed to activate is not fetched again until
/// the control plane publishes a new one. A new snapshot is verified (schema version,
/// sealed revision), compiled and applied in strict mode, then swapped in atomically;
/// any failure keeps the active config. While the control plane is unreachable the
/// interval backs off with jitter, up to 60 seconds.
pub(crate) struct RemoteConfigSource {
    client: reqwest::Client,
    url: String,
    bearer: String,
    poll_interval: std::time::Duration,
    state: Arc<std::sync::Mutex<WorkerSyncState>>,
    first: bool,
}

impl RemoteConfigSource {
    pub(crate) fn new(
        control_plane: &queryflux_core::config::ControlPlaneClientConfig,
        state: Arc<std::sync::Mutex<WorkerSyncState>>,
    ) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            url: format!("{}/worker/config", control_plane.url.trim_end_matches('/')),
            bearer: control_plane.auth.bearer().to_string(),
            poll_interval: std::time::Duration::from_secs(5),
            state,
            first: true,
        }
    }

    fn record_failure(&self, message: String) {
        tracing::warn!("Worker config sync: {message}");
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.last_error = Some(message);
        st.consecutive_failures = st.consecutive_failures.saturating_add(1);
    }

    fn next_delay(&self) -> std::time::Duration {
        let failures = self
            .state
            .lock()
            .map(|s| s.consecutive_failures)
            .unwrap_or(0);
        if failures == 0 {
            return self.poll_interval;
        }
        let base = 2u64.saturating_pow(failures.min(6)).min(60);
        let jitter_ms = (uuid::Uuid::new_v4().as_u128() % 1000) as u64;
        std::time::Duration::from_secs(base) + std::time::Duration::from_millis(jitter_ms)
    }

    /// Compile and apply a received snapshot strictly, then swap it in.
    async fn activate(
        &self,
        snapshot: &queryflux_core::worker_snapshot::WorkerSnapshotV1,
        handles: &LiveHandles,
    ) -> anyhow::Result<()> {
        use crate::live_config::{
            apply_settings, compile_live_config, inputs_from_snapshot, CompileMode, ConfigIds,
        };
        let inputs = inputs_from_snapshot(snapshot);
        let mut cache_guard = handles.cache.lock().await;
        let mut candidate_cache = cache_guard.clone();
        let mut new_live = compile_live_config(
            &inputs,
            &ConfigIds::default(),
            &mut candidate_cache,
            CompileMode::Strict,
        )
        .await?;
        apply_settings(
            &mut new_live,
            &inputs.settings,
            &inputs.groups,
            CompileMode::Strict,
        )
        .await?;
        let mut live_guard = handles.live.write().await;
        *cache_guard = candidate_cache;
        *live_guard = new_live;
        Ok(())
    }
}

#[async_trait]
impl ConfigSource for RemoteConfigSource {
    async fn changed(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool {
        if std::mem::take(&mut self.first) {
            return !*shutdown.borrow();
        }
        let delay = self.next_delay();
        tokio::select! {
            _ = shutdown.wait_for(|v| *v) => false,
            _ = tokio::time::sleep(delay) => true,
        }
    }

    async fn reload(&self, handles: &LiveHandles) {
        use queryflux_core::worker_snapshot::{WorkerSnapshotV1, WORKER_SNAPSHOT_SCHEMA_VERSION};
        let downloaded = self
            .state
            .lock()
            .ok()
            .and_then(|s| s.downloaded_revision.clone());
        let mut req = self.client.get(&self.url).bearer_auth(&self.bearer);
        if let Some(rev) = &downloaded {
            req = req.header(reqwest::header::IF_NONE_MATCH, format!("\"{rev}\""));
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return self.record_failure(format!("control plane unreachable: {e}")),
        };
        match resp.status() {
            reqwest::StatusCode::NOT_MODIFIED => {
                let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                st.last_successful_sync = Some(chrono::Utc::now());
                st.consecutive_failures = 0;
                return;
            }
            reqwest::StatusCode::OK => {}
            status => {
                let body = resp.text().await.unwrap_or_default();
                return self.record_failure(format!("control plane answered {status}: {body}"));
            }
        }
        let snapshot: WorkerSnapshotV1 = match resp.json().await {
            Ok(s) => s,
            Err(e) => return self.record_failure(format!("snapshot does not parse: {e}")),
        };
        if snapshot.schema_version != WORKER_SNAPSHOT_SCHEMA_VERSION {
            return self.record_failure(format!(
                "unsupported_snapshot_version: {} (this worker supports {})",
                snapshot.schema_version, WORKER_SNAPSHOT_SCHEMA_VERSION
            ));
        }
        if !snapshot.is_sealed() {
            return self.record_failure(format!(
                "snapshot {} failed its integrity check",
                snapshot.revision
            ));
        }
        {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.downloaded_revision = Some(snapshot.revision.clone());
            st.last_successful_sync = Some(chrono::Utc::now());
            st.consecutive_failures = 0;
        }
        match self.activate(&snapshot, handles).await {
            Ok(()) => {
                let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                st.active_revision = Some(snapshot.revision.clone());
                st.activated_at = Some(chrono::Utc::now());
                st.last_error = None;
                tracing::info!(revision = %snapshot.revision, "Worker config activated");
            }
            Err(e) => {
                handles.metrics.on_config_reload_failure("worker_activate");
                let message = format!(
                    "snapshot {} rejected, keeping the active config: {e:#}",
                    snapshot.revision
                );
                tracing::warn!("{message}");
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .last_error = Some(message);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn triggers(revisions: Option<mpsc::Receiver<u64>>) -> (ReloadTriggers, Arc<Notify>) {
        let notify = Arc::new(Notify::new());
        (
            ReloadTriggers::new(notify.clone(), revisions, None).await,
            notify,
        )
    }

    #[tokio::test]
    async fn local_admin_write_triggers_a_reload() {
        let (mut t, notify) = triggers(None).await;
        let (_tx, mut shutdown) = watch::channel(false);
        notify.notify_one();
        let fired = tokio::time::timeout(Duration::from_secs(1), t.wait(&mut shutdown))
            .await
            .expect("wait returns");
        assert!(fired);
    }

    #[tokio::test]
    async fn shutdown_stops_waiting() {
        let (mut t, _notify) = triggers(None).await;
        let (tx, mut shutdown) = watch::channel(false);
        tx.send(true).unwrap();
        let fired = tokio::time::timeout(Duration::from_secs(1), t.wait(&mut shutdown))
            .await
            .expect("wait returns");
        assert!(!fired);
    }

    /// A burst of revision bumps (one per admin write) collapses into one reload.
    #[tokio::test]
    async fn a_burst_of_revisions_is_coalesced_into_one_reload() {
        let (rev_tx, rev_rx) = mpsc::channel(16);
        let (mut t, _notify) = triggers(Some(rev_rx)).await;
        let (_tx, mut shutdown) = watch::channel(false);
        for rev in 1..=5 {
            rev_tx.send(rev).await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(2), t.wait(&mut shutdown))
                .await
                .expect("first wait returns")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), t.wait(&mut shutdown))
                .await
                .is_err(),
            "the rest of the burst must not trigger a second reload"
        );
    }

    #[tokio::test]
    async fn periodic_timer_triggers_after_the_interval_but_not_at_start() {
        let notify = Arc::new(Notify::new());
        let mut t = ReloadTriggers::new(notify, None, Some(1)).await;
        let (_tx, mut shutdown) = watch::channel(false);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), t.wait(&mut shutdown))
                .await
                .is_err(),
            "startup already loaded; no immediate tick"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), t.wait(&mut shutdown))
                .await
                .expect("timer fires")
        );
    }

    // -- RemoteConfigSource against an in-process control plane ----------------

    fn handles() -> LiveHandles {
        LiveHandles {
            live: Arc::new(RwLock::new(crate::live_config::empty_live_config())),
            cache: Arc::new(Mutex::new(AdapterReloadCache {
                adapters: HashMap::new(),
                config_json: HashMap::new(),
                cluster_states: HashMap::new(),
                routing_fallback: String::new(),
                routers_cfg: Vec::new(),
                strategies: HashMap::new(),
            })),
            metrics: Arc::new(queryflux_metrics::NoopMetricsStore),
        }
    }

    fn snapshot(max_running: i64) -> queryflux_core::worker_snapshot::WorkerSnapshotV1 {
        use queryflux_core::worker_snapshot::*;
        WorkerSnapshotV1 {
            schema_version: WORKER_SNAPSHOT_SCHEMA_VERSION,
            revision: String::new(),
            credentials: CredentialMode::Embedded,
            clusters: vec![SnapshotCluster {
                name: "duck".into(),
                enabled: true,
                engine_key: "duckDb".into(),
                max_running_queries: Some(max_running),
                config: serde_json::json!({}),
                variants: serde_json::json!([]),
            }],
            groups: std::collections::BTreeMap::from([(
                "g".to_string(),
                serde_json::from_value(serde_json::json!({
                    "members": ["duck"], "maxRunningQueries": 10
                }))
                .unwrap(),
            )]),
            routers: vec![],
            routing_fallback: "g".into(),
            group_translation_scripts: Default::default(),
            settings: SnapshotSettings::default(),
        }
        .seal()
    }

    /// Serve whatever is in `current` as the control plane's snapshot; return its URL.
    async fn control_plane(
        current: Arc<std::sync::Mutex<queryflux_core::worker_snapshot::WorkerSnapshotV1>>,
    ) -> String {
        let api = queryflux_frontend::worker_api::WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(move || {
                let s = current.lock().unwrap().clone();
                Box::pin(async move { Ok(s) })
            }),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, queryflux_frontend::worker_api::router(api))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    fn source(
        url: &str,
        secret: &str,
    ) -> (RemoteConfigSource, Arc<std::sync::Mutex<WorkerSyncState>>) {
        let cp: queryflux_core::config::ControlPlaneClientConfig =
            serde_json::from_value(serde_json::json!({
                "url": url,
                "allowInsecureHttp": true,
                "auth": { "mode": "sharedSecret", "sharedSecret": secret }
            }))
            .unwrap();
        let state = Arc::new(std::sync::Mutex::new(WorkerSyncState::default()));
        (RemoteConfigSource::new(&cp, state.clone()), state)
    }

    async fn group_limit(h: &LiveHandles) -> Option<u64> {
        let live = h.live.read().await;
        let states = live.cluster_manager.all_cluster_states().await.ok()?;
        states.first().map(|s| s.max_running_queries)
    }

    #[tokio::test]
    async fn worker_activates_a_snapshot_then_tracks_changes() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let url = control_plane(current.clone()).await;
        let (src, state) = source(&url, "s3cret");
        let h = handles();

        src.reload(&h).await;
        let first = current.lock().unwrap().revision.clone();
        {
            let st = state.lock().unwrap();
            assert_eq!(st.active_revision.as_deref(), Some(first.as_str()));
            assert!(st.last_error.is_none(), "{:?}", st.last_error);
        }
        assert!(h.live.read().await.is_configured());
        assert_eq!(group_limit(&h).await, Some(3));

        // Unchanged: a 304, nothing re-activated.
        let activated_at = state.lock().unwrap().activated_at;
        src.reload(&h).await;
        assert_eq!(state.lock().unwrap().activated_at, activated_at);

        // A new revision on the control plane is picked up.
        *current.lock().unwrap() = snapshot(7);
        src.reload(&h).await;
        assert_eq!(group_limit(&h).await, Some(7));
    }

    #[tokio::test]
    async fn a_tampered_snapshot_is_rejected_and_the_active_config_kept() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let url = control_plane(current.clone()).await;
        let (src, state) = source(&url, "s3cret");
        let h = handles();
        src.reload(&h).await;
        let active = state.lock().unwrap().active_revision.clone();

        let mut tampered = snapshot(9);
        tampered.clusters[0].max_running_queries = Some(99); // after sealing
        *current.lock().unwrap() = tampered;
        src.reload(&h).await;

        let st = state.lock().unwrap().clone();
        assert_eq!(st.active_revision, active);
        assert!(st.last_error.unwrap().contains("integrity"));
        assert_eq!(group_limit(&h).await, Some(3));
    }

    #[tokio::test]
    async fn a_wrong_secret_leaves_the_worker_unconfigured() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let url = control_plane(current).await;
        let (src, state) = source(&url, "wrong");
        let h = handles();
        src.reload(&h).await;
        let st = state.lock().unwrap().clone();
        assert!(st.active_revision.is_none());
        assert_eq!(st.consecutive_failures, 1);
        assert!(st.last_error.unwrap().contains("401"));
        assert!(!h.live.read().await.is_configured());
    }
}
