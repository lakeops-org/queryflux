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
    /// Bumped after every reload attempt; the worker API's long-polls wait on it.
    pub(crate) changed: Option<watch::Sender<u64>>,
    /// A worker applies its snapshot's translation policy here.
    pub(crate) translation: Option<Arc<queryflux_translation::TranslationService>>,
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
        if let Some(tx) = &handles.changed {
            tx.send_modify(|n| *n = n.wrapping_add(1));
        }
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
    /// The active snapshot's history policy (endpoint and redaction).
    pub(crate) history: Option<queryflux_core::worker_snapshot::HistoryPolicy>,
    /// Query history delivery to the control plane or the named endpoint.
    pub(crate) history_uplink: crate::history_uplink::HistoryUplinkStatus,
    /// Where the active config came from.
    pub(crate) config_source: Option<ConfigOrigin>,
    /// True while the active config has not been confirmed by the control plane since this
    /// worker started (it was copied from a peer).
    pub(crate) stale: bool,
    /// When the control plane last confirmed the active config. For a config copied from a
    /// peer, the peer's confirmation time.
    pub(crate) confirmed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The active snapshot, served to peers.
    #[serde(skip)]
    pub(crate) active_snapshot: Option<Arc<queryflux_core::worker_snapshot::WorkerSnapshotV1>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ConfigOrigin {
    ControlPlane,
    Peer,
}

/// What a worker serves a peer at `/worker/peer-config`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeerConfig {
    pub(crate) snapshot: queryflux_core::worker_snapshot::WorkerSnapshotV1,
    pub(crate) confirmed_at: chrono::DateTime<chrono::Utc>,
}

impl WorkerSyncState {
    /// The active config for a peer, once the control plane has confirmed it.
    pub(crate) fn peer_config(&self) -> Option<PeerConfig> {
        Some(PeerConfig {
            snapshot: self.active_snapshot.as_deref()?.clone(),
            confirmed_at: self.confirmed_at?,
        })
    }
}

/// A worker's config source: the control plane's `GET /worker/config` (#255).
///
/// Polls with `If-None-Match` set to the active revision, so an unchanged config costs a
/// 304. A new snapshot is verified (schema version, sealed revision), compiled and applied
/// in strict mode, then swapped in atomically; any failure keeps the active config. A
/// snapshot that fails to activate is fetched and tried again, because activation can
/// fail for transient reasons (an identity provider or catalog briefly unreachable while
/// building the auth or catalog provider). Every failure, unreachable control plane or
/// rejected snapshot alike, backs the interval off with jitter, up to 60 seconds.
pub(crate) struct RemoteConfigSource {
    client: reqwest::Client,
    url: String,
    bearer: String,
    poll_interval: std::time::Duration,
    /// How long the control plane may hold a long-poll.
    wait_secs: u64,
    state: Arc<std::sync::Mutex<WorkerSyncState>>,
    first: bool,
    peer: Option<queryflux_core::config::PeerBootstrapConfig>,
    started: std::time::Instant,
}

impl RemoteConfigSource {
    pub(crate) fn new(
        control_plane: &queryflux_core::config::ControlPlaneClientConfig,
        state: Arc<std::sync::Mutex<WorkerSyncState>>,
    ) -> Self {
        let wait_secs = control_plane.poll_timeout_secs.clamp(1, 60);
        Self {
            client: reqwest::Client::builder()
                // The long-poll is held up to `wait_secs`; leave room for the response.
                .timeout(std::time::Duration::from_secs(wait_secs + 15))
                .build()
                .unwrap_or_default(),
            url: format!("{}/worker/config", control_plane.url.trim_end_matches('/')),
            bearer: control_plane.auth.bearer().to_string(),
            // Only used when the control plane does not support long-poll.
            poll_interval: std::time::Duration::from_secs(5),
            wait_secs,
            state,
            first: true,
            peer: control_plane.peer_bootstrap.clone(),
            started: std::time::Instant::now(),
        }
    }

    /// The control plane answered for the active config: it is no longer stale.
    fn confirmed(&self, handles: &LiveHandles) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.active_revision.is_none() {
            return;
        }
        if st.stale {
            tracing::info!("Control plane confirmed the config copied from a peer");
        }
        st.config_source = Some(ConfigOrigin::ControlPlane);
        st.stale = false;
        st.confirmed_at = Some(chrono::Utc::now());
        drop(st);
        handles.metrics.on_worker_config_stale(false);
    }

    /// With no config yet and the control plane still unreachable after `afterSecs`, copy
    /// the active config from a running worker.
    async fn bootstrap_from_peer(&self, handles: &LiveHandles) {
        use queryflux_core::worker_snapshot::WORKER_SNAPSHOT_SCHEMA_VERSION;
        let Some(peer) = &self.peer else { return };
        if self
            .state
            .lock()
            .map(|s| s.active_revision.is_some())
            .unwrap_or(true)
            || self.started.elapsed() < std::time::Duration::from_secs(peer.after_secs)
        {
            return;
        }
        let mut addrs: Vec<std::net::SocketAddr> =
            match tokio::net::lookup_host(&peer.address).await {
                Ok(a) => a.collect(),
                Err(e) => {
                    tracing::warn!(address = %peer.address, "Peer lookup failed: {e}");
                    return;
                }
            };
        // Spread starting workers across peers.
        if !addrs.is_empty() {
            let start = (uuid::Uuid::new_v4().as_u128() % addrs.len() as u128) as usize;
            addrs.rotate_left(start);
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        let max_age = chrono::Duration::seconds(peer.max_age_secs as i64);
        for addr in addrs {
            let url = format!("http://{addr}/worker/peer-config");
            let config: PeerConfig = match client.get(&url).bearer_auth(&self.bearer).send().await {
                Ok(r) if r.status().is_success() => match r.json().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!(%addr, "Peer config does not parse: {e}");
                        continue;
                    }
                },
                Ok(r) => {
                    tracing::debug!(%addr, status = %r.status(), "Peer has no config to share");
                    continue;
                }
                Err(e) => {
                    tracing::debug!(%addr, "Peer unreachable: {e}");
                    continue;
                }
            };
            let snapshot = config.snapshot;
            if snapshot.schema_version != WORKER_SNAPSHOT_SCHEMA_VERSION || !snapshot.is_sealed() {
                tracing::warn!(%addr, "Peer config failed its version or integrity check");
                continue;
            }
            let age = chrono::Utc::now() - config.confirmed_at;
            if age > max_age {
                tracing::warn!(
                    %addr,
                    revision = %snapshot.revision,
                    confirmed_at = %config.confirmed_at,
                    "Peer config is older than peerBootstrap.maxAgeSecs; not using it"
                );
                continue;
            }
            match self.activate(&snapshot, handles).await {
                Ok(()) => {
                    let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    st.downloaded_revision = Some(snapshot.revision.clone());
                    st.active_revision = Some(snapshot.revision.clone());
                    st.activated_at = Some(chrono::Utc::now());
                    st.history = snapshot.history.clone();
                    st.config_source = Some(ConfigOrigin::Peer);
                    st.stale = true;
                    st.confirmed_at = Some(config.confirmed_at);
                    st.active_snapshot = Some(Arc::new(snapshot.clone()));
                    drop(st);
                    handles.metrics.on_worker_config_stale(true);
                    tracing::warn!(
                        %addr,
                        revision = %snapshot.revision,
                        confirmed_at = %config.confirmed_at,
                        "Serving a config copied from a peer until the control plane answers"
                    );
                    return;
                }
                Err(e) => tracing::warn!(%addr, "Peer config failed to activate: {e:#}"),
            }
        }
        tracing::warn!(address = %peer.address, "No peer could share a usable config");
    }

    /// Long-polling once a config is active (the request waits on the control plane).
    fn long_polling(&self) -> bool {
        self.state
            .lock()
            .map(|s| s.active_revision.is_some())
            .unwrap_or(false)
    }

    /// Record a failed sync with the control plane. Returns `false` (no answer).
    fn record_failure(&self, message: String) -> bool {
        tracing::warn!("Worker config sync: {message}");
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.last_error = Some(message);
        st.consecutive_failures = st.consecutive_failures.saturating_add(1);
        false
    }

    fn next_delay(&self) -> std::time::Duration {
        let failures = self
            .state
            .lock()
            .map(|s| s.consecutive_failures)
            .unwrap_or(0);
        if failures == 0 {
            // A long-poll already waited on the control plane; ask again right away.
            return if self.long_polling() {
                std::time::Duration::ZERO
            } else {
                self.poll_interval
            };
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
        if let (Some(service), Some(policy)) = (&handles.translation, snapshot.translation) {
            service.set_policy(policy.mode, policy.error_on_unsupported);
        }
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
        if !self.sync(handles).await {
            self.bootstrap_from_peer(handles).await;
        }
    }
}

impl RemoteConfigSource {
    /// One exchange with the control plane. Returns `true` if it answered with a usable
    /// response (a new snapshot, or "unchanged").
    async fn sync(&self, handles: &LiveHandles) -> bool {
        use queryflux_core::worker_snapshot::{WorkerSnapshotV1, WORKER_SNAPSHOT_SCHEMA_VERSION};
        // The active revision, not the last downloaded one: a snapshot that failed to
        // activate must be fetched and tried again rather than answered with a 304.
        let (active, stale) = self
            .state
            .lock()
            .map(|s| (s.active_revision.clone(), s.stale))
            .unwrap_or_default();
        // With an active revision, long-poll for the next one; otherwise fetch now. A config
        // copied from a peer is checked with a plain conditional GET instead, so the control
        // plane confirms it at once rather than at the end of a long-poll.
        let mut req = match &active {
            Some(rev) if stale => self
                .client
                .get(&self.url)
                .header(reqwest::header::IF_NONE_MATCH, format!("\"{rev}\"")),
            Some(rev) => self
                .client
                .get(format!("{}/wait", self.url))
                .query(&[
                    ("since", rev.as_str()),
                    ("timeoutSecs", &self.wait_secs.to_string()),
                ])
                .header(reqwest::header::IF_NONE_MATCH, format!("\"{rev}\"")),
            None => self.client.get(&self.url),
        };
        req = req.bearer_auth(&self.bearer);
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return self.record_failure(format!("control plane unreachable: {e}")),
        };
        match resp.status() {
            reqwest::StatusCode::NOT_MODIFIED => {
                {
                    let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    st.last_successful_sync = Some(chrono::Utc::now());
                    st.consecutive_failures = 0;
                }
                self.confirmed(handles);
                return true;
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
        }
        match self.activate(&snapshot, handles).await {
            Ok(()) => {
                let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                st.active_revision = Some(snapshot.revision.clone());
                st.activated_at = Some(chrono::Utc::now());
                st.history = snapshot.history.clone();
                st.last_error = None;
                st.consecutive_failures = 0;
                st.active_snapshot = Some(Arc::new(snapshot.clone()));
                drop(st);
                self.confirmed(handles);
                tracing::info!(revision = %snapshot.revision, "Worker config activated");
            }
            Err(e) => {
                handles.metrics.on_config_reload_failure("worker_activate");
                // Counted as a failure so the retry of this snapshot backs off.
                self.record_failure(format!(
                    "snapshot {} rejected, keeping the active config: {e:#}",
                    snapshot.revision
                ));
            }
        }
        true
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
            changed: None,
            translation: None,
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
            history: None,
            translation: None,
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
            registry: None,
            changes: None,
            coordination: None,
            history: None,
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
                "pollTimeoutSecs": 1,
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
    async fn worker_applies_the_control_planes_translation_policy() {
        use queryflux_core::config::TranslationMode;
        let mut snap = snapshot(3);
        snap.translation = Some(queryflux_core::worker_snapshot::TranslationPolicy {
            mode: TranslationMode::Strict,
            error_on_unsupported: false,
        });
        let current = Arc::new(std::sync::Mutex::new(snap.seal()));
        let url = control_plane(current).await;
        let (src, _state) = source(&url, "s3cret");
        let service = Arc::new(queryflux_translation::TranslationService::disabled());
        let h = LiveHandles {
            translation: Some(service.clone()),
            ..handles()
        };
        assert_eq!(service.policy(), (TranslationMode::BestEffort, false));
        src.reload(&h).await;
        assert_eq!(service.policy(), (TranslationMode::Strict, false));
    }

    /// A control plane that answers 503 while `up` is false.
    async fn switchable_control_plane(
        current: Arc<std::sync::Mutex<queryflux_core::worker_snapshot::WorkerSnapshotV1>>,
        up: Arc<std::sync::atomic::AtomicBool>,
    ) -> String {
        let api = queryflux_frontend::worker_api::WorkerApi {
            shared_secret: "s3cret".into(),
            snapshot: Arc::new(move || {
                let s = current.lock().unwrap().clone();
                Box::pin(async move { Ok(s) })
            }),
            registry: None,
            changes: None,
            coordination: None,
            history: None,
        };
        let app = queryflux_frontend::worker_api::router(api).layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let up = up.clone();
                async move {
                    if up.load(std::sync::atomic::Ordering::SeqCst) {
                        next.run(req).await
                    } else {
                        axum::response::IntoResponse::into_response(
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        )
                    }
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// A worker's ops listener serving its peer config; returns `host:port`.
    async fn peer(state: Arc<std::sync::Mutex<WorkerSyncState>>, secret: &str) -> String {
        let ops = queryflux_frontend::ops::OpsFrontend::new(
            Arc::new(queryflux_metrics::prometheus_store::PrometheusMetrics::new().unwrap()),
            Arc::new(RwLock::new(crate::live_config::empty_live_config())),
            0,
            Arc::new(|| serde_json::json!({})),
        )
        .with_peer_config(queryflux_frontend::ops::PeerConfigEndpoint {
            shared_secret: secret.into(),
            config: Arc::new(move || {
                let st = state.lock().unwrap();
                st.peer_config().and_then(|c| serde_json::to_value(c).ok())
            }),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = ops.router();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr.to_string()
    }

    fn peer_source(
        url: &str,
        peer_addr: &str,
        max_age_secs: u64,
    ) -> (RemoteConfigSource, Arc<std::sync::Mutex<WorkerSyncState>>) {
        let cp: queryflux_core::config::ControlPlaneClientConfig =
            serde_json::from_value(serde_json::json!({
                "url": url,
                "allowInsecureHttp": true,
                "pollTimeoutSecs": 1,
                "auth": { "mode": "sharedSecret", "sharedSecret": "s3cret" },
                "peerBootstrap": { "address": peer_addr, "afterSecs": 0, "maxAgeSecs": max_age_secs }
            }))
            .unwrap();
        let state = Arc::new(std::sync::Mutex::new(WorkerSyncState::default()));
        (RemoteConfigSource::new(&cp, state.clone()), state)
    }

    #[tokio::test]
    async fn a_worker_starts_from_a_peer_until_the_control_plane_answers() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let up = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let url = switchable_control_plane(current.clone(), up.clone()).await;

        // Worker A syncs from the control plane and serves its config to peers.
        let (a, a_state) = source(&url, "s3cret");
        a.reload(&handles()).await;
        assert!(a_state.lock().unwrap().peer_config().is_some());
        let a_addr = peer(a_state.clone(), "s3cret").await;

        // The control plane goes down; worker B starts and copies A's config.
        up.store(false, std::sync::atomic::Ordering::SeqCst);
        let (b, b_state) = peer_source(&url, &a_addr, 3600);
        let h = handles();
        b.reload(&h).await;
        {
            let st = b_state.lock().unwrap();
            assert_eq!(st.config_source, Some(ConfigOrigin::Peer));
            assert!(st.stale);
            assert_eq!(st.active_revision, a_state.lock().unwrap().active_revision);
        }
        assert!(h.live.read().await.is_configured());
        assert_eq!(group_limit(&h).await, Some(3));

        // The control plane returns: B's config is confirmed, no longer stale.
        up.store(true, std::sync::atomic::Ordering::SeqCst);
        b.reload(&h).await;
        let st = b_state.lock().unwrap();
        assert_eq!(st.config_source, Some(ConfigOrigin::ControlPlane));
        assert!(!st.stale);
    }

    #[tokio::test]
    async fn a_peer_config_older_than_max_age_is_refused() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let up = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let url = switchable_control_plane(current.clone(), up).await;
        let snap = current.lock().unwrap().clone();
        let old = Arc::new(std::sync::Mutex::new(WorkerSyncState {
            active_revision: Some(snap.revision.clone()),
            active_snapshot: Some(Arc::new(snap)),
            confirmed_at: Some(chrono::Utc::now() - chrono::Duration::hours(2)),
            ..Default::default()
        }));
        let old_addr = peer(old, "s3cret").await;
        let (b, b_state) = peer_source(&url, &old_addr, 3600);
        let h = handles();
        b.reload(&h).await;
        assert!(b_state.lock().unwrap().active_revision.is_none());
        assert!(!h.live.read().await.is_configured());
    }

    #[tokio::test]
    async fn a_peer_requires_the_shared_secret() {
        let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
        let up = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let url = switchable_control_plane(current.clone(), up).await;
        let snap = current.lock().unwrap().clone();
        let a_state = Arc::new(std::sync::Mutex::new(WorkerSyncState {
            active_revision: Some(snap.revision.clone()),
            active_snapshot: Some(Arc::new(snap)),
            confirmed_at: Some(chrono::Utc::now()),
            ..Default::default()
        }));
        let a_addr = peer(a_state, "other-fleet-secret").await;
        let (b, b_state) = peer_source(&url, &a_addr, 3600);
        b.reload(&handles()).await;
        assert!(b_state.lock().unwrap().active_revision.is_none());
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

    /// Activation can fail for transient reasons, so a rejected snapshot is fetched and
    /// tried again (not answered with a 304), each failure growing the backoff, until it
    /// or a newer one activates.
    #[tokio::test]
    async fn a_snapshot_that_fails_to_activate_is_retried_with_backoff() {
        let mut bad = snapshot(3);
        bad.clusters[0].engine_key = "noSuchEngine".into();
        let current = Arc::new(std::sync::Mutex::new(bad.seal()));
        let url = control_plane(current.clone()).await;
        let (src, state) = source(&url, "s3cret");
        let h = handles();

        src.reload(&h).await;
        src.reload(&h).await;
        {
            let st = state.lock().unwrap();
            assert!(st.active_revision.is_none());
            assert_eq!(
                st.consecutive_failures, 2,
                "both syncs tried to activate it; a 304 would have reset the count"
            );
            assert!(st.last_error.as_deref().unwrap_or("").contains("rejected"));
        }
        assert!(src.next_delay() >= std::time::Duration::from_secs(4));
        assert!(!h.live.read().await.is_configured());

        *current.lock().unwrap() = snapshot(3);
        src.reload(&h).await;
        let st = state.lock().unwrap().clone();
        assert!(st.active_revision.is_some());
        assert_eq!(st.consecutive_failures, 0);
        assert!(h.live.read().await.is_configured());
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
