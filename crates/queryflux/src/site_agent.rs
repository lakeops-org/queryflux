//! The site agent role (#255): one process per customer site between the site's workers
//! and a remote control plane.
//!
//! Workers connect to the agent exactly as they would to a control plane: it serves the
//! same worker API (`/worker/config`, `/wait`, `/register`, `/heartbeat`, `/capacity/*`,
//! `/sweep-lock/*`, `/query-records`), so a worker needs no change beyond its
//! `controlPlane.url`. The agent is the only process at the site that talks to upstream,
//! and every connection is one it opens:
//!
//! - **Config:** it long-polls upstream's `/worker/config/wait` and serves the snapshot it
//!   gets, unchanged, to its workers' long-polls. The last snapshot is kept on disk
//!   (`siteAgent.snapshotCache`), so after a restart with upstream unreachable the agent
//!   still serves it, marked stale.
//! - **Fleet:** workers register and heartbeat with the agent; it posts them upstream
//!   (`/worker/fleet`), where they appear as `<site>/<name>`, and registers itself.
//! - **History:** workers' records go to a spool on disk ([`HistorySpool`]) and from there
//!   upstream, redacted at the agent.
//! - **Capacity:** acquires, releases, waits, quota reports and sweep locks are forwarded
//!   upstream, so one `maxRunningQueries` holds across every site. With
//!   `capacity.mode: quotas` upstream, workers admit within their quota with no round trip.
//!
//! The agent runs no query frontends and no engine adapters: only the admin listener.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use chrono::{DateTime, Utc};
use queryflux_core::config::{
    ControlPlaneClientConfig, OnCoordinationUnavailable, ProxyConfig, SiteAgentConfig,
};
use queryflux_core::worker_snapshot::{
    HistoryPolicy, WorkerSnapshotV1, WORKER_SNAPSHOT_SCHEMA_VERSION,
};
use queryflux_frontend::worker_api::WorkerApi;
use queryflux_metrics::prometheus_store::PrometheusMetrics;
use queryflux_metrics::site_agent as metrics;
use queryflux_persistence::{in_memory::InMemoryPersistence, WorkerRecord, WorkerRegistryStore};
use tokio::sync::watch;

use crate::config_source::{fetch_snapshot, FetchMode, Fetched, PeerConfig};
use crate::history_spool::{HistorySpool, SpoolStatus};

/// Workers that reported within this window are relayed upstream as live.
const LIVE_WINDOW_SECS: i64 = 60;

/// The snapshot the agent serves, and where it came from.
#[derive(Clone)]
struct Served {
    /// What workers get.
    snapshot: Arc<WorkerSnapshotV1>,
    /// The revision upstream published. Differs from `snapshot.revision` only when
    /// upstream named a history endpoint (see [`for_workers`]).
    upstream_revision: String,
    /// Upstream's history policy: the redaction the agent applies and, if set, the
    /// endpoint it relays history to.
    history: Option<HistoryPolicy>,
}

/// Workers behind an agent send history to the agent and never past it, so a history
/// endpoint upstream named is removed from what they get (and the snapshot re-sealed);
/// the agent relays to that endpoint itself.
fn for_workers(upstream: WorkerSnapshotV1) -> Served {
    let upstream_revision = upstream.revision.clone();
    let history = upstream.history.clone();
    let snapshot = if history.as_ref().is_some_and(|h| h.endpoint.is_some()) {
        let mut s = upstream;
        if let Some(h) = s.history.as_mut() {
            h.endpoint = None;
        }
        s.seal()
    } else {
        upstream
    };
    Served {
        snapshot: Arc::new(snapshot),
        upstream_revision,
        history,
    }
}

/// `/agent/status`.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentStatus {
    site: String,
    upstream: String,
    upstream_up: bool,
    upstream_revision: Option<String>,
    served_revision: Option<String>,
    /// True while the served snapshot came from the disk cache and upstream has not
    /// confirmed it since this agent started.
    stale: bool,
    /// When upstream last confirmed the served snapshot.
    confirmed_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    consecutive_failures: u32,
    workers: usize,
    history: SpoolStatus,
}

/// The agent's copy of upstream's config.
struct Relay {
    served: Mutex<Option<Served>>,
    status: Mutex<AgentStatus>,
    /// Bumped on every new snapshot; wakes workers' long-polls.
    changes: watch::Sender<u64>,
    /// `stateDir/snapshot.json`, when `snapshotCache` is on.
    cache: Option<PathBuf>,
}

impl Relay {
    fn served(&self) -> Option<Served> {
        self.served
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn status(&self) -> std::sync::MutexGuard<'_, AgentStatus> {
        self.status.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set(&self, upstream: WorkerSnapshotV1, stale: bool, confirmed_at: DateTime<Utc>) {
        let served = for_workers(upstream);
        {
            let mut st = self.status();
            st.upstream_revision = Some(served.upstream_revision.clone());
            st.served_revision = Some(served.snapshot.revision.clone());
            st.stale = stale;
            st.confirmed_at = Some(confirmed_at);
        }
        metrics::SNAPSHOT_STALE.set(i64::from(stale));
        *self.served.lock().unwrap_or_else(|e| e.into_inner()) = Some(served);
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// A new snapshot from upstream: serve it and cache it.
    async fn publish(&self, snapshot: WorkerSnapshotV1) {
        let now = Utc::now();
        tracing::info!(revision = %snapshot.revision, "Site agent serving a new config from upstream");
        if let Some(path) = &self.cache {
            let cached = PeerConfig {
                snapshot: snapshot.clone(),
                confirmed_at: now,
            };
            if let Err(e) = write_private(path, &cached).await {
                tracing::warn!(file = %path.display(), "Could not cache the snapshot on disk: {e:#}");
            }
        }
        self.set(snapshot, false, now);
        self.succeeded();
    }

    fn succeeded(&self) {
        let mut st = self.status();
        if st.stale {
            tracing::info!("Upstream confirmed the cached config");
        }
        st.stale = false;
        st.confirmed_at = Some(Utc::now());
        st.upstream_up = true;
        st.last_error = None;
        st.consecutive_failures = 0;
        drop(st);
        metrics::SNAPSHOT_STALE.set(0);
        metrics::UPSTREAM_UP.set(1);
    }

    fn failed(&self, message: String) {
        tracing::warn!("Site agent upstream sync: {message}");
        let mut st = self.status();
        st.upstream_up = false;
        st.last_error = Some(message);
        st.consecutive_failures = st.consecutive_failures.saturating_add(1);
        metrics::UPSTREAM_UP.set(0);
    }

    /// With no config yet and upstream unreachable, serve the snapshot cached by an
    /// earlier run, marked stale.
    async fn load_cache(&self) {
        let Some(path) = &self.cache else { return };
        let cached: PeerConfig = match tokio::fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(file = %path.display(), "Cached snapshot does not parse: {e}");
                    return;
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(file = %path.display(), "Cached snapshot unreadable: {e}");
                return;
            }
        };
        let s = cached.snapshot;
        if s.schema_version != WORKER_SNAPSHOT_SCHEMA_VERSION || !s.is_sealed() {
            tracing::warn!(file = %path.display(), "Cached snapshot failed its version or integrity check");
            return;
        }
        tracing::warn!(
            revision = %s.revision,
            confirmed_at = %cached.confirmed_at,
            "Upstream unreachable: serving the cached config until it answers"
        );
        self.set(s, true, cached.confirmed_at);
    }

    fn next_delay(&self) -> Duration {
        let failures = self.status().consecutive_failures;
        if failures == 0 {
            return Duration::ZERO;
        }
        let base = 2u64.saturating_pow(failures.min(6)).min(60);
        let jitter_ms = (uuid::Uuid::new_v4().as_u128() % 1000) as u64;
        Duration::from_secs(base) + Duration::from_millis(jitter_ms)
    }
}

/// Write JSON readable only by this user: temp file, fsync, rename.
async fn write_private<T: serde::Serialize>(path: &std::path::Path, value: &T) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let body = serde_json::to_vec(value)?;
    let tmp = path.with_extension("json.tmp");
    let mut opts = tokio::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut f = opts.open(&tmp).await?;
    f.write_all(&body).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

/// Keep the agent's copy of upstream's config current until shutdown.
async fn run_relay(
    relay: Arc<Relay>,
    upstream: ControlPlaneClientConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let wait_secs = upstream.poll_timeout_secs.clamp(1, 60);
    let client = reqwest::Client::builder()
        // The long-poll is held up to `wait_secs`; leave room for the response.
        .timeout(Duration::from_secs(wait_secs + 15))
        .build()
        .unwrap_or_default();
    let url = format!("{}/worker/config", upstream.url.trim_end_matches('/'));
    let bearer = upstream.auth.bearer().to_string();
    let mut cache_tried = false;
    loop {
        let (current, stale) = {
            let st = relay.status();
            (st.upstream_revision.clone(), st.stale)
        };
        // A cached config is checked at once; a confirmed one is long-polled.
        let mode = match current.as_deref() {
            Some(rev) if stale => FetchMode::Check(rev),
            Some(rev) => FetchMode::Wait(rev, wait_secs),
            None => FetchMode::Now,
        };
        let result = tokio::select! {
            r = fetch_snapshot(&client, &url, &bearer, mode) => r,
            _ = shutdown.wait_for(|v| *v) => return,
        };
        match result {
            Ok(Fetched::Unchanged) => relay.succeeded(),
            Ok(Fetched::New(snapshot)) => relay.publish(*snapshot).await,
            // A snapshot that fails its checks never reaches workers; the served one stays.
            Err(message) => {
                relay.failed(message);
                if !cache_tried && relay.served().is_none() {
                    cache_tried = true;
                    relay.load_cache().await;
                }
            }
        }
        let delay = relay.next_delay();
        if !delay.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.wait_for(|v| *v) => return,
            }
        }
    }
}

/// Every heartbeat interval: post the site's live workers upstream, register this agent,
/// and refresh the gauges.
#[allow(clippy::too_many_arguments)]
async fn run_fleet(
    relay: Arc<Relay>,
    registry: Arc<InMemoryPersistence>,
    spool: Arc<HistorySpool>,
    upstream: ControlPlaneClientConfig,
    site: String,
    agent_name: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();
    let base = upstream.url.trim_end_matches('/').to_string();
    let mut interval =
        tokio::time::interval(Duration::from_secs(upstream.heartbeat_interval_secs.max(1)));
    let mut registered = false;
    loop {
        if !crate::startup::tasks::tick_or_shutdown(&mut interval, &mut shutdown).await {
            return;
        }
        let workers = registry
            .list_workers(Utc::now() - chrono::Duration::seconds(LIVE_WINDOW_SECS))
            .await
            .unwrap_or_default();
        metrics::WORKERS.set(workers.len() as i64);
        let st = {
            let mut st = relay.status();
            st.workers = workers.len();
            st.clone()
        };
        if let Some(at) = st.confirmed_at {
            metrics::SNAPSHOT_AGE.set((Utc::now() - at).num_seconds().max(0));
        }

        let fleet = queryflux_persistence::FleetReportV1 {
            site: site.clone(),
            workers,
        };
        match client
            .post(format!("{base}/worker/fleet"))
            .bearer_auth(upstream.auth.bearer())
            .json(&fleet)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => tracing::debug!(status = %r.status(), "Fleet report rejected upstream"),
            Err(e) => tracing::debug!("Fleet report failed: {e}"),
        }

        let history = spool.status();
        let report = WorkerRecord {
            name: agent_name.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            labels: [("site".to_string(), site.clone())].into(),
            coordination_mode: "controlPlane".to_string(),
            downloaded_revision: st.upstream_revision.clone(),
            active_revision: st.upstream_revision.clone(),
            last_error: st.last_error.clone(),
            last_seen: Utc::now(),
            registered_at: None,
            status: queryflux_persistence::WorkerStatus {
                config_source: Some(if st.stale { "cache" } else { "controlPlane" }.into()),
                stale: st.stale,
                confirmed_at: st.confirmed_at,
                consecutive_failures: st.consecutive_failures,
                heartbeat_interval_secs: Some(upstream.heartbeat_interval_secs),
                history: Some(queryflux_persistence::WorkerHistoryStatus {
                    redaction: relay.served().and_then(|s| {
                        s.history.and_then(|h| {
                            serde_json::to_value(h.redaction)
                                .ok()
                                .and_then(|v| v.as_str().map(str::to_string))
                        })
                    }),
                    endpoint: history.endpoint.clone(),
                    queued: history.queued_records,
                    sent: history.sent,
                    dropped: history.dropped,
                    last_error: history.last_error.clone(),
                }),
                ..Default::default()
            },
            kind: "siteAgent".to_string(),
            parent: None,
        };
        let path = if registered { "heartbeat" } else { "register" };
        match client
            .post(format!("{base}/worker/{path}"))
            .bearer_auth(upstream.auth.bearer())
            .json(&report)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                if !registered {
                    tracing::info!(agent = %agent_name, "Site agent registered upstream");
                }
                registered = true;
            }
            Ok(r) => tracing::debug!(status = %r.status(), "Site agent {path} rejected"),
            Err(e) => tracing::debug!("Site agent {path} failed: {e}"),
        }
    }
}

#[derive(Clone)]
struct OpsState {
    relay: Arc<Relay>,
    spool: Arc<HistorySpool>,
    prometheus: Arc<PrometheusMetrics>,
}

/// Ready once the agent has a config to serve (from upstream or its cache).
async fn readyz(State(s): State<OpsState>) -> axum::response::Response {
    match s.relay.served() {
        Some(_) => (StatusCode::OK, "ready").into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no config from upstream yet",
        )
            .into_response(),
    }
}

async fn agent_status(State(s): State<OpsState>) -> axum::response::Response {
    let mut st = s.relay.status().clone();
    st.history = s.spool.status();
    axum::Json(st).into_response()
}

/// The agent's listener: the worker API for this site's workers, plus `/health`,
/// `/readyz`, `/metrics` and `/agent/status`.
fn router(api: WorkerApi, ops: OpsState) -> Router {
    queryflux_frontend::worker_api::router(api).merge(
        Router::new()
            .route("/health", get(|| async { (StatusCode::OK, "ok") }))
            .route("/readyz", get(readyz))
            .route(
                "/metrics",
                get(|State(s): State<OpsState>| async move {
                    (
                        StatusCode::OK,
                        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                        s.prometheus.gather_text(),
                    )
                }),
            )
            .route("/agent/status", get(agent_status))
            .with_state(ops),
    )
}

/// Everything a running agent needs, wired: started tasks plus the router to serve.
struct Agent {
    router: Router,
    shutdown: watch::Sender<bool>,
}

fn start(
    agent: &SiteAgentConfig,
    worker_secret: String,
    prometheus: Arc<PrometheusMetrics>,
) -> Result<Agent> {
    let state_dir = PathBuf::from(&agent.state_dir);
    std::fs::create_dir_all(&state_dir).with_context(|| {
        format!(
            "create siteAgent.stateDir {} (it must be writable by this process)",
            state_dir.display()
        )
    })?;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (changes, changes_rx) = watch::channel(0u64);
    let relay = Arc::new(Relay {
        served: Mutex::new(None),
        status: Mutex::new(AgentStatus {
            site: agent.site_name.clone(),
            upstream: agent.upstream.url.clone(),
            ..Default::default()
        }),
        changes,
        cache: agent
            .snapshot_cache
            .then(|| state_dir.join("snapshot.json")),
    });
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "site-agent".to_string());
    let agent_name = format!("{}/{host}", agent.site_name);
    let spool = HistorySpool::open(
        state_dir.join("spool"),
        agent.history_spool.max_bytes,
        agent_name.clone(),
        agent.site_name.clone(),
        &agent.upstream,
        {
            let relay = relay.clone();
            Arc::new(move || relay.served().and_then(|s| s.history))
        },
    )?;
    let registry = Arc::new(InMemoryPersistence::new());
    // Capacity is forwarded; when upstream is unreachable the agent answers 503 and each
    // worker's `coordination.onUnavailable` decides.
    // Holders are qualified by site, like fleet and history names.
    let remote = Arc::new(
        crate::remote_coordination::RemoteCoordinationStore::new(
            &agent.upstream,
            OnCoordinationUnavailable::FailClosed,
        )
        .with_holder_prefix(format!("{}/", agent.site_name)),
    );
    let api = WorkerApi {
        shared_secret: worker_secret,
        snapshot: {
            let relay = relay.clone();
            Arc::new(move || {
                let served = relay.served();
                Box::pin(async move {
                    served
                        .map(|s| (*s.snapshot).clone())
                        .ok_or_else(|| "the site agent has no config from upstream yet".to_string())
                })
            })
        },
        registry: Some(registry.clone() as Arc<dyn WorkerRegistryStore>),
        changes: Some(changes_rx),
        coordination: Some(remote.clone() as Arc<dyn queryflux_persistence::CoordinationStore>),
        history: Some(spool.clone() as Arc<dyn queryflux_persistence::MetricsStore>),
        quotas: Some(remote as Arc<dyn queryflux_persistence::quota::QuotaStore>),
    };

    tokio::spawn(run_relay(
        relay.clone(),
        agent.upstream.clone(),
        shutdown_rx.clone(),
    ));
    tokio::spawn(run_fleet(
        relay.clone(),
        registry,
        spool.clone(),
        agent.upstream.clone(),
        agent.site_name.clone(),
        agent_name,
        shutdown_rx.clone(),
    ));
    spool.spawn_flusher(shutdown_rx);
    Ok(Agent {
        router: router(
            api,
            OpsState {
                relay,
                spool,
                prometheus,
            },
        ),
        shutdown,
    })
}

/// Run the site agent until SIGINT/SIGTERM.
pub(crate) async fn run(config: ProxyConfig) -> Result<()> {
    let q = &config.queryflux;
    let agent = q
        .site_agent
        .as_ref()
        .context("role: siteAgent needs queryflux.siteAgent")?;
    let worker_secret = q
        .worker_api
        .as_ref()
        .and_then(|a| a.shared_secret.clone())
        .context("role: siteAgent needs queryflux.workerApi.sharedSecret")?;
    if worker_secret == agent.upstream.auth.bearer() {
        tracing::warn!(
            "workerApi.sharedSecret equals siteAgent.upstream's secret: workers at this site \
             then hold the site's upstream credential. Use a different secret."
        );
    }
    let prometheus = Arc::new(
        PrometheusMetrics::new_with_deny_list(q.metrics.tags_deny_list.clone())
            .context("Failed to init Prometheus metrics")?,
    );
    let running = start(agent, worker_secret, prometheus)?;
    let addr = format!("0.0.0.0:{}", q.admin_api.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(
        site = %agent.site_name,
        upstream = %agent.upstream.url,
        "Site agent on {addr} — worker API, /readyz, /metrics, /agent/status"
    );
    let mut stop = running.shutdown.subscribe();
    let serve = axum::serve(listener, running.router).with_graceful_shutdown(async move {
        let _ = stop.wait_for(|v| *v).await;
    });
    let serving = tokio::spawn(async move { serve.await });
    crate::startup::shutdown::shutdown_signal().await;
    let _ = running.shutdown.send(true);
    // Accepted history is on disk already; the next start sends what is left.
    serving.await??;
    tracing::info!("Site agent shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::worker_snapshot::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn snapshot(max_running: i64) -> WorkerSnapshotV1 {
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
            history: Some(HistoryPolicy {
                endpoint: None,
                redaction: HistoryRedaction::MaskLiterals,
            }),
            translation: None,
        }
        .seal()
    }

    /// A control plane on in-memory stores, answering 503 while `up` is false.
    struct Upstream {
        url: String,
        current: Arc<std::sync::Mutex<WorkerSnapshotV1>>,
        changes: watch::Sender<u64>,
        up: Arc<AtomicBool>,
        store: Arc<InMemoryPersistence>,
    }

    impl Upstream {
        async fn start() -> Self {
            let current = Arc::new(std::sync::Mutex::new(snapshot(3)));
            let (changes, rx) = watch::channel(0u64);
            let up = Arc::new(AtomicBool::new(true));
            let store = Arc::new(InMemoryPersistence::new());
            let api = WorkerApi {
                shared_secret: "site-secret".into(),
                snapshot: {
                    let current = current.clone();
                    Arc::new(move || {
                        let s = current.lock().unwrap().clone();
                        Box::pin(async move { Ok(s) })
                    })
                },
                registry: Some(store.clone()),
                changes: Some(rx),
                coordination: Some(store.clone()),
                history: Some(store.clone()),
                quotas: None,
            };
            let gate = up.clone();
            let app = queryflux_frontend::worker_api::router(api).layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let gate = gate.clone();
                    async move {
                        if gate.load(Ordering::SeqCst) {
                            next.run(req).await
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE.into_response()
                        }
                    }
                },
            ));
            let url = serve(app).await;
            Self {
                url,
                current,
                changes,
                up,
                store,
            }
        }

        fn publish(&self, s: WorkerSnapshotV1) {
            *self.current.lock().unwrap() = s;
            self.changes.send_modify(|n| *n += 1);
        }
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn agent_config(upstream: &str, state_dir: &std::path::Path) -> SiteAgentConfig {
        serde_json::from_value(serde_json::json!({
            "siteName": "dc1",
            "stateDir": state_dir.to_string_lossy(),
            "upstream": {
                "url": upstream,
                "allowInsecureHttp": true,
                "pollTimeoutSecs": 1,
                "heartbeatIntervalSecs": 1,
                "auth": { "mode": "sharedSecret", "sharedSecret": "site-secret" }
            }
        }))
        .unwrap()
    }

    /// Start an agent in `dir`; returns its URL and shutdown handle.
    async fn agent(upstream: &str, dir: &std::path::Path) -> (String, watch::Sender<bool>) {
        let prometheus = Arc::new(PrometheusMetrics::new().unwrap());
        let a = start(
            &agent_config(upstream, dir),
            "worker-secret".into(),
            prometheus,
        )
        .unwrap();
        (serve(a.router).await, a.shutdown)
    }

    async fn get_json(url: &str) -> serde_json::Value {
        reqwest::get(url).await.unwrap().json().await.unwrap()
    }

    /// Poll `f` until it returns true, for at most 5 s.
    async fn eventually<F: std::future::Future<Output = bool>>(mut f: impl FnMut() -> F) {
        for _ in 0..100 {
            if f().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("condition not reached in 5s");
    }

    async fn worker_config(agent: &str, secret: &str) -> reqwest::Response {
        reqwest::Client::new()
            .get(format!("{agent}/worker/config"))
            .bearer_auth(secret)
            .send()
            .await
            .unwrap()
    }

    async fn served_revision(agent: &str) -> Option<String> {
        let r = worker_config(agent, "worker-secret").await;
        if !r.status().is_success() {
            return None;
        }
        let s: WorkerSnapshotV1 = r.json().await.unwrap();
        Some(s.revision)
    }

    #[tokio::test]
    async fn config_flows_from_upstream_through_the_agent_and_survives_an_outage() {
        let up = Upstream::start().await;
        let dir = tempfile::tempdir().unwrap();
        let (url, stop) = agent(&up.url, dir.path()).await;

        // The agent serves upstream's snapshot as is, behind the workers' own secret.
        let first = up.current.lock().unwrap().revision.clone();
        eventually(|| async { served_revision(&url).await.as_deref() == Some(first.as_str()) })
            .await;
        assert_eq!(
            worker_config(&url, "site-secret").await.status(),
            StatusCode::UNAUTHORIZED,
            "workers present the agent's secret, not upstream's"
        );
        assert_eq!(
            reqwest::get(format!("{url}/readyz"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        // An edit upstream reaches the agent through its long-poll.
        up.publish(snapshot(7));
        let second = up.current.lock().unwrap().revision.clone();
        eventually(|| async { served_revision(&url).await.as_deref() == Some(second.as_str()) })
            .await;

        // A tampered snapshot never reaches workers.
        let mut bad = snapshot(9);
        bad.clusters[0].max_running_queries = Some(99);
        up.publish(bad);
        eventually(|| async {
            get_json(&format!("{url}/agent/status")).await["lastError"]
                .as_str()
                .is_some_and(|e| e.contains("integrity"))
        })
        .await;
        assert_eq!(
            served_revision(&url).await.as_deref(),
            Some(second.as_str())
        );

        // Upstream down: the agent keeps serving what it has.
        up.publish(snapshot(7));
        up.up.store(false, Ordering::SeqCst);
        eventually(|| async {
            get_json(&format!("{url}/agent/status")).await["upstreamUp"] == false
        })
        .await;
        assert_eq!(
            served_revision(&url).await.as_deref(),
            Some(second.as_str())
        );
        stop.send(true).unwrap();

        // Restarted with upstream still down: serves the disk cache, marked stale.
        let (url2, _stop2) = agent(&up.url, dir.path()).await;
        eventually(|| async { served_revision(&url2).await.as_deref() == Some(second.as_str()) })
            .await;
        let st = get_json(&format!("{url2}/agent/status")).await;
        assert_eq!(st["stale"], true, "{st}");

        // Upstream back: confirmed, no longer stale.
        up.up.store(true, Ordering::SeqCst);
        eventually(|| async { get_json(&format!("{url2}/agent/status")).await["stale"] == false })
            .await;
    }

    #[tokio::test]
    async fn an_upstream_history_endpoint_is_kept_from_workers_and_used_by_the_agent() {
        let mut s = snapshot(3);
        s.history = Some(HistoryPolicy {
            endpoint: Some("https://ingest.example.com/records".into()),
            redaction: HistoryRedaction::Metadata,
        });
        let s = s.seal();
        let served = for_workers(s.clone());
        assert_eq!(served.upstream_revision, s.revision);
        assert!(served.snapshot.history.as_ref().unwrap().endpoint.is_none());
        assert!(served.snapshot.is_sealed());
        assert_ne!(served.snapshot.revision, s.revision);
        assert_eq!(
            served.history.unwrap().endpoint.as_deref(),
            Some("https://ingest.example.com/records")
        );

        // Without an endpoint the snapshot (and its revision) pass through unchanged.
        let plain = snapshot(3);
        assert_eq!(for_workers(plain.clone()).snapshot.revision, plain.revision);
    }

    fn record(id: &str, worker: &str) -> queryflux_persistence::QueryRecord {
        serde_json::from_value(serde_json::json!({
            "proxyQueryId": id,
            "clusterGroup": "g",
            "clusterName": "duck",
            "engineType": "duckDb",
            "frontendProtocol": "trinoHttp",
            "sourceDialect": "trino",
            "targetDialect": "duckDb",
            "wasRewritten": false,
            "wasTranslated": false,
            "sqlPreview": "SELECT * FROM t WHERE name = 'bob'",
            "status": "success",
            "queueDurationMs": 0,
            "executionDurationMs": 1,
            "createdAt": Utc::now(),
            "worker": worker,
        }))
        .unwrap()
    }

    async fn post_records(agent: &str, worker: &str, ids: &[&str]) -> StatusCode {
        let batch = queryflux_persistence::QueryRecordBatchV1 {
            schema_version: queryflux_persistence::QUERY_RECORDS_SCHEMA_VERSION,
            worker: worker.into(),
            records: ids.iter().map(|id| record(id, worker)).collect(),
            site: None,
        };
        reqwest::Client::new()
            .post(format!("{agent}/worker/query-records"))
            .bearer_auth("worker-secret")
            .json(&batch)
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn upstream_history(store: &InMemoryPersistence) -> Vec<(String, String, String)> {
        use queryflux_persistence::QueryHistoryStore;
        let mut rows: Vec<_> = store
            .list_queries(&queryflux_persistence::query_history::QueryFilters {
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|r| {
                (
                    r.proxy_query_id,
                    r.worker.unwrap_or_default(),
                    r.sql_preview,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn history_is_spooled_on_disk_and_reaches_upstream_redacted_and_attributed() {
        let up = Upstream::start().await;
        let dir = tempfile::tempdir().unwrap();
        let (url, stop) = agent(&up.url, dir.path()).await;
        eventually(|| async { served_revision(&url).await.is_some() }).await;

        // Upstream down: the agent still accepts, onto disk.
        up.up.store(false, Ordering::SeqCst);
        assert_eq!(
            post_records(&url, "worker-a", &["q1", "q2"]).await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            post_records(&url, "worker-b", &["q3"]).await,
            StatusCode::ACCEPTED
        );
        let spooled = std::fs::read_dir(dir.path().join("spool")).unwrap().count();
        assert_eq!(spooled, 2, "one file per batch");

        // The agent restarts before upstream returns: the files are picked up again.
        stop.send(true).unwrap();
        let (_url2, _stop2) = agent(&up.url, dir.path()).await;
        up.up.store(true, Ordering::SeqCst);
        eventually(|| async { upstream_history(&up.store).await.len() == 3 }).await;
        let rows = upstream_history(&up.store).await;
        assert_eq!(
            rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
            ["dc1/worker-a", "dc1/worker-a", "dc1/worker-b"],
            "each record keeps its worker, qualified by the site"
        );
        assert!(
            rows.iter().all(|r| !r.2.contains("bob")),
            "redacted at the agent (maskLiterals): {rows:?}"
        );
        eventually(|| async { std::fs::read_dir(dir.path().join("spool")).unwrap().count() == 0 })
            .await;
    }

    #[tokio::test]
    async fn a_full_spool_drops_the_oldest_batches() {
        let dir = tempfile::tempdir().unwrap();
        let upstream: ControlPlaneClientConfig = serde_json::from_value(serde_json::json!({
            "url": "http://127.0.0.1:9", "allowInsecureHttp": true,
            "auth": { "mode": "sharedSecret", "sharedSecret": "s" }
        }))
        .unwrap();
        let one = serde_json::to_vec(&vec![record("q0", "w")]).unwrap().len() as u64;
        let spool = HistorySpool::open(
            dir.path().to_path_buf(),
            one * 2 + one / 2,
            "dc1/agent".into(),
            "dc1".into(),
            &upstream,
            Arc::new(|| None),
        )
        .unwrap();
        use queryflux_persistence::MetricsStore;
        for i in 0..4 {
            spool
                .record_queries(vec![record(&format!("q{i}"), "w")])
                .await
                .unwrap();
        }
        let st = spool.status();
        assert_eq!((st.files, st.dropped), (2, 2), "{st:?}");
        assert!(spool.flush().await.is_err(), "upstream unreachable");
        assert_eq!(spool.status().files, 2, "a failed upload keeps its file");
    }

    /// A real worker config source pointed at the agent activates upstream's config, and
    /// its capacity calls reach upstream.
    #[tokio::test]
    async fn a_worker_behind_the_agent_activates_config_and_shares_capacity_upstream() {
        use crate::config_source::{ConfigSource, LiveHandles, RemoteConfigSource};
        use queryflux_persistence::quota::QuotaStore;
        use queryflux_persistence::CapacityStore;
        let up = Upstream::start().await;
        let dir = tempfile::tempdir().unwrap();
        let (url, _stop) = agent(&up.url, dir.path()).await;
        eventually(|| async { served_revision(&url).await.is_some() }).await;

        let cp: ControlPlaneClientConfig = serde_json::from_value(serde_json::json!({
            "url": url, "allowInsecureHttp": true, "pollTimeoutSecs": 1,
            "auth": { "mode": "sharedSecret", "sharedSecret": "worker-secret" }
        }))
        .unwrap();
        let state = Arc::new(std::sync::Mutex::new(
            crate::config_source::WorkerSyncState::default(),
        ));
        let handles = LiveHandles {
            live: Arc::new(tokio::sync::RwLock::new(
                crate::live_config::empty_live_config(),
            )),
            cache: Arc::new(tokio::sync::Mutex::new(
                crate::live_config::AdapterReloadCache {
                    adapters: Default::default(),
                    config_json: Default::default(),
                    cluster_states: Default::default(),
                    routing_fallback: String::new(),
                    routers_cfg: Vec::new(),
                    strategies: Default::default(),
                },
            )),
            metrics: Arc::new(queryflux_metrics::NoopMetricsStore),
            changed: None,
            translation: None,
        };
        RemoteConfigSource::new(&cp, state.clone())
            .reload(&handles)
            .await;
        let upstream_rev = up.current.lock().unwrap().revision.clone();
        assert_eq!(
            state.lock().unwrap().active_revision.as_deref(),
            Some(upstream_rev.as_str()),
            "the worker runs the revision upstream published: {:?}",
            state.lock().unwrap().last_error
        );
        assert!(handles.live.read().await.is_configured());

        // Capacity calls go through the agent to upstream's store.
        let worker = crate::remote_coordination::remote_coordination(
            &cp,
            OnCoordinationUnavailable::FailClosed,
        );
        worker.publish_running_count("duck", 4).await.unwrap();
        assert_eq!(up.store.active_count("duck").await.unwrap(), 4);
        assert!(worker.try_acquire("duck", 10, "w1", "q1").await.unwrap());
        // Upstream has quotas off: the worker is told so and keeps per-query leases.
        assert!(worker.report_demand("w1", &[]).await.unwrap().is_none());

        // Upstream unreachable: the agent answers with an error, and the worker's
        // `onUnavailable` decides.
        up.up.store(false, Ordering::SeqCst);
        assert!(worker.try_acquire("duck", 10, "w1", "q2").await.is_err());
    }

    #[tokio::test]
    async fn workers_are_relayed_upstream_under_the_site() {
        use queryflux_persistence::WorkerRegistryStore;
        let up = Upstream::start().await;
        let dir = tempfile::tempdir().unwrap();
        let (url, _stop) = agent(&up.url, dir.path()).await;
        let worker = serde_json::json!({ "name": "worker-0", "version": "test" });
        let r = reqwest::Client::new()
            .post(format!("{url}/worker/register"))
            .bearer_auth("worker-secret")
            .json(&worker)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT);

        let since = Utc::now() - chrono::Duration::seconds(60);
        eventually(|| async {
            let workers = up.store.list_workers(since).await.unwrap();
            workers
                .iter()
                .any(|w| w.name == "dc1/worker-0" && w.parent.as_deref() == Some("dc1"))
                && workers.iter().any(|w| w.kind == "siteAgent")
        })
        .await;
    }
}
