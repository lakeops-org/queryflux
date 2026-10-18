//! Serving config to workers from a standalone or control-plane process (#255).

use std::sync::Arc;

use anyhow::Result;
use queryflux_core::config::ProxyConfig;
use queryflux_core::worker_snapshot::{CredentialMode, WorkerSnapshotV1};
use queryflux_frontend::worker_api::WorkerApi;
use queryflux_persistence::ConfigSnapshotStore;

use crate::live_config::{
    build_worker_snapshot, history_policy, inline_script_files, snapshot_from_inputs,
    translation_policy, validate_snapshot_inputs, LiveConfigInputs, SettingsInputs,
};

/// The worker API for this process, if `queryflux.workerApi.enabled`.
///
/// With Postgres, the snapshot is built from one consistent read and reused until this
/// process reloads its config (`changes`), so admin edits reach workers on their next
/// poll while thousands of waiting workers share one build. Without Postgres the config
/// cannot change at runtime, so the snapshot is built once, now, from the startup inputs.
pub(crate) fn worker_api(
    config: &ProxyConfig,
    store: Option<Arc<dyn ConfigSnapshotStore>>,
    startup_inputs: &LiveConfigInputs,
    registry: Arc<dyn queryflux_persistence::WorkerRegistryStore>,
    changes: tokio::sync::watch::Receiver<u64>,
    coordination: Option<Arc<dyn queryflux_persistence::CoordinationStore>>,
    history: Arc<dyn queryflux_persistence::MetricsStore>,
) -> Result<Option<WorkerApi>> {
    let Some(api_cfg) = config.queryflux.worker_api.as_ref().filter(|a| a.enabled) else {
        return Ok(None);
    };
    let shared_secret = api_cfg.shared_secret.clone().unwrap_or_default();
    // Embedded over TLS: the trusted single-fleet model. Secret references for lower-trust
    // workers come later.
    let credentials = CredentialMode::Embedded;

    let snapshot: queryflux_frontend::worker_api::WorkerSnapshotFn = match store {
        Some(store) => {
            let yaml = Arc::new(config.clone());
            let build: queryflux_frontend::worker_api::WorkerSnapshotFn = Arc::new(move || {
                let store = store.clone();
                let yaml = yaml.clone();
                Box::pin(async move {
                    build_worker_snapshot(store.as_ref(), &yaml, credentials)
                        .await
                        .map_err(|e| format!("{e:#}"))
                })
            });
            // The last good snapshot wraps the per-reload cache: requests normally share
            // the cached build, and if a rebuild fails or stalls (Postgres down), workers
            // still get the last snapshot that built.
            let served = serve_last_good(cache_until_reload(build, changes.clone()));
            // Build once now, so a Postgres outage before the first worker request still
            // has a config to serve.
            tokio::spawn({
                let served = served.clone();
                async move {
                    if let Err(e) = served().await {
                        tracing::warn!("Initial worker config snapshot failed: {e}");
                    }
                }
            });
            served
        }
        None => {
            let mut inputs = startup_inputs.clone();
            inputs.settings = SettingsInputs::from_yaml(config);
            inline_script_files(&mut inputs)?;
            let issues = validate_snapshot_inputs(&inputs);
            if !issues.is_empty() {
                anyhow::bail!(
                    "workerApi: config cannot be published to workers: {}",
                    issues.join("; ")
                );
            }
            let mut fixed = snapshot_from_inputs(&inputs, credentials)?;
            fixed.history = history_policy(config);
            fixed.translation = Some(translation_policy(config));
            let fixed = fixed.seal();
            tracing::info!(revision = %fixed.revision, "Serving a fixed worker config snapshot (no Postgres)");
            Arc::new(move || {
                let fixed = fixed.clone();
                Box::pin(async move { Ok(fixed) })
            })
        }
    };
    tracing::info!(
        shared_capacity = coordination.is_some(),
        "Worker API enabled on the admin listener (/worker/config)"
    );
    Ok(Some(WorkerApi {
        shared_secret,
        snapshot,
        registry: Some(registry),
        changes: Some(changes),
        coordination,
        history: Some(history),
    }))
}

/// Reuse the last built snapshot until this process reloads its config. Every worker poll
/// and every long-poll wake-up asks for a snapshot, so without this each one would re-read
/// the whole config from Postgres: steady polling would cost one consistent read per
/// worker per poll, and every config edit a burst of one read per waiting worker.
///
/// `changes` is bumped after each reload (admin writes, other replicas' revision
/// notifications, the periodic timer), which is when the stored config can differ. The
/// generation is read before building, so a reload that lands mid-build makes the next
/// request rebuild. Concurrent requests wait for one build instead of each starting their
/// own; a failed build is not cached.
pub(crate) fn cache_until_reload(
    build: queryflux_frontend::worker_api::WorkerSnapshotFn,
    changes: tokio::sync::watch::Receiver<u64>,
) -> queryflux_frontend::worker_api::WorkerSnapshotFn {
    let cached: Arc<tokio::sync::Mutex<Option<(u64, WorkerSnapshotV1)>>> = Arc::default();
    Arc::new(move || {
        let build = build.clone();
        let cached = cached.clone();
        let generation = *changes.borrow();
        Box::pin(async move {
            let mut slot = cached.lock().await;
            if let Some((built_at, snapshot)) = slot.as_ref() {
                if *built_at == generation {
                    return Ok(snapshot.clone());
                }
            }
            let snapshot = build().await?;
            *slot = Some((generation, snapshot.clone()));
            Ok(snapshot)
        })
    })
}

/// Longest a snapshot build may take before the last good snapshot is served instead. A
/// Postgres outage otherwise surfaces only after the pool's acquire timeout (30s default).
const SNAPSHOT_BUILD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Serve the last snapshot that built successfully when a build fails or takes longer than
/// [`SNAPSHOT_BUILD_TIMEOUT`]: Postgres is down, or the stored config no longer validates. Workers then keep getting a config that was valid
/// (a worker starting during the outage too), instead of an error. It can only lack edits
/// that could not be read or published. The error is still logged on every failed build.
pub(crate) fn serve_last_good(
    build: queryflux_frontend::worker_api::WorkerSnapshotFn,
) -> queryflux_frontend::worker_api::WorkerSnapshotFn {
    let last_good: Arc<std::sync::Mutex<Option<WorkerSnapshotV1>>> = Arc::default();
    Arc::new(move || {
        let build = build.clone();
        let last_good = last_good.clone();
        Box::pin(async move {
            let built = match tokio::time::timeout(SNAPSHOT_BUILD_TIMEOUT, build()).await {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "config snapshot build took longer than {}s",
                    SNAPSHOT_BUILD_TIMEOUT.as_secs()
                )),
            };
            match built {
                Ok(snapshot) => {
                    *last_good.lock().unwrap_or_else(|e| e.into_inner()) = Some(snapshot.clone());
                    Ok(snapshot)
                }
                Err(e) => {
                    let cached = last_good.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    match cached {
                        Some(snapshot) => {
                            tracing::warn!(
                                revision = %snapshot.revision,
                                "Worker config snapshot build failed, serving the last good \
                                 revision: {e}"
                            );
                            Ok(snapshot)
                        }
                        None => Err(e),
                    }
                }
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn counting_build(
        builds: Arc<AtomicUsize>,
        fail: Arc<std::sync::atomic::AtomicBool>,
    ) -> queryflux_frontend::worker_api::WorkerSnapshotFn {
        Arc::new(move || {
            let n = builds.fetch_add(1, Ordering::SeqCst);
            let fail = fail.load(Ordering::SeqCst);
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                if fail {
                    return Err("postgres down".to_string());
                }
                let inputs = LiveConfigInputs {
                    routing_fallback: format!("g{n}"),
                    ..Default::default()
                };
                Ok(snapshot_from_inputs(&inputs, CredentialMode::Embedded).unwrap())
            })
        })
    }

    #[tokio::test]
    async fn snapshots_are_built_once_per_reload() {
        let builds = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        let served = cache_until_reload(counting_build(builds.clone(), fail.clone()), rx);

        // Many concurrent polls share one build.
        let polls: Vec<_> = (0..50).map(|_| tokio::spawn(served())).collect();
        let mut revisions = std::collections::HashSet::new();
        for p in polls {
            revisions.insert(p.await.unwrap().unwrap().revision);
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(revisions.len(), 1);
        let first = revisions.into_iter().next().unwrap();
        assert_eq!(served().await.unwrap().revision, first);
        assert_eq!(builds.load(Ordering::SeqCst), 1);

        // A reload invalidates it.
        tx.send(1).unwrap();
        assert_ne!(served().await.unwrap().revision, first);
        assert_eq!(builds.load(Ordering::SeqCst), 2);

        // A failed build is not cached.
        tx.send(2).unwrap();
        fail.store(true, Ordering::SeqCst);
        assert!(served().await.is_err());
        fail.store(false, Ordering::SeqCst);
        assert!(served().await.is_ok());
        assert_eq!(builds.load(Ordering::SeqCst), 4);
    }

    fn snapshot(fallback: &str) -> WorkerSnapshotV1 {
        let inputs = LiveConfigInputs {
            routing_fallback: fallback.into(),
            ..Default::default()
        };
        snapshot_from_inputs(&inputs, CredentialMode::Embedded).unwrap()
    }

    #[tokio::test]
    async fn a_failed_build_serves_the_last_good_snapshot() {
        let healthy = Arc::new(AtomicBool::new(false));
        let next = Arc::new(std::sync::Mutex::new(snapshot("a")));
        let build: queryflux_frontend::worker_api::WorkerSnapshotFn = Arc::new({
            let healthy = healthy.clone();
            let next = next.clone();
            move || {
                let ok = healthy.load(Ordering::SeqCst);
                let s = next.lock().unwrap().clone();
                Box::pin(async move {
                    if ok {
                        Ok(s)
                    } else {
                        Err("postgres down".to_string())
                    }
                })
            }
        });
        let served = serve_last_good(build);

        // Nothing built yet: the error passes through.
        assert!(served().await.is_err());

        healthy.store(true, Ordering::SeqCst);
        let first = served().await.unwrap();

        healthy.store(false, Ordering::SeqCst);
        *next.lock().unwrap() = snapshot("b");
        assert_eq!(served().await.unwrap().revision, first.revision);

        healthy.store(true, Ordering::SeqCst);
        assert_ne!(served().await.unwrap().revision, first.revision);
    }
}
