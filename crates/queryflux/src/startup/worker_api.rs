//! Serving config to workers from a standalone or control-plane process (#255).

use std::sync::Arc;

use anyhow::Result;
use queryflux_core::config::ProxyConfig;
use queryflux_core::worker_snapshot::CredentialMode;
use queryflux_frontend::worker_api::WorkerApi;
use queryflux_persistence::ConfigSnapshotStore;

use crate::live_config::{
    build_worker_snapshot, inline_script_files, snapshot_from_inputs, validate_snapshot_inputs,
    LiveConfigInputs, SettingsInputs,
};

/// The worker API for this process, if `queryflux.workerApi.enabled`.
///
/// With Postgres, every request builds the snapshot from one consistent read, so admin
/// edits reach workers on their next poll. Without Postgres the config cannot change at
/// runtime, so the snapshot is built once, now, from the startup inputs.
pub(crate) fn worker_api(
    config: &ProxyConfig,
    store: Option<Arc<dyn ConfigSnapshotStore>>,
    startup_inputs: &LiveConfigInputs,
    registry: Arc<dyn queryflux_persistence::WorkerRegistryStore>,
    changes: tokio::sync::watch::Receiver<u64>,
    coordination: Option<Arc<dyn queryflux_persistence::CoordinationStore>>,
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
            Arc::new(move || {
                let store = store.clone();
                let yaml = yaml.clone();
                Box::pin(async move {
                    build_worker_snapshot(store.as_ref(), &yaml, credentials)
                        .await
                        .map_err(|e| format!("{e:#}"))
                })
            })
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
            let fixed = snapshot_from_inputs(&inputs, credentials)?;
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
    }))
}
