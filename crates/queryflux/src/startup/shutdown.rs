//! Waiting for a shutdown signal and shutting down gracefully.

use std::sync::Arc;

use queryflux_frontend::state::AppState;
use tokio::sync::watch;

use super::frontends::FrontendTasks;

/// Wait for SIGINT/SIGTERM, or for a frontend to exit unexpectedly.
pub(crate) async fn wait_for_shutdown(frontend_tasks: &mut FrontendTasks) {
    // Wait for either a shutdown signal or an unexpected frontend exit.
    let shutdown_signal = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => tracing::info!("Received SIGINT — initiating graceful shutdown"),
                _ = sigterm.recv() => tracing::info!("Received SIGTERM — initiating graceful shutdown"),
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            tracing::info!("Received Ctrl-C — initiating graceful shutdown");
        }
    };

    tokio::select! {
        _ = shutdown_signal => {},
        exit = frontend_tasks.join_next() => {
            if let Some(exit) = exit {
                match exit.result {
                    Ok(Ok(())) => tracing::warn!(frontend = exit.name, "Frontend exited unexpectedly"),
                    Ok(Err(error)) => tracing::error!(frontend = exit.name, "Frontend exited unexpectedly: {error}"),
                    Err(error) => tracing::error!(frontend = exit.name, "Frontend task failed: {error}"),
                }
            }
        },
    }
}

/// Stop accepting, drain in-flight work (bounded by `drain_timeout_secs`), then release
/// this replica's capacity leases.
pub(crate) async fn graceful_shutdown(
    shutdown_tx: &watch::Sender<bool>,
    frontend_tasks: &mut FrontendTasks,
    app_state: &Arc<AppState>,
    drain_timeout_secs: u64,
) {
    // --- Phase 1: signal all frontends to stop accepting new connections ---
    let _ = shutdown_tx.send(true);

    // --- Phase 2: drain in-flight requests ---
    let drain_timeout = std::time::Duration::from_secs(drain_timeout_secs);
    tracing::info!("Draining in-flight requests (timeout: {drain_timeout_secs}s)...");

    let drain_future = async {
        // Wait for enabled frontends to finish processing in-flight requests.
        // Axum frontends complete when all connections are done; wire frontends
        // return immediately from their accept loop but spawned connection tasks
        // continue running.
        frontend_tasks.drain().await;

        // Poll persistence until no executing or queued queries remain (or until
        // the outer timeout fires). This covers spawned wire-protocol connection
        // handlers that are still mid-query after the accept loop exited.
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            interval.tick().await;
            let executing = app_state
                .persistence
                .list_all()
                .await
                .map(|v| v.len())
                .unwrap_or(0);
            let queued = app_state
                .persistence
                .list_queued()
                .await
                .map(|v| v.len())
                .unwrap_or(0);
            if executing == 0 && queued == 0 {
                tracing::info!("All in-flight queries drained");
                break;
            }
            tracing::info!(executing, queued, "Waiting for queries to drain...");
        }
    };

    if tokio::time::timeout(drain_timeout, drain_future)
        .await
        .is_err()
    {
        let outstanding_frontends = frontend_tasks.outstanding_names();
        let executing = app_state
            .persistence
            .list_all()
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        let queued = app_state
            .persistence
            .list_queued()
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        tracing::warn!(
            executing,
            queued,
            ?outstanding_frontends,
            "Drain timeout reached after {drain_timeout_secs}s — forcing shutdown"
        );
    }

    // --- Phase 3: release capacity leases owned by this replica ---
    tracing::info!("Releasing capacity leases for this replica...");
    if let Some(cap) = &app_state.capacity_store {
        if let Err(e) = cap.release_all_for_instance(&app_state.instance_id).await {
            tracing::warn!("Failed to release capacity leases on shutdown: {e}");
        } else {
            tracing::info!("Capacity leases released");
        }
    }
}
