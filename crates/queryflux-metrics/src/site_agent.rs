//! Site agent metrics (#255): the upstream link, the served snapshot and the history
//! spool. Registered on every process; they stay at zero except on a site agent.
use std::sync::LazyLock;

use prometheus::{IntCounter, IntGauge, Registry};

fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::new(name, help).expect("valid site agent gauge descriptor")
}

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::new(name, help).expect("valid site agent counter descriptor")
}

/// 1 while the last exchange with upstream succeeded.
pub static UPSTREAM_UP: LazyLock<IntGauge> = LazyLock::new(|| {
    gauge(
        "queryflux_agent_upstream_up",
        "1 while the site agent's last exchange with its upstream control plane succeeded",
    )
});
/// Seconds since upstream last confirmed the served snapshot.
pub static SNAPSHOT_AGE: LazyLock<IntGauge> = LazyLock::new(|| {
    gauge(
        "queryflux_agent_snapshot_age_seconds",
        "Seconds since upstream last confirmed the snapshot this agent serves",
    )
});
/// 1 while the served snapshot came from the disk cache, not upstream.
pub static SNAPSHOT_STALE: LazyLock<IntGauge> = LazyLock::new(|| {
    gauge(
        "queryflux_agent_snapshot_stale",
        "1 while the site agent serves a snapshot from its disk cache that upstream has not confirmed",
    )
});
pub static WORKERS: LazyLock<IntGauge> = LazyLock::new(|| {
    gauge(
        "queryflux_agent_workers",
        "Workers that reported to this site agent recently",
    )
});
pub static SPOOL_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    gauge(
        "queryflux_agent_history_spool_bytes",
        "Query history waiting on disk to be sent upstream",
    )
});
pub static HISTORY_SENT: LazyLock<IntCounter> = LazyLock::new(|| {
    counter(
        "queryflux_agent_history_sent_total",
        "Query records the site agent delivered upstream",
    )
});
pub static HISTORY_DROPPED: LazyLock<IntCounter> = LazyLock::new(|| {
    counter(
        "queryflux_agent_history_dropped_total",
        "Query records dropped because the history spool was full",
    )
});

pub(crate) fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    for g in [
        &*UPSTREAM_UP,
        &*SNAPSHOT_AGE,
        &*SNAPSHOT_STALE,
        &*WORKERS,
        &*SPOOL_BYTES,
    ] {
        registry.register(Box::new(g.clone()))?;
    }
    registry.register(Box::new(HISTORY_SENT.clone()))?;
    registry.register(Box::new(HISTORY_DROPPED.clone()))
}
