//! Data-level access control: the pluggable policy-decision layer.
//!
//! `queryflux-core::access_model` owns the neutral request/response types. This crate owns
//! the *mechanism*: the [`PolicyDecisionProvider`] trait, the [`AccessController`]
//! (provider call + fail-open + TTL cache + metrics hook), the config, and the one shipped
//! provider implementation ([`providers::opa`]).
//!
//! The `Guard` implementation that plugs this into the guardrail chain lives in
//! `queryflux-frontend` (it needs `queryflux-guardrails` + `queryflux-translation`, which
//! this crate deliberately does not).

pub mod config;
pub mod controller;
pub mod metrics;
pub mod provider;
pub mod providers;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub use config::{AccessConnectionConfig, AccessControlConfig, OnMissingSchema, ProviderKind};
pub use controller::{AccessController, AccessControllerConfig};
pub use metrics::{AccessMetricsSink, DecisionMetric, NoopMetrics};
pub use provider::{PolicyDecisionProvider, PolicyError};
pub use providers::opa::OpaProvider;
pub use queryflux_core::access_model::{
    AccessDecision, AccessRequest, AccessResource, ColumnMask, Columns, Identity, MaskType,
    Operation, RequestContext, ResourceDecision, RowFilter,
};

/// Build the runtime [`AccessController`] for one named connection.
pub fn build_controller(
    conn: &AccessConnectionConfig,
    group_fail_open: HashMap<String, bool>,
    metrics: Arc<dyn AccessMetricsSink>,
) -> Result<AccessController, String> {
    let opa = conn.opa_config()?;
    let provider: Arc<dyn PolicyDecisionProvider> = Arc::new(OpaProvider::new(opa)?);

    Ok(AccessController::new(AccessControllerConfig {
        provider,
        metrics,
        operations: conn
            .operations
            .iter()
            .map(|s| Operation(s.clone()))
            .collect(),
        fail_open_default: conn.fail_open,
        group_fail_open,
        cache_ttl: Duration::from_millis(conn.cache_ttl_ms),
        cache_capacity: conn.cache_capacity,
    }))
}

/// Build one [`AccessController`] per named connection in `cfg.connections`, keyed by
/// connection name. `cfg.validate()` should have run first (guarantees a `"default"` entry
/// and that every `groups.<name>.connection` override references a defined connection).
pub fn build_controllers(
    cfg: &AccessControlConfig,
    metrics: Arc<dyn AccessMetricsSink>,
) -> Result<HashMap<String, AccessController>, String> {
    let group_fail_open: HashMap<String, bool> = cfg
        .groups
        .iter()
        .filter_map(|(g, o)| o.fail_open.map(|v| (g.clone(), v)))
        .collect();

    cfg.connections
        .iter()
        .map(|(name, conn)| {
            build_controller(conn, group_fail_open.clone(), metrics.clone())
                .map(|c| (name.clone(), c))
                .map_err(|e| format!("accessControl.connections.{name}: {e}"))
        })
        .collect()
}
