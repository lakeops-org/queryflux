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

pub use config::{AccessControlConfig, OnMissingSchema, ProviderConfig};
pub use controller::{AccessController, AccessControllerConfig};
pub use metrics::{AccessMetricsSink, NoopMetrics};
pub use provider::{PolicyDecisionProvider, PolicyError};
pub use queryflux_core::access_model::{
    AccessDecision, AccessRequest, AccessResource, ColumnMask, Columns, Identity, MaskType,
    Operation, RequestContext, ResourceDecision, RowFilter,
};
