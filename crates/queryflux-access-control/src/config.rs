//! Config for the access-control layer. The plain serde structs live in
//! `queryflux_core::access_config` (so `ProxyConfig` can reference them without a
//! dependency cycle); this module re-exports them and adds the runtime builders.

pub use queryflux_core::access_config::{
    AccessControlConfig, ClientCredentials, GroupOverride, OnMissingSchema, OpaProviderConfig,
    ProviderKind,
};
