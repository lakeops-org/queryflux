//! Provider implementations. OPA is the only one shipped in v1; Cerbos / Cedar / etc.
//! would each be a new submodule + a [`crate::config::ProviderConfig`] variant.

pub mod opa;
