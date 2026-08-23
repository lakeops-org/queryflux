//! Re-export of the shared polyglot worker pool from `queryflux-core`.
//!
//! Kept as a module path for existing `crate::polyglot_pool::run` call sites.

pub use queryflux_core::polyglot_pool::run;
