//! Test-only support code, shared across this crate's provider test modules.
//! Compiled only under `#[cfg(test)]` — never part of a release build.

pub mod fake_hms_server;
