//! Startup phases of a QueryFlux process, so each role can choose which to run.

pub(crate) mod frontends;
pub(crate) mod shutdown;
pub(crate) mod stores;
pub(crate) mod tasks;
