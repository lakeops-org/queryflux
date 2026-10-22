//! `queryflux-collector`: polls engines for what they know about each query run and
//! reports it to the QueryFlux control plane (`plans/query-enrichment-plan.md`).
//!
//! Poll only: engines are never configured to send anything, and QueryFlux's query path
//! is untouched. Each engine gets an [`EngineCollector`]; the [`runner`] reads every
//! cluster's history by time window from its watermark and posts the runs upstream.

pub mod config;
pub mod runner;
pub mod trino;
pub mod upstream;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::query_runs::QueryRun;

/// Reads one cluster's history.
#[async_trait]
pub trait EngineCollector: Send + Sync {
    /// The engine type, as `query_records.engine_type` names it (`Trino`, …).
    fn engine(&self) -> &'static str;

    /// Runs that ended in `[start, end)`, one per engine query id.
    async fn list_runs(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<QueryRun>>;
}

/// Hashes of a statement's text, computed the way QueryFlux computes them for the SQL it
/// sends, so a run can be matched to its query and grouped by shape.
pub fn hashes(sql: &str, dialect: &str) -> (Option<i64>, Option<i64>) {
    let exact = queryflux_fingerprint::rich_fingerprint(sql, None, dialect, dialect)
        .map(|fp| fp.query_hash as i64);
    let shape = queryflux_fingerprint::fast_hash(sql) as i64;
    (exact, Some(shape))
}
