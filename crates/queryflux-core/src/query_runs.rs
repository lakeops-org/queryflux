//! What the collector (`queryflux-collector`) reports about runs on an engine, and what
//! the control plane tells it back (`plans/query-enrichment-plan.md`).
//!
//! A run is keyed by `(cluster, engine_query_id)`: the engine's own id for one execution.
//! Runs QueryFlux routed are linked to their `query_records` row on ingest; runs sent
//! straight to the engine are kept as they are.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `schemaVersion` of [`QueryRunBatchV1`].
pub const QUERY_RUNS_SCHEMA_VERSION: u32 = 1;

/// The polling clock a batch came from. The fast clock reads short-lived engine data
/// (Trino query info) before it expires; the slow clock reads lagging history views.
pub const FAST_CLOCK: &str = "fast";

/// One engine's measurements of one run, in fixed units. Every field is optional because
/// engines expose different things (`plans/engine-metrics-mapping.md`). A number only one
/// engine reports goes in [`QueryRun::extra`] until a second engine reports it too.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecMetrics {
    pub elapsed_ms: Option<u64>,
    /// Waiting for capacity (queue), not for compute to start.
    pub queued_ms: Option<u64>,
    /// Waiting for compute to start (a suspended warehouse resuming).
    pub provisioning_ms: Option<u64>,
    pub planning_ms: Option<u64>,
    pub execution_ms: Option<u64>,
    pub cpu_ms: Option<u64>,
    pub bytes_scanned: Option<u64>,
    pub rows_scanned: Option<u64>,
    pub peak_memory_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
    pub network_bytes: Option<u64>,
    pub rows_returned: Option<u64>,
    pub bytes_returned: Option<u64>,
    pub rows_written: Option<u64>,
    pub bytes_written: Option<u64>,
    pub result_cache_hit: Option<bool>,
}

/// One run as the engine reported it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRun {
    pub engine_query_id: String,
    pub user_name: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    /// The engine's final state, as it names it (`FINISHED`, `FAILED`, …).
    pub status: Option<String>,
    pub statement_type: Option<String>,
    /// Exact hash of the statement text (`rich_fingerprint`'s `query_hash`, in the engine's
    /// dialect), for matching against the SQL QueryFlux sent. `u64` bits stored as `i64`.
    pub sql_hash: Option<i64>,
    /// `fast_hash` of the statement text: the shape, for runs QueryFlux didn't route.
    pub fast_hash: Option<i64>,
    #[serde(flatten)]
    pub metrics: ExecMetrics,
    /// Engine-specific numbers, under namespaced keys (`trino.processedInputPositions`).
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Runs one cluster finished in `[windowStart, windowEnd)`, sent by a collector
/// (`POST /collector/runs`). Ingest is idempotent: a run sent twice is one row. The
/// control plane advances the collector's watermark to `windowEnd` in the same
/// transaction that stores the runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRunBatchV1 {
    pub schema_version: u32,
    /// The collector's name: the owner of the watermark.
    pub collector: String,
    pub cluster: String,
    /// The engine type, as `query_records.engine_type` names it (`Trino`, …). Decides
    /// whether runs can be linked by engine query id.
    pub engine: String,
    pub clock: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub runs: Vec<QueryRun>,
}

/// What the control plane tells a collector (`GET /collector/config?collector=NAME`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectorConfigV1 {
    /// Where each cluster's clock last stopped: cluster → clock → window end.
    #[serde(default)]
    pub watermarks: BTreeMap<String, BTreeMap<String, DateTime<Utc>>>,
}

/// Whether an engine's `backend_query_id` is a real query id, so runs can be linked to
/// QueryFlux records by it. StarRocks and MySQL record the connection id used for `KILL`
/// there, which is not a query id: those runs are linked by SQL hash and time instead.
pub fn links_by_engine_id(engine: &str) -> bool {
    matches!(engine, "Trino" | "Athena" | "ClickHouse")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_round_trips_with_metrics_flattened() {
        let run = QueryRun {
            engine_query_id: "20261022_000001_00001_abcde".into(),
            metrics: ExecMetrics {
                bytes_scanned: Some(42),
                ..Default::default()
            },
            ..Default::default()
        };
        let json = serde_json::to_value(&run).unwrap();
        assert_eq!(json["bytesScanned"], 42);
        assert_eq!(serde_json::from_value::<QueryRun>(json).unwrap(), run);
    }

    #[test]
    fn only_query_ids_link_by_engine_id() {
        assert!(links_by_engine_id("Trino"));
        assert!(!links_by_engine_id("StarRocks"));
        assert!(!links_by_engine_id("MySql"));
    }
}
