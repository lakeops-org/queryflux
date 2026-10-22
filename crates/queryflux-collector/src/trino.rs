//! Trino: `GET /v1/query` lists the queries the coordinator still holds (finished ones
//! until `query.min-expire-age`, 15 minutes by default), each with its `queryStats`.
//! Per-query detail (`/v1/query/{id}`: plan, Iceberg split-source metrics) comes later.

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::config::{ClusterAuth, ClusterConfig};
use queryflux_core::query_runs::{ExecMetrics, QueryRun};
use serde::Deserialize;
use serde_json::Value;

use crate::{hashes, EngineCollector};

/// User sent as `X-Trino-User` when the cluster's auth names none.
const DEFAULT_USER: &str = "queryflux-collector";

pub struct TrinoCollector {
    http: reqwest::Client,
    endpoint: String,
    user: String,
    auth: Option<ClusterAuth>,
}

impl TrinoCollector {
    pub fn new(name: &str, cluster: &ClusterConfig) -> anyhow::Result<Self> {
        let endpoint = cluster
            .endpoint
            .as_deref()
            .with_context(|| format!("cluster {name}: endpoint is required for Trino"))?
            .trim_end_matches('/')
            .to_string();
        let user = match &cluster.auth {
            Some(ClusterAuth::Basic { username, .. }) => username.clone(),
            _ => DEFAULT_USER.to_string(),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(
                cluster.tls.as_ref().is_some_and(|t| t.insecure_skip_verify),
            )
            .build()?;
        Ok(Self {
            http,
            endpoint,
            user,
            auth: cluster.auth.clone(),
        })
    }
}

#[async_trait]
impl EngineCollector for TrinoCollector {
    fn engine(&self) -> &'static str {
        "Trino"
    }

    async fn list_runs(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<QueryRun>> {
        let mut req = self
            .http
            .get(format!("{}/v1/query", self.endpoint))
            .header("X-Trino-User", &self.user);
        req = match &self.auth {
            Some(ClusterAuth::Basic { username, password }) => {
                req.basic_auth(username, Some(password))
            }
            Some(ClusterAuth::Bearer { token }) => req.bearer_auth(token),
            _ => req,
        };
        let resp = req.send().await.context("reach Trino")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("GET /v1/query: {status}");
        }
        let queries: Vec<BasicQueryInfo> = resp.json().await.context("decode /v1/query")?;
        Ok(runs_in_window(queries, start, end))
    }
}

/// Finished queries (those with an end time) that ended in `[start, end)`.
fn runs_in_window(
    queries: Vec<BasicQueryInfo>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<QueryRun> {
    queries
        .into_iter()
        .filter_map(|q| {
            let ended = instant(q.query_stats.end_time.as_ref()?)?;
            (start <= ended && ended < end).then(|| to_run(q, ended))
        })
        .collect()
}

fn to_run(q: BasicQueryInfo, ended: DateTime<Utc>) -> QueryRun {
    let s = &q.query_stats;
    let (sql_hash, fast_hash) = hashes(&q.query, "trino");
    let ms = |d: &Option<String>| d.as_deref().and_then(duration_ms);
    let bytes = |d: &Option<String>| d.as_deref().and_then(data_size_bytes);
    let planning = match (ms(&s.analysis_time), ms(&s.planning_time)) {
        (None, None) => None,
        (a, p) => Some(a.unwrap_or(0) + p.unwrap_or(0)),
    };
    let mut extra = serde_json::Map::new();
    let mut put = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            extra.insert(format!("trino.{k}"), v);
        }
    };
    put(
        "resourceWaitingTimeMs",
        ms(&s.resource_waiting_time).map(Into::into),
    );
    put(
        "scheduledTimeMs",
        ms(&s.total_scheduled_time).map(Into::into),
    );
    put(
        "processedInputPositions",
        s.processed_input_positions.map(Into::into),
    );
    put(
        "cumulativeUserMemory",
        s.cumulative_user_memory.map(Into::into),
    );
    put("failedTasks", s.failed_tasks.map(Into::into));
    put(
        "errorCode",
        q.error_code.as_ref().and_then(|e| e.get("name")).cloned(),
    );
    put("resourceGroupId", q.resource_group_id.clone());
    QueryRun {
        engine_query_id: q.query_id,
        user_name: q.session.and_then(|s| s.user),
        started_at: s.create_time.as_ref().and_then(instant),
        ended_at: Some(ended),
        status: Some(q.state),
        statement_type: q.query_type,
        sql_hash,
        fast_hash,
        metrics: ExecMetrics {
            elapsed_ms: ms(&s.elapsed_time),
            queued_ms: ms(&s.queued_time),
            planning_ms: planning,
            execution_ms: ms(&s.execution_time),
            cpu_ms: ms(&s.total_cpu_time),
            bytes_scanned: bytes(&s.physical_input_data_size),
            rows_scanned: s.processed_input_positions,
            peak_memory_bytes: bytes(&s.peak_user_memory_reservation),
            spilled_bytes: bytes(&s.spilled_data_size),
            network_bytes: bytes(&s.internal_network_input_data_size),
            bytes_written: bytes(&s.physical_written_data_size),
            ..Default::default()
        },
        extra,
    }
}

/// The parts of Trino's `BasicQueryInfo` we read. Unknown fields are ignored, missing
/// ones are `None`, so other Trino versions decode too.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BasicQueryInfo {
    query_id: String,
    #[serde(default)]
    session: Option<Session>,
    state: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    query_type: Option<String>,
    #[serde(default)]
    resource_group_id: Option<Value>,
    #[serde(default)]
    error_code: Option<Value>,
    query_stats: BasicQueryStats,
}

#[derive(Debug, Deserialize)]
struct Session {
    #[serde(default)]
    user: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct BasicQueryStats {
    create_time: Option<Value>,
    end_time: Option<Value>,
    queued_time: Option<String>,
    resource_waiting_time: Option<String>,
    elapsed_time: Option<String>,
    execution_time: Option<String>,
    analysis_time: Option<String>,
    planning_time: Option<String>,
    total_cpu_time: Option<String>,
    total_scheduled_time: Option<String>,
    failed_tasks: Option<u64>,
    processed_input_positions: Option<u64>,
    spilled_data_size: Option<String>,
    physical_input_data_size: Option<String>,
    physical_written_data_size: Option<String>,
    internal_network_input_data_size: Option<String>,
    peak_user_memory_reservation: Option<String>,
    cumulative_user_memory: Option<f64>,
}

/// A Trino timestamp: an ISO-8601 string, or epoch milliseconds.
fn instant(v: &Value) -> Option<DateTime<Utc>> {
    match v {
        Value::String(s) => DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.with_timezone(&Utc)),
        Value::Number(n) => DateTime::from_timestamp_millis(n.as_f64()? as i64),
        _ => None,
    }
}

/// An airlift `Duration` (`"1.25s"`, `"340.00ms"`, `"2.00m"`) in milliseconds.
fn duration_ms(s: &str) -> Option<u64> {
    let (value, unit) = split_unit(s)?;
    let ms_per = match unit {
        "ns" => 1e-6,
        "us" => 1e-3,
        "ms" => 1.0,
        "s" => 1e3,
        "m" => 60e3,
        "h" => 3_600e3,
        "d" => 86_400e3,
        _ => return None,
    };
    Some((value * ms_per).round() as u64)
}

/// An airlift `DataSize` (`"1234B"`, `"1.50MB"`) in bytes.
fn data_size_bytes(s: &str) -> Option<u64> {
    let (value, unit) = split_unit(s)?;
    let shift = match unit {
        "B" => 0,
        "kB" => 10,
        "MB" => 20,
        "GB" => 30,
        "TB" => 40,
        "PB" => 50,
        "EB" => 60,
        _ => return None,
    };
    Some((value * (1u64 << shift) as f64).round() as u64)
}

fn split_unit(s: &str) -> Option<(f64, &str)> {
    let s = s.trim();
    let at = s.find(|c: char| c.is_ascii_alphabetic())?;
    Some((s[..at].trim().parse().ok()?, s[at..].trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_airlift_units() {
        assert_eq!(duration_ms("1.25s"), Some(1250));
        assert_eq!(duration_ms("340.00ms"), Some(340));
        assert_eq!(duration_ms("2.00m"), Some(120_000));
        assert_eq!(duration_ms("500.00us"), Some(1));
        assert_eq!(data_size_bytes("1234B"), Some(1234));
        assert_eq!(data_size_bytes("1.50MB"), Some(1_572_864));
        assert_eq!(duration_ms("soon"), None);
    }

    fn fixture() -> Vec<BasicQueryInfo> {
        serde_json::from_str(include_str!("../tests/fixtures/trino_v1_query.json")).unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn maps_finished_queries_in_the_window() {
        let runs = runs_in_window(
            fixture(),
            at("2026-10-22T10:00:00Z"),
            at("2026-10-22T11:00:00Z"),
        );
        // The running query has no end time; the old one ended before the window.
        assert_eq!(runs.len(), 2);
        let run = runs
            .iter()
            .find(|r| r.engine_query_id == "20261022_100501_00042_abcde")
            .unwrap();
        assert_eq!(run.user_name.as_deref(), Some("alice"));
        assert_eq!(run.status.as_deref(), Some("FINISHED"));
        assert_eq!(run.statement_type.as_deref(), Some("SELECT"));
        assert_eq!(run.started_at, Some(at("2026-10-22T10:05:01.120Z")));
        let m = &run.metrics;
        assert_eq!(m.elapsed_ms, Some(4_210));
        assert_eq!(m.queued_ms, Some(12));
        assert_eq!(m.planning_ms, Some(180));
        assert_eq!(m.cpu_ms, Some(9_500));
        assert_eq!(m.bytes_scanned, Some(52_428_800));
        assert_eq!(m.peak_memory_bytes, Some(8_388_608));
        assert_eq!(m.rows_scanned, Some(1_000_000));
        assert_eq!(run.extra["trino.failedTasks"], 0);
        assert!(run.sql_hash.is_some() && run.fast_hash.is_some());

        let failed = runs
            .iter()
            .find(|r| r.status.as_deref() == Some("FAILED"))
            .unwrap();
        assert_eq!(failed.extra["trino.errorCode"], "TABLE_NOT_FOUND");
    }

    #[test]
    fn the_engine_hash_matches_what_queryflux_computes_for_the_sql_it_sent() {
        // QueryFlux fingerprints the SQL it sent with the same function. Trino keeps the
        // text as sent; layout and keyword case must not change the hash.
        let sent =
            "SELECT region, count(*) FROM orders WHERE dt = DATE '2026-10-01' GROUP BY region";
        let stored =
            "select region,\n  COUNT(*)\nfrom orders where dt = DATE '2026-10-01' group by region";
        assert_eq!(hashes(sent, "trino").0, hashes(stored, "trino").0);
        // Comments are kept by the fingerprint: an engine that strips them (StarRocks)
        // needs a comment-insensitive hash before it can be matched this way.
        let commented = format!("{sent} -- dashboard 7");
        assert_ne!(hashes(sent, "trino").0, hashes(&commented, "trino").0);
        // A different literal is a different query.
        let other = sent.replace("2026-10-01", "2026-10-02");
        assert_ne!(hashes(sent, "trino").0, hashes(&other, "trino").0);
    }
}
