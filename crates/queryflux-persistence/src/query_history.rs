use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// A single row from `query_records`, returned by the admin API.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct QuerySummary {
    pub id: i64,
    pub proxy_query_id: String,
    /// The query ID assigned by the backend engine (e.g. Trino's query ID).
    pub backend_query_id: Option<String>,
    pub cluster_group: String,
    pub cluster_name: String,
    /// FK to `cluster_group_configs.id`. `None` if the group was deleted after the query ran.
    pub cluster_group_id: Option<i64>,
    /// FK to `cluster_configs.id`. `None` if the cluster was deleted after the query ran.
    pub cluster_id: Option<i64>,
    pub engine_type: String,
    /// The wire protocol used by the client (e.g. "TrinoHttp", "PostgresWire").
    #[sqlx(rename = "frontend_protocol")]
    #[serde(rename = "frontend_protocol")]
    pub protocol: String,
    pub username: Option<String>,
    pub sql_preview: String,
    /// Translation stage outcome; null for records written before this field existed.
    #[serde(default)]
    #[sqlx(default)]
    #[schema(value_type = Option<Object>)]
    pub translation: Option<serde_json::Value>,
    /// Source-dialect SQL after access-control rewrite. Only present when `was_rewritten` is true.
    #[serde(default)]
    pub rewritten_sql: Option<String>,
    /// The SQL after dialect translation. Only present when `was_translated` is true.
    pub translated_sql: Option<String>,
    pub status: String,
    #[serde(default)]
    pub was_rewritten: bool,
    pub was_translated: bool,
    pub source_dialect: String,
    pub target_dialect: String,
    pub queue_duration_ms: i64,
    pub execution_duration_ms: i64,
    pub rows_returned: Option<i64>,
    pub error_message: Option<String>,
    /// Full routing trace — which router matched, which cluster was chosen.
    #[schema(value_type = Option<Object>)]
    pub routing_trace: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    // Engine-reported execution stats
    /// Engine's own elapsed time (ms). Subtract from `execution_duration_ms` for proxy overhead.
    pub engine_elapsed_time_ms: Option<i64>,
    pub cpu_time_ms: Option<i64>,
    pub processed_rows: Option<i64>,
    pub processed_bytes: Option<i64>,
    pub physical_input_bytes: Option<i64>,
    pub peak_memory_bytes: Option<i64>,
    pub spilled_bytes: Option<i64>,
    pub total_splits: Option<i32>,
    /// Tags attached to the query at submit time. `null` for older rows without tags.
    #[schema(value_type = Option<Object>)]
    #[serde(default)]
    pub query_tags: Option<serde_json::Value>,
    /// xxHash-64 of the normalized original SQL (as signed i64 / BIGINT).
    #[serde(default)]
    pub query_hash: Option<i64>,
    /// xxHash-64 of the parameterized original SQL.
    #[serde(default)]
    pub query_parameterized_hash: Option<i64>,
    /// xxHash-64 of the parameterized translated SQL. None when no translation occurred.
    #[serde(default)]
    pub translated_query_hash: Option<i64>,
    // Agent context (present when X-Agent-Id / X-Conversation-Id headers were sent)
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub step_index: Option<i32>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub query_intent: Option<String>,
    // Guard evaluation results
    #[schema(value_type = Option<Vec<Object>>)]
    #[serde(default)]
    pub guard_actions: Option<serde_json::Value>,
    #[serde(default)]
    pub was_guard_blocked: bool,
    /// True when the result was served from the query result cache.
    #[serde(default)]
    #[sqlx(default)]
    pub cache_hit: bool,
    /// The worker that ran the query; `None` when the proxy itself ran it.
    #[serde(default)]
    #[sqlx(default)]
    pub worker: Option<String>,
}

/// Aggregated stats for the last hour, shown on the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct DashboardStats {
    /// Total queries run in the last hour.
    pub queries_last_hour: i64,
    /// Fraction of failed queries (0.0 – 1.0).
    pub error_rate_last_hour: f64,
    /// Average execution time in milliseconds.
    pub avg_duration_ms_last_hour: f64,
    /// Fraction of queries that were dialect-translated (0.0 – 1.0).
    pub translation_rate_last_hour: f64,
    /// Fraction of queries rewritten by access control (0.0 – 1.0).
    pub rewrite_rate_last_hour: f64,
}

/// Per-group aggregated stats returned by `GET /admin/group-stats`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct GroupStatRow {
    pub cluster_group: String,
    pub engine_type: String,
    pub total_queries: i64,
    pub successful_queries: i64,
    pub failed_queries: i64,
    pub cancelled_queries: i64,
    /// Average execution time in milliseconds.
    pub avg_execution_ms: f64,
    /// Minimum execution time in milliseconds.
    pub min_execution_ms: i64,
    /// Maximum execution time in milliseconds.
    pub max_execution_ms: i64,
    /// Average time spent queued before execution, in milliseconds.
    pub avg_queue_ms: f64,
    pub translated_queries: i64,
    pub rewritten_queries: i64,
    pub total_rows_returned: i64,
}

/// Per-engine aggregated stats returned by `GET /admin/engine-stats`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct EngineStatRow {
    pub engine_type: String,
    pub total_queries: i64,
    pub successful_queries: i64,
    pub failed_queries: i64,
    pub cancelled_queries: i64,
    /// Average execution time in milliseconds.
    pub avg_execution_ms: f64,
    /// Minimum execution time in milliseconds.
    pub min_execution_ms: i64,
    /// Maximum execution time in milliseconds.
    pub max_execution_ms: i64,
    /// Average time spent queued before execution, in milliseconds.
    pub avg_queue_ms: f64,
    pub translated_queries: i64,
    pub rewritten_queries: i64,
    pub total_rows_returned: i64,
}

/// A distinct agent seen in query_records — returned by `GET /admin/agents`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct AgentSummary {
    pub agent_id: String,
    pub query_count: i64,
    pub conversation_count: i64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// A conversation (grouped steps) — returned by `GET /admin/conversations`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct ConversationSummary {
    pub conversation_id: String,
    pub agent_id: Option<String>,
    pub step_count: i64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub has_blocked: bool,
}

/// Filters for `GET /admin/queries` and `GET /admin/queries/histogram`.
///
/// Each list filter matches when any of its values does; different filters must all
/// match. An empty list does not filter. In the query string, a list filter repeats:
/// `?status=Failed&status=Cancelled&worker=us-east-dc1/`.
#[derive(Debug, Default, Clone, Deserialize, IntoParams)]
pub struct QueryFilters {
    /// Full-text search on SQL preview (case-insensitive).
    pub search: Option<String>,
    /// Query status, e.g. `Success`, `Failed`, `Cancelled`, `Denied` (case-insensitive).
    #[serde(default)]
    pub status: Vec<String>,
    /// Cluster group name.
    #[serde(default)]
    pub cluster_group: Vec<String>,
    /// Cluster name. A cluster also matches its variants (`<cluster>::<variant>`).
    #[serde(default)]
    pub cluster: Vec<String>,
    /// Engine type, e.g. `DuckDb`, `Trino` (case-insensitive).
    #[serde(default)]
    pub engine: Vec<String>,
    /// The worker that ran the query. A value ending in `/` (`us-east-dc1/`) matches every
    /// worker behind that site's hub.
    #[serde(default)]
    pub worker: Vec<String>,
    /// The user who ran the query.
    #[serde(default)]
    pub user: Vec<String>,
    /// Only queries created at or after this time (RFC 3339).
    pub from: Option<DateTime<Utc>>,
    /// Only queries created before this time (RFC 3339).
    pub to: Option<DateTime<Utc>>,
    /// Keyset cursor: only queries older than this row (`created_at`, then `id`). Pass the
    /// last row's `created_at` and `id` to get the next page; stable while new rows arrive.
    pub before: Option<DateTime<Utc>>,
    pub before_id: Option<i64>,
    /// Max rows to return (default 50, at most 1000).
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Rows to skip. Prefer `before`/`before_id` for paging.
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

impl QueryFilters {
    /// Parse a query string's key/value pairs. List filters may repeat.
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Result<Self, String> {
        let mut f = QueryFilters {
            limit: default_limit(),
            ..Default::default()
        };
        let time = |k: &str, v: &str| {
            DateTime::parse_from_rfc3339(v)
                .map(|t| t.with_timezone(&Utc))
                .map_err(|e| format!("{k}: not an RFC 3339 time ({e})"))
        };
        let int = |k: &str, v: &str| v.parse::<i64>().map_err(|_| format!("{k}: not a number"));
        for (k, v) in pairs {
            if v.is_empty() {
                continue;
            }
            match k.as_str() {
                "search" => f.search = Some(v),
                "status" => f.status.push(v),
                "cluster_group" | "group" => f.cluster_group.push(v),
                "cluster" => f.cluster.push(v),
                "engine" => f.engine.push(v),
                "worker" => f.worker.push(v),
                "user" => f.user.push(v),
                "from" => f.from = Some(time(&k, &v)?),
                "to" => f.to = Some(time(&k, &v)?),
                "before" => f.before = Some(time(&k, &v)?),
                "before_id" => f.before_id = Some(int(&k, &v)?),
                "limit" => f.limit = int(&k, &v)?,
                "offset" => f.offset = int(&k, &v)?,
                _ => {} // ponytail: unknown keys are ignored, as before
            }
        }
        if let (Some(from), Some(to)) = (f.from, f.to) {
            if from >= to {
                return Err("from must be before to".into());
            }
        }
        if f.before_id.is_some() && f.before.is_none() {
            return Err("before_id needs before".into());
        }
        f.limit = f.limit.clamp(1, 1000);
        f.offset = f.offset.max(0);
        Ok(f)
    }

    /// Whether a record passes every filter except paging. The Postgres store applies the
    /// same rules in SQL; the in-memory store uses this.
    pub fn matches(&self, r: &QuerySummary) -> bool {
        let any = |vs: &[String], test: &dyn Fn(&str) -> bool| {
            vs.is_empty() || vs.iter().any(|v| test(v))
        };
        let worker = r.worker.as_deref().unwrap_or("");
        any(&self.status, &|v| r.status.eq_ignore_ascii_case(v))
            && any(&self.cluster_group, &|v| r.cluster_group == v)
            && any(&self.cluster, &|v| {
                r.cluster_name == v || r.cluster_name.starts_with(&format!("{v}::"))
            })
            && any(&self.engine, &|v| r.engine_type.eq_ignore_ascii_case(v))
            && any(&self.worker, &|v| {
                worker == v || (v.ends_with('/') && worker.starts_with(v))
            })
            && any(&self.user, &|v| r.username.as_deref() == Some(v))
            && self.from.is_none_or(|t| r.created_at >= t)
            && self.to.is_none_or(|t| r.created_at < t)
            && self
                .before
                .is_none_or(|t| (r.created_at, r.id) < (t, self.before_id.unwrap_or(i64::MAX)))
            && self
                .search
                .as_ref()
                .is_none_or(|s| r.sql_preview.to_lowercase().contains(&s.to_lowercase()))
    }
}

/// A time window for aggregate stats. `to` defaults to now.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
}

impl TimeRange {
    pub fn last_hours(hours: i64) -> Self {
        TimeRange {
            from: Utc::now() - chrono::Duration::hours(hours),
            to: None,
        }
    }

    pub fn contains(&self, t: DateTime<Utc>) -> bool {
        t >= self.from && self.to.is_none_or(|to| t < to)
    }
}

/// What a histogram splits each bucket by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistogramBy {
    Status,
    ClusterGroup,
}

/// One histogram cell: how many queries in `[bucket, bucket + bucketSecs)` had `key`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct HistogramRow {
    pub bucket: DateTime<Utc>,
    pub key: String,
    pub count: i64,
}

/// A bucket width that splits `secs` into at most about 60 buckets, on a round step.
pub fn histogram_bucket_secs(secs: i64) -> i64 {
    const STEPS: [i64; 14] = [
        10, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10_800, 21_600, 43_200, 86_400,
    ];
    let want = (secs / 60).max(1);
    STEPS
        .iter()
        .copied()
        .find(|s| *s >= want)
        .unwrap_or(86_400 * ((want + 86_399) / 86_400))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(q: &[(&str, &str)]) -> Vec<(String, String)> {
        q.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn list_filters_repeat_and_times_parse() {
        let f = QueryFilters::from_pairs(pairs(&[
            ("status", "Failed"),
            ("status", "Cancelled"),
            ("group", "bi"),
            ("worker", "us-east-dc1/"),
            ("from", "2026-10-27T10:00:00Z"),
            ("to", "2026-10-27T12:00:00+01:00"),
            ("limit", "5000"),
            ("worker", ""),
        ]))
        .unwrap();
        assert_eq!(f.status, ["Failed", "Cancelled"]);
        assert_eq!(f.cluster_group, ["bi"]);
        assert_eq!(f.worker, ["us-east-dc1/"], "empty values are skipped");
        assert_eq!(
            f.to.unwrap().to_rfc3339(),
            "2026-10-27T11:00:00+00:00",
            "times become UTC"
        );
        assert_eq!(f.limit, 1000, "limit is capped");
        assert_eq!(QueryFilters::from_pairs(vec![]).unwrap().limit, 50);
    }

    #[test]
    fn bad_ranges_and_cursors_are_refused() {
        let err = |q: &[(&str, &str)]| QueryFilters::from_pairs(pairs(q)).unwrap_err();
        assert!(err(&[("from", "yesterday")]).contains("RFC 3339"));
        assert!(err(&[
            ("from", "2026-10-27T11:00:00Z"),
            ("to", "2026-10-27T10:00:00Z")
        ])
        .contains("before"));
        assert!(err(&[("before_id", "7")]).contains("needs before"));
    }

    #[test]
    fn buckets_are_round_and_about_sixty_per_range() {
        assert_eq!(histogram_bucket_secs(15 * 60), 30);
        assert_eq!(histogram_bucket_secs(3600), 60);
        assert_eq!(histogram_bucket_secs(24 * 3600), 1800);
        assert_eq!(histogram_bucket_secs(7 * 24 * 3600), 10_800);
        assert_eq!(histogram_bucket_secs(90 * 24 * 3600), 2 * 86_400);
    }
}
