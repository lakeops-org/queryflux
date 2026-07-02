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
    /// The SQL after dialect translation. Only present when `was_translated` is true.
    pub translated_sql: Option<String>,
    pub status: String,
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
    /// Fraction of queries that were translated (0.0 – 1.0).
    pub translation_rate_last_hour: f64,
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

/// Filters for `GET /admin/queries`.
#[derive(Debug, Default, Deserialize, IntoParams)]
pub struct QueryFilters {
    /// Full-text search on SQL preview (case-insensitive).
    pub search: Option<String>,
    /// Filter by query status, e.g. `Success`, `Failed`, `Cancelled`.
    pub status: Option<String>,
    /// Filter by cluster group name.
    pub cluster_group: Option<String>,
    /// Filter by engine type, e.g. `DuckDb`, `Trino`.
    pub engine: Option<String>,
    /// Max rows to return (default 50).
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Rows to skip (for pagination).
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}
