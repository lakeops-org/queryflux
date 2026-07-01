use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use queryflux_core::{
    error::Result,
    query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryEngineStats, QueryStatus,
        SqlDialect,
    },
    tags::QueryTags,
};

/// A single guard evaluation result — stored as JSONB in `query_history.guard_actions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardAction {
    pub guard: String,
    pub action: String,
    pub reason: Option<String>,
    pub code: Option<String>,
    /// Free-form key/value metadata returned by the guard (e.g. matched rule name,
    /// estimated row count). Omitted from JSON when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

/// A record of one completed (or failed/cancelled) query execution.
/// Written to the metrics store at the end of every query, regardless of outcome.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub proxy_query_id: String,
    /// The query ID assigned by the backend engine (e.g. Trino's `20240319_123456_00001_xxxxx`).
    pub backend_query_id: Option<String>,
    pub cluster_group: ClusterGroupName,
    pub cluster_name: ClusterName,
    /// Postgres `cluster_group_configs.id` when config is DB-backed.
    pub cluster_group_config_id: Option<i64>,
    /// Postgres `cluster_configs.id` when config is DB-backed.
    pub cluster_config_id: Option<i64>,
    pub engine_type: EngineType,
    pub frontend_protocol: FrontendProtocol,
    pub source_dialect: SqlDialect,
    pub target_dialect: SqlDialect,
    pub was_translated: bool,
    /// The SQL after dialect translation. Only set when `was_translated` is true.
    pub translated_sql: Option<String>,
    pub user: Option<String>,
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// First 500 chars of the original SQL.
    pub sql_preview: String,
    pub status: QueryStatus,
    /// Full routing trace serialized as JSON. Stored in the `routing_trace` JSONB column.
    pub routing_trace: Option<Value>,
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: Option<u64>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Engine-reported execution stats (CPU, bytes scanned, memory, etc.).
    pub engine_stats: Option<QueryEngineStats>,
    /// Effective query tags: group default_tags merged with session tags.
    /// Session tags win on key conflicts. Empty map when no tags were set.
    pub query_tags: QueryTags,
    /// xxHash-64 of the normalized original SQL (no literal replacement).
    /// Stored as i64 in Postgres (reinterpret the bit pattern; use to_hex() in queries).
    pub query_hash: Option<i64>,
    /// xxHash-64 of the parameterized original SQL (literals → `?`).
    pub query_parameterized_hash: Option<i64>,
    /// xxHash-64 of the parameterized translated SQL. None when no translation occurred.
    pub translated_query_hash: Option<i64>,
    /// Parameterized (literals → `?`) original SQL — the human-readable digest stored in `query_digest_stats`.
    pub digest_text: Option<String>,
    /// Parameterized translated SQL digest. None when no translation occurred.
    pub translated_digest_text: Option<String>,
    // --- Agent context fields (present when X-Agent-Id / X-Conversation-Id headers sent) ---
    pub agent_id: Option<String>,
    pub conversation_id: Option<String>,
    pub step_index: Option<i32>,
    pub tool_call_id: Option<String>,
    pub query_intent: Option<String>,
    // --- Guard fields ---
    /// All guards that evaluated this query and their verdicts.
    pub guard_actions: Vec<GuardAction>,
    /// True if any guard returned Deny.
    pub was_guard_blocked: bool,
    /// True when the result was served from the query result cache.
    pub cache_hit: bool,
}

/// A periodic snapshot of one cluster's live utilization.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub engine_type: EngineType,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    pub recorded_at: DateTime<Utc>,
}

/// Write side of the metrics pipeline — records completed queries and cluster snapshots
/// for later display in the admin Studio UI.
///
/// Prometheus handles real-time alerting; this trait handles historical persistence.
/// Any persistence backend that wants to power the query history page must implement this.
#[async_trait]
pub trait MetricsStore: Send + Sync {
    async fn record_query(&self, record: QueryRecord) -> Result<()>;
    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()>;

    /// Called synchronously when a cluster slot is acquired (query starts executing).
    /// Used to maintain real-time running-query gauges in Prometheus.
    fn on_query_started(&self, _group: &str, _cluster: &str) {}

    /// Called synchronously when a cluster slot is released (query finished or failed).
    fn on_query_finished(&self, _group: &str, _cluster: &str) {}

    /// Called when a distributed-coordination operation against the persistence
    /// backend fails and the replica falls back to local-only behavior (fail-open).
    /// `operation` is a low-cardinality label such as `capacity_acquire`,
    /// `capacity_release`, `capacity_heartbeat`, `capacity_expire`, or `queue_claim`.
    ///
    /// A sustained non-zero rate means the global `max_running_queries` limit and
    /// single-owner queue claiming are NOT being enforced — alert on it.
    fn on_coordination_failure(&self, _operation: &str) {}

    /// Called when a query is admitted in degraded mode — the global capacity
    /// lease could not be acquired (Postgres unreachable) and the replica fell
    /// back to local-only limits. The query proceeds, but global `max_running_queries`
    /// is not enforced for this admit. A sustained rate indicates the coordination
    /// backend is degraded; pair with `coordination_failures_total` alerts.
    fn on_capacity_degraded(&self, _cluster_group: &str, _cluster_name: &str) {}

    /// Called when an authentication attempt fails (wrong password, expired token, etc.).
    /// A sustained spike can indicate a credential-stuffing attack.
    fn on_auth_failure(&self, _protocol: &str) {}

    /// Called when a queue admission is rejected because `maxQueuedQueries` was reached.
    fn on_queue_full(&self, _cluster_group: &str) {}

    /// Called on a query result cache hit.
    fn on_cache_hit(&self, _cluster_group: &str) {}

    /// Called on a query result cache miss.
    fn on_cache_miss(&self, _cluster_group: &str) {}

    /// Called when a query result is successfully written to cache.
    fn on_cache_write(&self, _cluster_group: &str) {}
}
