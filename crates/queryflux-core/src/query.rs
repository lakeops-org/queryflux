use std::time::SystemTime;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::session::SessionContext;

// --- Identifiers ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProxyQueryId(pub String);

impl ProxyQueryId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for ProxyQueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ProxyQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackendQueryId(pub String);

impl std::fmt::Display for BackendQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterGroupName(pub String);

impl std::fmt::Display for ClusterGroupName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterName(pub String);

impl std::fmt::Display for ClusterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// --- Protocol & Engine ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FrontendProtocol {
    TrinoHttp,
    PostgresWire,
    MySqlWire,
    ClickHouseHttp,
    FlightSql,
    /// Snowflake HTTP wire (session + query endpoints) used by JDBC/ODBC/Python connectors.
    SnowflakeHttp,
    /// Snowflake SQL REST API v2 (`/api/v2/statements`).
    SnowflakeSqlApi,
}

impl FrontendProtocol {
    /// The SQL dialect naturally associated with this protocol's clients.
    pub fn default_dialect(&self) -> SqlDialect {
        match self {
            FrontendProtocol::TrinoHttp => SqlDialect::Trino,
            FrontendProtocol::PostgresWire => SqlDialect::Postgres,
            FrontendProtocol::MySqlWire => SqlDialect::MySql,
            FrontendProtocol::ClickHouseHttp => SqlDialect::ClickHouse,
            FrontendProtocol::FlightSql => SqlDialect::Generic,
            FrontendProtocol::SnowflakeHttp | FrontendProtocol::SnowflakeSqlApi => {
                SqlDialect::Snowflake
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineType {
    Trino,
    DuckDb,
    /// DuckDB running as a remote HTTP server.
    DuckDbHttp,
    StarRocks,
    ClickHouse,
    /// Amazon Athena (Presto/Trino-compatible SQL over S3).
    Athena,
    /// Generic ADBC adapter — dialect depends on the configured driver.
    Adbc,
    /// ADBC adapter backed by a PostgreSQL driver.
    Postgres,
    /// ADBC adapter backed by a MySQL driver.
    MySql,
    /// ADBC adapter backed by a SQLite driver.
    Sqlite,
    /// ADBC adapter backed by a Snowflake driver.
    Snowflake,
    /// ADBC adapter backed by a BigQuery driver.
    BigQuery,
    /// ADBC adapter backed by a Databricks driver.
    Databricks,
    /// ADBC adapter backed by a SQL Server (MSSQL) driver.
    MsSql,
    /// ADBC adapter backed by an Amazon Redshift driver.
    Redshift,
    /// ADBC adapter backed by an Exasol driver.
    Exasol,
    /// ADBC adapter backed by a SingleStore driver (MySQL-compatible dialect).
    SingleStore,
    /// Result served from cache — no backend engine involved.
    Cache,
}

impl EngineType {
    pub fn dialect(&self) -> SqlDialect {
        match self {
            EngineType::Trino => SqlDialect::Trino,
            EngineType::Athena => SqlDialect::Athena,
            EngineType::DuckDb | EngineType::DuckDbHttp => SqlDialect::DuckDb,
            EngineType::StarRocks => SqlDialect::StarRocks,
            EngineType::ClickHouse => SqlDialect::ClickHouse,
            EngineType::Adbc => SqlDialect::Generic,
            EngineType::Postgres => SqlDialect::Postgres,
            EngineType::MySql => SqlDialect::MySql,
            EngineType::Sqlite => SqlDialect::Sqlite,
            EngineType::Snowflake => SqlDialect::Snowflake,
            EngineType::BigQuery => SqlDialect::BigQuery,
            EngineType::Databricks => SqlDialect::Databricks,
            EngineType::MsSql => SqlDialect::MsSql,
            EngineType::Redshift => SqlDialect::Redshift,
            EngineType::Exasol => SqlDialect::Exasol,
            EngineType::SingleStore => SqlDialect::MySql,
            EngineType::Cache => SqlDialect::Generic,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SqlDialect {
    Trino,
    Athena,
    DuckDb,
    StarRocks,
    ClickHouse,
    MySql,
    Postgres,
    Sqlite,
    Snowflake,
    BigQuery,
    Databricks,
    MsSql,
    Redshift,
    Exasol,
    Generic,
    /// Any other sqlglot `read` / `write` dialect name (e.g. `hive`, `spark`, `oracle`).
    #[serde(rename = "sqlglot")]
    Sqlglot(String),
}

impl SqlDialect {
    /// Returns true if translating between these two dialects is a no-op.
    /// MySql and StarRocks share the same wire protocol and SQL syntax.
    pub fn is_compatible_with(&self, other: &SqlDialect) -> bool {
        self == other
            || matches!(
                (self, other),
                (SqlDialect::MySql, SqlDialect::StarRocks)
                    | (SqlDialect::StarRocks, SqlDialect::MySql)
            )
    }

    /// The dialect name as sqlglot expects it (built-in variants only).
    pub fn sqlglot_name(&self) -> &'static str {
        match self {
            SqlDialect::Sqlglot(_) => "",
            SqlDialect::Trino => "trino",
            SqlDialect::Athena => "athena",
            SqlDialect::DuckDb => "duckdb",
            SqlDialect::StarRocks => "starrocks",
            SqlDialect::ClickHouse => "clickhouse",
            SqlDialect::MySql => "mysql",
            SqlDialect::Postgres => "postgres",
            SqlDialect::Sqlite => "sqlite",
            SqlDialect::Snowflake => "snowflake",
            SqlDialect::BigQuery => "bigquery",
            SqlDialect::Databricks => "databricks",
            SqlDialect::MsSql => "tsql",
            SqlDialect::Redshift => "redshift",
            SqlDialect::Exasol => "exasol",
            SqlDialect::Generic => "",
        }
    }

    /// sqlglot `read` / `write` string for `transpile` (includes [`SqlDialect::Sqlglot`]).
    pub fn sqlglot_write_name(&self) -> String {
        match self {
            SqlDialect::Sqlglot(s) => s.clone(),
            _ => {
                let s = self.sqlglot_name();
                s.to_string()
            }
        }
    }
}

// --- Incoming query (before routing) ---

#[derive(Debug, Clone)]
pub struct IncomingQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub session: SessionContext,
    pub frontend_protocol: FrontendProtocol,
    pub creation_time: SystemTime,
}

impl IncomingQuery {
    pub fn new(sql: String, session: SessionContext, frontend_protocol: FrontendProtocol) -> Self {
        Self {
            id: ProxyQueryId::new(),
            sql,
            session,
            frontend_protocol,
            creation_time: SystemTime::now(),
        }
    }
}

// --- Executing query (after routing, being dispatched) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutingQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub translated_sql: Option<String>,
    pub cluster_group: ClusterGroupName,
    pub cluster_name: ClusterName,
    /// Postgres `cluster_group_configs.id` when known (DB-backed config).
    #[serde(default)]
    pub cluster_group_config_id: Option<i64>,
    /// Postgres `cluster_configs.id` when known.
    #[serde(default)]
    pub cluster_config_id: Option<i64>,
    /// The backend engine's query ID (e.g. Trino's `20260319_084733_00386_kqwci`).
    /// Used as the persistence key and embedded in the client-facing poll URL.
    pub backend_query_id: BackendQueryId,
    /// Opaque base URL for the backend cluster (e.g. `http://trino:8080`).
    /// The poll handler uses this together with the client-supplied path to reconstruct
    /// the backend poll URL. Never changes after submit.
    pub poll_base_url: Option<String>,
    pub creation_time: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    /// Effective tags at submit time (group defaults merged with session tags).
    /// Stored here because poll requests don't repeat the original client headers.
    #[serde(default)]
    pub query_tags: crate::tags::QueryTags,
    /// Agent identity captured at submit time. Poll requests don't re-send agent headers.
    #[serde(default)]
    pub agent_context: Option<crate::session::AgentContext>,
    /// Guard actions collected when the guard chain ran at submit time.
    /// Stored as raw JSON to avoid a core→persistence dependency.
    /// Deserialized back to `Vec<GuardAction>` at poll time.
    #[serde(default)]
    pub submitted_guard_actions: Vec<serde_json::Value>,
    /// True when a guard blocked this query at submit time.
    #[serde(default)]
    pub was_guard_blocked: bool,
}

// --- Query execution result model ---

/// A query waiting for cluster capacity to become available.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub session: SessionContext,
    pub frontend_protocol: FrontendProtocol,
    pub cluster_group: ClusterGroupName,
    pub creation_time: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    /// How many times the client has polled. Used for exponential backoff.
    pub sequence: u64,
}

/// Returned by `AsyncAdapter::submit_query`.
///
/// `Running` — query is executing; the client must poll using `poll_token`.
/// `Completed` — query finished on the initial submit (fast queries, immediate errors).
///   Dispatch records the outcome and releases the slot without waiting for any poll.
#[derive(Debug)]
pub enum QueryExecution {
    Running {
        backend_query_id: BackendQueryId,
        /// Opaque polling hint — each adapter interprets this in its own way.
        /// Trino puts the full `nextUri` URL here; other engines may use a job ID.
        poll_token: Option<String>,
        /// Raw bytes from the initial submit response, forwarded as-is to the client.
        initial_response: Option<Bytes>,
    },
    Completed {
        backend_query_id: BackendQueryId,
        status: QueryStatus,
        error: Option<String>,
        engine_stats: Option<QueryEngineStats>,
        /// Raw bytes from the terminal submit response, forwarded as-is to the client.
        initial_response: Option<Bytes>,
    },
}

/// Returned by `AsyncAdapter::poll_query`.
#[derive(Debug)]
pub enum QueryPollResult {
    Pending {
        progress: Option<f32>,
        /// Opaque polling hint for the next poll (same semantics as `QueryExecution::Running.poll_token`).
        poll_token: Option<String>,
    },
    Failed {
        message: String,
        error_code: Option<String>,
    },
    /// Raw response bytes for transparent protocol forwarding (e.g. Trino → Trino HTTP).
    /// The frontend handler rewrites any embedded poll URL and returns the bytes as-is.
    Raw {
        body: Bytes,
        /// Opaque polling hint for the next poll (None means query is complete).
        poll_token: Option<String>,
        /// Engine stats extracted from the final response (only set when poll_token is None).
        engine_stats: Option<QueryEngineStats>,
    },
}

// --- Query stats ---

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: u64,
    pub bytes_returned: Option<u64>,
}

/// Engine-level execution statistics captured from the final query response.
/// Fields are optional since different engines expose different metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryEngineStats {
    /// Elapsed (wall-clock) time as reported by the engine (ms).
    /// Comparing this against QueryFlux's own `execution_duration_ms` gives the proxy overhead.
    pub engine_elapsed_time_ms: Option<u64>,
    /// CPU time consumed by the query across all workers (ms).
    pub cpu_time_ms: Option<u64>,
    /// Number of rows read/processed by the engine.
    pub processed_rows: Option<u64>,
    /// Logical bytes processed (in-memory representation).
    pub processed_bytes: Option<u64>,
    /// Physical bytes read from storage (I/O cost).
    pub physical_input_bytes: Option<u64>,
    /// Peak memory usage across all workers (bytes).
    pub peak_memory_bytes: Option<u64>,
    /// Data spilled to disk during execution (bytes).
    pub spilled_bytes: Option<u64>,
    /// Number of execution splits/tasks.
    pub total_splits: Option<u32>,
}

// --- Query status (for metrics) ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Success,
    Failed,
    Cancelled,
}
