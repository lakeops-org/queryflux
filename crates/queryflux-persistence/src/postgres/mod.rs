use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    cache_store::{CacheEntryMeta, CacheEntryRef, CacheStore},
    cluster_config::{
        ClusterConfigRecord, ClusterGroupConfigRecord, UpsertClusterConfig,
        UpsertClusterGroupConfig,
    },
    metrics_store::{ClusterSnapshot, MetricsStore, QueryRecord},
    query_history::{
        AgentSummary, ConversationSummary, DashboardStats, EngineStatRow, GroupStatRow,
        QueryFilters, QuerySummary,
    },
    routing_slices::{
        collapse_rows_to_routers, expand_router_for_persistence, RoutingRulePersistRow,
    },
    script_library::{
        is_valid_script_kind, UpsertUserScript, UserScriptRecord, KIND_TRANSLATION_FIXUP,
    },
    BackendCapabilities, CapacityStore, ClusterConfigStore, ConfigRevisionStore,
    LoadedRoutingConfig, Persistence, ProxySettingsStore, QueryHistoryStore, QueueCoordinator,
    RoutingConfigStore, ScriptLibraryStore, SweepCoordinator, SweepGuard,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{BackendQueryId, ExecutingQuery, ProxyQueryId, QueuedQuery},
};
use sqlx::PgPool;

mod history_batch;
mod hub_store;
pub mod tenants;

/// Select group rows with `members` as cluster **names** (joined from `cluster_configs` by id).
const CLUSTER_GROUP_CONFIG_SELECT: &str = r#"
SELECT
    g.id,
    g.name,
    g.enabled,
    COALESCE(

        (
            SELECT array_agg(c.name ORDER BY u.ord)
            FROM unnest(g.members) WITH ORDINALITY AS u(cid, ord)
            JOIN cluster_configs c ON c.id = u.cid
        ),
        ARRAY[]::text[]
    ) AS members,
    g.max_running_queries,
    g.max_queued_queries,
    g.strategy,
    g.circuit_breaker,
    g.allow_groups,
    g.allow_users,
    g.translation_script_ids,
    g.default_tags,
    g.cache,
    g.created_at,
    g.updated_at
FROM cluster_group_configs g
"#;

/// Postgres backend — implements both `Persistence` (in-flight query state)
/// and `MetricsStore` (historical query records + cluster snapshots).
///
/// Uses isolated connection pools so the dispatch hot path, distributed
/// coordination, and admin/reload workloads cannot exhaust each other.
pub struct PostgresStore {
    query_pool: PgPool,
    coordination_pool: PgPool,
    admin_pool: PgPool,
    database_url: String,
    /// `persistence.schema`, already in `database_url` as the `search_path`.
    schema: Option<String>,
    /// `capacity.quotaLeaseSecs` and `capacity.reportIntervalMs`, returned to holders with
    /// their grants.
    quota_lease_secs: std::sync::atomic::AtomicU64,
    quota_report_interval_ms: std::sync::atomic::AtomicU64,
    /// Queries waiting in `wait_for_grant`, woken by `queryflux_capacity_granted`.
    grant_waiters: Arc<dashmap::DashMap<String, Arc<tokio::sync::Notify>>>,
    grant_listener: tokio::sync::OnceCell<()>,
}

impl PostgresStore {
    /// Connect to Postgres and return a ready instance.
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::connect_with_pool_size(database_url, None).await
    }

    /// Connect with explicit pool tuning. `None` values keep sqlx defaults.
    /// Splits the legacy single `max_connections` budget across query/coordination/admin pools.
    pub async fn connect_with_pool_size(
        database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        Self::connect_with_pool_opts(database_url, max_connections, None, None).await
    }

    pub async fn connect_with_pool_opts(
        database_url: &str,
        max_connections: Option<u32>,
        acquire_timeout_secs: Option<u64>,
        statement_timeout_secs: Option<u64>,
    ) -> Result<Self> {
        let conn = queryflux_core::config::PostgresPersistenceConfig {
            pool_size: max_connections,
            acquire_timeout_secs,
            statement_timeout_secs,
            ..Default::default()
        };
        Self::connect_from_config(database_url, &conn).await
    }

    /// Connect using full Postgres persistence config (isolated pools + timeouts).
    pub async fn connect_from_config(
        database_url: &str,
        conn: &queryflux_core::config::PostgresPersistenceConfig,
    ) -> Result<Self> {
        let (query_max, coordination_max, admin_max) = conn
            .resolve_pool_sizes()
            .map_err(QueryFluxError::Persistence)?;
        let query_pool = build_pg_pool(
            database_url,
            query_max,
            conn.acquire_timeout_secs,
            conn.statement_timeout_secs,
        )
        .await?;
        let coordination_pool = build_pg_pool(
            database_url,
            coordination_max,
            conn.acquire_timeout_secs,
            conn.statement_timeout_secs,
        )
        .await?;
        let admin_pool = build_pg_pool(
            database_url,
            admin_max,
            conn.acquire_timeout_secs,
            conn.statement_timeout_secs,
        )
        .await?;
        Ok(Self {
            query_pool,
            coordination_pool,
            admin_pool,
            database_url: database_url.to_string(),
            schema: conn
                .pinned_schema()
                .map_err(QueryFluxError::Persistence)?
                .map(String::from),
            quota_lease_secs: std::sync::atomic::AtomicU64::new(15),
            quota_report_interval_ms: std::sync::atomic::AtomicU64::new(1000),
            grant_waiters: Arc::default(),
            grant_listener: tokio::sync::OnceCell::new(),
        })
    }

    /// Run all migrations via Refinery. Tracks applied migrations in `refinery_schema_history`.
    ///
    /// Prefer [`crate::SchemaMigrator`] / [`crate::run_persistence_migrations`] from CLI and
    /// other callers so non-Postgres backends can plug in the same interface later.
    pub async fn migrate(&self) -> Result<()> {
        crate::migrate::run_postgres_refinery_migrations(&self.database_url, self.schema.as_deref())
            .await
    }

    /// With a pinned schema, check that connections resolve to it: a missing schema, or a
    /// role whose own `search_path` setting wins, would otherwise read and write another
    /// schema's tables. Call after migrations (which create the schema).
    pub async fn verify_schema(&self) -> Result<()> {
        let Some(expected) = self.schema.as_deref() else {
            return Ok(());
        };
        let current: Option<String> = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("schema check: {e}")))?;
        match current.as_deref() {
            Some(c) if c == expected => Ok(()),
            Some(c) => Err(QueryFluxError::Persistence(format!(
                "persistence.schema is '{expected}' but connections resolve to '{c}'"
            ))),
            None => Err(QueryFluxError::Persistence(format!(
                "persistence.schema '{expected}' does not exist (run migrations, or create it)"
            ))),
        }
    }
}

async fn build_pg_pool(
    database_url: &str,
    max_connections: u32,
    acquire_timeout_secs: Option<u64>,
    statement_timeout_secs: Option<u64>,
) -> Result<PgPool> {
    let mut opts = sqlx::postgres::PgPoolOptions::new().max_connections(max_connections);
    let timeout = std::time::Duration::from_secs(acquire_timeout_secs.unwrap_or(30));
    opts = opts.acquire_timeout(timeout);

    let stmt_ms = statement_timeout_secs.unwrap_or(60).saturating_mul(1000);
    opts = opts.after_connect(move |conn, _meta| {
        Box::pin(async move {
            use sqlx::Executor;
            conn.execute(format!("SET statement_timeout = {stmt_ms}").as_str())
                .await?;
            Ok(())
        })
    });
    opts.connect(database_url)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Failed to connect to Postgres: {e}")))
}

/// LISTEN on `base`'s channel in this connection's schema (`queryflux_channel`, V27): the
/// plain name in `public`, a schema-specific one otherwise, so deployments sharing a
/// database never wake each other.
async fn listen_scoped(
    listener: &mut sqlx::postgres::PgListener,
    base: &str,
) -> std::result::Result<(), sqlx::Error> {
    let channel: String = sqlx::query_scalar("SELECT queryflux_channel($1)")
        .bind(base)
        .fetch_one(&mut *listener)
        .await?;
    listener.listen(&channel).await
}

/// Rows deleted per statement by the history purge.
const PURGE_BATCH_ROWS: i64 = 10_000;

impl PostgresStore {
    /// Delete rows of `table` whose `column` is older than `older_than`,
    /// [`PURGE_BATCH_ROWS`] at a time, until none are left. Rows are picked through the
    /// index on `column` and deleted by `ctid`. `table` and `column` are fixed names
    /// from [`Self::purge_old_query_records`], never user input.
    async fn purge_in_batches(
        &self,
        table: &str,
        column: &str,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {table} WHERE ctid = ANY(ARRAY(\
                SELECT ctid FROM {table} WHERE {column} < $1 LIMIT $2))"
        );
        let mut deleted = 0u64;
        loop {
            let n = sqlx::query(&sql)
                .bind(older_than)
                .bind(PURGE_BATCH_ROWS)
                .execute(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("purge {table}: {e}")))?
                .rows_affected();
            deleted += n;
            if n < PURGE_BATCH_ROWS as u64 {
                return Ok(deleted);
            }
            // Leave room for the admin API and ingest that share this pool.
            tokio::task::yield_now().await;
        }
    }
}

/// The Postgres sweep lock is a session-scoped advisory lock held on a
/// dedicated direct connection (outside the coordination pool), so sweeps
/// never pin pool slots and concurrent try-lock attempts can each open a
/// connection to call `pg_try_advisory_lock`.
#[async_trait]
impl SweepCoordinator for PostgresStore {
    async fn try_sweep_lock(&self, name: &str) -> Result<Option<Box<dyn SweepGuard>>> {
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(&self.database_url)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("sweep lock connect: {e}")))?;

        let got: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(queryflux_lock_key('sweep:' || $1))")
                .bind(name)
                .fetch_one(&mut conn)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("sweep lock try: {e}")))?;

        if got {
            Ok(Some(Box::new(SweepLock {
                conn: Some(conn),
                name: name.to_string(),
            }) as Box<dyn SweepGuard>))
        } else {
            Ok(None)
        }
    }
}

impl BackendCapabilities for PostgresStore {
    fn supports_distributed_coordination(&self) -> bool {
        true
    }
}

/// Guard for a single-owner background sweep (see [`PostgresStore::try_sweep_lock`]).
///
/// Releases the advisory lock on [`Self::release`] or on drop. Session advisory
/// locks are per-connection; if unlocking fails, the connection is closed so the
/// lock can never leak onto a recycled connection.
pub struct SweepLock {
    conn: Option<sqlx::PgConnection>,
    name: String,
}

impl SweepLock {
    async fn unlock(mut conn: sqlx::PgConnection, name: &str) {
        let res = sqlx::query("SELECT pg_advisory_unlock(queryflux_lock_key('sweep:' || $1))")
            .bind(name)
            .execute(&mut conn)
            .await;
        if let Err(e) = res {
            tracing::warn!("Failed to release sweep lock '{name}': {e}; closing connection");
        }
        use sqlx::Connection;
        let _ = conn.close().await;
    }

    /// Release the lock now (deterministic, preferred at the end of a sweep).
    pub async fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            Self::unlock(conn, &self.name).await;
        }
    }
}

#[async_trait]
impl SweepGuard for SweepLock {
    async fn release(self: Box<Self>) {
        SweepLock::release(*self).await;
    }
}

impl Drop for SweepLock {
    fn drop(&mut self) {
        // Fallback for early exits (`continue`, `?`): spawn the unlock so the
        // connection is closed rather than dropped while still holding the lock.
        if let Some(conn) = self.conn.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let name = std::mem::take(&mut self.name);
                handle.spawn(async move {
                    Self::unlock(conn, &name).await;
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// QueryHistoryStore
// ---------------------------------------------------------------------------

#[async_trait]
impl QueryHistoryStore for PostgresStore {
    async fn list_queries(&self, filters: &QueryFilters) -> Result<Vec<QuerySummary>> {
        sqlx::query_as::<_, QuerySummary>(
            r#"SELECT qr.id, qr.proxy_query_id, qr.backend_query_id,
                      COALESCE(cg.name, qr.cluster_group) AS cluster_group,
                      COALESCE(cc.name, qr.cluster_name) AS cluster_name,
                      qr.cluster_group_id, qr.cluster_id,
                      qr.engine_type, qr.frontend_protocol, qr.username, qr.sql_preview,
                      qr.rewritten_sql, qr.translated_sql,
                      qr.status, qr.was_rewritten, qr.was_translated, qr.translation,
                      qr.source_dialect, qr.target_dialect, qr.queue_duration_ms, qr.execution_duration_ms,
                      qr.rows_returned, qr.error_message, qr.routing_trace, qr.created_at,
                      qr.engine_elapsed_time_ms, qr.cpu_time_ms, qr.processed_rows, qr.processed_bytes,
                      qr.physical_input_bytes, qr.peak_memory_bytes, qr.spilled_bytes, qr.total_splits,
                      qr.query_tags, qr.query_hash, qr.query_parameterized_hash, qr.translated_query_hash,
                      qr.agent_id, qr.conversation_id, qr.step_index, qr.tool_call_id, qr.query_intent,
                      qr.guard_actions, qr.was_guard_blocked, qr.cache_hit, qr.worker
               FROM query_records qr
               LEFT JOIN cluster_group_configs cg ON cg.id = qr.cluster_group_id
               LEFT JOIN cluster_configs cc ON cc.id = qr.cluster_id
               WHERE ($1::text IS NULL OR qr.sql_preview ILIKE '%' || $1 || '%')
                 AND ($2::text IS NULL OR qr.status = $2)
                 AND ($3::text IS NULL OR COALESCE(cg.name, qr.cluster_group) = $3)
                 AND ($4::text IS NULL OR qr.engine_type = $4)
                 AND ($7::text IS NULL OR qr.worker = $7)
               ORDER BY qr.created_at DESC
               LIMIT $5 OFFSET $6"#,
        )
        .bind(&filters.search)
        .bind(&filters.status)
        .bind(&filters.cluster_group)
        .bind(&filters.engine)
        .bind(filters.limit)
        .bind(filters.offset)
        .bind(&filters.worker)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_queries: {e}")))
    }

    async fn get_dashboard_stats(&self) -> Result<DashboardStats> {
        let row: (i64, i64, i64, i64, f64) = sqlx::query_as(
            r#"SELECT
                COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status != 'Success')::bigint,
                COUNT(*) FILTER (WHERE was_translated)::bigint,
                COUNT(*) FILTER (WHERE was_rewritten)::bigint,
                COALESCE(AVG(execution_duration_ms), 0)::float8
               FROM query_records
               WHERE created_at > NOW() - INTERVAL '1 hour'"#,
        )
        .fetch_one(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_dashboard_stats: {e}")))?;

        let (total, failed, translated, rewritten, avg_ms) = row;
        Ok(DashboardStats {
            queries_last_hour: total,
            error_rate_last_hour: if total > 0 {
                failed as f64 / total as f64
            } else {
                0.0
            },
            avg_duration_ms_last_hour: avg_ms,
            translation_rate_last_hour: if total > 0 {
                translated as f64 / total as f64
            } else {
                0.0
            },
            rewrite_rate_last_hour: if total > 0 {
                rewritten as f64 / total as f64
            } else {
                0.0
            },
        })
    }

    async fn get_engine_stats(&self, hours: i64) -> Result<Vec<EngineStatRow>> {
        sqlx::query_as::<_, EngineStatRow>(
            r#"SELECT
                engine_type,
                COUNT(*)::bigint                                                AS total_queries,
                COUNT(*) FILTER (WHERE status = 'Success')::bigint             AS successful_queries,
                COUNT(*) FILTER (WHERE status = 'Failed')::bigint              AS failed_queries,
                COUNT(*) FILTER (WHERE status = 'Cancelled')::bigint           AS cancelled_queries,
                COALESCE(AVG(execution_duration_ms), 0)::float8                AS avg_execution_ms,
                COALESCE(MIN(execution_duration_ms), 0)::bigint                AS min_execution_ms,
                COALESCE(MAX(execution_duration_ms), 0)::bigint                AS max_execution_ms,
                COALESCE(AVG(queue_duration_ms), 0)::float8                    AS avg_queue_ms,
                COUNT(*) FILTER (WHERE was_translated)::bigint                 AS translated_queries,
                COUNT(*) FILTER (WHERE was_rewritten)::bigint                  AS rewritten_queries,
                COALESCE(SUM(rows_returned), 0)::bigint                        AS total_rows_returned
               FROM query_records
               WHERE created_at > NOW() - ($1 * INTERVAL '1 hour')
               GROUP BY engine_type
               ORDER BY total_queries DESC"#,
        )
        .bind(hours)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_engine_stats: {e}")))
    }

    async fn get_group_stats(&self, hours: i64) -> Result<Vec<GroupStatRow>> {
        // Group by stable id when present so renamed groups don't split into two buckets (denormalized
        // `cluster_group` text vs joined current name). Legacy rows without `cluster_group_id` still
        // group by stored name only.
        sqlx::query_as::<_, GroupStatRow>(
            r#"SELECT
                MAX(COALESCE(cg.name, qr.cluster_group))                           AS cluster_group,
                MAX(qr.engine_type)                                                 AS engine_type,
                COUNT(*)::bigint                                                    AS total_queries,
                COUNT(*) FILTER (WHERE qr.status = 'Success')::bigint               AS successful_queries,
                COUNT(*) FILTER (WHERE qr.status = 'Failed')::bigint                AS failed_queries,
                COUNT(*) FILTER (WHERE qr.status = 'Cancelled')::bigint             AS cancelled_queries,
                COALESCE(AVG(qr.execution_duration_ms), 0)::float8                 AS avg_execution_ms,
                COALESCE(MIN(qr.execution_duration_ms), 0)::bigint                  AS min_execution_ms,
                COALESCE(MAX(qr.execution_duration_ms), 0)::bigint                AS max_execution_ms,
                COALESCE(AVG(qr.queue_duration_ms), 0)::float8                      AS avg_queue_ms,
                COUNT(*) FILTER (WHERE qr.was_translated)::bigint                   AS translated_queries,
                COUNT(*) FILTER (WHERE qr.was_rewritten)::bigint                    AS rewritten_queries,
                COALESCE(SUM(qr.rows_returned), 0)::bigint                         AS total_rows_returned
               FROM query_records qr
               LEFT JOIN cluster_group_configs cg ON cg.id = qr.cluster_group_id
               WHERE qr.created_at > NOW() - ($1 * INTERVAL '1 hour')
               GROUP BY
                   CASE
                       WHEN qr.cluster_group_id IS NOT NULL
                       THEN ('id:' || qr.cluster_group_id::text)
                       ELSE ('name:' || qr.cluster_group)
                   END
               ORDER BY total_queries DESC"#,
        )
        .bind(hours)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_group_stats: {e}")))
    }

    async fn list_engines(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT engine_type FROM query_records ORDER BY engine_type")
                .fetch_all(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("list_engines: {e}")))?;
        Ok(rows.into_iter().map(|(e,)| e).collect())
    }

    async fn list_agents(&self, limit: i64, offset: i64) -> Result<Vec<AgentSummary>> {
        sqlx::query_as::<_, AgentSummary>(
            r#"SELECT
                   agent_id,
                   COUNT(*)::bigint                           AS query_count,
                   COUNT(DISTINCT conversation_id)::bigint    AS conversation_count,
                   MIN(created_at)                            AS first_seen,
                   MAX(created_at)                            AS last_seen
               FROM query_records
               WHERE agent_id IS NOT NULL
               GROUP BY agent_id
               ORDER BY last_seen DESC
               LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_agents: {e}")))
    }

    async fn list_conversations(
        &self,
        agent_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ConversationSummary>> {
        sqlx::query_as::<_, ConversationSummary>(
            r#"SELECT
                   conversation_id,
                   agent_id,
                   COUNT(*)::bigint                                AS step_count,
                   MIN(created_at)                                 AS first_seen,
                   MAX(created_at)                                 AS last_seen,
                   BOOL_OR(was_guard_blocked)                      AS has_blocked
               FROM query_records
               WHERE conversation_id IS NOT NULL
                 AND ($1::text IS NULL OR agent_id = $1)
               GROUP BY conversation_id, agent_id
               ORDER BY last_seen DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(agent_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_conversations: {e}")))
    }

    async fn get_conversation(&self, conversation_id: &str) -> Result<Vec<QuerySummary>> {
        sqlx::query_as::<_, QuerySummary>(
            r#"SELECT qr.id, qr.proxy_query_id, qr.backend_query_id,
                      COALESCE(cg.name, qr.cluster_group) AS cluster_group,
                      COALESCE(cc.name, qr.cluster_name)  AS cluster_name,
                      qr.cluster_group_id, qr.cluster_id,
                      qr.engine_type, qr.frontend_protocol, qr.username, qr.sql_preview,
                      qr.rewritten_sql, qr.translated_sql,
                      qr.status, qr.was_rewritten, qr.was_translated, qr.translation,
                      qr.source_dialect, qr.target_dialect, qr.queue_duration_ms, qr.execution_duration_ms,
                      qr.rows_returned, qr.error_message, qr.routing_trace, qr.created_at,
                      qr.engine_elapsed_time_ms, qr.cpu_time_ms, qr.processed_rows, qr.processed_bytes,
                      qr.physical_input_bytes, qr.peak_memory_bytes, qr.spilled_bytes, qr.total_splits,
                      qr.query_tags, qr.query_hash, qr.query_parameterized_hash, qr.translated_query_hash,
                      qr.agent_id, qr.conversation_id, qr.step_index, qr.tool_call_id, qr.query_intent,
                      qr.guard_actions, qr.was_guard_blocked, qr.cache_hit, qr.worker
               FROM query_records qr
               LEFT JOIN cluster_group_configs cg ON cg.id = qr.cluster_group_id
               LEFT JOIN cluster_configs cc ON cc.id = qr.cluster_id
               WHERE qr.conversation_id = $1
               ORDER BY qr.step_index ASC NULLS LAST, qr.created_at ASC"#,
        )
        .bind(conversation_id)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_conversation: {e}")))
    }

    /// Deletes in batches of [`PURGE_BATCH_ROWS`], each its own short transaction.
    ///
    /// One `DELETE` of everything expired held an admin connection and row locks for as
    /// long as it ran, wrote the whole delete as one transaction, and at high ingest
    /// rates (an hour of history can be tens of millions of rows) ran into
    /// `statement_timeout`, so nothing was purged at all. Small batches keep each
    /// statement short, let autovacuum keep up, and make progress even when the backlog
    /// is large.
    async fn purge_old_query_records(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let mut total = 0;
        for (table, column) in [
            ("query_records", "created_at"),
            ("query_digest_stats", "last_seen"),
            ("cluster_snapshots", "recorded_at"),
        ] {
            total += self.purge_in_batches(table, column, older_than).await?;
        }
        Ok(total)
    }
}

// ---------------------------------------------------------------------------
// ClusterConfigStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::WorkerRegistryStore for PostgresStore {
    async fn upsert_worker(&self, w: &crate::WorkerRecord) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO worker_registry
                   (name, version, labels, coordination_mode, downloaded_revision,
                    active_revision, last_error, status, kind, parent, last_seen)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
               ON CONFLICT (name) DO UPDATE SET
                   version             = EXCLUDED.version,
                   labels              = EXCLUDED.labels,
                   coordination_mode   = EXCLUDED.coordination_mode,
                   downloaded_revision = EXCLUDED.downloaded_revision,
                   active_revision     = EXCLUDED.active_revision,
                   last_error          = EXCLUDED.last_error,
                   status              = EXCLUDED.status,
                   kind                = EXCLUDED.kind,
                   parent              = EXCLUDED.parent,
                   last_seen           = now()"#,
        )
        .bind(&w.name)
        .bind(&w.version)
        .bind(sqlx::types::Json(&w.labels))
        .bind(&w.coordination_mode)
        .bind(&w.downloaded_revision)
        .bind(&w.active_revision)
        .bind(&w.last_error)
        .bind(sqlx::types::Json(&w.status))
        .bind(&w.kind)
        .bind(&w.parent)
        .execute(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_worker: {e}")))?;
        Ok(())
    }

    async fn list_workers(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<crate::WorkerRecord>> {
        type Row = (
            String,
            String,
            sqlx::types::Json<std::collections::BTreeMap<String, String>>,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
            sqlx::types::Json<serde_json::Value>,
            String,
            Option<String>,
        );
        let rows: Vec<Row> = sqlx::query_as(
            r#"SELECT name, version, labels, coordination_mode, downloaded_revision,
                      active_revision, last_error, last_seen, registered_at, status,
                      kind, parent
                 FROM worker_registry
                WHERE last_seen >= $1
                ORDER BY name"#,
        )
        .bind(since)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_workers: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| crate::WorkerRecord {
                name: r.0,
                version: r.1,
                labels: r.2 .0,
                coordination_mode: r.3,
                downloaded_revision: r.4,
                active_revision: r.5,
                last_error: r.6,
                last_seen: r.7,
                registered_at: Some(r.8),
                // A row written by a different version may not match; show what we can.
                status: serde_json::from_value(r.9 .0).unwrap_or_default(),
                kind: r.10,
                parent: r.11,
            })
            .collect())
    }
}

#[async_trait]
impl crate::ConfigSnapshotStore for PostgresStore {
    async fn load_config_snapshot_rows(&self) -> Result<crate::ConfigSnapshotRows> {
        let err = |what: &str, e: sqlx::Error| {
            QueryFluxError::Persistence(format!("load_config_snapshot_rows {what}: {e}"))
        };
        let mut tx = self.begin_snapshot_tx().await?;
        let rows = crate::ConfigSnapshotRows {
            clusters: Self::list_cluster_configs_in(&mut tx).await?,
            groups: Self::list_group_configs_in(&mut tx).await?,
            routing: Self::load_routing_config_in(&mut tx).await?,
            group_translation_bodies: Self::load_group_translation_bodies_in(&mut tx).await?,
            guardrails: Self::get_proxy_setting_in(&mut tx, "guardrails_config").await?,
            security: Self::get_proxy_setting_in(&mut tx, "security_config").await?,
            catalog: Self::get_proxy_setting_in(&mut tx, "catalog_config").await?,
            access_control: Self::get_proxy_setting_in(&mut tx, "access_control_config").await?,
            guard_scripts: Self::list_user_scripts_in(
                &mut tx,
                Some(crate::script_library::KIND_GUARD),
            )
            .await?,
        };
        tx.commit().await.map_err(|e| err("commit", e))?;
        Ok(rows)
    }
}

impl PostgresStore {
    /// A read-only transaction in which every read sees the same committed state, even if
    /// an admin edit commits in between.
    async fn begin_snapshot_tx(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
        let mut tx = self.admin_pool.begin().await.map_err(|e| {
            QueryFluxError::Persistence(format!("load_config_snapshot_rows begin: {e}"))
        })?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                QueryFluxError::Persistence(format!("load_config_snapshot_rows isolation: {e}"))
            })?;
        Ok(tx)
    }
}

/// Config reads that take a connection, so they can run alone (through the pool) or
/// together inside one transaction (`load_config_snapshot_rows`).
impl PostgresStore {
    async fn list_cluster_configs_in(
        conn: &mut sqlx::PgConnection,
    ) -> Result<Vec<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>("SELECT * FROM cluster_configs ORDER BY name")
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("list_cluster_configs: {e}")))
    }

    async fn list_group_configs_in(
        conn: &mut sqlx::PgConnection,
    ) -> Result<Vec<ClusterGroupConfigRecord>> {
        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} ORDER BY g.name");
        sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("list_group_configs: {e}")))
    }

    async fn load_group_translation_bodies_in(
        conn: &mut sqlx::PgConnection,
    ) -> Result<HashMap<String, Vec<String>>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT g.name, s.body
               FROM cluster_group_configs g
               CROSS JOIN LATERAL unnest(g.translation_script_ids) WITH ORDINALITY AS u(sid, ord)
               JOIN user_scripts s ON s.id = u.sid AND s.kind = 'translation_fixup'
               ORDER BY g.name, u.ord"#,
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("load_group_translation_bodies: {e}")))?;

        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (name, body) in rows {
            map.entry(name).or_default().push(body);
        }
        Ok(map)
    }

    async fn get_proxy_setting_in(
        conn: &mut sqlx::PgConnection,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        match key {
            "security_config" => {
                let row: Option<(serde_json::Value,)> = sqlx::query_as(
                    r#"SELECT config FROM security_settings WHERE singleton = TRUE"#,
                )
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("get_proxy_setting: {e}")))?;
                Ok(row.map(|(v,)| v))
            }
            "guardrails_config" => {
                let rows: Vec<(String, serde_json::Value)> =
                    sqlx::query_as(r#"SELECT kind, guards FROM guardrails ORDER BY kind"#)
                        .fetch_all(&mut *conn)
                        .await
                        .map_err(|e| QueryFluxError::Persistence(format!("get guardrails: {e}")))?;
                if rows.is_empty() {
                    return Ok(None);
                }
                let mut global = serde_json::Value::Array(vec![]);
                let mut groups = serde_json::Map::new();
                for (kind, guards) in rows {
                    if kind == "global" {
                        global = guards;
                    } else {
                        groups.insert(kind, guards);
                    }
                }
                Ok(Some(
                    serde_json::json!({ "global": global, "groups": groups }),
                ))
            }
            other => {
                let row: Option<(serde_json::Value,)> =
                    sqlx::query_as(r#"SELECT value FROM proxy_settings WHERE key = $1"#)
                        .bind(other)
                        .fetch_optional(&mut *conn)
                        .await
                        .map_err(|e| {
                            QueryFluxError::Persistence(format!("get_proxy_setting: {e}"))
                        })?;
                Ok(row.map(|(v,)| v))
            }
        }
    }

    async fn load_routing_config_in(
        conn: &mut sqlx::PgConnection,
    ) -> Result<Option<LoadedRoutingConfig>> {
        let row: Option<(bool, String, Option<i64>)> = sqlx::query_as(
            r#"SELECT routing_persist_active, routing_fallback, fallback_group_id
                 FROM routing_settings
                WHERE singleton = true"#,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("load_routing_config settings: {e}")))?;

        let Some((persist_active, fallback, fallback_gid)) = row else {
            return Ok(None);
        };
        if !persist_active {
            return Ok(None);
        }

        let id_rows: Vec<(i64, String)> =
            sqlx::query_as(r#"SELECT id, name FROM cluster_group_configs"#)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("load_routing_config groups: {e}"))
                })?;
        let id_to_name: HashMap<i64, String> = id_rows.into_iter().collect();

        // (sort_order, router_logical_index, slice_index, target_group_id, definition)
        type RoutingRuleRow = (
            i32,
            i32,
            i32,
            Option<i64>,
            sqlx::types::Json<serde_json::Value>,
        );
        let sql_rows: Vec<RoutingRuleRow> = sqlx::query_as(
            r#"SELECT sort_order, router_logical_index, slice_index, target_group_id, definition
                     FROM routing_rules
                    ORDER BY sort_order ASC, id ASC"#,
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("load_routing_config rules: {e}")))?;

        let persist_rows: Vec<RoutingRulePersistRow> = sql_rows
            .into_iter()
            .map(
                |(sort_order, router_logical_index, slice_index, target_group_id, def)| {
                    RoutingRulePersistRow {
                        sort_order,
                        router_logical_index,
                        slice_index,
                        target_group_id,
                        definition: def.0,
                    }
                },
            )
            .collect();

        let routers = collapse_rows_to_routers(&persist_rows, &id_to_name)?;

        Ok(Some(LoadedRoutingConfig {
            routing_fallback: fallback,
            routing_fallback_group_id: fallback_gid,
            routers,
        }))
    }

    async fn list_user_scripts_in(
        conn: &mut sqlx::PgConnection,
        kind: Option<&str>,
    ) -> Result<Vec<UserScriptRecord>> {
        let rows = if let Some(k) = kind {
            sqlx::query_as::<_, UserScriptRecord>(
                "SELECT * FROM user_scripts WHERE kind = $1 ORDER BY name",
            )
            .bind(k)
            .fetch_all(&mut *conn)
            .await
        } else {
            sqlx::query_as::<_, UserScriptRecord>("SELECT * FROM user_scripts ORDER BY name")
                .fetch_all(&mut *conn)
                .await
        }
        .map_err(|e| QueryFluxError::Persistence(format!("list_user_scripts: {e}")))?;
        Ok(rows)
    }
}

#[async_trait]
impl ClusterConfigStore for PostgresStore {
    async fn list_cluster_configs(&self) -> Result<Vec<ClusterConfigRecord>> {
        let mut conn = self.admin_pool.acquire().await.map_err(|e| {
            QueryFluxError::Persistence(format!("list_cluster_configs acquire: {e}"))
        })?;
        Self::list_cluster_configs_in(&mut conn).await
    }

    async fn get_cluster_config(&self, name: &str) -> Result<Option<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>("SELECT * FROM cluster_configs WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("get_cluster_config: {e}")))
    }

    async fn upsert_cluster_config(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<ClusterConfigRecord> {
        sqlx::query_as::<_, ClusterConfigRecord>(
            r#"INSERT INTO cluster_configs (name, engine_key, enabled, max_running_queries, config, variants)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (name) DO UPDATE SET
                   engine_key          = EXCLUDED.engine_key,
                   enabled             = EXCLUDED.enabled,
                   max_running_queries = EXCLUDED.max_running_queries,
                   config              = EXCLUDED.config,
                   variants            = EXCLUDED.variants,
                   updated_at          = now()
               RETURNING *"#,
        )
        .bind(name)
        .bind(&cfg.engine_key)
        .bind(cfg.enabled)
        .bind(cfg.max_running_queries)
        .bind(&cfg.config)
        .bind(&cfg.variants)
        .fetch_one(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_cluster_config: {e}")))
    }

    async fn insert_cluster_config_if_missing(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<bool> {
        let inserted = sqlx::query_as::<_, ClusterConfigRecord>(
            r#"INSERT INTO cluster_configs (name, engine_key, enabled, max_running_queries, config)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO NOTHING
               RETURNING *"#,
        )
        .bind(name)
        .bind(&cfg.engine_key)
        .bind(cfg.enabled)
        .bind(cfg.max_running_queries)
        .bind(&cfg.config)
        .fetch_optional(&self.admin_pool)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("insert_cluster_config_if_missing: {e}"))
        })?;
        Ok(inserted.is_some())
    }

    async fn delete_cluster_config(&self, name: &str) -> Result<bool> {
        let mut tx = self.admin_pool.begin().await.map_err(|e| {
            QueryFluxError::Persistence(format!("delete_cluster_config begin: {e}"))
        })?;

        let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM cluster_configs WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                QueryFluxError::Persistence(format!("delete_cluster_config lookup: {e}"))
            })?;

        let Some((cluster_id,)) = row else {
            tx.rollback().await.map_err(|e| {
                QueryFluxError::Persistence(format!("delete_cluster_config rollback: {e}"))
            })?;
            return Ok(false);
        };

        sqlx::query(
            r#"UPDATE cluster_group_configs
               SET members = array_remove(members, $1),
                   updated_at = now()
               WHERE $1 = ANY(members)"#,
        )
        .bind(cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("delete_cluster_config strip groups: {e}"))
        })?;

        let r = sqlx::query("DELETE FROM cluster_configs WHERE name = $1")
            .bind(name)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                QueryFluxError::Persistence(format!("delete_cluster_config delete: {e}"))
            })?;

        tx.commit().await.map_err(|e| {
            QueryFluxError::Persistence(format!("delete_cluster_config commit: {e}"))
        })?;

        Ok(r.rows_affected() > 0)
    }

    async fn cluster_configs_count(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cluster_configs")
            .fetch_one(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("cluster_configs_count: {e}")))?;
        Ok(n)
    }

    async fn rename_cluster_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterConfigRecord> {
        let old_name = old_name.trim();
        let new_name = new_name.trim();
        if new_name.is_empty() {
            return Err(QueryFluxError::Persistence(
                "New cluster name must not be empty".to_string(),
            ));
        }
        if old_name == new_name {
            return self.get_cluster_config(old_name).await?.ok_or_else(|| {
                QueryFluxError::Persistence(format!("Cluster '{old_name}' not found"))
            });
        }

        let mut tx = self.admin_pool.begin().await.map_err(|e| {
            QueryFluxError::Persistence(format!("rename_cluster_config begin: {e}"))
        })?;

        let taken: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM cluster_configs WHERE name = $1")
                .bind(new_name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("rename_cluster_config check new: {e}"))
                })?;
        if taken.is_some() {
            return Err(QueryFluxError::Persistence(format!(
                "Cluster name '{new_name}' is already in use"
            )));
        }

        let row = sqlx::query_as::<_, ClusterConfigRecord>(
            r#"UPDATE cluster_configs
                  SET name = $2, updated_at = now()
                WHERE name = $1
            RETURNING *"#,
        )
        .bind(old_name)
        .bind(new_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(|db| db.code() == Some(Cow::Borrowed("23505")))
            {
                QueryFluxError::Persistence(format!("Cluster name '{new_name}' is already in use"))
            } else {
                QueryFluxError::Persistence(format!("rename_cluster_config: {e}"))
            }
        })?;

        let Some(record) = row else {
            return Err(QueryFluxError::Persistence(format!(
                "Cluster '{old_name}' not found"
            )));
        };

        tx.commit().await.map_err(|e| {
            QueryFluxError::Persistence(format!("rename_cluster_config commit: {e}"))
        })?;
        Ok(record)
    }

    async fn list_group_configs(&self) -> Result<Vec<ClusterGroupConfigRecord>> {
        let mut conn =
            self.admin_pool.acquire().await.map_err(|e| {
                QueryFluxError::Persistence(format!("list_group_configs acquire: {e}"))
            })?;
        Self::list_group_configs_in(&mut conn).await
    }

    async fn get_group_config(&self, name: &str) -> Result<Option<ClusterGroupConfigRecord>> {
        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} WHERE g.name = $1");
        sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .bind(name)
            .fetch_optional(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("get_group_config: {e}")))
    }

    async fn upsert_group_config(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<ClusterGroupConfigRecord> {
        cfg.validate_circuit_breaker()
            .map_err(QueryFluxError::Config)?;
        let mut tx =
            self.admin_pool.begin().await.map_err(|e| {
                QueryFluxError::Persistence(format!("upsert_group_config begin: {e}"))
            })?;

        let mut member_ids: Vec<i64> = Vec::with_capacity(cfg.members.len());
        for m in &cfg.members {
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT id FROM cluster_configs WHERE name = $1")
                    .bind(m)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!(
                            "upsert_group_config member lookup: {e}"
                        ))
                    })?;
            let Some((cid,)) = row else {
                return Err(QueryFluxError::Persistence(format!(
                    "Unknown cluster '{m}' in group members (clusters must exist first)"
                )));
            };
            member_ids.push(cid);
        }

        for sid in &cfg.translation_script_ids {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT kind FROM user_scripts WHERE id = $1")
                    .bind(sid)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!(
                            "upsert_group_config script lookup: {e}"
                        ))
                    })?;
            let Some((kind,)) = row else {
                return Err(QueryFluxError::Persistence(format!(
                    "Unknown translation script id {sid}"
                )));
            };
            if kind != KIND_TRANSLATION_FIXUP {
                return Err(QueryFluxError::Persistence(format!(
                    "Script id {sid} has kind '{kind}', expected '{KIND_TRANSLATION_FIXUP}' for group translation"
                )));
            }
        }

        sqlx::query(
            r#"INSERT INTO cluster_group_configs
                   (name, enabled, members, max_running_queries, max_queued_queries, strategy, circuit_breaker, allow_groups, allow_users, translation_script_ids, default_tags, cache)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
               ON CONFLICT (name) DO UPDATE SET
                   enabled                = EXCLUDED.enabled,
                   members                = EXCLUDED.members,
                   max_running_queries    = EXCLUDED.max_running_queries,
                   max_queued_queries     = EXCLUDED.max_queued_queries,
                   strategy               = EXCLUDED.strategy,
                   circuit_breaker        = EXCLUDED.circuit_breaker,
                   allow_groups           = EXCLUDED.allow_groups,
                   allow_users            = EXCLUDED.allow_users,
                   translation_script_ids = EXCLUDED.translation_script_ids,
                   default_tags           = EXCLUDED.default_tags,
                   cache                  = EXCLUDED.cache,
                   updated_at             = now()"#,
        )
        .bind(name)
        .bind(cfg.enabled)
        .bind(&member_ids)
        .bind(cfg.max_running_queries)
        .bind(cfg.max_queued_queries)
        .bind(&cfg.strategy)
        .bind(&cfg.circuit_breaker)
        .bind(&cfg.allow_groups)
        .bind(&cfg.allow_users)
        .bind(&cfg.translation_script_ids)
        .bind(&cfg.default_tags)
        .bind(&cfg.cache)
        .execute(&mut *tx)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_group_config: {e}")))?;

        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} WHERE g.name = $1");
        let record = sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .bind(name)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("upsert_group_config reload: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("upsert_group_config commit: {e}")))?;
        Ok(record)
    }

    async fn insert_group_config_if_missing(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<bool> {
        cfg.validate_circuit_breaker()
            .map_err(QueryFluxError::Config)?;
        let mut tx = self.admin_pool.begin().await.map_err(|e| {
            QueryFluxError::Persistence(format!("insert_group_config_if_missing begin: {e}"))
        })?;

        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM cluster_group_configs WHERE name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!(
                        "insert_group_config_if_missing lookup: {e}"
                    ))
                })?;
        if existing.is_some() {
            tx.commit().await.map_err(|e| {
                QueryFluxError::Persistence(format!("insert_group_config_if_missing commit: {e}"))
            })?;
            return Ok(false);
        }

        let mut member_ids: Vec<i64> = Vec::with_capacity(cfg.members.len());
        for m in &cfg.members {
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT id FROM cluster_configs WHERE name = $1")
                    .bind(m)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!(
                            "insert_group_config_if_missing member lookup: {e}"
                        ))
                    })?;
            let Some((cid,)) = row else {
                return Err(QueryFluxError::Persistence(format!(
                    "Unknown cluster '{m}' in group members (clusters must exist first)"
                )));
            };
            member_ids.push(cid);
        }

        for sid in &cfg.translation_script_ids {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT kind FROM user_scripts WHERE id = $1")
                    .bind(sid)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!(
                            "insert_group_config_if_missing script lookup: {e}"
                        ))
                    })?;
            let Some((kind,)) = row else {
                return Err(QueryFluxError::Persistence(format!(
                    "Unknown translation script id {sid}"
                )));
            };
            if kind != KIND_TRANSLATION_FIXUP {
                return Err(QueryFluxError::Persistence(format!(
                    "Script id {sid} has kind '{kind}', expected '{KIND_TRANSLATION_FIXUP}' for group translation"
                )));
            }
        }

        let inserted: Option<(i64,)> = sqlx::query_as(
            r#"INSERT INTO cluster_group_configs
                   (name, enabled, members, max_running_queries, max_queued_queries, strategy, circuit_breaker, allow_groups, allow_users, translation_script_ids, default_tags, cache)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
               ON CONFLICT (name) DO NOTHING
               RETURNING id"#,
        )
        .bind(name)
        .bind(cfg.enabled)
        .bind(&member_ids)
        .bind(cfg.max_running_queries)
        .bind(cfg.max_queued_queries)
        .bind(&cfg.strategy)
        .bind(&cfg.circuit_breaker)
        .bind(&cfg.allow_groups)
        .bind(&cfg.allow_users)
        .bind(&cfg.translation_script_ids)
        .bind(&cfg.default_tags)
        .bind(&cfg.cache)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("insert_group_config_if_missing: {e}"))
        })?;

        tx.commit().await.map_err(|e| {
            QueryFluxError::Persistence(format!("insert_group_config_if_missing commit: {e}"))
        })?;
        Ok(inserted.is_some())
    }

    async fn delete_group_config(&self, name: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM cluster_group_configs WHERE name = $1")
            .bind(name)
            .execute(&self.admin_pool)
            .await
            .map_err(|e| {
                if let Some(db) = e.as_database_error() {
                    if db.code() == Some(std::borrow::Cow::Borrowed("23503")) {
                        return QueryFluxError::Persistence(format!(
                            "Cannot delete group '{name}': still referenced by routing rules"
                        ));
                    }
                }
                QueryFluxError::Persistence(format!("delete_group_config: {e}"))
            })?;
        Ok(r.rows_affected() > 0)
    }

    async fn group_configs_count(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cluster_group_configs")
            .fetch_one(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("group_configs_count: {e}")))?;
        Ok(n)
    }

    async fn rename_group_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterGroupConfigRecord> {
        let old_name = old_name.trim();
        let new_name = new_name.trim();
        if new_name.is_empty() {
            return Err(QueryFluxError::Persistence(
                "New group name must not be empty".to_string(),
            ));
        }
        if old_name == new_name {
            let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} WHERE g.name = $1");
            return sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
                .bind(old_name)
                .fetch_optional(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("rename_group_config: {e}")))?
                .ok_or_else(|| {
                    QueryFluxError::Persistence(format!("Group '{old_name}' not found"))
                });
        }

        let mut tx =
            self.admin_pool.begin().await.map_err(|e| {
                QueryFluxError::Persistence(format!("rename_group_config begin: {e}"))
            })?;

        let taken: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM cluster_group_configs WHERE name = $1")
                .bind(new_name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("rename_group_config check new: {e}"))
                })?;
        if taken.is_some() {
            return Err(QueryFluxError::Persistence(format!(
                "Group name '{new_name}' is already in use"
            )));
        }

        let updated = sqlx::query(
            r#"UPDATE cluster_group_configs
                  SET name = $2, updated_at = now()
                WHERE name = $1"#,
        )
        .bind(old_name)
        .bind(new_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(|db| db.code() == Some(Cow::Borrowed("23505")))
            {
                QueryFluxError::Persistence(format!("Group name '{new_name}' is already in use"))
            } else {
                QueryFluxError::Persistence(format!("rename_group_config update group: {e}"))
            }
        })?;

        if updated.rows_affected() == 0 {
            return Err(QueryFluxError::Persistence(format!(
                "Group '{old_name}' not found"
            )));
        }

        sqlx::query(
            r#"UPDATE routing_settings
                  SET routing_fallback = $1
                WHERE singleton = true
                  AND routing_fallback = $2"#,
        )
        .bind(new_name)
        .bind(old_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("rename_group_config routing_fallback: {e}"))
        })?;

        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} WHERE g.name = $1");
        let record = sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .bind(new_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("rename_group_config reload: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("rename_group_config commit: {e}")))?;
        Ok(record)
    }
}

#[async_trait]
impl ScriptLibraryStore for PostgresStore {
    async fn list_user_scripts(&self, kind: Option<&str>) -> Result<Vec<UserScriptRecord>> {
        let mut conn =
            self.admin_pool.acquire().await.map_err(|e| {
                QueryFluxError::Persistence(format!("list_user_scripts acquire: {e}"))
            })?;
        Self::list_user_scripts_in(&mut conn, kind).await
    }

    async fn get_user_script(&self, id: i64) -> Result<Option<UserScriptRecord>> {
        sqlx::query_as::<_, UserScriptRecord>("SELECT * FROM user_scripts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("get_user_script: {e}")))
    }

    async fn create_user_script(&self, body: &UpsertUserScript) -> Result<UserScriptRecord> {
        if !is_valid_script_kind(&body.kind) {
            return Err(QueryFluxError::Persistence(format!(
                "Invalid script kind '{}'",
                body.kind
            )));
        }
        sqlx::query_as::<_, UserScriptRecord>(
            r#"INSERT INTO user_scripts (name, description, kind, body)
               VALUES ($1, $2, $3, $4)
               RETURNING *"#,
        )
        .bind(&body.name)
        .bind(&body.description)
        .bind(&body.kind)
        .bind(&body.body)
        .fetch_one(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("create_user_script: {e}")))
    }

    async fn update_user_script(
        &self,
        id: i64,
        body: &UpsertUserScript,
    ) -> Result<UserScriptRecord> {
        if !is_valid_script_kind(&body.kind) {
            return Err(QueryFluxError::Persistence(format!(
                "Invalid script kind '{}'",
                body.kind
            )));
        }
        sqlx::query_as::<_, UserScriptRecord>(
            r#"UPDATE user_scripts SET
                   name = $2,
                   description = $3,
                   kind = $4,
                   body = $5,
                   updated_at = now()
               WHERE id = $1
               RETURNING *"#,
        )
        .bind(id)
        .bind(&body.name)
        .bind(&body.description)
        .bind(&body.kind)
        .bind(&body.body)
        .fetch_optional(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("update_user_script: {e}")))?
        .ok_or_else(|| QueryFluxError::Persistence(format!("user script id {id} not found")))
    }

    async fn delete_user_script(&self, id: i64) -> Result<bool> {
        let r = sqlx::query("DELETE FROM user_scripts WHERE id = $1")
            .bind(id)
            .execute(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("delete_user_script: {e}")))?;
        Ok(r.rows_affected() > 0)
    }

    async fn load_group_translation_bodies(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut conn = self.admin_pool.acquire().await.map_err(|e| {
            QueryFluxError::Persistence(format!("load_group_translation_bodies acquire: {e}"))
        })?;
        Self::load_group_translation_bodies_in(&mut conn).await
    }
}

// ---------------------------------------------------------------------------
// ProxySettingsStore — `security_config` backed by `security_settings` (singleton JSON)
// ---------------------------------------------------------------------------

#[async_trait]
impl ProxySettingsStore for PostgresStore {
    async fn get_proxy_setting(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let mut conn =
            self.admin_pool.acquire().await.map_err(|e| {
                QueryFluxError::Persistence(format!("get_proxy_setting acquire: {e}"))
            })?;
        Self::get_proxy_setting_in(&mut conn, key).await
    }

    async fn set_proxy_setting(&self, key: &str, value: serde_json::Value) -> Result<()> {
        match key {
            "security_config" => {
                sqlx::query(
                    r#"INSERT INTO security_settings (singleton, config) VALUES (TRUE, $1)
                       ON CONFLICT (singleton) DO UPDATE SET config = EXCLUDED.config, updated_at = now()"#,
                )
                .bind(&value)
                .execute(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("set_proxy_setting: {e}")))?;
            }
            "guardrails_config" => {
                let global = value
                    .get("global")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]));
                let groups = value
                    .get("groups")
                    .and_then(|g| g.as_object())
                    .cloned()
                    .unwrap_or_default();

                let mut tx = self
                    .admin_pool
                    .begin()
                    .await
                    .map_err(|e| QueryFluxError::Persistence(format!("guardrails tx: {e}")))?;

                // Upsert global row.
                sqlx::query(
                    r#"INSERT INTO guardrails (kind, guards) VALUES ('global', $1)
                       ON CONFLICT (kind) DO UPDATE SET guards = EXCLUDED.guards, updated_at = now()"#,
                )
                .bind(&global)
                .execute(&mut *tx)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("upsert global guardrail: {e}")))?;

                // Upsert per-group rows.
                for (group, guards) in &groups {
                    sqlx::query(
                        r#"INSERT INTO guardrails (kind, guards) VALUES ($1, $2)
                           ON CONFLICT (kind) DO UPDATE SET guards = EXCLUDED.guards, updated_at = now()"#,
                    )
                    .bind(group)
                    .bind(guards)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| QueryFluxError::Persistence(format!("upsert group guardrail: {e}")))?;
                }

                // Remove rows for groups no longer in the config.
                let kept_kinds: Vec<String> = std::iter::once("global".to_string())
                    .chain(groups.keys().cloned())
                    .collect();
                sqlx::query(r#"DELETE FROM guardrails WHERE kind != ALL($1)"#)
                    .bind(&kept_kinds)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!("delete stale guardrails: {e}"))
                    })?;

                tx.commit().await.map_err(|e| {
                    QueryFluxError::Persistence(format!("guardrails tx commit: {e}"))
                })?;
            }
            other => {
                sqlx::query(
                    r#"INSERT INTO proxy_settings (key, value) VALUES ($1, $2)
                       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()"#,
                )
                .bind(other)
                .bind(&value)
                .execute(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("set_proxy_setting: {e}")))?;
            }
        }
        Ok(())
    }

    async fn delete_proxy_setting(&self, key: &str) -> Result<()> {
        match key {
            "security_config" => {
                sqlx::query(
                    r#"UPDATE security_settings SET config = '{}'::jsonb, updated_at = now() WHERE singleton = TRUE"#,
                )
                .execute(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("delete_proxy_setting: {e}")))?;
            }
            "guardrails_config" => {
                sqlx::query(r#"DELETE FROM guardrails"#)
                    .execute(&self.admin_pool)
                    .await
                    .map_err(|e| QueryFluxError::Persistence(format!("delete guardrails: {e}")))?;
            }
            other => {
                sqlx::query(r#"DELETE FROM proxy_settings WHERE key = $1"#)
                    .bind(other)
                    .execute(&self.admin_pool)
                    .await
                    .map_err(|e| {
                        QueryFluxError::Persistence(format!("delete_proxy_setting: {e}"))
                    })?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RoutingConfigStore — expand/collapse routers ↔ `routing_rules` slices
// ---------------------------------------------------------------------------

#[async_trait]
impl RoutingConfigStore for PostgresStore {
    async fn load_routing_config(&self) -> Result<Option<LoadedRoutingConfig>> {
        let mut conn = self.admin_pool.acquire().await.map_err(|e| {
            QueryFluxError::Persistence(format!("load_routing_config acquire: {e}"))
        })?;
        Self::load_routing_config_in(&mut conn).await
    }

    async fn replace_routing_config(
        &self,
        routing_fallback: &str,
        routing_fallback_group_id: Option<i64>,
        routers: &[serde_json::Value],
    ) -> Result<()> {
        let mut tx = self.admin_pool.begin().await.map_err(|e| {
            QueryFluxError::Persistence(format!("replace_routing_config begin: {e}"))
        })?;

        sqlx::query(
            r#"UPDATE routing_settings
               SET routing_fallback = $1,
                   fallback_group_id = $2,
                   routing_persist_active = true
             WHERE singleton = true"#,
        )
        .bind(routing_fallback)
        .bind(routing_fallback_group_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("replace_routing_config settings: {e}"))
        })?;

        sqlx::query("DELETE FROM routing_rules")
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                QueryFluxError::Persistence(format!("replace_routing_config delete: {e}"))
            })?;

        let rows: Vec<(String, i64)> =
            sqlx::query_as(r#"SELECT name, id FROM cluster_group_configs"#)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("replace_routing_config groups: {e}"))
                })?;
        let name_to_id: HashMap<String, i64> = rows.into_iter().collect();

        let mut sort_key: i32 = 0;
        for (logical_idx, def) in routers.iter().enumerate() {
            let slices = expand_router_for_persistence(def, &name_to_id).map_err(|e| {
                QueryFluxError::Persistence(format!("replace_routing_config expand: {e}"))
            })?;
            if slices.is_empty() {
                return Err(QueryFluxError::Persistence(format!(
                    "router at index {logical_idx} produced no routing slices (empty mappings?)"
                )));
            }
            for (slice_i, (stripped_def, gid)) in slices.iter().enumerate() {
                sqlx::query(
                    r#"INSERT INTO routing_rules (sort_order, router_logical_index, slice_index, definition, target_group_id)
                       VALUES ($1, $2, $3, $4, $5)"#,
                )
                .bind(sort_key)
                .bind(logical_idx as i32)
                .bind(slice_i as i32)
                .bind(stripped_def)
                .bind(gid)
                .execute(&mut *tx)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("replace_routing_config insert: {e}")))?;
                sort_key = sort_key
                    .checked_add(1)
                    .ok_or_else(|| QueryFluxError::Persistence("too many routing slices".into()))?;
            }
        }

        tx.commit().await.map_err(|e| {
            QueryFluxError::Persistence(format!("replace_routing_config commit: {e}"))
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Persistence — in-flight query state (short-lived rows, deleted on completion)
// ---------------------------------------------------------------------------

#[async_trait]
impl Persistence for PostgresStore {
    async fn upsert(&self, query: ExecutingQuery) -> Result<()> {
        // Key by backend_query_id (Trino's ID) — matches the client poll URL.
        let id = query.backend_query_id.0.clone();
        let data = serde_json::to_value(&query)
            .map_err(|e| QueryFluxError::Persistence(format!("Serialize error: {e}")))?;
        sqlx::query(
            "INSERT INTO executing_queries (id, data) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&id)
        .bind(data)
        .execute(&self.query_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert executing_queries: {e}")))?;
        Ok(())
    }

    async fn get(&self, id: &BackendQueryId) -> Result<Option<ExecutingQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.query_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("Get executing_queries: {e}")))?;
        match row {
            None => Ok(None),
            Some((data,)) => {
                let q = serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))?;
                Ok(Some(q))
            }
        }
    }

    async fn delete(&self, id: &BackendQueryId) -> Result<()> {
        sqlx::query("DELETE FROM executing_queries WHERE id = $1")
            .bind(&id.0)
            .execute(&self.query_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete executing_queries: {e}")))?;
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<ExecutingQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries ORDER BY created_at")
                .fetch_all(&self.query_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("List executing_queries: {e}")))?;
        rows.into_iter()
            .map(|(data,)| {
                serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))
            })
            .collect()
    }

    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()> {
        let id = query.id.0.clone();
        let group = query.cluster_group.0.clone();
        let last_accessed = query.last_accessed;
        let data = serde_json::to_value(&query)
            .map_err(|e| QueryFluxError::Persistence(format!("Serialize error: {e}")))?;
        // Re-queues rebuild the QueuedQuery with a fresh creation_time; keep
        // the original so it always means "first enqueued at" — the fairness
        // gate orders waiters by it.
        sqlx::query(
            r#"
            INSERT INTO queued_queries (id, data, cluster_group, last_accessed)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                data = jsonb_set(
                    EXCLUDED.data,
                    '{creation_time}',
                    COALESCE(
                        queued_queries.data->'creation_time',
                        EXCLUDED.data->'creation_time'
                    )
                ),
                cluster_group = EXCLUDED.cluster_group,
                last_accessed = EXCLUDED.last_accessed
            "#,
        )
        .bind(&id)
        .bind(data)
        .bind(&group)
        .bind(last_accessed)
        .execute(&self.query_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert queued_queries: {e}")))?;
        Ok(())
    }

    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.query_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("Get queued_queries: {e}")))?;
        match row {
            None => Ok(None),
            Some((data,)) => {
                let q = serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))?;
                Ok(Some(q))
            }
        }
    }

    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()> {
        sqlx::query("DELETE FROM queued_queries WHERE id = $1")
            .bind(&id.0)
            .execute(&self.query_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete queued_queries: {e}")))?;
        Ok(())
    }

    async fn take_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("DELETE FROM queued_queries WHERE id = $1 RETURNING data")
                .bind(&id.0)
                .fetch_optional(&self.query_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("take_queued: {e}")))?;
        match row {
            None => Ok(None),
            Some((data,)) => {
                let q = serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))?;
                Ok(Some(q))
            }
        }
    }

    async fn list_queued(&self) -> Result<Vec<QueuedQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries ORDER BY created_at")
                .fetch_all(&self.query_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("List queued_queries: {e}")))?;
        rows.into_iter()
            .map(|(data,)| {
                serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))
            })
            .collect()
    }

    async fn touch_queued_last_accessed(&self, id: &ProxyQueryId) -> Result<()> {
        sqlx::query("UPDATE queued_queries SET last_accessed = now() WHERE id = $1")
            .bind(&id.0)
            .execute(&self.query_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("touch_queued_last_accessed: {e}")))?;
        Ok(())
    }

    async fn delete_queued_not_accessed_since(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let result = sqlx::query("DELETE FROM queued_queries WHERE last_accessed < $1")
            .bind(cutoff)
            .execute(&self.query_pool)
            .await
            .map_err(|e| {
                QueryFluxError::Persistence(format!("delete_queued_not_accessed_since: {e}"))
            })?;
        Ok(result.rows_affected())
    }

    async fn count_active_queued_before(
        &self,
        cluster_group: &str,
        enqueued_before: Option<DateTime<Utc>>,
        active_after: DateTime<Utc>,
    ) -> Result<u64> {
        // creation_time is compared from the JSONB blob (preserved across
        // re-queues by upsert_queued) so both sides of the comparison come
        // from the application clock; the indexed columns narrow the scan.
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM queued_queries
            WHERE cluster_group = $1
              AND last_accessed >= $2
              AND ($3::timestamptz IS NULL
                   OR (data->>'creation_time')::timestamptz < $3)
            "#,
        )
        .bind(cluster_group)
        .bind(active_after)
        .bind(enqueued_before)
        .fetch_one(&self.query_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("count_active_queued_before: {e}")))?;
        Ok(count.max(0) as u64)
    }
}

// ---------------------------------------------------------------------------
// MetricsStore — historical data for the management UI
// ---------------------------------------------------------------------------

#[async_trait]
impl MetricsStore for PostgresStore {
    async fn record_query(&self, r: QueryRecord) -> Result<()> {
        self.insert_query_records(vec![r]).await.map(|_| ())
    }

    /// One insert and one digest upsert per chunk of up to
    /// [`history_batch::MAX_RECORDS_PER_STATEMENT`] records, in one transaction.
    async fn record_queries(&self, records: Vec<QueryRecord>) -> Result<()> {
        self.insert_query_records(records).await.map(|_| ())
    }

    async fn record_cluster_snapshot(&self, s: ClusterSnapshot) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO cluster_snapshots
                (cluster_name, group_name, engine_type, running_queries, queued_queries,
                 max_running_queries, recorded_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7)"#,
        )
        .bind(&s.cluster_name.0)
        .bind(&s.group_name.0)
        .bind(format!("{:?}", s.engine_type))
        .bind(s.running_queries as i32)
        .bind(s.queued_queries as i32)
        .bind(s.max_running_queries as i32)
        .bind(s.recorded_at)
        .execute(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert cluster_snapshots: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConfigRevisionStore — Postgres (revision table + LISTEN/NOTIFY)
// ---------------------------------------------------------------------------

#[async_trait]
impl ConfigRevisionStore for PostgresStore {
    async fn current_revision(&self) -> Result<u64> {
        let row: (i64,) = sqlx::query_as("SELECT revision FROM config_revision WHERE id = TRUE")
            .fetch_one(&self.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Read config_revision: {e}")))?;
        Ok(row.0 as u64)
    }

    async fn bump_revision(&self) -> Result<u64> {
        let row: (i64,) = sqlx::query_as(
            "UPDATE config_revision SET revision = revision + 1, updated_at = now() WHERE id = TRUE RETURNING revision",
        )
        .fetch_one(&self.admin_pool)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("Bump config_revision: {e}"))
        })?;
        Ok(row.0 as u64)
    }

    async fn subscribe_revisions(&self) -> Result<Option<tokio::sync::mpsc::Receiver<u64>>> {
        let database_url = self.database_url.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<u64>(16);

        tokio::spawn(async move {
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
            let mut backoff = std::time::Duration::from_secs(1);

            'reconnect: loop {
                let mut listener = match sqlx::postgres::PgListener::connect(&database_url).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::warn!("PgListener connect failed, retrying in {backoff:?}: {e}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(e) = listen_scoped(&mut listener, "config_revision_changed").await {
                    tracing::warn!("PgListener LISTEN failed, retrying in {backoff:?}: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                backoff = std::time::Duration::from_secs(1);

                loop {
                    match listener.recv().await {
                        Ok(notification) => {
                            let rev = notification.payload().parse::<u64>().unwrap_or(0);
                            if tx.send(rev).await.is_err() {
                                break 'reconnect;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "PgListener recv error, reconnecting in {backoff:?}: {e}"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                            break; // drop listener, reconnect from outer loop
                        }
                    }
                }
            }
        });

        Ok(Some(rx))
    }
}

// ---------------------------------------------------------------------------
// CapacityStore — Postgres implementation
// ---------------------------------------------------------------------------

/// Holder rows silent this long are deleted, like stale leases.
const QUOTA_HOLDER_EXPIRY_SECS: i64 = 300;

#[async_trait]
impl crate::quota::QuotaStore for PostgresStore {
    async fn report_demand(
        &self,
        instance_id: &str,
        reports: &[crate::quota::DemandReport],
    ) -> Result<Option<crate::quota::QuotaResponse>> {
        let clusters: Vec<&str> = reports.iter().map(|r| r.cluster.as_str()).collect();
        let to_i64 = |v: u64| v.min(i64::MAX as u64) as i64;
        let running: Vec<i64> = reports.iter().map(|r| to_i64(r.running)).collect();
        let waiting: Vec<i64> = reports.iter().map(|r| to_i64(r.waiting)).collect();
        let max: Vec<i64> = reports
            .iter()
            .map(|r| to_i64(r.max_running_queries))
            .collect();
        let seen: Vec<i64> = reports.iter().map(|r| to_i64(r.seen_seq)).collect();
        // One statement for every cluster of this holder. A holder that has applied the
        // latest grant (seen_seq >= grant_seq) is enforcing exactly the grant, so the
        // ceiling drops to it.
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            r#"
            INSERT INTO capacity_holders
                (cluster_name, instance_id, running, waiting, max_running, seen_seq, reported_at)
            SELECT r.cluster, $1, r.running, r.waiting, r.max, r.seen, now()
            FROM UNNEST($2::text[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[])
                AS r(cluster, running, waiting, max, seen)
            ON CONFLICT (cluster_name, instance_id) DO UPDATE SET
                running = EXCLUDED.running,
                waiting = EXCLUDED.waiting,
                max_running = EXCLUDED.max_running,
                seen_seq = EXCLUDED.seen_seq,
                reported_at = now(),
                ceiling = CASE
                    WHEN EXCLUDED.seen_seq >= capacity_holders.grant_seq
                        THEN capacity_holders.granted
                    ELSE capacity_holders.ceiling
                END
            RETURNING cluster_name, granted, grant_seq
            "#,
        )
        .bind(instance_id)
        .bind(&clusters)
        .bind(&running)
        .bind(&waiting)
        .bind(&max)
        .bind(&seen)
        .fetch_all(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("report_demand: {e}")))?;
        Ok(Some(crate::quota::QuotaResponse {
            grants: rows
                .into_iter()
                .map(|(cluster, granted, seq)| crate::quota::QuotaGrant {
                    cluster,
                    quota: granted.max(0) as u64,
                    seq: seq.max(0) as u64,
                })
                .collect(),
            lease_secs: self
                .quota_lease_secs
                .load(std::sync::atomic::Ordering::Relaxed),
            report_interval_ms: self
                .quota_report_interval_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        }))
    }

    async fn release_holder(&self, instance_id: &str) -> Result<u64> {
        let r = sqlx::query("DELETE FROM capacity_holders WHERE instance_id = $1")
            .bind(instance_id)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("release_holder: {e}")))?;
        Ok(r.rows_affected())
    }
}

impl PostgresStore {
    /// What holders are told with every grant: how long they may admit on it
    /// (`capacity.quotaLeaseSecs`) and how often to report (`capacity.reportIntervalMs`).
    /// Start (once) the listener that wakes `wait_for_grant` callers when a release hands
    /// their query a slot. One extra connection, opened the first time a query waits.
    async fn ensure_grant_listener(&self) {
        let url = self.database_url.clone();
        let waiters = self.grant_waiters.clone();
        self.grant_listener
            .get_or_init(|| async move {
                tokio::spawn(async move {
                    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
                    let mut backoff = std::time::Duration::from_secs(1);
                    loop {
                        let mut listener = match sqlx::postgres::PgListener::connect(&url).await {
                            Ok(l) => l,
                            Err(e) => {
                                tracing::warn!("Capacity grant listener connect failed: {e}");
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(MAX_BACKOFF);
                                continue;
                            }
                        };
                        if let Err(e) =
                            listen_scoped(&mut listener, "queryflux_capacity_granted").await
                        {
                            tracing::warn!("Capacity grant LISTEN failed: {e}");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                            continue;
                        }
                        backoff = std::time::Duration::from_secs(1);
                        // A notification missed while reconnecting only delays its waiter
                        // until its timeout, when it retries the acquire and claims the slot.
                        while let Ok(n) = listener.recv().await {
                            for id in n.payload().split(' ') {
                                if let Some(w) = waiters.get(id) {
                                    w.notify_one();
                                }
                            }
                        }
                        tracing::warn!("Capacity grant listener lost its connection; reconnecting");
                    }
                });
            })
            .await;
    }

    pub fn set_quota_timing(&self, lease_secs: u64, report_interval_ms: u64) {
        use std::sync::atomic::Ordering;
        self.quota_lease_secs
            .store(lease_secs.max(1), Ordering::Relaxed);
        self.quota_report_interval_ms
            .store(report_interval_ms.clamp(100, 60_000), Ordering::Relaxed);
    }
}

#[async_trait]
impl crate::quota::QuotaAllocator for PostgresStore {
    /// Recompute every cluster's quotas, in one transaction.
    ///
    /// Takes each cluster's advisory lock (in name order, so it can't deadlock with
    /// another statement taking several), reads the lease counters and holder rows, runs
    /// [`crate::quota::allocate`] per cluster, and writes changed grants and each
    /// cluster's `quota_committed`. Per-query acquires on those clusters wait for the
    /// transaction; it is a handful of statements. Run by one replica at a time (the
    /// `capacity-allocator` sweep lock).
    async fn run_allocation(
        &self,
        policy: &crate::quota::AllocatorPolicy,
        metrics: &dyn crate::MetricsStore,
    ) -> Result<crate::quota::AllocationPass> {
        use crate::quota::{allocate, HolderState};
        let err = |what: &str, e: sqlx::Error| QueryFluxError::Persistence(format!("{what}: {e}"));
        let mut tx = self
            .coordination_pool
            .begin()
            .await
            .map_err(|e| err("allocation begin", e))?;

        sqlx::query(
            "DELETE FROM capacity_holders WHERE reported_at < now() - make_interval(secs => $1)",
        )
        .bind(QUOTA_HOLDER_EXPIRY_SECS as f64)
        .execute(&mut *tx)
        .await
        .map_err(|e| err("expire holders", e))?;

        // Clusters with holders, plus any that still have committed quota to clear.
        let clusters: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT cluster_name FROM capacity_holders
            UNION
            SELECT cluster_name FROM cluster_capacity_counters WHERE quota_committed > 0
            ORDER BY 1
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| err("allocation clusters", e))?;
        if clusters.is_empty() {
            tx.commit().await.map_err(|e| err("allocation commit", e))?;
            return Ok(crate::quota::AllocationPass::default());
        }

        sqlx::query(
            "SELECT pg_advisory_xact_lock(queryflux_lock_key('capacity:' || c)) FROM UNNEST($1::text[]) AS c ORDER BY c",
        )
        .bind(&clusters)
        .execute(&mut *tx)
        .await
        .map_err(|e| err("allocation locks", e))?;

        let leased: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
            "SELECT cluster_name, leased FROM cluster_capacity_counters WHERE cluster_name = ANY($1)",
        )
        .bind(&clusters)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| err("allocation counters", e))?
        .into_iter()
        .collect();

        #[allow(clippy::type_complexity)]
        let rows: Vec<(String, String, i64, i64, i64, i64, i64, f64)> = sqlx::query_as(
            r#"
            SELECT cluster_name, instance_id, running, waiting, max_running, granted, ceiling,
                   EXTRACT(EPOCH FROM now() - reported_at)::float8
            FROM capacity_holders
            WHERE cluster_name = ANY($1)
            ORDER BY cluster_name, instance_id
            FOR UPDATE
            "#,
        )
        .bind(&clusters)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| err("allocation holders", e))?;

        let mut by_cluster: HashMap<&str, Vec<(HolderState, u64)>> = HashMap::new();
        for (cluster, instance, running, waiting, max, granted, ceiling, silent) in &rows {
            by_cluster.entry(cluster.as_str()).or_default().push((
                HolderState {
                    instance_id: instance.clone(),
                    running: (*running).max(0) as u64,
                    waiting: (*waiting).max(0) as u64,
                    granted: (*granted).max(0) as u64,
                    ceiling: (*ceiling).max(0) as u64,
                    silent_secs: silent.max(0.0) as u64,
                },
                (*max).max(0) as u64,
            ));
        }

        let mut upd_cluster = Vec::new();
        let mut upd_instance = Vec::new();
        let mut upd_granted = Vec::new();
        let mut upd_ceiling = Vec::new();
        let mut committed_cluster = Vec::new();
        let mut committed_value = Vec::new();
        let mut wanted_value = Vec::new();
        let mut pass = crate::quota::AllocationPass {
            clusters: clusters.len(),
            holders: rows.len(),
            changed: 0,
        };
        for cluster in &clusters {
            let entries = by_cluster.remove(cluster.as_str()).unwrap_or_default();
            // The smallest limit any holder reports, so a holder with a stale config
            // can't widen the limit for the others.
            let max = entries.iter().map(|(_, m)| *m).min().unwrap_or(0);
            let holders: Vec<HolderState> = entries.into_iter().map(|(h, _)| h).collect();
            let leased = leased.get(cluster).copied().unwrap_or(0).max(0) as u64;
            let a = allocate(max, leased, &holders, policy);
            metrics.on_capacity_allocation(cluster, a.quota_committed, holders.len() as u64);
            for c in a.changed {
                upd_cluster.push(cluster.clone());
                upd_instance.push(c.instance_id);
                upd_granted.push(c.granted.min(i64::MAX as u64) as i64);
                upd_ceiling.push(c.ceiling.min(i64::MAX as u64) as i64);
            }
            committed_cluster.push(cluster.clone());
            committed_value.push(a.quota_committed.min(i64::MAX as u64) as i64);
            wanted_value.push(a.quota_wanted.min(i64::MAX as u64) as i64);
        }
        pass.changed = upd_cluster.len();

        if !upd_cluster.is_empty() {
            sqlx::query(
                r#"
                UPDATE capacity_holders h SET
                    grant_seq = CASE WHEN h.granted <> u.granted THEN h.grant_seq + 1 ELSE h.grant_seq END,
                    granted = u.granted,
                    ceiling = u.ceiling
                FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::bigint[])
                    AS u(cluster, instance, granted, ceiling)
                WHERE h.cluster_name = u.cluster AND h.instance_id = u.instance
                "#,
            )
            .bind(&upd_cluster)
            .bind(&upd_instance)
            .bind(&upd_granted)
            .bind(&upd_ceiling)
            .execute(&mut *tx)
            .await
            .map_err(|e| err("allocation grants", e))?;
        }
        sqlx::query(
            r#"
            INSERT INTO cluster_capacity_counters (cluster_name, quota_committed, quota_wanted)
            SELECT c, q, w FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]) AS u(c, q, w)
            ON CONFLICT (cluster_name) DO UPDATE SET
                quota_committed = EXCLUDED.quota_committed,
                quota_wanted = EXCLUDED.quota_wanted
            "#,
        )
        .bind(&committed_cluster)
        .bind(&committed_value)
        .bind(&wanted_value)
        .execute(&mut *tx)
        .await
        .map_err(|e| err("allocation committed", e))?;
        // Capacity a shrinking quota gave back goes to waiters right away.
        sqlx::query(
            "SELECT queryflux_capacity_grant_waiters(c) FROM UNNEST($1::text[]) AS c ORDER BY c",
        )
        .bind(&committed_cluster)
        .execute(&mut *tx)
        .await
        .map_err(|e| err("allocation grants to waiters", e))?;

        tx.commit().await.map_err(|e| err("allocation commit", e))?;
        Ok(pass)
    }
}

impl PostgresStore {
    /// Recount each cluster whose lease counter differs from its lease rows. The counter
    /// only drifts when leases are removed without the delete trigger (`TRUNCATE`, or a
    /// manual fix with triggers disabled). The comparison below is a hint taken without
    /// locks; `queryflux_capacity_recount` redoes it under the cluster's locks.
    async fn recount_drifted_counters(&self) -> Result<()> {
        let drifted: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT c.cluster_name
            FROM cluster_capacity_counters c
            LEFT JOIN (
                SELECT cluster_name, COUNT(*) AS n FROM cluster_capacity_leases GROUP BY cluster_name
            ) l ON l.cluster_name = c.cluster_name
            WHERE c.leased <> COALESCE(l.n, 0)
            "#,
        )
        .fetch_all(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("lease counter check: {e}")))?;
        for cluster in drifted {
            let count: i64 = sqlx::query_scalar("SELECT queryflux_capacity_recount($1)")
                .bind(&cluster)
                .fetch_one(&self.coordination_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("lease recount: {e}")))?;
            tracing::info!(cluster = %cluster, leases = count, "Corrected capacity lease counter");
        }
        Ok(())
    }
}

#[async_trait]
impl CapacityStore for PostgresStore {
    async fn try_acquire(
        &self,
        cluster_name: &str,
        max_running_queries: u64,
        instance_id: &str,
        query_id: &str,
    ) -> Result<bool> {
        // Unlimited: nothing to coordinate, and no lease row to write and delete.
        if max_running_queries == u64::MAX {
            return Ok(true);
        }
        let max_rq = max_running_queries.min(i64::MAX as u64) as i64;

        // Admission in one round trip (V21): the function takes the per-cluster advisory
        // lock, reads the lease counter that triggers keep exact, and inserts the lease if
        // the cluster is below the limit. The lock is held for the call and its commit
        // only, instead of across BEGIN, a COUNT(*) scan, the INSERT and COMMIT.
        sqlx::query_scalar::<_, bool>("SELECT queryflux_capacity_try_acquire($1, $2, $3, $4)")
            .bind(cluster_name)
            .bind(instance_id)
            .bind(query_id)
            .bind(max_rq)
            .fetch_one(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("try_acquire: {e}")))
    }

    async fn release(&self, _cluster_name: &str, query_id: &str) -> Result<()> {
        // Also drop the query's waiter row, so a query that gives up stops holding its
        // place in line.
        sqlx::query("SELECT queryflux_capacity_release(ARRAY[$1])")
            .bind(query_id)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("release: {e}")))?;
        Ok(())
    }

    async fn wait_for_grant(
        &self,
        cluster_name: &str,
        query_id: &str,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        /// Removes the registration however the wait ends, including when the caller's
        /// future is dropped (a cancelled long-poll, or a quota slot winning a select).
        struct Registered<'a>(
            &'a dashmap::DashMap<String, Arc<tokio::sync::Notify>>,
            &'a str,
        );
        impl Drop for Registered<'_> {
            fn drop(&mut self) {
                self.0.remove(self.1);
            }
        }

        self.ensure_grant_listener().await;
        let notify = Arc::new(tokio::sync::Notify::new());
        self.grant_waiters
            .insert(query_id.to_string(), notify.clone());
        let _registered = Registered(&self.grant_waiters, query_id);
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Park in the line (only parked waiters are handed slots), and check for a slot
        // handed over before we started listening, which is already a lease.
        let already: bool = sqlx::query_scalar("SELECT queryflux_capacity_park($1, $2)")
            .bind(cluster_name)
            .bind(query_id)
            .fetch_one(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("wait_for_grant: {e}")))?;
        Ok(already || tokio::time::timeout(timeout, notified).await.is_ok())
    }

    async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
        if leases.is_empty() {
            return Ok(());
        }
        let ids: Vec<&str> = leases.iter().map(|(_, q)| q.as_str()).collect();
        sqlx::query("SELECT queryflux_capacity_release($1)")
            .bind(&ids)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("release_many: {e}")))?;
        Ok(())
    }

    async fn heartbeat(&self, instance_id: &str) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE cluster_capacity_leases SET heartbeat_at = now() WHERE instance_id = $1",
        )
        .bind(instance_id)
        .execute(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("heartbeat: {e}")))?;
        Ok(result.rows_affected())
    }

    async fn expire_stale(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let result = sqlx::query("DELETE FROM cluster_capacity_leases WHERE heartbeat_at < $1")
            .bind(cutoff)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("expire_stale: {e}")))?;
        // Leases handed to waiters that never came back for them, and waiters that
        // stopped retrying long ago.
        // Through the release function, which takes the clusters' locks first.
        let unclaimed: i64 = sqlx::query_scalar(
            r#"
            SELECT queryflux_capacity_release(ARRAY(
                SELECT query_id FROM cluster_capacity_leases
                WHERE NOT claimed AND acquired_at < now() - interval '10 seconds'
            ))
            "#,
        )
        .fetch_one(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("expire unclaimed grants: {e}")))?;
        sqlx::query(
            "DELETE FROM capacity_waiters WHERE refreshed_at < now() - interval '60 seconds'",
        )
        .execute(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("expire waiters: {e}")))?;
        if unclaimed > 0 {
            tracing::info!(
                leases = unclaimed,
                "Expired capacity handed to queries that stopped waiting"
            );
        }
        // This sweep runs on one replica at a time, every couple of minutes: also put
        // right any lease counter that no longer matches its leases.
        if let Err(e) = self.recount_drifted_counters().await {
            tracing::warn!("Capacity lease counter check failed: {e}");
        }
        Ok(result.rows_affected())
    }

    async fn active_count(&self, cluster_name: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(
                (SELECT running FROM cluster_capacity_counters WHERE cluster_name = $1),
                0
            )
            "#,
        )
        .bind(cluster_name)
        .fetch_one(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("active_count: {e}")))?;
        Ok(count.max(0) as u64)
    }

    async fn publish_running_count(&self, cluster_name: &str, count: u64) -> Result<()> {
        let running = count.min(i64::MAX as u64) as i64;
        sqlx::query(
            r#"
            INSERT INTO cluster_capacity_counters (cluster_name, running)
            VALUES ($1, $2)
            ON CONFLICT (cluster_name) DO UPDATE
                SET running = EXCLUDED.running
            "#,
        )
        .bind(cluster_name)
        .bind(running)
        .execute(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("publish_running_count: {e}")))?;
        Ok(())
    }

    async fn active_lease_count(&self, cluster_name: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COALESCE((SELECT leased FROM cluster_capacity_counters WHERE cluster_name = $1), 0)",
        )
        .bind(cluster_name)
        .fetch_one(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("active_lease_count: {e}")))?;
        Ok(count.max(0) as u64)
    }

    async fn release_all_for_instance(&self, instance_id: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM cluster_capacity_leases WHERE instance_id = $1")
            .bind(instance_id)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("release_all_for_instance: {e}")))?;
        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// QueueCoordinator — Postgres implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl QueueCoordinator for PostgresStore {
    async fn try_claim(
        &self,
        query_id: &str,
        instance_id: &str,
        stale_before: DateTime<Utc>,
    ) -> Result<Option<QueuedQuery>> {
        // Atomically claim a queued query that is unclaimed, or whose claim is
        // stale (the claiming replica crashed mid-dispatch). The UPDATE's row
        // lock makes concurrent takeover attempts serialize; only one wins.
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            r#"
            UPDATE queued_queries
            SET claimed_by = $2, claimed_at = now()
            WHERE id = $1 AND (claimed_by IS NULL OR claimed_at < $3)
            RETURNING data
            "#,
        )
        .bind(query_id)
        .bind(instance_id)
        .bind(stale_before)
        .fetch_optional(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("try_claim: {e}")))?;

        match row {
            Some((data,)) => {
                let q: QueuedQuery = serde_json::from_value(data).map_err(|e| {
                    QueryFluxError::Persistence(format!("try_claim deserialize: {e}"))
                })?;
                Ok(Some(q))
            }
            None => Ok(None),
        }
    }

    async fn release_claim(&self, query_id: &str) -> Result<()> {
        sqlx::query("UPDATE queued_queries SET claimed_by = NULL, claimed_at = NULL WHERE id = $1")
            .bind(query_id)
            .execute(&self.coordination_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("release_claim: {e}")))?;
        Ok(())
    }

    async fn refresh_claim(&self, query_id: &str, instance_id: &str) -> Result<bool> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            UPDATE queued_queries
            SET claimed_at = now()
            WHERE id = $1 AND claimed_by = $2
            RETURNING id
            "#,
        )
        .bind(query_id)
        .bind(instance_id)
        .fetch_optional(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("refresh_claim: {e}")))?;
        Ok(row.is_some())
    }

    async fn list_unclaimed(&self, stale_before: DateTime<Utc>) -> Result<Vec<QueuedQuery>> {
        let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
            r#"
            SELECT data FROM queued_queries
            WHERE claimed_by IS NULL OR claimed_at < $1
            ORDER BY created_at
            "#,
        )
        .bind(stale_before)
        .fetch_all(&self.coordination_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_unclaimed: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for (data,) in rows {
            match serde_json::from_value::<QueuedQuery>(data) {
                Ok(q) => result.push(q),
                Err(e) => tracing::warn!("list_unclaimed: skipping malformed row: {e}"),
            }
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// CacheStore — cache entry metadata
// ---------------------------------------------------------------------------

#[async_trait]
impl CacheStore for PostgresStore {
    async fn cache_get_valid(&self, cache_key: &str) -> Result<Option<CacheEntryMeta>> {
        let row: Option<(
            String,
            String,
            DateTime<Utc>,
            DateTime<Utc>,
            Option<i64>,
            Option<i64>,
        )> = sqlx::query_as(
            r#"SELECT cache_key, group_name, created_at, expires_at, row_count, size_bytes
                   FROM cache_entries
                   WHERE cache_key = $1 AND expires_at > NOW()"#,
        )
        .bind(cache_key)
        .fetch_optional(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("cache_get_valid: {e}")))?;

        Ok(row.map(
            |(cache_key, group_name, created_at, expires_at, row_count, size_bytes)| {
                CacheEntryMeta {
                    cache_key,
                    group_name,
                    created_at,
                    expires_at,
                    row_count,
                    size_bytes,
                }
            },
        ))
    }

    async fn cache_upsert(&self, entry: &CacheEntryMeta) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO cache_entries (cache_key, group_name, created_at, expires_at, row_count, size_bytes)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (cache_key) DO UPDATE SET
                   group_name = EXCLUDED.group_name,
                   created_at = EXCLUDED.created_at,
                   expires_at = EXCLUDED.expires_at,
                   row_count  = EXCLUDED.row_count,
                   size_bytes = EXCLUDED.size_bytes"#,
        )
        .bind(&entry.cache_key)
        .bind(&entry.group_name)
        .bind(entry.created_at)
        .bind(entry.expires_at)
        .bind(entry.row_count)
        .bind(entry.size_bytes)
        .execute(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("cache_upsert: {e}")))?;
        Ok(())
    }

    async fn cache_delete_expired(&self) -> Result<Vec<CacheEntryRef>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "DELETE FROM cache_entries WHERE expires_at < NOW() RETURNING cache_key, group_name",
        )
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("cache_delete_expired: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|(cache_key, group_name)| CacheEntryRef {
                cache_key,
                group_name,
            })
            .collect())
    }

    async fn cache_delete_by_group(&self, group: &str) -> Result<Vec<CacheEntryRef>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "DELETE FROM cache_entries WHERE group_name = $1 RETURNING cache_key, group_name",
        )
        .bind(group)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("cache_delete_by_group: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|(cache_key, group_name)| CacheEntryRef {
                cache_key,
                group_name,
            })
            .collect())
    }

    async fn cache_delete_all(&self) -> Result<Vec<CacheEntryRef>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("DELETE FROM cache_entries RETURNING cache_key, group_name")
                .fetch_all(&self.admin_pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("cache_delete_all: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|(cache_key, group_name)| CacheEntryRef {
                cache_key,
                group_name,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Integration tests — require a running Postgres with DATABASE_URL set.
// Run with: cargo test -p queryflux-persistence -- --ignored postgres
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapacityStore, ConfigRevisionStore, Persistence, QueueCoordinator};
    use queryflux_core::config::PostgresPersistenceConfig;
    use queryflux_core::query::{ClusterGroupName, FrontendProtocol, ProxyQueryId, QueuedQuery};
    use queryflux_core::session::SessionContext;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn isolated_pool_sizes_from_config() {
        let cfg = PostgresPersistenceConfig {
            query_pool_size: Some(12),
            coordination_pool_size: Some(3),
            admin_pool_size: Some(5),
            ..Default::default()
        };
        assert_eq!(cfg.resolve_pool_sizes().unwrap(), (12, 3, 5));
    }

    async fn test_store() -> PostgresStore {
        let url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for Postgres integration tests");
        let store = PostgresStore::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    fn unique_id(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn make_queued(id: &str) -> QueuedQuery {
        QueuedQuery {
            id: ProxyQueryId(id.to_string()),
            sql: "SELECT 1".to_string(),
            session: SessionContext::default(),
            frontend_protocol: FrontendProtocol::TrinoHttp,
            cluster_group: ClusterGroupName("test".to_string()),
            creation_time: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            sequence: 0,
            submitted_by: String::new(),
        }
    }

    /// A cutoff in the past: fresh claims are NOT stale relative to this.
    fn no_stale() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() - chrono::Duration::seconds(60)
    }

    /// Serializes the CapacityStore tests: `expire_stale` and `heartbeat` operate on
    /// the whole `cluster_capacity_leases` table, so tests running in parallel would
    /// delete or renew each other's leases.
    static CAPACITY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // -- ConfigSnapshotStore ------------------------------------------------

    async fn seed_snapshot_pair(store: &PostgresStore, cluster: &str, group: &str) {
        let upsert: crate::cluster_config::UpsertClusterConfig =
            serde_json::from_value(serde_json::json!({
                "engineKey": "duckDb",
                "maxRunningQueries": 1,
                "config": {}
            }))
            .unwrap();
        store.upsert_cluster_config(cluster, &upsert).await.unwrap();
        let g: crate::cluster_config::UpsertClusterGroupConfig =
            serde_json::from_value(serde_json::json!({
                "members": [cluster],
                "maxRunningQueries": 1
            }))
            .unwrap();
        store.upsert_group_config(group, &g).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_config_snapshot_rows_include_written_config() {
        use crate::ConfigSnapshotStore;
        let store = test_store().await;
        let cluster = unique_id("snap-c");
        let group = unique_id("snap-g");
        seed_snapshot_pair(&store, &cluster, &group).await;

        let rows = store.load_config_snapshot_rows().await.unwrap();
        assert!(rows.clusters.iter().any(|c| c.name == cluster));
        assert!(rows.groups.iter().any(|g| g.name == group));

        store.delete_group_config(&group).await.unwrap();
        store.delete_cluster_config(&cluster).await.unwrap();
    }

    /// A cluster's limit and its group's limit change together in one commit that lands
    /// between the snapshot's cluster read and its group read. The group read must still
    /// see the old value: reads in the snapshot transaction cannot straddle a commit.
    #[tokio::test]
    #[ignore]
    async fn pg_config_snapshot_reads_do_not_straddle_a_commit() {
        let store = test_store().await;
        let cluster = unique_id("snap-mix-c");
        let group = unique_id("snap-mix-g");
        seed_snapshot_pair(&store, &cluster, &group).await;

        let mut tx = store.begin_snapshot_tx().await.unwrap();
        let clusters = PostgresStore::list_cluster_configs_in(&mut tx)
            .await
            .unwrap();
        let c = clusters.iter().find(|c| c.name == cluster).unwrap();
        assert_eq!(c.max_running_queries, Some(1));

        // A separate connection, like an admin edit from another replica.
        let url = std::env::var("DATABASE_URL").unwrap();
        let mut other = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .unwrap();
        let mut writer = sqlx::Connection::begin(&mut other).await.unwrap();
        sqlx::query("UPDATE cluster_configs SET max_running_queries = 2 WHERE name = $1")
            .bind(&cluster)
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::query("UPDATE cluster_group_configs SET max_running_queries = 2 WHERE name = $1")
            .bind(&group)
            .execute(&mut *writer)
            .await
            .unwrap();
        writer.commit().await.unwrap();

        let groups = PostgresStore::list_group_configs_in(&mut tx).await.unwrap();
        let g = groups.iter().find(|g| g.name == group).unwrap();
        assert_eq!(
            g.max_running_queries, 1,
            "snapshot saw a commit made after it began"
        );
        tx.commit().await.unwrap();

        store.delete_group_config(&group).await.unwrap();
        store.delete_cluster_config(&cluster).await.unwrap();
    }

    // -- WorkerRegistryStore ------------------------------------------------

    #[tokio::test]
    #[ignore]
    async fn pg_worker_registry_upserts_and_lists_recent_workers() {
        use crate::WorkerRegistryStore;
        let store = test_store().await;
        let name = unique_id("worker");
        let mut w = crate::WorkerRecord {
            name: name.clone(),
            version: "0.4.0".into(),
            labels: [("app".to_string(), "checkout".to_string())].into(),
            coordination_mode: "local".into(),
            downloaded_revision: Some("a".into()),
            active_revision: Some("a".into()),
            last_error: None,
            last_seen: chrono::Utc::now(),
            registered_at: None,
            status: crate::WorkerStatus {
                config_source: Some("peer".into()),
                stale: true,
                consecutive_failures: 2,
                frontends: vec!["trinoHttp:8080".into()],
                history: Some(crate::WorkerHistoryStatus {
                    sent: 7,
                    ..Default::default()
                }),
                ..Default::default()
            },
            kind: "worker".into(),
            parent: Some("dc1".into()),
        };
        store.upsert_worker(&w).await.unwrap();
        w.active_revision = Some("b".into());
        store.upsert_worker(&w).await.unwrap();

        let since = chrono::Utc::now() - chrono::Duration::seconds(60);
        let found: Vec<_> = store
            .list_workers(since)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.name == name)
            .collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].active_revision.as_deref(), Some("b"));
        assert_eq!(found[0].status, w.status);
        assert_eq!(found[0].parent.as_deref(), Some("dc1"));
        assert!(found[0].registered_at.is_some());
        assert_eq!(
            found[0].labels.get("app").map(String::as_str),
            Some("checkout")
        );
        let future = chrono::Utc::now() + chrono::Duration::seconds(60);
        assert!(store
            .list_workers(future)
            .await
            .unwrap()
            .iter()
            .all(|r| r.name != name));
    }

    // -- ConfigRevisionStore ------------------------------------------------

    #[tokio::test]
    #[ignore]
    async fn pg_config_revision_bump_and_read() {
        let store = test_store().await;
        let before = store.current_revision().await.unwrap();
        let bumped = store.bump_revision().await.unwrap();
        assert!(
            bumped > before,
            "bumped ({bumped}) should be > before ({before})"
        );
        let after = store.current_revision().await.unwrap();
        assert_eq!(after, bumped);
    }

    #[tokio::test]
    #[ignore]
    async fn pg_config_revision_subscribe() {
        let store = test_store().await;
        let rx = store.subscribe_revisions().await.unwrap();
        assert!(
            rx.is_some(),
            "Postgres should return a notification receiver"
        );
    }

    // -- CapacityStore ------------------------------------------------------

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_acquire_release_cycle() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let qid = unique_id("cap");

        let acquired = store
            .try_acquire("test-cluster", 1_000_000, "inst-1", &qid)
            .await
            .unwrap();
        assert!(acquired);

        let count = store.active_lease_count("test-cluster").await.unwrap();
        assert!(count >= 1);

        store.release("test-cluster", &qid).await.unwrap();

        assert_eq!(store.active_lease_count("test-cluster").await.unwrap(), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_denies_at_caller_provided_limit() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("lim");

        assert!(store
            .try_acquire(&cluster, 2, "inst-1", &format!("{cluster}-q1"))
            .await
            .unwrap());
        assert!(store
            .try_acquire(&cluster, 2, "inst-1", &format!("{cluster}-q2"))
            .await
            .unwrap());
        assert!(
            !store
                .try_acquire(&cluster, 2, "inst-1", &format!("{cluster}-q3"))
                .await
                .unwrap(),
            "third acquire must be denied at limit 2"
        );

        store
            .release(&cluster, &format!("{cluster}-q1"))
            .await
            .unwrap();
        assert!(
            store
                .try_acquire(&cluster, 2, "inst-1", &format!("{cluster}-q3"))
                .await
                .unwrap(),
            "slot freed by release must be grantable again"
        );

        store
            .release(&cluster, &format!("{cluster}-q2"))
            .await
            .unwrap();
        store
            .release(&cluster, &format!("{cluster}-q3"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_expire_stale_cleans_old_leases() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let qid = unique_id("exp");

        store
            .try_acquire("test-cluster-exp", 1_000_000, "inst-stale", &qid)
            .await
            .unwrap();

        let far_future = chrono::Utc::now() + chrono::Duration::hours(1);
        let expired = store.expire_stale(far_future).await.unwrap();
        assert!(
            expired >= 1,
            "should expire at least the lease we just created"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_concurrent_acquires_respect_limit() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = std::sync::Arc::new(test_store().await);
        let cluster = unique_id("limited");
        const LIMIT: u64 = 5;

        // 4x the limit racing for slots: the advisory lock must serialize the
        // count-then-insert so exactly LIMIT acquires win.
        let mut handles = Vec::new();
        for i in 0..(LIMIT * 4) {
            let store = store.clone();
            let cluster = cluster.clone();
            handles.push(tokio::spawn(async move {
                store
                    .try_acquire(
                        &cluster,
                        LIMIT,
                        &format!("inst-{}", i % 3),
                        &format!("{cluster}-q{i}"),
                    )
                    .await
                    .unwrap()
            }));
        }
        let mut granted = 0;
        for h in handles {
            if h.await.unwrap() {
                granted += 1;
            }
        }
        assert_eq!(
            granted, LIMIT,
            "exactly the limit must be granted under contention"
        );
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), LIMIT);

        for i in 0..(LIMIT * 4) {
            let _ = store.release(&cluster, &format!("{cluster}-q{i}")).await;
        }
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_leases_stay_consistent() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("cnt");

        for i in 1..=3 {
            assert!(store
                .try_acquire(&cluster, 10, "inst-1", &format!("{cluster}-q{i}"))
                .await
                .unwrap());
        }
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 3);

        store
            .release(&cluster, &format!("{cluster}-q2"))
            .await
            .unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 2);

        // Double release is a no-op.
        store
            .release(&cluster, &format!("{cluster}-q2"))
            .await
            .unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 2);

        // Expiring every lease clears admission slots.
        let far_future = chrono::Utc::now() + chrono::Duration::hours(1);
        store.expire_stale(far_future).await.unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);
    }

    async fn schema_store(schema: &str) -> PostgresStore {
        let url = std::env::var("DATABASE_URL").unwrap();
        let conn = queryflux_core::config::PostgresPersistenceConfig {
            url: Some(url),
            schema: Some(schema.to_string()),
            ..Default::default()
        };
        let store = PostgresStore::connect_from_config(&conn.connection_url().unwrap(), &conn)
            .await
            .unwrap();
        store.migrate().await.unwrap();
        store.verify_schema().await.unwrap();
        store
    }

    /// Two deployments pinned to two schemas of one database: each migrates into its own
    /// schema with its own ledger, and their locks and channels never collide with each
    /// other or with `public`, which keeps its names.
    #[tokio::test]
    #[ignore]
    async fn pg_pinned_schemas_share_a_database_without_interfering() {
        let suffix = unique_id("").trim_start_matches('-').to_string();
        let (a, b) = (format!("qf_a_{suffix}"), format!("qf_b_{suffix}"));
        let (sa, sb) = (schema_store(&a).await, schema_store(&b).await);
        let public = test_store().await;

        let tables_in = |schema: String| {
            let pool = public.admin_pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 \
                     AND table_name IN ('query_records', 'refinery_schema_history')",
                )
                .bind(schema)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        assert_eq!(tables_in(a.clone()).await, 2);
        assert_eq!(tables_in(b.clone()).await, 2);
        // Migrating again is a no-op: the ledger is per schema.
        sa.migrate().await.unwrap();

        // Sweep locks: one holder per schema, and each schema's own holders still exclude
        // each other.
        let held_a = sa.try_sweep_lock("zombie-eviction").await.unwrap();
        assert!(held_a.is_some());
        let held_b = sb.try_sweep_lock("zombie-eviction").await.unwrap();
        assert!(held_b.is_some(), "schema b must not wait on schema a");
        assert!(sa
            .try_sweep_lock("zombie-eviction")
            .await
            .unwrap()
            .is_none());
        drop((held_a, held_b));

        // Channels and lock keys: plain in public, distinct per schema.
        let channel = |pool: PgPool| async move {
            sqlx::query_scalar::<_, String>("SELECT queryflux_channel('config_revision_changed')")
                .fetch_one(&pool)
                .await
                .unwrap()
        };
        let (ca, cb, cp) = (
            channel(sa.admin_pool.clone()).await,
            channel(sb.admin_pool.clone()).await,
            channel(public.admin_pool.clone()).await,
        );
        assert_eq!(cp, "config_revision_changed");
        assert_ne!(ca, cb);
        assert!(ca.len() < 64 && cb.len() < 64);
        let key = |pool: PgPool| async move {
            sqlx::query_scalar::<_, i64>("SELECT queryflux_lock_key('capacity:trino')")
                .fetch_one(&pool)
                .await
                .unwrap()
        };
        let public_key: i64 = sqlx::query_scalar("SELECT hashtext('capacity:trino')::bigint")
            .fetch_one(&public.admin_pool)
            .await
            .unwrap();
        assert_eq!(key(public.admin_pool.clone()).await, public_key);
        assert_ne!(
            key(sa.admin_pool.clone()).await,
            key(sb.admin_pool.clone()).await
        );

        // Capacity works inside a pinned schema.
        let cluster = unique_id("pinned");
        assert!(sa.try_acquire(&cluster, 1, "w1", "q1").await.unwrap());
        assert!(!sa.try_acquire(&cluster, 1, "w1", "q2").await.unwrap());
        assert!(
            sb.try_acquire(&cluster, 1, "w1", "q1").await.unwrap(),
            "separate tables"
        );

        for s in [&a, &b] {
            sqlx::query(&format!("DROP SCHEMA {s} CASCADE"))
                .execute(&public.admin_pool)
                .await
                .unwrap();
        }
    }

    /// Without migrations, a pinned schema that doesn't exist is refused at startup rather
    /// than silently resolving to another schema.
    #[tokio::test]
    #[ignore]
    async fn pg_a_missing_pinned_schema_is_refused() {
        let url = std::env::var("DATABASE_URL").unwrap();
        let conn = queryflux_core::config::PostgresPersistenceConfig {
            url: Some(url),
            schema: Some(format!(
                "qf_missing_{}",
                unique_id("").trim_start_matches('-')
            )),
            ..Default::default()
        };
        let store = PostgresStore::connect_from_config(&conn.connection_url().unwrap(), &conn)
            .await
            .unwrap();
        let err = store.verify_schema().await.unwrap_err().to_string();
        assert!(err.contains("does not exist"), "{err}");
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_publish_running_count() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("reconcile");

        assert_eq!(store.active_count(&cluster).await.unwrap(), 0);

        store.publish_running_count(&cluster, 42).await.unwrap();
        assert_eq!(store.active_count(&cluster).await.unwrap(), 42);

        store.publish_running_count(&cluster, 7).await.unwrap();
        assert_eq!(store.active_count(&cluster).await.unwrap(), 7);
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_heartbeat_protects_leases_from_expiry() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let inst = unique_id("inst-hb");
        let qid = unique_id("hb");

        store
            .try_acquire("test-cluster-hb", 1_000_000, &inst, &qid)
            .await
            .unwrap();

        // Renew, then expire with a cutoff just before the renewal: the lease
        // must survive because heartbeat_at was bumped past the cutoff.
        // The cutoff must come from the *database* clock — heartbeat_at is written
        // with the server's now(), and sub-second skew against the client clock
        // makes a client-side cutoff flaky. (Production is insensitive: the expiry
        // cutoff there has a 300s margin against a 60s heartbeat interval.)
        let cutoff: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT now()")
            .fetch_one(&store.admin_pool)
            .await
            .unwrap();
        let renewed = store.heartbeat(&inst).await.unwrap();
        assert!(renewed >= 1, "heartbeat should renew the lease we hold");

        store.expire_stale(cutoff).await.unwrap();
        let count = store.active_lease_count("test-cluster-hb").await.unwrap();
        assert!(count >= 1, "heartbeated lease must survive expiry");

        store.release("test-cluster-hb", &qid).await.unwrap();
    }

    /// Two logical replicas (distinct `instance_id`s) must not exceed the fleet
    /// `maxRunningQueries` limit — the epic distributed-admission guarantee.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_two_replicas_cannot_exceed_max_running() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("fleet");
        const LIMIT: u64 = 2;
        let q1 = format!("{cluster}-a1");
        let q2 = format!("{cluster}-b1");
        let q3 = format!("{cluster}-a2");
        let q4 = format!("{cluster}-b2");

        assert!(
            store
                .try_acquire(&cluster, LIMIT, "replica-a", &q1)
                .await
                .unwrap(),
            "replica-a should acquire first slot"
        );
        assert!(
            store
                .try_acquire(&cluster, LIMIT, "replica-b", &q2)
                .await
                .unwrap(),
            "replica-b should acquire second slot"
        );
        assert!(
            !store
                .try_acquire(&cluster, LIMIT, "replica-a", &q3)
                .await
                .unwrap(),
            "replica-a must be denied once fleet is at limit"
        );
        assert!(
            !store
                .try_acquire(&cluster, LIMIT, "replica-b", &q4)
                .await
                .unwrap(),
            "replica-b must be denied once fleet is at limit"
        );
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), LIMIT);

        store.release(&cluster, &q1).await.unwrap();
        store.release(&cluster, &q2).await.unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);
    }

    /// Crash path: holder never calls `release`. After `expire_stale`, a survivor
    /// replica can acquire the slot again.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_crash_without_release_recovered_by_expire_stale() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("crash");
        const LIMIT: u64 = 1;
        let held = format!("{cluster}-held");
        let next = format!("{cluster}-next");

        assert!(store
            .try_acquire(&cluster, LIMIT, "crashed-inst", &held)
            .await
            .unwrap());
        assert!(
            !store
                .try_acquire(&cluster, LIMIT, "survivor", &next)
                .await
                .unwrap(),
            "survivor must wait while crashed lease still counts"
        );

        // Treat every lease as stale (same approach as expire_stale_cleans_old_leases).
        let far_future = chrono::Utc::now() + chrono::Duration::hours(1);
        let expired = store.expire_stale(far_future).await.unwrap();
        assert!(
            expired >= 1,
            "expire_stale must reclaim the crashed instance lease"
        );
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);

        assert!(
            store
                .try_acquire(&cluster, LIMIT, "survivor", &next)
                .await
                .unwrap(),
            "survivor must acquire after lease expiry reclaim"
        );
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 1);

        store.release(&cluster, &next).await.unwrap();
    }

    /// Graceful shutdown path: `release_all_for_instance` frees every lease owned
    /// by the departing replica so other replicas can admit.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_release_all_for_instance_frees_slots() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("drain");
        const LIMIT: u64 = 2;
        let departing = unique_id("departing");
        let q1 = format!("{cluster}-d1");
        let q2 = format!("{cluster}-d2");
        let q3 = format!("{cluster}-s1");

        assert!(store
            .try_acquire(&cluster, LIMIT, &departing, &q1)
            .await
            .unwrap());
        assert!(store
            .try_acquire(&cluster, LIMIT, &departing, &q2)
            .await
            .unwrap());
        assert!(!store
            .try_acquire(&cluster, LIMIT, "survivor", &q3)
            .await
            .unwrap());

        let freed = store.release_all_for_instance(&departing).await.unwrap();
        assert_eq!(freed, 2);
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);

        assert!(store
            .try_acquire(&cluster, LIMIT, "survivor", &q3)
            .await
            .unwrap());
        store.release(&cluster, &q3).await.unwrap();
    }

    /// More expired rows than one batch: all of them go, newer rows stay.
    #[tokio::test]
    #[ignore]
    async fn pg_purge_deletes_expired_history_in_batches() {
        use crate::QueryHistoryStore;
        let store = test_store().await;
        let tag = unique_id("purge");
        let insert = |n: i64, age: &str| {
            format!(
                "INSERT INTO query_records (proxy_query_id, cluster_group, cluster_name, engine_type, \
                 frontend_protocol, source_dialect, target_dialect, status, created_at) \
                 SELECT '{tag}-{age}-' || g, '{tag}', 'c', 'duckDb', 'trinoHttp', 'trino', 'duckdb', \
                 'success', now() - interval '{age}' FROM generate_series(1, {n}) g"
            )
        };
        let expired = PURGE_BATCH_ROWS * 2 + 7;
        sqlx::query(&insert(expired, "40 days"))
            .execute(&store.admin_pool)
            .await
            .unwrap();
        sqlx::query(&insert(5, "1 day"))
            .execute(&store.admin_pool)
            .await
            .unwrap();

        let cutoff = chrono::Utc::now() - chrono::Duration::days(30);
        let deleted = store.purge_old_query_records(cutoff).await.unwrap();
        assert!(deleted >= expired as u64, "deleted {deleted}");
        let left: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM query_records WHERE cluster_group = $1")
                .bind(&tag)
                .fetch_one(&store.admin_pool)
                .await
                .unwrap();
        assert_eq!(left, 5);
        sqlx::query("DELETE FROM query_records WHERE cluster_group = $1")
            .bind(&tag)
            .execute(&store.admin_pool)
            .await
            .unwrap();
    }

    async fn lease_rows(store: &PostgresStore, cluster: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM cluster_capacity_leases WHERE cluster_name = $1")
            .bind(cluster)
            .fetch_one(&store.coordination_pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_unlimited_takes_no_lease() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("unlimited");
        assert!(store
            .try_acquire(&cluster, u64::MAX, "inst-1", &format!("{cluster}-q"))
            .await
            .unwrap());
        assert_eq!(lease_rows(&store, &cluster).await, 0);
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);
        // Releasing a lease that was never written is a no-op.
        store
            .release(&cluster, &format!("{cluster}-q"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_release_many_frees_every_slot_in_one_call() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let a = unique_id("batch-a");
        let b = unique_id("batch-b");
        let mut held = Vec::new();
        for (cluster, n) in [(&a, 3), (&b, 2)] {
            for i in 0..n {
                let q = format!("{cluster}-q{i}");
                assert!(store.try_acquire(cluster, 3, "inst-1", &q).await.unwrap());
                held.push((cluster.clone(), q));
            }
        }
        assert!(!store
            .try_acquire(&a, 3, "inst-1", &format!("{a}-full"))
            .await
            .unwrap());

        // Includes an id with no lease: ignored.
        held.push((a.clone(), format!("{a}-never")));
        store.release_many(&held).await.unwrap();
        assert_eq!(store.active_lease_count(&a).await.unwrap(), 0);
        assert_eq!(store.active_lease_count(&b).await.unwrap(), 0);
        assert!(store
            .try_acquire(&a, 3, "inst-1", &format!("{a}-again"))
            .await
            .unwrap());
        store
            .release_many(&[
                (a.clone(), format!("{a}-again")),
                (a.clone(), format!("{a}-full")),
            ])
            .await
            .unwrap();
        assert_eq!(store.active_lease_count(&a).await.unwrap(), 0);
    }

    /// A replica still on the previous version writes lease rows directly (advisory
    /// lock, COUNT(*), INSERT; DELETE on release). The triggers must count those too, or
    /// a mixed fleet would admit past the limit during a rolling upgrade.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_counter_includes_leases_written_by_older_replicas() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("mixed");
        const LIMIT: u64 = 2;

        sqlx::query(
            "INSERT INTO cluster_capacity_leases (query_id, cluster_name, instance_id) VALUES ($1, $2, 'old-replica')",
        )
        .bind(format!("{cluster}-old"))
        .bind(&cluster)
        .execute(&store.coordination_pool)
        .await
        .unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 1);

        assert!(store
            .try_acquire(&cluster, LIMIT, "new", &format!("{cluster}-new1"))
            .await
            .unwrap());
        assert!(
            !store
                .try_acquire(&cluster, LIMIT, "new", &format!("{cluster}-new2"))
                .await
                .unwrap(),
            "the old replica's lease counts against the limit"
        );

        sqlx::query("DELETE FROM cluster_capacity_leases WHERE query_id = $1")
            .bind(format!("{cluster}-old"))
            .execute(&store.coordination_pool)
            .await
            .unwrap();
        assert!(store
            .try_acquire(&cluster, LIMIT, "new", &format!("{cluster}-new2"))
            .await
            .unwrap());
        store
            .release_many(&[
                (cluster.clone(), format!("{cluster}-new1")),
                (cluster.clone(), format!("{cluster}-new2")),
            ])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_expiry_sweep_corrects_a_drifted_counter() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let cluster = unique_id("drift");
        let q = format!("{cluster}-q");
        assert!(store.try_acquire(&cluster, 5, "inst-1", &q).await.unwrap());

        // Drift both ways: a counter too high blocks admission, one too low over-admits.
        sqlx::query("UPDATE cluster_capacity_counters SET leased = 5 WHERE cluster_name = $1")
            .bind(&cluster)
            .execute(&store.coordination_pool)
            .await
            .unwrap();
        assert!(!store
            .try_acquire(&cluster, 5, "inst-1", &format!("{cluster}-blocked"))
            .await
            .unwrap());

        // A cutoff in the past expires nothing, but the sweep still recounts.
        let past = chrono::Utc::now() - chrono::Duration::hours(1);
        store.expire_stale(past).await.unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 1);

        store.release(&cluster, &q).await.unwrap();
        assert_eq!(store.active_lease_count(&cluster).await.unwrap(), 0);
    }

    /// Batches of releases spanning the same clusters in different orders, at the same
    /// time: the triggers lock counter rows in name order, so none deadlocks, and the
    /// counters end at zero.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_concurrent_release_batches_do_not_deadlock() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = std::sync::Arc::new(test_store().await);
        let clusters: Vec<String> = (0..4).map(|i| unique_id(&format!("dl{i}"))).collect();
        let mut batches: Vec<Vec<(String, String)>> = Vec::new();
        for w in 0..8 {
            let mut batch = Vec::new();
            for c in &clusters {
                let q = format!("{c}-w{w}");
                assert!(store.try_acquire(c, 1000, "inst-1", &q).await.unwrap());
                batch.push((c.clone(), q));
            }
            if w % 2 == 1 {
                batch.reverse();
            }
            batches.push(batch);
        }
        let handles: Vec<_> = batches
            .into_iter()
            .map(|b| {
                let store = store.clone();
                tokio::spawn(async move { store.release_many(&b).await })
            })
            .collect();
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for c in &clusters {
            assert_eq!(store.active_lease_count(c).await.unwrap(), 0);
            assert_eq!(lease_rows(&store, c).await, 0);
        }
    }

    fn demand(
        cluster: &str,
        running: u64,
        waiting: u64,
        seen: u64,
        max: u64,
    ) -> crate::quota::DemandReport {
        crate::quota::DemandReport {
            cluster: cluster.into(),
            running,
            waiting,
            seen_seq: seen,
            max_running_queries: max,
        }
    }

    async fn grant(store: &PostgresStore, inst: &str, r: crate::quota::DemandReport) -> (u64, u64) {
        use crate::quota::QuotaStore;
        let resp = store.report_demand(inst, &[r]).await.unwrap().unwrap();
        (resp.grants[0].quota, resp.grants[0].seq)
    }

    async fn allocate_now(store: &PostgresStore, reserve: f64) {
        use crate::quota::QuotaAllocator;
        let policy = crate::quota::AllocatorPolicy {
            reserve_fraction: reserve,
            ..Default::default()
        };
        store
            .run_allocation(&policy, &crate::in_memory::InMemoryPersistence::new())
            .await
            .unwrap();
    }

    async fn quota_committed(store: &PostgresStore, cluster: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COALESCE((SELECT quota_committed FROM cluster_capacity_counters WHERE cluster_name = $1), 0)",
        )
        .bind(cluster)
        .fetch_one(&store.coordination_pool)
        .await
        .unwrap()
    }

    /// Two holders report demand, the allocator splits the limit, per-query leases get
    /// only what the quotas leave, and a holder's confirmation lowers its ceiling.
    #[tokio::test]
    #[ignore]
    async fn pg_quota_allocation_shares_the_limit_with_leases() {
        use crate::quota::QuotaStore;
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("quota");
        let (a, b) = (unique_id("qa"), unique_id("qb"));

        // First reports: nothing granted yet.
        assert_eq!(grant(&store, &a, demand(&c, 0, 30, 0, 100)).await, (0, 0));
        assert_eq!(grant(&store, &b, demand(&c, 0, 30, 0, 100)).await, (0, 0));
        allocate_now(&store, 0.2).await;

        let (qa, sa) = grant(&store, &a, demand(&c, 0, 30, 0, 100)).await;
        let (qb, _) = grant(&store, &b, demand(&c, 0, 30, 0, 100)).await;
        assert_eq!(
            (qa, qb),
            (37, 37),
            "demand fits: each gets its demand plus a quarter as headroom"
        );
        assert_eq!(sa, 1);
        assert_eq!(quota_committed(&store, &c).await, 74);

        // Per-query leases get the 26 the quotas leave.
        let mut granted = 0;
        for i in 0..50 {
            if store
                .try_acquire(&c, 100, "lease-holder", &format!("{c}-l{i}"))
                .await
                .unwrap()
            {
                granted += 1;
            }
        }
        assert_eq!(granted, 26);
        store
            .release_many(
                &(0..50)
                    .map(|i| (c.clone(), format!("{c}-l{i}")))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        // b's demand drops to 5: its grant drops at once, but its ceiling (37) stays
        // until it confirms, so a's increase waits.
        grant(&store, &b, demand(&c, 2, 3, 1, 100)).await;
        grant(&store, &a, demand(&c, 30, 60, 1, 100)).await;
        allocate_now(&store, 0.2).await;
        let (qb2, sb2) = grant(&store, &b, demand(&c, 2, 3, 1, 100)).await;
        assert!(qb2 < 37);
        assert_eq!(sb2, 2);
        let committed = quota_committed(&store, &c).await;
        assert!(committed <= 100, "committed {committed}");

        // b confirms its new grant; a can now grow into what b gave up.
        grant(&store, &b, demand(&c, 2, 3, sb2, 100)).await;
        allocate_now(&store, 0.2).await;
        let (qa2, _) = grant(&store, &a, demand(&c, 30, 60, 1, 100)).await;
        assert!(qa2 > qa, "a grew from {qa} to {qa2}");
        assert!(quota_committed(&store, &c).await <= 100);

        // Holders leaving free everything.
        store.release_holder(&a).await.unwrap();
        store.release_holder(&b).await.unwrap();
        allocate_now(&store, 0.2).await;
        assert_eq!(quota_committed(&store, &c).await, 0);
    }

    #[tokio::test]
    #[ignore]
    async fn pg_quota_holder_that_goes_silent_loses_its_grant() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("silent");
        let h = unique_id("qh");
        grant(&store, &h, demand(&c, 0, 10, 0, 50)).await;
        allocate_now(&store, 0.0).await;
        let (q, s) = grant(&store, &h, demand(&c, 4, 6, 0, 50)).await;
        assert!(q > 0);
        grant(&store, &h, demand(&c, 4, 6, s, 50)).await;

        // Silent past its lease and margin (15 + 5 s).
        sqlx::query("UPDATE capacity_holders SET reported_at = now() - interval '30 seconds' WHERE instance_id = $1")
            .bind(&h)
            .execute(&store.coordination_pool)
            .await
            .unwrap();
        allocate_now(&store, 0.0).await;
        assert_eq!(
            quota_committed(&store, &c).await,
            4,
            "only its running queries count"
        );

        // It comes back: no stale grant is handed back.
        let (q2, _) = grant(&store, &h, demand(&c, 4, 6, s, 50)).await;
        assert_eq!(q2, 0);
    }

    /// Wait in line for a hand-over without blocking, as dispatch does after a denial.
    async fn park(store: &PostgresStore, cluster: &str, query_id: &str) -> bool {
        store
            .wait_for_grant(cluster, query_id, std::time::Duration::ZERO)
            .await
            .unwrap()
    }

    async fn waiter_rows(store: &PostgresStore, cluster: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM capacity_waiters WHERE cluster_name = $1")
            .bind(cluster)
            .fetch_one(&store.coordination_pool)
            .await
            .unwrap()
    }

    /// A release hands the slot to the oldest waiter, not to whoever asks next.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_release_hands_the_slot_to_the_oldest_waiter() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("fifo");
        let (q1, q2, q3) = (format!("{c}-1"), format!("{c}-2"), format!("{c}-3"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(!store.try_acquire(&c, 1, "i", &q3).await.unwrap());
        park(&store, &c, &q3).await;
        assert_eq!(waiter_rows(&store, &c).await, 2);

        store.release(&c, &q1).await.unwrap();
        assert!(
            !store.try_acquire(&c, 1, "i", &q3).await.unwrap(),
            "q3 came after q2"
        );
        assert!(
            store.try_acquire(&c, 1, "i", &q2).await.unwrap(),
            "q2 claims its slot"
        );
        assert_eq!(store.active_lease_count(&c).await.unwrap(), 1);
        // Claiming twice is still one lease.
        assert!(store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        assert_eq!(store.active_lease_count(&c).await.unwrap(), 1);

        store.release(&c, &q2).await.unwrap();
        assert!(store.try_acquire(&c, 1, "i", &q3).await.unwrap());
        store.release(&c, &q3).await.unwrap();
        assert_eq!(waiter_rows(&store, &c).await, 0);
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_wait_for_grant_wakes_on_release() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = std::sync::Arc::new(test_store().await);
        let c = unique_id("wake");
        let (q1, q2) = (format!("{c}-1"), format!("{c}-2"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;

        let waiter = {
            let (store, c, q2) = (store.clone(), c.clone(), q2.clone());
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                let granted = store
                    .wait_for_grant(&c, &q2, std::time::Duration::from_secs(10))
                    .await
                    .unwrap();
                (granted, start.elapsed())
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        store.release(&c, &q1).await.unwrap();
        let (granted, waited) = waiter.await.unwrap();
        assert!(granted);
        assert!(
            waited < std::time::Duration::from_secs(3),
            "woke after {waited:?}"
        );
        assert!(store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        store.release(&c, &q2).await.unwrap();

        // Nothing frees: the wait times out.
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;
        assert!(!store
            .wait_for_grant(&c, &q2, std::time::Duration::from_millis(200))
            .await
            .unwrap());
        store
            .release_many(&[(c.clone(), q1), (c.clone(), q2)])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_newcomers_do_not_jump_the_line_but_stale_waiters_stop_counting() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("line");
        let (q1, q2, q3) = (format!("{c}-1"), format!("{c}-2"), format!("{c}-3"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;
        // The limit goes up to 2: one slot is free, but q2 waited first.
        assert!(!store.try_acquire(&c, 2, "i", &q3).await.unwrap());
        park(&store, &c, &q3).await;
        assert!(store.try_acquire(&c, 2, "i", &q2).await.unwrap());
        store.release(&c, &q2).await.unwrap();

        // A waiter that stopped retrying no longer holds anyone up.
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;
        sqlx::query("UPDATE capacity_waiters SET refreshed_at = now() - interval '10 seconds' WHERE query_id = $1")
            .bind(&q2)
            .execute(&store.coordination_pool)
            .await
            .unwrap();
        assert!(store.try_acquire(&c, 2, "i", &q3).await.unwrap());
        store
            .release_many(&[(c.clone(), q1), (c.clone(), q2), (c.clone(), q3)])
            .await
            .unwrap();
    }

    /// A client that releases its query right after a denial (0.5.0 and earlier) never
    /// parks, so no slot is handed to it to be given straight back.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_queries_that_do_not_wait_are_not_handed_slots() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("noPark");
        let (q1, q2, q3) = (format!("{c}-1"), format!("{c}-2"), format!("{c}-3"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "old", &q2).await.unwrap());
        store.release(&c, &q1).await.unwrap();
        assert_eq!(
            store.active_lease_count(&c).await.unwrap(),
            0,
            "nothing handed to q2"
        );
        // q2's row doesn't hold up a newcomer either.
        assert!(store.try_acquire(&c, 1, "i", &q3).await.unwrap());
        store
            .release_many(&[(c.clone(), q2), (c.clone(), q3)])
            .await
            .unwrap();
    }

    /// A query leaves the line while another release is handing it a slot: its release
    /// must remove that slot too, not leave it held by nobody.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_release_during_a_hand_over_frees_the_slot() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = std::sync::Arc::new(test_store().await);
        let c = unique_id("race");
        let (h, w) = (format!("{c}-h"), format!("{c}-w"));
        assert!(store.try_acquire(&c, 1, "i", &h).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &w).await.unwrap());
        park(&store, &c, &w).await;

        // Release h in a transaction that stays open: its trigger hands the slot to w.
        let mut tx = store.coordination_pool.begin().await.unwrap();
        sqlx::query("SELECT queryflux_capacity_release(ARRAY[$1])")
            .bind(&h)
            .execute(&mut *tx)
            .await
            .unwrap();
        // Meanwhile w gives up; its release blocks on the waiter row until tx commits.
        let leave = {
            let (store, c, w) = (store.clone(), c.clone(), w.clone());
            tokio::spawn(async move { store.release(&c, &w).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        tx.commit().await.unwrap();
        leave.await.unwrap().unwrap();

        assert_eq!(store.active_lease_count(&c).await.unwrap(), 0);
        assert_eq!(
            lease_rows(&store, &c).await,
            0,
            "no unclaimed slot left behind"
        );
    }

    /// Many parked waiters leave the line at once while releases hand slots over, across
    /// two clusters: releases take the clusters' locks before the waiter rows, so none
    /// deadlocks (Postgres would fail one of them), and nothing is left behind.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_mass_departure_during_hand_overs_does_not_deadlock() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = std::sync::Arc::new(test_store().await);
        for round in 0..5 {
            let clusters = [
                unique_id(&format!("md{round}a")),
                unique_id(&format!("md{round}b")),
            ];
            let mut holders = Vec::new();
            let mut waiters = Vec::new();
            for c in &clusters {
                for i in 0..2 {
                    let q = format!("{c}-h{i}");
                    assert!(store.try_acquire(c, 2, "i", &q).await.unwrap());
                    holders.push((c.clone(), q));
                }
                for i in 0..20 {
                    let q = format!("{c}-w{i}");
                    assert!(!store.try_acquire(c, 2, "i", &q).await.unwrap());
                    park(&store, c, &q).await;
                    waiters.push((c.clone(), q));
                }
            }
            let mut tasks = Vec::new();
            for chunk in holders
                .into_iter()
                .chain(waiters)
                .collect::<Vec<_>>()
                .chunks(3)
            {
                let (store, chunk) = (store.clone(), chunk.to_vec());
                tasks.push(tokio::spawn(
                    async move { store.release_many(&chunk).await },
                ));
            }
            for t in tasks {
                t.await
                    .unwrap()
                    .expect("no release fails (a deadlock would)");
            }
            for c in &clusters {
                assert_eq!(lease_rows(&store, c).await, 0);
                assert_eq!(waiter_rows(&store, c).await, 0);
                assert_eq!(store.active_lease_count(c).await.unwrap(), 0);
            }
        }
    }

    /// Capacity that frees without a release (here the limit goes up) reaches a parked
    /// waiter when it renews its wait.
    #[tokio::test]
    #[ignore]
    async fn pg_capacity_renewing_a_wait_picks_up_free_capacity() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("renew");
        let (q1, q2) = (format!("{c}-1"), format!("{c}-2"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        assert!(!park(&store, &c, &q2).await);
        // Another query's acquire records the higher limit; q2 is ahead of it in line.
        assert!(!store
            .try_acquire(&c, 2, "i", &format!("{c}-3"))
            .await
            .unwrap());
        assert!(
            park(&store, &c, &q2).await,
            "handed the new slot on renewal"
        );
        assert!(store.try_acquire(&c, 2, "i", &q2).await.unwrap());
        store
            .release_many(&[
                (c.clone(), q1),
                (c.clone(), q2),
                (c.clone(), format!("{c}-3")),
            ])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_capacity_unclaimed_grants_are_swept() {
        let _guard = CAPACITY_TEST_LOCK.lock().await;
        let store = test_store().await;
        let c = unique_id("unclaimed");
        let (q1, q2) = (format!("{c}-1"), format!("{c}-2"));
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        assert!(!store.try_acquire(&c, 1, "i", &q2).await.unwrap());
        park(&store, &c, &q2).await;
        store.release(&c, &q1).await.unwrap();
        assert_eq!(
            store.active_lease_count(&c).await.unwrap(),
            1,
            "handed to q2"
        );

        // q2 never comes back for it.
        sqlx::query("UPDATE cluster_capacity_leases SET acquired_at = now() - interval '1 minute' WHERE query_id = $1")
            .bind(&q2)
            .execute(&store.coordination_pool)
            .await
            .unwrap();
        let past = chrono::Utc::now() - chrono::Duration::hours(1);
        store.expire_stale(past).await.unwrap();
        assert_eq!(store.active_lease_count(&c).await.unwrap(), 0);
        assert!(store.try_acquire(&c, 1, "i", &q1).await.unwrap());
        store.release(&c, &q1).await.unwrap();
    }

    // -- Admission fairness ---------------------------------------------------

    fn make_queued_at(
        id: &str,
        group: &str,
        creation_time: chrono::DateTime<chrono::Utc>,
        last_accessed: chrono::DateTime<chrono::Utc>,
    ) -> QueuedQuery {
        QueuedQuery {
            id: ProxyQueryId(id.to_string()),
            sql: "SELECT 1".to_string(),
            session: SessionContext::default(),
            frontend_protocol: FrontendProtocol::TrinoHttp,
            cluster_group: ClusterGroupName(group.to_string()),
            creation_time,
            last_accessed,
            sequence: 0,
            submitted_by: String::new(),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn pg_queued_creation_time_preserved_on_requeue() {
        let store = test_store().await;
        let qid = unique_id("ct");
        let group = unique_id("g");
        let original = chrono::Utc::now() - chrono::Duration::minutes(10);

        store
            .upsert_queued(make_queued_at(&qid, &group, original, chrono::Utc::now()))
            .await
            .unwrap();
        // Re-queue with a fresh creation_time, as persist_queued_query does.
        store
            .upsert_queued(make_queued_at(
                &qid,
                &group,
                chrono::Utc::now(),
                chrono::Utc::now(),
            ))
            .await
            .unwrap();

        let q = store
            .get_queued(&ProxyQueryId(qid.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            q.creation_time.timestamp_millis(),
            original.timestamp_millis(),
            "creation_time must survive re-queues"
        );
        store.delete_queued(&ProxyQueryId(qid)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_count_active_queued_before_orders_and_filters() {
        let store = test_store().await;
        let group = unique_id("fair");
        let now = chrono::Utc::now();
        let active_after = now - chrono::Duration::seconds(15);

        let q_old = unique_id("old");
        let q_new = unique_id("new");
        let q_dead = unique_id("dead");
        // Old and new are actively polling; dead stopped 10 minutes ago.
        store
            .upsert_queued(make_queued_at(
                &q_old,
                &group,
                now - chrono::Duration::minutes(5),
                now,
            ))
            .await
            .unwrap();
        store
            .upsert_queued(make_queued_at(
                &q_new,
                &group,
                now - chrono::Duration::seconds(2),
                now,
            ))
            .await
            .unwrap();
        store
            .upsert_queued(make_queued_at(
                &q_dead,
                &group,
                now - chrono::Duration::minutes(20),
                now - chrono::Duration::minutes(10),
            ))
            .await
            .unwrap();

        // A never-queued query sees both live waiters (dead client excluded).
        assert_eq!(
            store
                .count_active_queued_before(&group, None, active_after)
                .await
                .unwrap(),
            2
        );
        // The newer waiter sees only the older one ahead of it.
        assert_eq!(
            store
                .count_active_queued_before(
                    &group,
                    Some(now - chrono::Duration::seconds(2)),
                    active_after
                )
                .await
                .unwrap(),
            1
        );
        // The oldest live waiter has nobody ahead.
        assert_eq!(
            store
                .count_active_queued_before(
                    &group,
                    Some(now - chrono::Duration::minutes(5)),
                    active_after
                )
                .await
                .unwrap(),
            0
        );
        // Other groups are unaffected.
        assert_eq!(
            store
                .count_active_queued_before("some-other-group", None, active_after)
                .await
                .unwrap(),
            0
        );

        for q in [q_old, q_new, q_dead] {
            store.delete_queued(&ProxyQueryId(q)).await.unwrap();
        }
    }

    // -- SweepLock ------------------------------------------------------------

    #[tokio::test]
    #[ignore]
    async fn pg_sweep_lock_single_owner() {
        let store = test_store().await;
        let name = unique_id("sweep");

        let first = store.try_sweep_lock(&name).await.unwrap();
        assert!(first.is_some(), "first lock attempt should succeed");

        let second = store.try_sweep_lock(&name).await.unwrap();
        assert!(second.is_none(), "lock must not be granted twice");

        // A different sweep name does not contend.
        let other = store
            .try_sweep_lock(&format!("{name}-other"))
            .await
            .unwrap();
        assert!(other.is_some(), "different sweep names are independent");
        other.unwrap().release().await;

        first.unwrap().release().await;
        let reacquired = store.try_sweep_lock(&name).await.unwrap();
        assert!(reacquired.is_some(), "lock must be grantable after release");
        reacquired.unwrap().release().await;
    }

    // -- QueueCoordinator ---------------------------------------------------

    #[tokio::test]
    #[ignore]
    async fn pg_queue_claim_grants_once() {
        let store = test_store().await;
        let qid = unique_id("qc");

        store.upsert_queued(make_queued(&qid)).await.unwrap();

        let first = store.try_claim(&qid, "inst-A", no_stale()).await.unwrap();
        assert!(first.is_some(), "first claim should succeed");

        let second = store.try_claim(&qid, "inst-B", no_stale()).await.unwrap();
        assert!(
            second.is_none(),
            "second claim by different instance should fail"
        );

        store.delete_queued(&ProxyQueryId(qid)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_queue_refresh_claim_only_for_owner() {
        let store = test_store().await;
        let qid = unique_id("hb");
        store.upsert_queued(make_queued(&qid)).await.unwrap();
        assert!(store
            .try_claim(&qid, "inst-A", no_stale())
            .await
            .unwrap()
            .is_some());
        assert!(store.refresh_claim(&qid, "inst-A").await.unwrap());
        assert!(
            !store.refresh_claim(&qid, "inst-B").await.unwrap(),
            "other instance must not refresh the claim"
        );
        store.delete_queued(&ProxyQueryId(qid)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_queue_stale_claim_can_be_taken_over() {
        let store = test_store().await;
        let qid = unique_id("st");

        store.upsert_queued(make_queued(&qid)).await.unwrap();
        store
            .try_claim(&qid, "inst-dead", no_stale())
            .await
            .unwrap();

        // A cutoff in the future makes the fresh claim count as stale,
        // simulating a claim whose owner crashed long ago.
        let all_stale = chrono::Utc::now() + chrono::Duration::seconds(60);
        let taken = store.try_claim(&qid, "inst-B", all_stale).await.unwrap();
        assert!(taken.is_some(), "stale claim should be taken over");

        store.delete_queued(&ProxyQueryId(qid)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_queue_release_claim_allows_reclaim() {
        let store = test_store().await;
        let qid = unique_id("rc");

        store.upsert_queued(make_queued(&qid)).await.unwrap();
        store.try_claim(&qid, "inst-A", no_stale()).await.unwrap();

        store.release_claim(&qid).await.unwrap();

        let reclaimed = store.try_claim(&qid, "inst-B", no_stale()).await.unwrap();
        assert!(
            reclaimed.is_some(),
            "after release, another instance should claim"
        );

        store.delete_queued(&ProxyQueryId(qid)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_queue_list_unclaimed_excludes_claimed() {
        let store = test_store().await;
        let qid1 = unique_id("uc1");
        let qid2 = unique_id("uc2");

        store.upsert_queued(make_queued(&qid1)).await.unwrap();
        store.upsert_queued(make_queued(&qid2)).await.unwrap();

        store.try_claim(&qid1, "inst-A", no_stale()).await.unwrap();

        let unclaimed = store.list_unclaimed(no_stale()).await.unwrap();
        let unclaimed_ids: Vec<&str> = unclaimed.iter().map(|q| q.id.0.as_str()).collect();
        assert!(
            !unclaimed_ids.contains(&qid1.as_str()),
            "claimed query should not appear"
        );
        assert!(
            unclaimed_ids.contains(&qid2.as_str()),
            "unclaimed query should appear"
        );

        store.delete_queued(&ProxyQueryId(qid1)).await.unwrap();
        store.delete_queued(&ProxyQueryId(qid2)).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn pg_take_queued_returns_row_once() {
        let store = test_store().await;
        let qid = unique_id("take");

        store.upsert_queued(make_queued(&qid)).await.unwrap();
        assert!(store
            .take_queued(&ProxyQueryId(qid.clone()))
            .await
            .unwrap()
            .is_some());
        assert!(store
            .take_queued(&ProxyQueryId(qid))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn pg_record_query_keeps_first_terminal_row_per_proxy_id() {
        use crate::{MetricsStore, QueryHistoryStore, QueryRecord};
        use queryflux_core::query::{
            ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStatus, SqlDialect,
            TranslationOutcome, TranslationReason, TranslationStatus,
        };

        let store = test_store().await;
        let proxy_id = unique_id("dedup");
        // Unique group: other tests write history concurrently to the same database.
        let group = unique_id("dedup-g");
        let digest_hash = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let mut first = QueryRecord {
            proxy_query_id: proxy_id.clone(),
            backend_query_id: None,
            cluster_group: ClusterGroupName(group.clone()),
            cluster_name: ClusterName("c".into()),
            cluster_group_config_id: None,
            cluster_config_id: None,
            engine_type: EngineType::Undispatched,
            frontend_protocol: FrontendProtocol::TrinoHttp,
            source_dialect: SqlDialect::Trino,
            target_dialect: SqlDialect::Generic,
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: false,
            translation: Some(TranslationOutcome {
                status: TranslationStatus::Fallback,
                reason: Some(TranslationReason::NoSchema),
            }),
            translated_sql: None,
            user: None,
            catalog: None,
            database: None,
            sql_preview: "SELECT 1".into(),
            status: QueryStatus::Failed,
            routing_trace: None,
            queue_duration_ms: 100,
            execution_duration_ms: 0,
            rows_returned: None,
            error_message: Some("capacity wait timeout".into()),
            created_at: chrono::Utc::now(),
            engine_stats: None,
            query_tags: Default::default(),
            query_hash: None,
            query_parameterized_hash: Some(digest_hash),
            fast_hash: None,
            translated_query_hash: None,
            digest_text: Some("select 1".into()),
            translated_digest_text: None,
            agent_id: None,
            conversation_id: Some(proxy_id.clone()),
            step_index: None,
            tool_call_id: None,
            query_intent: None,
            guard_actions: vec![],
            was_guard_blocked: false,
            cache_hit: false,
            worker: None,
        };
        first.was_rewritten = true;
        first.rewritten_sql = Some("SELECT 1 WHERE tenant_id = 42".into());
        store.record_query(first.clone()).await.unwrap();
        first.status = QueryStatus::Cancelled;
        first.error_message = Some("client cancelled".into());
        store.record_query(first.clone()).await.unwrap();

        let rows = store
            .list_queries(&QueryFilters {
                cluster_group: Some(group.clone()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].proxy_query_id, proxy_id);
        assert!(rows[0].status.contains("Failed"));
        let expected = Some(serde_json::json!({"status": "fallback", "reason": "no_schema"}));
        assert_eq!(rows[0].translation, expected);
        let conversation = store.get_conversation(&proxy_id).await.unwrap();
        assert_eq!(conversation.len(), 1);
        assert_eq!(conversation[0].translation, expected);
        for row in [&rows[0], &conversation[0]] {
            assert!(row.was_rewritten);
            assert_eq!(row.rewritten_sql, first.rewritten_sql);
            assert!(!row.was_translated);
            assert!(row.translated_sql.is_none());
        }

        let digest_count: (i64,) = sqlx::query_as(
            "SELECT call_count FROM query_digest_stats WHERE query_parameterized_hash = $1",
        )
        .bind(digest_hash)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(digest_count.0, 1);

        // A query that never reached translation must retain an unknown outcome.
        first.proxy_query_id = unique_id("untranslated");
        first.translation = None;
        first.conversation_id = Some(first.proxy_query_id.clone());
        let untranslated_id = first.proxy_query_id.clone();
        store.record_query(first).await.unwrap();
        let untranslated = store.get_conversation(&untranslated_id).await.unwrap();
        assert_eq!(untranslated.len(), 1);
        assert!(untranslated[0].translation.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn pg_a_token_enrolls_one_hub_of_its_site() {
        use crate::hub_store::{HubKey, HubStore};
        let store = test_store().await;
        let site = unique_id("site");
        let hub = |id: &str, site: &str| HubKey {
            id: id.into(),
            site: site.into(),
            public_key: "pk".into(),
            host: Some("h".into()),
            enrolled_at: Utc::now(),
        };
        let later = Utc::now() + chrono::Duration::hours(1);
        let token = unique_id("token");
        assert!(!store
            .create_enrollment_token(&site, &token, later)
            .await
            .unwrap());
        store
            .upsert_hub_site(&site, Some(vec!["c1".into()]))
            .await
            .unwrap();
        assert!(store
            .create_enrollment_token(&site, &token, later)
            .await
            .unwrap());

        let id = unique_id("hub");
        assert!(!store.enroll_hub(&token, &hub(&id, "other")).await.unwrap());
        assert!(store.enroll_hub(&token, &hub(&id, &site)).await.unwrap());
        assert!(!store
            .enroll_hub(&token, &hub(&unique_id("hub"), &site))
            .await
            .unwrap());

        let expired = unique_id("token");
        store
            .create_enrollment_token(&site, &expired, Utc::now() - chrono::Duration::seconds(1))
            .await
            .unwrap();
        assert!(!store
            .enroll_hub(&expired, &hub(&unique_id("hub"), &site))
            .await
            .unwrap());

        let sites = store.list_hub_sites().await.unwrap();
        let mine = sites.iter().find(|s| s.name == site).unwrap();
        assert_eq!(mine.clusters, Some(vec!["c1".to_string()]));
        let keys: Vec<_> = store
            .list_hub_keys()
            .await
            .unwrap()
            .into_iter()
            .filter(|k| k.site == site)
            .collect();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id, id);

        assert!(store.delete_hub_site(&site).await.unwrap());
        assert!(!store
            .list_hub_keys()
            .await
            .unwrap()
            .iter()
            .any(|k| k.site == site));
    }
}
