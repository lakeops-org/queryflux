use std::borrow::Cow;
use std::collections::HashMap;

use crate::{
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
    ClusterConfigStore, LoadedRoutingConfig, Persistence, ProxySettingsStore, QueryHistoryStore,
    RoutingConfigStore, ScriptLibraryStore,
};
use async_trait::async_trait;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{BackendQueryId, ExecutingQuery, ProxyQueryId, QueuedQuery},
    tags::tags_to_json,
};
use sqlx::PgPool;

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
    g.allow_groups,
    g.allow_users,
    g.translation_script_ids,
    g.default_tags,
    g.created_at,
    g.updated_at
FROM cluster_group_configs g
"#;

/// Postgres backend — implements both `Persistence` (in-flight query state)
/// and `MetricsStore` (historical query records + cluster snapshots).
///
/// A single shared pool covers all tables. Run `migrate()` once at startup.
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to Postgres and return a ready instance.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await.map_err(|e| {
            QueryFluxError::Persistence(format!("Failed to connect to Postgres: {e}"))
        })?;
        Ok(Self { pool })
    }

    /// Run all migrations (persistence + metrics). Tracks applied migrations in `_sqlx_migrations`.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("src/postgres/migrations")
            .run(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Migration failed: {e}")))?;
        Ok(())
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
                      qr.engine_type, qr.frontend_protocol, qr.username, qr.sql_preview, qr.translated_sql,
                      qr.status, qr.was_translated,
                      qr.source_dialect, qr.target_dialect, qr.queue_duration_ms, qr.execution_duration_ms,
                      qr.rows_returned, qr.error_message, qr.routing_trace, qr.created_at,
                      qr.engine_elapsed_time_ms, qr.cpu_time_ms, qr.processed_rows, qr.processed_bytes,
                      qr.physical_input_bytes, qr.peak_memory_bytes, qr.spilled_bytes, qr.total_splits,
                      qr.query_tags, qr.query_hash, qr.query_parameterized_hash, qr.translated_query_hash,
                      qr.agent_id, qr.conversation_id, qr.step_index, qr.tool_call_id, qr.query_intent,
                      qr.guard_actions, qr.was_guard_blocked
               FROM query_records qr
               LEFT JOIN cluster_group_configs cg ON cg.id = qr.cluster_group_id
               LEFT JOIN cluster_configs cc ON cc.id = qr.cluster_id
               WHERE ($1::text IS NULL OR qr.sql_preview ILIKE '%' || $1 || '%')
                 AND ($2::text IS NULL OR qr.status = $2)
                 AND ($3::text IS NULL OR COALESCE(cg.name, qr.cluster_group) = $3)
                 AND ($4::text IS NULL OR qr.engine_type = $4)
               ORDER BY qr.created_at DESC
               LIMIT $5 OFFSET $6"#,
        )
        .bind(&filters.search)
        .bind(&filters.status)
        .bind(&filters.cluster_group)
        .bind(&filters.engine)
        .bind(filters.limit)
        .bind(filters.offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_queries: {e}")))
    }

    async fn get_dashboard_stats(&self) -> Result<DashboardStats> {
        let row: (i64, i64, i64, f64) = sqlx::query_as(
            r#"SELECT
                COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status != 'Success')::bigint,
                COUNT(*) FILTER (WHERE was_translated)::bigint,
                COALESCE(AVG(execution_duration_ms), 0)::float8
               FROM query_records
               WHERE created_at > NOW() - INTERVAL '1 hour'"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_dashboard_stats: {e}")))?;

        let (total, failed, translated, avg_ms) = row;
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
                COALESCE(SUM(rows_returned), 0)::bigint                        AS total_rows_returned
               FROM query_records
               WHERE created_at > NOW() - ($1 * INTERVAL '1 hour')
               GROUP BY engine_type
               ORDER BY total_queries DESC"#,
        )
        .bind(hours)
        .fetch_all(&self.pool)
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
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_group_stats: {e}")))
    }

    async fn list_engines(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT engine_type FROM query_records ORDER BY engine_type")
                .fetch_all(&self.pool)
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
        .fetch_all(&self.pool)
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
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_conversations: {e}")))
    }

    async fn get_conversation(&self, conversation_id: &str) -> Result<Vec<QuerySummary>> {
        sqlx::query_as::<_, QuerySummary>(
            r#"SELECT qr.id, qr.proxy_query_id, qr.backend_query_id,
                      COALESCE(cg.name, qr.cluster_group) AS cluster_group,
                      COALESCE(cc.name, qr.cluster_name)  AS cluster_name,
                      qr.cluster_group_id, qr.cluster_id,
                      qr.engine_type, qr.frontend_protocol, qr.username, qr.sql_preview, qr.translated_sql,
                      qr.status, qr.was_translated,
                      qr.source_dialect, qr.target_dialect, qr.queue_duration_ms, qr.execution_duration_ms,
                      qr.rows_returned, qr.error_message, qr.routing_trace, qr.created_at,
                      qr.engine_elapsed_time_ms, qr.cpu_time_ms, qr.processed_rows, qr.processed_bytes,
                      qr.physical_input_bytes, qr.peak_memory_bytes, qr.spilled_bytes, qr.total_splits,
                      qr.query_tags, qr.query_hash, qr.query_parameterized_hash, qr.translated_query_hash,
                      qr.agent_id, qr.conversation_id, qr.step_index, qr.tool_call_id, qr.query_intent,
                      qr.guard_actions, qr.was_guard_blocked
               FROM query_records qr
               LEFT JOIN cluster_group_configs cg ON cg.id = qr.cluster_group_id
               LEFT JOIN cluster_configs cc ON cc.id = qr.cluster_id
               WHERE qr.conversation_id = $1
               ORDER BY qr.step_index ASC NULLS LAST, qr.created_at ASC"#,
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_conversation: {e}")))
    }
}

impl PostgresStore {
    /// Delete all `query_records` rows older than `older_than`. Returns the number of rows deleted.
    pub async fn purge_old_query_records(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let r = sqlx::query("DELETE FROM query_records WHERE created_at < $1")
            .bind(older_than)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("purge_old_query_records: {e}")))?;
        Ok(r.rows_affected())
    }

    /// Ordered translation fixup bodies per cluster group name (for `LiveConfig`).
    pub async fn load_group_translation_bodies(&self) -> Result<HashMap<String, Vec<String>>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT g.name, s.body
               FROM cluster_group_configs g
               CROSS JOIN LATERAL unnest(g.translation_script_ids) WITH ORDINALITY AS u(sid, ord)
               JOIN user_scripts s ON s.id = u.sid AND s.kind = 'translation_fixup'
               ORDER BY g.name, u.ord"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("load_group_translation_bodies: {e}")))?;

        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (name, body) in rows {
            map.entry(name).or_default().push(body);
        }
        Ok(map)
    }
}

// ---------------------------------------------------------------------------
// ClusterConfigStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterConfigStore for PostgresStore {
    async fn list_cluster_configs(&self) -> Result<Vec<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>("SELECT * FROM cluster_configs ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("list_cluster_configs: {e}")))
    }

    async fn get_cluster_config(&self, name: &str) -> Result<Option<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>("SELECT * FROM cluster_configs WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("get_cluster_config: {e}")))
    }

    async fn upsert_cluster_config(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<ClusterConfigRecord> {
        sqlx::query_as::<_, ClusterConfigRecord>(
            r#"INSERT INTO cluster_configs (name, engine_key, enabled, max_running_queries, config)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO UPDATE SET
                   engine_key          = EXCLUDED.engine_key,
                   enabled             = EXCLUDED.enabled,
                   max_running_queries = EXCLUDED.max_running_queries,
                   config              = EXCLUDED.config,
                   updated_at          = now()
               RETURNING *"#,
        )
        .bind(name)
        .bind(&cfg.engine_key)
        .bind(cfg.enabled)
        .bind(cfg.max_running_queries)
        .bind(&cfg.config)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_cluster_config: {e}")))
    }

    async fn delete_cluster_config(&self, name: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(|e| {
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
            .fetch_one(&self.pool)
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

        let mut tx = self.pool.begin().await.map_err(|e| {
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
        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} ORDER BY g.name");
        sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("list_group_configs: {e}")))
    }

    async fn get_group_config(&self, name: &str) -> Result<Option<ClusterGroupConfigRecord>> {
        let q = format!("{CLUSTER_GROUP_CONFIG_SELECT} WHERE g.name = $1");
        sqlx::query_as::<_, ClusterGroupConfigRecord>(&q)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("get_group_config: {e}")))
    }

    async fn upsert_group_config(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<ClusterGroupConfigRecord> {
        let mut tx =
            self.pool.begin().await.map_err(|e| {
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
                   (name, enabled, members, max_running_queries, max_queued_queries, strategy, allow_groups, allow_users, translation_script_ids, default_tags)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
               ON CONFLICT (name) DO UPDATE SET
                   enabled                = EXCLUDED.enabled,
                   members                = EXCLUDED.members,
                   max_running_queries    = EXCLUDED.max_running_queries,
                   max_queued_queries     = EXCLUDED.max_queued_queries,
                   strategy               = EXCLUDED.strategy,
                   allow_groups           = EXCLUDED.allow_groups,
                   allow_users            = EXCLUDED.allow_users,
                   translation_script_ids = EXCLUDED.translation_script_ids,
                   default_tags           = EXCLUDED.default_tags,
                   updated_at             = now()"#,
        )
        .bind(name)
        .bind(cfg.enabled)
        .bind(&member_ids)
        .bind(cfg.max_running_queries)
        .bind(cfg.max_queued_queries)
        .bind(&cfg.strategy)
        .bind(&cfg.allow_groups)
        .bind(&cfg.allow_users)
        .bind(&cfg.translation_script_ids)
        .bind(&cfg.default_tags)
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

    async fn delete_group_config(&self, name: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM cluster_group_configs WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
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
            .fetch_one(&self.pool)
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
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("rename_group_config: {e}")))?
                .ok_or_else(|| {
                    QueryFluxError::Persistence(format!("Group '{old_name}' not found"))
                });
        }

        let mut tx =
            self.pool.begin().await.map_err(|e| {
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
        let rows = if let Some(k) = kind {
            sqlx::query_as::<_, UserScriptRecord>(
                "SELECT * FROM user_scripts WHERE kind = $1 ORDER BY name",
            )
            .bind(k)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, UserScriptRecord>("SELECT * FROM user_scripts ORDER BY name")
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|e| QueryFluxError::Persistence(format!("list_user_scripts: {e}")))?;
        Ok(rows)
    }

    async fn get_user_script(&self, id: i64) -> Result<Option<UserScriptRecord>> {
        sqlx::query_as::<_, UserScriptRecord>("SELECT * FROM user_scripts WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
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
        .fetch_one(&self.pool)
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
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("update_user_script: {e}")))?
        .ok_or_else(|| QueryFluxError::Persistence(format!("user script id {id} not found")))
    }

    async fn delete_user_script(&self, id: i64) -> Result<bool> {
        let r = sqlx::query("DELETE FROM user_scripts WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("delete_user_script: {e}")))?;
        Ok(r.rows_affected() > 0)
    }
}

// ---------------------------------------------------------------------------
// ProxySettingsStore — `security_config` backed by `security_settings` (singleton JSON)
// ---------------------------------------------------------------------------

#[async_trait]
impl ProxySettingsStore for PostgresStore {
    async fn get_proxy_setting(&self, key: &str) -> Result<Option<serde_json::Value>> {
        match key {
            "security_config" => {
                let row: Option<(serde_json::Value,)> = sqlx::query_as(
                    r#"SELECT config FROM security_settings WHERE singleton = TRUE"#,
                )
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("get_proxy_setting: {e}")))?;
                Ok(row.map(|(v,)| v))
            }
            "guardrails_config" => {
                let rows: Vec<(String, serde_json::Value)> =
                    sqlx::query_as(r#"SELECT kind, guards FROM guardrails ORDER BY kind"#)
                        .fetch_all(&self.pool)
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
            _ => Ok(None),
        }
    }

    async fn set_proxy_setting(&self, key: &str, value: serde_json::Value) -> Result<()> {
        match key {
            "security_config" => {
                sqlx::query(
                    r#"INSERT INTO security_settings (singleton, config) VALUES (TRUE, $1)
                       ON CONFLICT (singleton) DO UPDATE SET config = EXCLUDED.config, updated_at = now()"#,
                )
                .bind(&value)
                .execute(&self.pool)
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
                    .pool
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
            _ => {}
        }
        Ok(())
    }

    async fn delete_proxy_setting(&self, key: &str) -> Result<()> {
        match key {
            "security_config" => {
                sqlx::query(
                    r#"UPDATE security_settings SET config = '{}'::jsonb, updated_at = now() WHERE singleton = TRUE"#,
                )
                .execute(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("delete_proxy_setting: {e}")))?;
            }
            "guardrails_config" => {
                sqlx::query(r#"DELETE FROM guardrails"#)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| QueryFluxError::Persistence(format!("delete guardrails: {e}")))?;
            }
            _ => {}
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
        let row: Option<(bool, String, Option<i64>)> = sqlx::query_as(
            r#"SELECT routing_persist_active, routing_fallback, fallback_group_id
                 FROM routing_settings
                WHERE singleton = true"#,
        )
        .fetch_optional(&self.pool)
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
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    QueryFluxError::Persistence(format!("load_routing_config groups: {e}"))
                })?;
        let id_to_name: HashMap<i64, String> = id_rows.into_iter().collect();

        let sql_rows: Vec<(
            i32,
            i32,
            i32,
            Option<i64>,
            sqlx::types::Json<serde_json::Value>,
        )> = sqlx::query_as(
            r#"SELECT sort_order, router_logical_index, slice_index, target_group_id, definition
                     FROM routing_rules
                    ORDER BY sort_order ASC, id ASC"#,
        )
        .fetch_all(&self.pool)
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

    async fn replace_routing_config(
        &self,
        routing_fallback: &str,
        routing_fallback_group_id: Option<i64>,
        routers: &[serde_json::Value],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| {
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
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert executing_queries: {e}")))?;
        Ok(())
    }

    async fn get(&self, id: &BackendQueryId) -> Result<Option<ExecutingQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.pool)
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
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete executing_queries: {e}")))?;
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<ExecutingQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries ORDER BY created_at")
                .fetch_all(&self.pool)
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
        let data = serde_json::to_value(&query)
            .map_err(|e| QueryFluxError::Persistence(format!("Serialize error: {e}")))?;
        sqlx::query(
            "INSERT INTO queued_queries (id, data) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&id)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert queued_queries: {e}")))?;
        Ok(())
    }

    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.pool)
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
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete queued_queries: {e}")))?;
        Ok(())
    }

    async fn list_queued(&self) -> Result<Vec<QueuedQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries ORDER BY created_at")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("List queued_queries: {e}")))?;
        rows.into_iter()
            .map(|(data,)| {
                serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))
            })
            .collect()
    }

    async fn delete_queued_not_accessed_since(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        // last_accessed is stored inside the JSONB data blob.
        let result = sqlx::query(
            "DELETE FROM queued_queries WHERE (data->>'last_accessed')::timestamptz < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("delete_queued_not_accessed_since: {e}"))
        })?;
        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// MetricsStore — historical data for the management UI
// ---------------------------------------------------------------------------

#[async_trait]
impl MetricsStore for PostgresStore {
    async fn record_query(&self, r: QueryRecord) -> Result<()> {
        let (
            engine_elapsed_ms,
            cpu_ms,
            proc_rows,
            proc_bytes,
            phys_bytes,
            peak_mem,
            spilled,
            splits,
        ) = match &r.engine_stats {
            Some(s) => (
                s.engine_elapsed_time_ms.map(|v| v as i64),
                s.cpu_time_ms.map(|v| v as i64),
                s.processed_rows.map(|v| v as i64),
                s.processed_bytes.map(|v| v as i64),
                s.physical_input_bytes.map(|v| v as i64),
                s.peak_memory_bytes.map(|v| v as i64),
                s.spilled_bytes.map(|v| v as i64),
                s.total_splits.map(|v| v as i32),
            ),
            None => (None, None, None, None, None, None, None, None),
        };

        let query_tags_json = tags_to_json(&r.query_tags);
        let guard_actions_json =
            serde_json::to_value(&r.guard_actions).unwrap_or(serde_json::Value::Array(vec![]));
        sqlx::query(
            r#"INSERT INTO query_records
                (proxy_query_id, backend_query_id, cluster_group, cluster_name, engine_type,
                 frontend_protocol, source_dialect, target_dialect, was_translated, username,
                 catalog, db_name, sql_preview, translated_sql, status, routing_trace,
                 queue_duration_ms, execution_duration_ms, rows_returned, error_message,
                 created_at, engine_elapsed_time_ms, cpu_time_ms, processed_rows, processed_bytes,
                 physical_input_bytes, peak_memory_bytes, spilled_bytes, total_splits,
                 cluster_group_id, cluster_id, query_tags,
                 query_hash, query_parameterized_hash, translated_query_hash,
                 agent_id, conversation_id, step_index, tool_call_id, query_intent,
                 guard_actions, was_guard_blocked)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,
                       $36,$37,$38,$39,$40,$41,$42)"#,
        )
        .bind(&r.proxy_query_id)
        .bind(&r.backend_query_id)
        .bind(&r.cluster_group.0)
        .bind(&r.cluster_name.0)
        .bind(format!("{:?}", r.engine_type))
        .bind(format!("{:?}", r.frontend_protocol))
        .bind(format!("{:?}", r.source_dialect))
        .bind(format!("{:?}", r.target_dialect))
        .bind(r.was_translated)
        .bind(&r.user)
        .bind(&r.catalog)
        .bind(&r.database)
        .bind(&r.sql_preview)
        .bind(&r.translated_sql)
        .bind(format!("{:?}", r.status))
        .bind(&r.routing_trace)
        .bind(r.queue_duration_ms as i64)
        .bind(r.execution_duration_ms as i64)
        .bind(r.rows_returned.map(|v| v as i64))
        .bind(&r.error_message)
        .bind(r.created_at)
        .bind(engine_elapsed_ms)
        .bind(cpu_ms)
        .bind(proc_rows)
        .bind(proc_bytes)
        .bind(phys_bytes)
        .bind(peak_mem)
        .bind(spilled)
        .bind(splits)
        .bind(r.cluster_group_config_id)
        .bind(r.cluster_config_id)
        .bind(query_tags_json)
        .bind(r.query_hash)
        .bind(r.query_parameterized_hash)
        .bind(r.translated_query_hash)
        .bind(&r.agent_id)
        .bind(&r.conversation_id)
        .bind(r.step_index)
        .bind(&r.tool_call_id)
        .bind(&r.query_intent)
        .bind(guard_actions_json)
        .bind(r.was_guard_blocked)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert query_records: {e}")))?;

        // Upsert into query_digest_stats.
        if let Some(phash) = r.query_parameterized_hash {
            let rows = r.rows_returned.map(|v| v as i64).unwrap_or(0);
            let exec_ms = r.execution_duration_ms as i64;
            sqlx::query(
                r#"INSERT INTO query_digest_stats
                    (query_parameterized_hash, digest_text,
                     translated_query_hash, translated_digest_text,
                     first_seen, last_seen, call_count, sum_execution_ms, sum_rows_returned,
                     cluster_group)
                   VALUES ($1,$2,$3,$4,$5,$5,1,$6,$7,$8)
                   ON CONFLICT (query_parameterized_hash) DO UPDATE SET
                     last_seen = EXCLUDED.last_seen,
                     call_count = query_digest_stats.call_count + 1,
                     sum_execution_ms = query_digest_stats.sum_execution_ms + EXCLUDED.sum_execution_ms,
                     sum_rows_returned = query_digest_stats.sum_rows_returned + EXCLUDED.sum_rows_returned,
                     cluster_group = EXCLUDED.cluster_group,
                     digest_text = CASE WHEN query_digest_stats.digest_text = '' THEN EXCLUDED.digest_text ELSE query_digest_stats.digest_text END,
                     translated_digest_text = COALESCE(query_digest_stats.translated_digest_text, EXCLUDED.translated_digest_text)"#,
            )
            .bind(phash)
            .bind(r.digest_text.as_deref().unwrap_or(""))
            .bind(r.translated_query_hash)
            .bind(r.translated_digest_text.as_deref())
            .bind(r.created_at)
            .bind(exec_ms)
            .bind(rows)
            .bind(&r.cluster_group.0)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Upsert query_digest_stats: {e}")))?;
        }

        Ok(())
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
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert cluster_snapshots: {e}")))?;
        Ok(())
    }
}
