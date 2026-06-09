use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use queryflux_core::{
    error::Result,
    query::{BackendQueryId, ExecutingQuery, ProxyQueryId, QueuedQuery},
};

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
    script_library::{
        is_valid_script_kind, UpsertUserScript, UserScriptRecord, KIND_TRANSLATION_FIXUP,
    },
    ClusterConfigStore, LoadedRoutingConfig, Persistence, ProxySettingsStore, QueryHistoryStore,
    RoutingConfigStore, ScriptLibraryStore,
};

pub struct InMemoryPersistence {
    // --- in-flight state ---
    /// Keyed by BackendQueryId (Trino's query ID) — matches the client poll URL.
    executing: DashMap<String, ExecutingQuery>,
    queued: DashMap<String, QueuedQuery>,

    // --- query history (write side) ---
    next_id: AtomicI64,
    query_records: RwLock<Vec<QuerySummary>>,
    // cluster snapshots are accepted but not surfaced in read queries for now
    _snapshots: RwLock<Vec<ClusterSnapshot>>,

    // --- cluster / group config ---
    cluster_configs: DashMap<String, ClusterConfigRecord>,
    group_configs: DashMap<String, ClusterGroupConfigRecord>,
    next_cluster_id: AtomicI64,
    next_group_id: AtomicI64,
    user_scripts: DashMap<i64, UserScriptRecord>,
    next_script_id: AtomicI64,

    // --- proxy-level settings ---
    proxy_settings: std::sync::RwLock<std::collections::HashMap<String, serde_json::Value>>,
}

impl Default for InMemoryPersistence {
    fn default() -> Self {
        Self {
            executing: DashMap::default(),
            queued: DashMap::default(),
            next_id: AtomicI64::new(0),
            query_records: RwLock::new(Vec::new()),
            _snapshots: RwLock::new(Vec::new()),
            cluster_configs: DashMap::default(),
            group_configs: DashMap::default(),
            next_cluster_id: AtomicI64::new(1),
            next_group_id: AtomicI64::new(1),
            user_scripts: DashMap::default(),
            next_script_id: AtomicI64::new(1),
            proxy_settings: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl InMemoryPersistence {
    pub fn new() -> Self {
        Self::default()
    }

    fn record_to_summary(&self, record: QueryRecord) -> QuerySummary {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let stats = record.engine_stats.as_ref();
        QuerySummary {
            id,
            proxy_query_id: record.proxy_query_id,
            backend_query_id: record.backend_query_id,
            cluster_group: record.cluster_group.to_string(),
            cluster_name: record.cluster_name.to_string(),
            cluster_group_id: record.cluster_group_config_id,
            cluster_id: record.cluster_config_id,
            engine_type: format!("{:?}", record.engine_type),
            protocol: format!("{:?}", record.frontend_protocol),
            username: record.user,
            sql_preview: record.sql_preview,
            translated_sql: record.translated_sql,
            status: format!("{:?}", record.status),
            was_translated: record.was_translated,
            source_dialect: format!("{:?}", record.source_dialect),
            target_dialect: format!("{:?}", record.target_dialect),
            routing_trace: record.routing_trace,
            queue_duration_ms: record.queue_duration_ms as i64,
            execution_duration_ms: record.execution_duration_ms as i64,
            rows_returned: record.rows_returned.map(|v| v as i64),
            error_message: record.error_message,
            created_at: record.created_at,
            engine_elapsed_time_ms: stats
                .and_then(|s| s.engine_elapsed_time_ms)
                .map(|v| v as i64),
            cpu_time_ms: stats.and_then(|s| s.cpu_time_ms).map(|v| v as i64),
            processed_rows: stats.and_then(|s| s.processed_rows).map(|v| v as i64),
            processed_bytes: stats.and_then(|s| s.processed_bytes).map(|v| v as i64),
            physical_input_bytes: stats.and_then(|s| s.physical_input_bytes).map(|v| v as i64),
            peak_memory_bytes: stats.and_then(|s| s.peak_memory_bytes).map(|v| v as i64),
            spilled_bytes: stats.and_then(|s| s.spilled_bytes).map(|v| v as i64),
            total_splits: stats.and_then(|s| s.total_splits).map(|v| v as i32),
            query_tags: Some(queryflux_core::tags::tags_to_json(&record.query_tags)),
            query_hash: record.query_hash,
            query_parameterized_hash: record.query_parameterized_hash,
            translated_query_hash: record.translated_query_hash,
            agent_id: record.agent_id,
            conversation_id: record.conversation_id,
            step_index: record.step_index,
            tool_call_id: record.tool_call_id,
            query_intent: record.query_intent,
            guard_actions: serde_json::to_value(&record.guard_actions).ok(),
            was_guard_blocked: record.was_guard_blocked,
        }
    }
}

// ---------------------------------------------------------------------------
// Persistence — in-flight query state
// ---------------------------------------------------------------------------

#[async_trait]
impl Persistence for InMemoryPersistence {
    async fn upsert(&self, query: ExecutingQuery) -> Result<()> {
        self.executing
            .insert(query.backend_query_id.0.clone(), query);
        Ok(())
    }
    async fn get(&self, id: &BackendQueryId) -> Result<Option<ExecutingQuery>> {
        Ok(self.executing.get(&id.0).map(|e| e.value().clone()))
    }
    async fn delete(&self, id: &BackendQueryId) -> Result<()> {
        self.executing.remove(&id.0);
        Ok(())
    }
    async fn list_all(&self) -> Result<Vec<ExecutingQuery>> {
        Ok(self.executing.iter().map(|e| e.value().clone()).collect())
    }

    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()> {
        self.queued.insert(query.id.0.clone(), query);
        Ok(())
    }
    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        Ok(self.queued.get(&id.0).map(|e| e.value().clone()))
    }
    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()> {
        self.queued.remove(&id.0);
        Ok(())
    }
    async fn list_queued(&self) -> Result<Vec<QueuedQuery>> {
        Ok(self.queued.iter().map(|e| e.value().clone()).collect())
    }

    async fn delete_queued_not_accessed_since(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let mut removed = 0u64;
        self.queued.retain(|_, q| {
            if q.last_accessed >= cutoff {
                true
            } else {
                removed += 1;
                false
            }
        });
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// MetricsStore — write completed query records and cluster snapshots
// ---------------------------------------------------------------------------

#[async_trait]
impl MetricsStore for InMemoryPersistence {
    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        let summary = self.record_to_summary(record);
        self.query_records.write().unwrap().push(summary);
        Ok(())
    }

    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()> {
        self._snapshots.write().unwrap().push(snapshot);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// QueryHistoryStore — read analytics for the admin UI
// ---------------------------------------------------------------------------

#[async_trait]
impl QueryHistoryStore for InMemoryPersistence {
    async fn list_queries(&self, filters: &QueryFilters) -> Result<Vec<QuerySummary>> {
        let records = self.query_records.read().unwrap();
        let mut results: Vec<&QuerySummary> = records
            .iter()
            .filter(|r| {
                if let Some(s) = &filters.status {
                    if !r.status.eq_ignore_ascii_case(s) {
                        return false;
                    }
                }
                if let Some(g) = &filters.cluster_group {
                    if !r.cluster_group.eq_ignore_ascii_case(g) {
                        return false;
                    }
                }
                if let Some(e) = &filters.engine {
                    if !r.engine_type.eq_ignore_ascii_case(e) {
                        return false;
                    }
                }
                if let Some(search) = &filters.search {
                    let needle = search.to_lowercase();
                    if !r.sql_preview.to_lowercase().contains(&needle) {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Newest first
        results.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(results
            .into_iter()
            .skip(filters.offset as usize)
            .take(filters.limit as usize)
            .cloned()
            .collect())
    }

    async fn get_dashboard_stats(&self) -> Result<DashboardStats> {
        let cutoff = Utc::now() - chrono::Duration::hours(1);
        let records = self.query_records.read().unwrap();
        let recent: Vec<&QuerySummary> =
            records.iter().filter(|r| r.created_at >= cutoff).collect();

        let total = recent.len() as i64;
        if total == 0 {
            return Ok(DashboardStats::default());
        }

        let failed = recent.iter().filter(|r| r.status == "Failed").count() as f64;
        let translated = recent.iter().filter(|r| r.was_translated).count() as f64;
        let avg_ms = recent
            .iter()
            .map(|r| r.execution_duration_ms as f64)
            .sum::<f64>()
            / total as f64;

        Ok(DashboardStats {
            queries_last_hour: total,
            error_rate_last_hour: failed / total as f64,
            avg_duration_ms_last_hour: avg_ms,
            translation_rate_last_hour: translated / total as f64,
        })
    }

    async fn get_engine_stats(&self, hours: i64) -> Result<Vec<EngineStatRow>> {
        let cutoff = Utc::now() - chrono::Duration::hours(hours);
        let records = self.query_records.read().unwrap();

        let mut map: std::collections::HashMap<String, Vec<&QuerySummary>> =
            std::collections::HashMap::new();
        for r in records.iter().filter(|r| r.created_at >= cutoff) {
            map.entry(r.engine_type.clone()).or_default().push(r);
        }

        Ok(map
            .into_iter()
            .map(|(engine_type, rows)| engine_stat_row(engine_type, &rows))
            .collect())
    }

    async fn get_group_stats(&self, hours: i64) -> Result<Vec<GroupStatRow>> {
        let cutoff = Utc::now() - chrono::Duration::hours(hours);
        let records = self.query_records.read().unwrap();

        let mut map: std::collections::HashMap<(String, String), Vec<&QuerySummary>> =
            std::collections::HashMap::new();
        for r in records.iter().filter(|r| r.created_at >= cutoff) {
            map.entry((r.cluster_group.clone(), r.engine_type.clone()))
                .or_default()
                .push(r);
        }

        Ok(map
            .into_iter()
            .map(|((cluster_group, engine_type), rows)| {
                group_stat_row(cluster_group, engine_type, &rows)
            })
            .collect())
    }

    async fn list_engines(&self) -> Result<Vec<String>> {
        let records = self.query_records.read().unwrap();
        let mut engines: Vec<String> = records
            .iter()
            .map(|r| r.engine_type.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        engines.sort();
        Ok(engines)
    }

    async fn list_agents(&self, _limit: i64, _offset: i64) -> Result<Vec<AgentSummary>> {
        Ok(vec![])
    }

    async fn list_conversations(
        &self,
        _agent_id: Option<&str>,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<ConversationSummary>> {
        Ok(vec![])
    }

    async fn get_conversation(&self, _conversation_id: &str) -> Result<Vec<QuerySummary>> {
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// ClusterConfigStore — in-memory CRUD for cluster / group config
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterConfigStore for InMemoryPersistence {
    async fn list_cluster_configs(&self) -> Result<Vec<ClusterConfigRecord>> {
        Ok(self
            .cluster_configs
            .iter()
            .map(|e| e.value().clone())
            .collect())
    }

    async fn get_cluster_config(&self, name: &str) -> Result<Option<ClusterConfigRecord>> {
        Ok(self.cluster_configs.get(name).map(|e| e.value().clone()))
    }

    async fn upsert_cluster_config(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<ClusterConfigRecord> {
        let now = Utc::now();
        let existing = self.cluster_configs.get(name).map(|e| e.value().clone());
        let existing_created_at = existing.as_ref().map(|r| r.created_at);
        let id = existing
            .as_ref()
            .map(|r| r.id)
            .unwrap_or_else(|| self.next_cluster_id.fetch_add(1, Ordering::Relaxed));
        let record = ClusterConfigRecord {
            id,
            name: name.to_string(),
            engine_key: cfg.engine_key.clone(),
            enabled: cfg.enabled,
            max_running_queries: cfg.max_running_queries,
            config: cfg.config.clone(),
            created_at: existing_created_at.unwrap_or(now),
            updated_at: now,
        };
        self.cluster_configs
            .insert(name.to_string(), record.clone());
        Ok(record)
    }

    async fn delete_cluster_config(&self, name: &str) -> Result<bool> {
        if self.cluster_configs.remove(name).is_none() {
            return Ok(false);
        }
        let now = Utc::now();
        for mut entry in self.group_configs.iter_mut() {
            let record = entry.value_mut();
            let before = record.members.len();
            record.members.retain(|m| m != name);
            if record.members.len() != before {
                record.updated_at = now;
            }
        }
        Ok(true)
    }

    async fn cluster_configs_count(&self) -> Result<i64> {
        Ok(self.cluster_configs.len() as i64)
    }

    async fn list_group_configs(&self) -> Result<Vec<ClusterGroupConfigRecord>> {
        Ok(self
            .group_configs
            .iter()
            .map(|e| e.value().clone())
            .collect())
    }

    async fn get_group_config(&self, name: &str) -> Result<Option<ClusterGroupConfigRecord>> {
        Ok(self.group_configs.get(name).map(|e| e.value().clone()))
    }

    async fn upsert_group_config(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<ClusterGroupConfigRecord> {
        for m in &cfg.members {
            if !self.cluster_configs.contains_key(m) {
                return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                    "Unknown cluster '{m}' in group members (clusters must exist first)"
                )));
            }
        }

        let now = Utc::now();
        let existing = self.group_configs.get(name).map(|e| e.value().clone());
        let id = existing
            .as_ref()
            .map(|r| r.id)
            .unwrap_or_else(|| self.next_group_id.fetch_add(1, Ordering::Relaxed));
        for sid in &cfg.translation_script_ids {
            let ok = self
                .user_scripts
                .get(sid)
                .map(|r| r.kind == KIND_TRANSLATION_FIXUP)
                .unwrap_or(false);
            if !ok {
                return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                    "Unknown or invalid translation script id {sid}"
                )));
            }
        }

        let record = ClusterGroupConfigRecord {
            id,
            name: name.to_string(),
            enabled: cfg.enabled,
            members: cfg.members.clone(),
            max_running_queries: cfg.max_running_queries,
            max_queued_queries: cfg.max_queued_queries,
            strategy: cfg.strategy.clone(),
            allow_groups: cfg.allow_groups.clone(),
            allow_users: cfg.allow_users.clone(),
            translation_script_ids: cfg.translation_script_ids.clone(),
            default_tags: cfg.default_tags.clone(),
            created_at: existing.as_ref().map(|r| r.created_at).unwrap_or(now),
            updated_at: now,
        };
        self.group_configs.insert(name.to_string(), record.clone());
        Ok(record)
    }

    async fn delete_group_config(&self, name: &str) -> Result<bool> {
        Ok(self.group_configs.remove(name).is_some())
    }

    async fn group_configs_count(&self) -> Result<i64> {
        Ok(self.group_configs.len() as i64)
    }

    async fn rename_cluster_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterConfigRecord> {
        let old_name = old_name.trim();
        let new_name = new_name.trim();
        if new_name.is_empty() {
            return Err(queryflux_core::error::QueryFluxError::Persistence(
                "New cluster name must not be empty".to_string(),
            ));
        }
        if old_name == new_name {
            return self.get_cluster_config(old_name).await?.ok_or_else(|| {
                queryflux_core::error::QueryFluxError::Persistence(format!(
                    "Cluster '{old_name}' not found"
                ))
            });
        }
        if self.cluster_configs.contains_key(new_name) {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Cluster name '{new_name}' is already in use"
            )));
        }
        let (_, mut record) = self.cluster_configs.remove(old_name).ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Persistence(format!(
                "Cluster '{old_name}' not found"
            ))
        })?;
        let now = Utc::now();
        record.name = new_name.to_string();
        record.updated_at = now;
        self.cluster_configs
            .insert(new_name.to_string(), record.clone());

        for mut entry in self.group_configs.iter_mut() {
            let gr = entry.value_mut();
            let mut touched = false;
            for m in gr.members.iter_mut() {
                if m == old_name {
                    *m = new_name.to_string();
                    touched = true;
                }
            }
            if touched {
                gr.updated_at = now;
            }
        }

        Ok(record)
    }

    async fn rename_group_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterGroupConfigRecord> {
        let old_name = old_name.trim();
        let new_name = new_name.trim();
        if new_name.is_empty() {
            return Err(queryflux_core::error::QueryFluxError::Persistence(
                "New group name must not be empty".to_string(),
            ));
        }
        if old_name == new_name {
            return self.get_group_config(old_name).await?.ok_or_else(|| {
                queryflux_core::error::QueryFluxError::Persistence(format!(
                    "Group '{old_name}' not found"
                ))
            });
        }
        if self.group_configs.contains_key(new_name) {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Group name '{new_name}' is already in use"
            )));
        }
        let (_, mut record) = self.group_configs.remove(old_name).ok_or_else(|| {
            queryflux_core::error::QueryFluxError::Persistence(format!(
                "Group '{old_name}' not found"
            ))
        })?;
        let now = Utc::now();
        record.name = new_name.to_string();
        record.updated_at = now;
        self.group_configs
            .insert(new_name.to_string(), record.clone());
        Ok(record)
    }
}

// ---------------------------------------------------------------------------
// ProxySettingsStore — in-memory key-value store for proxy-level config
// ---------------------------------------------------------------------------

#[async_trait]
impl ProxySettingsStore for InMemoryPersistence {
    async fn get_proxy_setting(&self, key: &str) -> Result<Option<serde_json::Value>> {
        Ok(self.proxy_settings.read().unwrap().get(key).cloned())
    }

    async fn set_proxy_setting(&self, key: &str, value: serde_json::Value) -> Result<()> {
        self.proxy_settings
            .write()
            .unwrap()
            .insert(key.to_string(), value);
        Ok(())
    }

    async fn delete_proxy_setting(&self, key: &str) -> Result<()> {
        self.proxy_settings.write().unwrap().remove(key);
        Ok(())
    }
}

#[async_trait]
impl RoutingConfigStore for InMemoryPersistence {
    async fn load_routing_config(&self) -> Result<Option<LoadedRoutingConfig>> {
        Ok(None)
    }

    async fn replace_routing_config(
        &self,
        _routing_fallback: &str,
        _routing_fallback_group_id: Option<i64>,
        _routers: &[serde_json::Value],
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ScriptLibraryStore for InMemoryPersistence {
    async fn list_user_scripts(&self, kind: Option<&str>) -> Result<Vec<UserScriptRecord>> {
        let mut v: Vec<UserScriptRecord> = self
            .user_scripts
            .iter()
            .map(|e| e.value().clone())
            .filter(|r| kind.map(|k| r.kind == k).unwrap_or(true))
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    async fn get_user_script(&self, id: i64) -> Result<Option<UserScriptRecord>> {
        Ok(self.user_scripts.get(&id).map(|e| e.value().clone()))
    }

    async fn create_user_script(&self, body: &UpsertUserScript) -> Result<UserScriptRecord> {
        if !is_valid_script_kind(&body.kind) {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Invalid script kind '{}'",
                body.kind
            )));
        }
        if self
            .user_scripts
            .iter()
            .any(|e| e.value().name == body.name)
        {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Script name '{}' already exists",
                body.name
            )));
        }
        let id = self.next_script_id.fetch_add(1, Ordering::Relaxed);
        let now = Utc::now();
        let record = UserScriptRecord {
            id,
            name: body.name.clone(),
            description: body.description.clone(),
            kind: body.kind.clone(),
            body: body.body.clone(),
            created_at: now,
            updated_at: now,
        };
        self.user_scripts.insert(id, record.clone());
        Ok(record)
    }

    async fn update_user_script(
        &self,
        id: i64,
        body: &UpsertUserScript,
    ) -> Result<UserScriptRecord> {
        if !is_valid_script_kind(&body.kind) {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Invalid script kind '{}'",
                body.kind
            )));
        }
        if self
            .user_scripts
            .iter()
            .any(|e| *e.key() != id && e.value().name == body.name)
        {
            return Err(queryflux_core::error::QueryFluxError::Persistence(format!(
                "Script name '{}' already exists",
                body.name
            )));
        }
        let out = {
            let mut rm = self.user_scripts.get_mut(&id).ok_or_else(|| {
                queryflux_core::error::QueryFluxError::Persistence(format!(
                    "user script id {id} not found"
                ))
            })?;
            let r = rm.value_mut();
            r.name = body.name.clone();
            r.description = body.description.clone();
            r.kind = body.kind.clone();
            r.body = body.body.clone();
            r.updated_at = Utc::now();
            r.clone()
        };
        Ok(out)
    }

    async fn delete_user_script(&self, id: i64) -> Result<bool> {
        for mut e in self.group_configs.iter_mut() {
            e.value_mut().translation_script_ids.retain(|s| *s != id);
        }
        Ok(self.user_scripts.remove(&id).is_some())
    }
}

// ---------------------------------------------------------------------------
// Aggregation helpers
// ---------------------------------------------------------------------------

fn engine_stat_row(engine_type: String, rows: &[&QuerySummary]) -> EngineStatRow {
    let total = rows.len() as i64;
    let successful = rows.iter().filter(|r| r.status == "Success").count() as i64;
    let failed = rows.iter().filter(|r| r.status == "Failed").count() as i64;
    let cancelled = rows.iter().filter(|r| r.status == "Cancelled").count() as i64;
    let translated = rows.iter().filter(|r| r.was_translated).count() as i64;
    let total_rows = rows.iter().filter_map(|r| r.rows_returned).sum::<i64>();
    let exec_times: Vec<i64> = rows.iter().map(|r| r.execution_duration_ms).collect();
    let queue_times: Vec<i64> = rows.iter().map(|r| r.queue_duration_ms).collect();

    EngineStatRow {
        engine_type,
        total_queries: total,
        successful_queries: successful,
        failed_queries: failed,
        cancelled_queries: cancelled,
        avg_execution_ms: mean(&exec_times),
        min_execution_ms: exec_times.iter().copied().min().unwrap_or(0),
        max_execution_ms: exec_times.iter().copied().max().unwrap_or(0),
        avg_queue_ms: mean(&queue_times),
        translated_queries: translated,
        total_rows_returned: total_rows,
    }
}

fn group_stat_row(
    cluster_group: String,
    engine_type: String,
    rows: &[&QuerySummary],
) -> GroupStatRow {
    let total = rows.len() as i64;
    let successful = rows.iter().filter(|r| r.status == "Success").count() as i64;
    let failed = rows.iter().filter(|r| r.status == "Failed").count() as i64;
    let cancelled = rows.iter().filter(|r| r.status == "Cancelled").count() as i64;
    let translated = rows.iter().filter(|r| r.was_translated).count() as i64;
    let total_rows = rows.iter().filter_map(|r| r.rows_returned).sum::<i64>();
    let exec_times: Vec<i64> = rows.iter().map(|r| r.execution_duration_ms).collect();
    let queue_times: Vec<i64> = rows.iter().map(|r| r.queue_duration_ms).collect();

    GroupStatRow {
        cluster_group,
        engine_type,
        total_queries: total,
        successful_queries: successful,
        failed_queries: failed,
        cancelled_queries: cancelled,
        avg_execution_ms: mean(&exec_times),
        min_execution_ms: exec_times.iter().copied().min().unwrap_or(0),
        max_execution_ms: exec_times.iter().copied().max().unwrap_or(0),
        avg_queue_ms: mean(&queue_times),
        translated_queries: translated,
        total_rows_returned: total_rows,
    }
}

fn mean(values: &[i64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<i64>() as f64 / values.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClusterConfigStore;

    fn base_cluster_upsert() -> UpsertClusterConfig {
        UpsertClusterConfig {
            engine_key: "trino".to_string(),
            enabled: true,
            max_running_queries: None,
            config: serde_json::json!({"endpoint": "http://localhost:8080"}),
        }
    }

    fn group_upsert(tags: serde_json::Value) -> UpsertClusterGroupConfig {
        UpsertClusterGroupConfig {
            enabled: true,
            members: vec!["c1".to_string()],
            max_running_queries: 10,
            max_queued_queries: None,
            strategy: None,
            allow_groups: vec![],
            allow_users: vec![],
            translation_script_ids: vec![],
            default_tags: tags,
        }
    }

    async fn store_with_cluster() -> InMemoryPersistence {
        let store = InMemoryPersistence::new();
        store
            .upsert_cluster_config("c1", &base_cluster_upsert())
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn upsert_group_stores_default_tags() {
        let store = store_with_cluster().await;
        let tags = serde_json::json!({"env": "prod", "batch": null});
        let record = store
            .upsert_group_config("g1", &group_upsert(tags.clone()))
            .await
            .unwrap();

        assert_eq!(record.default_tags, tags);
        let fetched = store.get_group_config("g1").await.unwrap().unwrap();
        assert_eq!(fetched.default_tags, tags);
    }

    #[tokio::test]
    async fn upsert_group_empty_default_tags() {
        let store = store_with_cluster().await;
        let record = store
            .upsert_group_config("g1", &group_upsert(serde_json::json!({})))
            .await
            .unwrap();
        assert!(record.default_tags.as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn update_group_replaces_default_tags() {
        let store = store_with_cluster().await;

        store
            .upsert_group_config("g1", &group_upsert(serde_json::json!({"env": "staging"})))
            .await
            .unwrap();

        let updated = store
            .upsert_group_config(
                "g1",
                &group_upsert(serde_json::json!({"env": "prod", "team": "infra"})),
            )
            .await
            .unwrap();

        assert_eq!(updated.default_tags["env"], "prod");
        assert_eq!(updated.default_tags["team"], "infra");
        assert_eq!(updated.default_tags.as_object().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn to_core_propagates_tags_from_record() {
        let store = store_with_cluster().await;
        let tags = serde_json::json!({"team": "eng", "batch": null});
        let record = store
            .upsert_group_config("g1", &group_upsert(tags))
            .await
            .unwrap();

        let core = record.to_core();
        assert_eq!(
            core.default_tags.get("team"),
            Some(&Some("eng".to_string()))
        );
        assert_eq!(core.default_tags.get("batch"), Some(&None));
    }

    #[tokio::test]
    async fn list_groups_preserves_default_tags() {
        let store = store_with_cluster().await;
        store
            .upsert_group_config("g1", &group_upsert(serde_json::json!({"env": "prod"})))
            .await
            .unwrap();

        let list = store.list_group_configs().await.unwrap();
        let g = list.iter().find(|r| r.name == "g1").unwrap();
        assert_eq!(g.default_tags["env"], "prod");
    }
}
