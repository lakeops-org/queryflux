use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use queryflux_auth::{AuthProvider, AuthorizationChecker, BackendIdentityResolver};
use queryflux_cluster_manager::{cluster_state::ClusterState, ClusterGroupManager};
use queryflux_core::{
    config::ClusterConfig,
    params::QueryParams,
    query::{
        ClusterGroupName, ClusterName, EngineType, ExecutingQuery, FrontendProtocol, ProxyQueryId,
        QueryEngineStats, QueryStatus, QueuedQuery, SqlDialect,
    },
    session::{AgentContext, SessionContext},
    tags::QueryTags,
};
use queryflux_engine_adapters::AdapterKind;
use queryflux_fingerprint::{polyglot_dialect, rich_fingerprint};
use queryflux_guardrails::GuardChain;
use queryflux_metrics::{GuardAction, MetricsStore, QueryRecord};
use queryflux_persistence::{CapacityStore, Persistence, QueueCoordinator};
use queryflux_routing::chain::{RouterChain, RoutingTrace};
use queryflux_translation::TranslationService;

/// Everything that can be hot-reloaded from the DB without restarting the proxy.
///
/// Wrapped in `Arc<tokio::sync::RwLock<LiveConfig>>` inside `AppState` so
/// that any handler can cheaply read a consistent snapshot, and a background
/// task can atomically swap the whole bundle on each reload tick.
pub struct LiveConfig {
    pub router_chain: RouterChain,
    /// Global guard chain — runs for every query regardless of cluster group.
    /// `None` means no global guardrails are configured.
    pub guard_chain: Option<Arc<GuardChain>>,
    /// Per-group guard chains — appended after the global chain for queries routed
    /// to that group. Missing entry means no group-specific guards for that group.
    pub group_guard_chains: HashMap<String, Arc<GuardChain>>,
    pub cluster_manager: Arc<dyn ClusterGroupManager>,
    /// cluster_name → adapter (one adapter per physical cluster, shared across groups).
    pub adapters: HashMap<String, AdapterKind>,
    /// One `(adapter, ClusterState)` per physical cluster (first group membership wins).
    /// Used by background health / reconcile tasks so they track the **current** reload generation.
    pub health_check_targets: Vec<(AdapterKind, Arc<ClusterState>)>,
    /// Cluster configs keyed by cluster name — used by `BackendIdentityResolver` to
    /// look up `queryAuth` after a cluster is selected.
    pub cluster_configs: HashMap<String, ClusterConfig>,
    /// group_name → ordered list of cluster names in that group.
    pub group_members: HashMap<String, Vec<String>>,
    /// Ordered list of group names as they appear in config — used for authorization-aware
    /// first-fit when the router chain falls back to the static default.
    pub group_order: Vec<String>,
    /// group_name → ordered post-sqlglot Python fixup bodies (from `user_scripts` + group link).
    pub group_translation_scripts: HashMap<String, Vec<String>>,
    /// group_name → default tags configured on the group.
    /// Merged with session tags at dispatch time; session tags win on key conflicts.
    pub group_default_tags: HashMap<String, QueryTags>,
    /// group_name → optional `maxQueuedQueries` from ClusterGroupConfig.
    /// `None` / missing entry means unlimited. `Some(0)` is treated as unlimited
    /// (same as the enforce check requiring `limit > 0`).
    pub group_max_queued_queries: HashMap<String, Option<u64>>,
    /// group_name → resolved capacity wait timeout in seconds (default 300).
    pub group_capacity_wait_timeout_secs: HashMap<String, u64>,
    /// group_name → cache settings for groups that have caching enabled.
    pub group_cache_settings: HashMap<String, queryflux_core::config::GroupCacheConfig>,
    /// Verifies client identity — hot-reloaded when security config changes via admin API.
    pub auth_provider: Arc<dyn AuthProvider>,
    /// Checks whether an authenticated user may access a cluster group — hot-reloaded
    /// when security config changes via admin API.
    pub authorization: Arc<dyn AuthorizationChecker>,
}

/// Shared application state — passed to every handler via `axum::extract::State`.
/// Shared across all frontend protocol implementations (Trino HTTP, PG wire, etc.).
pub struct AppState {
    /// The external URL clients use to reach QueryFlux (used for nextUri rewriting).
    pub external_address: String,
    /// Hot-reloadable: routing rules, cluster registry, auth, authorization.
    pub live: Arc<tokio::sync::RwLock<LiveConfig>>,
    // Static (never reloaded):
    pub persistence: Arc<dyn Persistence>,
    pub translation: Arc<TranslationService>,
    pub metrics: Arc<dyn MetricsStore>,
    /// Resolves per-user `QueryCredentials` from `AuthContext` + cluster `queryAuth` config.
    pub identity_resolver: Arc<BackendIdentityResolver>,
    /// Global cluster capacity coordination — ensures `max_running_queries` is enforced
    /// across all replicas, not just per-process. `None` in InMemory mode (local atomics
    /// are the source of truth).
    pub capacity_store: Option<Arc<dyn CapacityStore>>,
    /// Prevents multiple replicas from dequeuing the same queued query. `None` in InMemory mode.
    pub queue_coordinator: Option<Arc<dyn QueueCoordinator>>,
    /// Unique identifier for this replica instance, used for capacity leases and queue claims.
    pub instance_id: String,
    /// Shared HTTP client for backend-facing fire-and-forget calls (e.g. cancel forwarding).
    /// Pre-configured with a 30-second timeout; reusing the client avoids per-request
    /// connection-pool churn.
    pub http_client: reqwest::Client,
    /// Query result cache — `NoopResultCache` when no cache backend is configured.
    pub result_cache: Arc<dyn queryflux_cache::QueryResultCache>,
}

/// Stable per-query metadata that does not change across the query's lifecycle.
/// Built once (after cluster selection and SQL translation) and passed to every
/// `record_query` call within the same dispatch function.
#[derive(Clone)]
pub struct QueryContext {
    pub query_id: ProxyQueryId,
    pub sql: String,
    pub session: SessionContext,
    pub protocol: FrontendProtocol,
    pub group: ClusterGroupName,
    pub cluster: ClusterName,
    pub cluster_group_config_id: Option<i64>,
    pub cluster_config_id: Option<i64>,
    pub engine_type: EngineType,
    pub src_dialect: SqlDialect,
    pub tgt_dialect: SqlDialect,
    pub was_translated: bool,
    pub translated_sql: Option<String>,
    pub query_tags: QueryTags,
    /// Typed positional parameters extracted from the client's wire protocol.
    /// Empty when the query is not parameterized.
    pub query_params: QueryParams,
    /// Agent identity — present when the client sends `X-Agent-Id` / `X-Conversation-Id`.
    pub agent_context: Option<AgentContext>,
}

/// How the query ended — the fields that vary between success, failure, and cancellation.
pub struct QueryOutcome {
    /// Backend engine query ID (Trino query ID, Athena execution ID, etc.).
    pub backend_query_id: Option<String>,
    pub status: QueryStatus,
    pub execution_ms: u64,
    pub rows: Option<u64>,
    pub error: Option<String>,
    pub routing_trace: Option<RoutingTrace>,
    pub engine_stats: Option<QueryEngineStats>,
    /// All guards that evaluated this query and their verdicts.
    pub guard_actions: Vec<GuardAction>,
    /// True if any guard returned Deny — fast filter for Studio.
    pub was_guard_blocked: bool,
    /// Milliseconds spent waiting in the proxy queue before dispatch.
    /// Zero for queries that were dispatched immediately.
    pub queue_duration_ms: u64,
    /// True when the result was served from the query result cache.
    pub cache_hit: bool,
}

impl AppState {
    pub async fn adapter(&self, cluster: &str) -> Option<AdapterKind> {
        self.live.read().await.adapters.get(cluster).cloned()
    }

    pub async fn cluster_config_cloned(&self, cluster: &str) -> Option<ClusterConfig> {
        self.live.read().await.cluster_configs.get(cluster).cloned()
    }

    /// Returns true if any cluster in the group supports async execution (e.g. Trino).
    pub async fn group_supports_async(&self, group: &str) -> bool {
        let live = self.live.read().await;
        live.group_members
            .get(group)
            .map(|members| {
                members
                    .iter()
                    .any(|name| matches!(live.adapters.get(name), Some(AdapterKind::Async(_))))
            })
            .unwrap_or(false)
    }

    /// Release a query's cluster capacity slot — local counter, global Postgres lease,
    /// and metrics. Call this **once** at every terminal path (success, failure, cancel,
    /// error) instead of manually calling `release_cluster` + `capacity_store.release`
    /// + `on_query_finished` separately.
    pub async fn release_query_slot(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
        query_id: &str,
    ) {
        self.metrics.on_query_finished(&group.0, &cluster.0);
        let cluster_manager = self.live.read().await.cluster_manager.clone();
        let _ = cluster_manager.release_cluster(group, cluster).await;
        if let Some(cap) = &self.capacity_store {
            if let Err(e) = cap.release(&cluster.0, query_id).await {
                self.metrics.on_coordination_failure("capacity_release");
                tracing::warn!("CapacityStore release failed for query {query_id}: {e}");
            }
        }
    }

    /// Fire-and-forget: build a `QueryRecord` and write it to the metrics store asynchronously.
    /// Called once per query at completion (success, failure, or cancellation).
    pub fn record_query(&self, ctx: &QueryContext, outcome: QueryOutcome) {
        // Capture what we need for rich fingerprinting before moving into the spawn.
        let original_sql = ctx.sql.to_owned();
        let translated_sql_for_fp = ctx.translated_sql.clone();
        let src_dialect = polyglot_dialect(&ctx.src_dialect);
        let tgt_dialect = polyglot_dialect(&ctx.tgt_dialect);

        let agent_id = ctx.agent_context.as_ref().map(|a| a.agent_id.clone());
        let conversation_id = ctx
            .agent_context
            .as_ref()
            .map(|a| a.conversation_id.clone());
        let step_index = ctx
            .agent_context
            .as_ref()
            .and_then(|a| a.step_index)
            .map(|s| i32::try_from(s).unwrap_or(i32::MAX));
        let tool_call_id = ctx
            .agent_context
            .as_ref()
            .and_then(|a| a.tool_call_id.clone());
        let query_intent = ctx
            .agent_context
            .as_ref()
            .map(|a| a.query_intent.as_str().to_string());

        let mut record = QueryRecord {
            proxy_query_id: ctx.query_id.0.clone(),
            backend_query_id: outcome.backend_query_id,
            cluster_group: ctx.group.clone(),
            cluster_name: ctx.cluster.clone(),
            cluster_group_config_id: ctx.cluster_group_config_id,
            cluster_config_id: ctx.cluster_config_id,
            engine_type: ctx.engine_type.clone(),
            frontend_protocol: ctx.protocol.clone(),
            source_dialect: ctx.src_dialect.clone(),
            target_dialect: ctx.tgt_dialect.clone(),
            was_translated: ctx.was_translated,
            translated_sql: ctx.translated_sql.clone(),
            user: ctx.session.user().map(|s| s.to_string()),
            catalog: ctx.session.database().map(|s| s.to_string()),
            database: None,
            sql_preview: ctx.sql.chars().take(500).collect(),
            status: outcome.status,
            routing_trace: outcome
                .routing_trace
                .as_ref()
                .and_then(|t| serde_json::to_value(t).ok()),
            queue_duration_ms: outcome.queue_duration_ms,
            execution_duration_ms: outcome.execution_ms,
            rows_returned: outcome.rows,
            error_message: outcome.error,
            created_at: Utc::now(),
            engine_stats: outcome.engine_stats,
            query_tags: ctx.query_tags.clone(),
            query_hash: None,
            query_parameterized_hash: None,
            translated_query_hash: None,
            digest_text: None,
            translated_digest_text: None,
            agent_id,
            conversation_id,
            step_index,
            tool_call_id,
            query_intent,
            guard_actions: outcome.guard_actions,
            was_guard_blocked: outcome.was_guard_blocked,
            cache_hit: outcome.cache_hit,
        };
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            if let Some(fp) = rich_fingerprint(
                &original_sql,
                translated_sql_for_fp.as_deref(),
                src_dialect.as_str(),
                tgt_dialect.as_str(),
            ) {
                record.query_hash = Some(fp.query_hash as i64);
                record.query_parameterized_hash = Some(fp.query_parameterized_hash as i64);
                record.translated_query_hash = fp.translated_query_hash.map(|h| h as i64);
                record.digest_text = Some(fp.digest_text);
                record.translated_digest_text = fp.translated_digest_text;
            }
            let _ = metrics.record_query(record).await;
        });
    }

    /// Persist a `Cancelled` audit row for an executing query (client cancel, admin
    /// cancel, or zombie eviction). Prefer calling this before deleting the
    /// executing record so a raced poll completion still wins if it recorded first.
    pub fn record_executing_cancelled(
        &self,
        executing: &ExecutingQuery,
        protocol: FrontendProtocol,
        engine_type: EngineType,
        tgt_dialect: SqlDialect,
        reason: &str,
    ) {
        let was_translated = executing.translated_sql.is_some();
        let execution_ms = (Utc::now() - executing.creation_time)
            .num_milliseconds()
            .max(0) as u64;
        let guard_actions: Vec<GuardAction> = serde_json::from_value(serde_json::Value::Array(
            executing.submitted_guard_actions.clone(),
        ))
        .unwrap_or_default();
        let session = SessionContext {
            user: (!executing.submitted_by.is_empty()).then(|| executing.submitted_by.clone()),
            ..Default::default()
        };
        let src_dialect = protocol.default_dialect();
        let ctx = QueryContext {
            query_id: executing.id.clone(),
            sql: executing
                .translated_sql
                .as_deref()
                .unwrap_or(&executing.sql)
                .to_string(),
            session,
            protocol,
            group: executing.cluster_group.clone(),
            cluster: executing.cluster_name.clone(),
            cluster_group_config_id: executing.cluster_group_config_id,
            cluster_config_id: executing.cluster_config_id,
            engine_type,
            src_dialect,
            tgt_dialect,
            was_translated,
            translated_sql: if was_translated {
                Some(executing.sql.clone())
            } else {
                None
            },
            query_tags: executing.query_tags.clone(),
            query_params: vec![],
            agent_context: executing.agent_context.clone(),
        };
        self.record_query(
            &ctx,
            QueryOutcome {
                backend_query_id: Some(executing.backend_query_id.0.clone()),
                status: QueryStatus::Cancelled,
                execution_ms,
                rows: None,
                error: Some(reason.to_string()),
                routing_trace: None,
                engine_stats: None,
                guard_actions,
                was_guard_blocked: executing.was_guard_blocked,
                queue_duration_ms: 0,
                cache_hit: false,
            },
        );
    }

    /// Persist a terminal audit row for a queued query that never reached an engine
    /// (capacity wait timeout, stale client disconnect).
    pub fn record_queued_terminal(&self, queued: &QueuedQuery, status: QueryStatus, reason: &str) {
        let queue_duration_ms = (Utc::now() - queued.creation_time)
            .num_milliseconds()
            .max(0) as u64;
        let session = {
            let mut s = queued.session.clone();
            if s.user.is_none() && !queued.submitted_by.is_empty() {
                s.user = Some(queued.submitted_by.clone());
            }
            s
        };
        let dialect = queued.frontend_protocol.default_dialect();
        let ctx = QueryContext {
            query_id: queued.id.clone(),
            sql: queued.sql.clone(),
            session,
            protocol: queued.frontend_protocol.clone(),
            group: queued.cluster_group.clone(),
            cluster: ClusterName("(not-dispatched)".into()),
            cluster_group_config_id: None,
            cluster_config_id: None,
            engine_type: EngineType::Trino,
            src_dialect: dialect.clone(),
            tgt_dialect: dialect,
            was_translated: false,
            translated_sql: None,
            query_tags: QueryTags::default(),
            query_params: vec![],
            agent_context: None,
        };
        self.record_query(
            &ctx,
            QueryOutcome {
                backend_query_id: None,
                status,
                execution_ms: 0,
                rows: None,
                error: Some(reason.to_string()),
                routing_trace: None,
                engine_stats: None,
                guard_actions: vec![],
                was_guard_blocked: false,
                queue_duration_ms,
                cache_hit: false,
            },
        );
    }
}

#[cfg(test)]
pub mod test_fixtures {
    use std::collections::HashMap;
    use std::sync::Arc;

    use queryflux_auth::{
        AllowAllAuthorization, AuthProvider, AuthorizationChecker, BackendIdentityResolver,
        NoneAuthProvider,
    };
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
    };
    use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
    use queryflux_metrics::NoopMetricsStore;
    use queryflux_persistence::in_memory::InMemoryPersistence;
    use queryflux_routing::chain::RouterChain;
    use queryflux_translation::TranslationService;
    use tokio::sync::RwLock;

    use super::{AppState, LiveConfig};

    pub fn app_state(auth_required: bool) -> Arc<AppState> {
        let group_name = ClusterGroupName("default".into());
        let cluster_name = ClusterName("trino".into());
        let cluster_state = Arc::new(ClusterState::new(
            cluster_name.clone(),
            group_name.clone(),
            None,
            None,
            EngineType::Trino,
            Some("http://trino.test:8080".into()),
            10,
            true,
        ));
        let mut group_members = HashMap::new();
        group_members.insert(group_name.0.clone(), vec![cluster_name.0.clone()]);
        let mut groups = HashMap::new();
        groups.insert(
            group_name.clone(),
            (
                vec![cluster_state],
                Arc::new(queryflux_cluster_manager::strategy::RoundRobinStrategy::new())
                    as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
            ),
        );
        let live = LiveConfig {
            router_chain: RouterChain::new(vec![], group_name.clone()),
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager: Arc::new(SimpleClusterGroupManager::new(groups)),
            adapters: HashMap::new(),
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members,
            group_order: vec![group_name.0.clone()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_max_queued_queries: HashMap::new(),
            group_capacity_wait_timeout_secs: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(NoneAuthProvider::new(auth_required)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization::default())
                as Arc<dyn AuthorizationChecker>,
        };
        Arc::new(AppState {
            external_address: "http://127.0.0.1:8080".into(),
            live: Arc::new(RwLock::new(live)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation: Arc::new(TranslationService::disabled()),
            metrics: Arc::new(NoopMetricsStore),
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            capacity_store: None,
            queue_coordinator: None,
            instance_id: "test".into(),
            http_client: reqwest::Client::new(),
            result_cache: Arc::new(queryflux_cache::noop::NoopResultCache),
        })
    }
}
