pub mod cluster_config;
pub mod in_memory;
pub mod metrics_store;
pub mod postgres;
pub mod query_history;
pub mod routing_json;
pub mod routing_slices;
pub mod script_library;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::{
    error::Result,
    query::{BackendQueryId, ExecutingQuery, ProxyQueryId, QueuedQuery},
};

use crate::{
    cluster_config::{
        ClusterConfigRecord, ClusterGroupConfigRecord, UpsertClusterConfig,
        UpsertClusterGroupConfig,
    },
    query_history::{DashboardStats, EngineStatRow, GroupStatRow, QueryFilters},
};

// Re-export so callers can do `queryflux_persistence::MetricsStore` etc.
pub use metrics_store::{ClusterSnapshot, MetricsStore, QueryRecord};
// GuardAction now lives in queryflux-core; re-export for backward compatibility.
pub use query_history::{AgentSummary, ConversationSummary, QuerySummary};
pub use queryflux_core::query::GuardAction;
pub use script_library::{
    is_valid_script_kind, UpsertUserScript, UserScriptRecord, KIND_GUARD, KIND_ROUTING,
    KIND_TRANSLATION_FIXUP,
};

// ---------------------------------------------------------------------------
// Persistence — in-flight query state
// ---------------------------------------------------------------------------

/// Handles short-lived query state: queries currently executing on a backend
/// engine, and queries waiting in the proxy's queue for cluster capacity.
///
/// Every persistence backend (Postgres, Redis, in-memory) must implement this.
#[async_trait]
pub trait Persistence: Send + Sync {
    // --- Executing queries (submitted to an engine backend) ---
    async fn upsert(&self, query: ExecutingQuery) -> Result<()>;
    async fn get(&self, id: &BackendQueryId) -> Result<Option<ExecutingQuery>>;
    async fn delete(&self, id: &BackendQueryId) -> Result<()>;
    async fn list_all(&self) -> Result<Vec<ExecutingQuery>>;

    // --- Queued queries (waiting for cluster capacity) ---
    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()>;
    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>>;
    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()>;
    async fn list_queued(&self) -> Result<Vec<QueuedQuery>>;

    /// Delete all queued queries whose `last_accessed` is older than `cutoff`.
    async fn delete_queued_not_accessed_since(&self, cutoff: DateTime<Utc>) -> Result<u64>;
}

// ---------------------------------------------------------------------------
// MetricsStore — write-side query history (re-exported from metrics_store mod)
// ---------------------------------------------------------------------------
//
// `MetricsStore`, `QueryRecord`, and `ClusterSnapshot` live in
// `queryflux_persistence::metrics_store` and are re-exported above.
// `queryflux-metrics` re-exports them from here so existing call sites
// (`use queryflux_metrics::MetricsStore`) continue to compile unchanged.

// ---------------------------------------------------------------------------
// QueryHistoryStore — read-side analytics for the admin UI
// ---------------------------------------------------------------------------

/// Read access to the historical query record log.
///
/// Any persistence backend that wants to power the admin Studio UI (query
/// history page, dashboard stats, engine/group breakdowns) must implement this.
#[async_trait]
pub trait QueryHistoryStore: Send + Sync {
    /// Paginated, filterable list of past queries — newest first.
    async fn list_queries(&self, filters: &QueryFilters) -> Result<Vec<QuerySummary>>;

    /// Aggregated stats for the last hour (used by the dashboard).
    async fn get_dashboard_stats(&self) -> Result<DashboardStats>;

    /// Per-engine aggregated stats over the last `hours` hours.
    async fn get_engine_stats(&self, hours: i64) -> Result<Vec<EngineStatRow>>;

    /// Per-cluster-group aggregated stats over the last `hours` hours.
    async fn get_group_stats(&self, hours: i64) -> Result<Vec<GroupStatRow>>;

    /// Distinct engine type strings that appear in the query log.
    async fn list_engines(&self) -> Result<Vec<String>>;

    /// Distinct agents that have run queries, with aggregate stats.
    async fn list_agents(&self, limit: i64, offset: i64) -> Result<Vec<AgentSummary>>;

    /// Conversations for a given agent (or all agents if `agent_id` is None), paginated.
    async fn list_conversations(
        &self,
        agent_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ConversationSummary>>;

    /// All query records belonging to a conversation, ordered by step_index.
    async fn get_conversation(&self, conversation_id: &str) -> Result<Vec<QuerySummary>>;
}

// ---------------------------------------------------------------------------
// ClusterConfigStore — persisted cluster / group configuration CRUD
// ---------------------------------------------------------------------------

/// Full CRUD for cluster and cluster-group configuration records.
///
/// When Postgres persistence is configured, QueryFlux reads cluster/group
/// config from this store instead of the YAML file.  The YAML is only used to
/// seed on the very first run (when both tables are empty).
///
/// Any persistence backend that wants to support runtime config management
/// must implement this.
#[async_trait]
pub trait ClusterConfigStore: Send + Sync {
    // --- Cluster configs ---
    async fn list_cluster_configs(&self) -> Result<Vec<ClusterConfigRecord>>;
    async fn get_cluster_config(&self, name: &str) -> Result<Option<ClusterConfigRecord>>;
    async fn upsert_cluster_config(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<ClusterConfigRecord>;
    /// Deletes the cluster row and removes its id from every group's `members` array
    /// (Postgres) or drops its name from each group's member list (in-memory).
    async fn delete_cluster_config(&self, name: &str) -> Result<bool>;
    /// Returns the number of stored cluster configs (used for first-run seeding).
    async fn cluster_configs_count(&self) -> Result<i64>;
    /// Rename a cluster row. The stable `id` is unchanged; group `members` arrays store ids and need no update.
    async fn rename_cluster_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterConfigRecord>;

    // --- Cluster group configs ---
    async fn list_group_configs(&self) -> Result<Vec<ClusterGroupConfigRecord>>;
    async fn get_group_config(&self, name: &str) -> Result<Option<ClusterGroupConfigRecord>>;
    async fn upsert_group_config(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<ClusterGroupConfigRecord>;
    async fn delete_group_config(&self, name: &str) -> Result<bool>;
    /// Returns the number of stored group configs (used for first-run seeding).
    async fn group_configs_count(&self) -> Result<i64>;
    /// Rename a cluster group. `routing_settings.routing_fallback` is updated when it matched the old name.
    async fn rename_group_config(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<ClusterGroupConfigRecord>;
}

// ---------------------------------------------------------------------------
// ScriptLibraryStore — reusable Python snippets (translation / routing)
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ScriptLibraryStore: Send + Sync {
    async fn list_user_scripts(&self, kind: Option<&str>) -> Result<Vec<UserScriptRecord>>;
    async fn get_user_script(&self, id: i64) -> Result<Option<UserScriptRecord>>;
    async fn create_user_script(&self, body: &UpsertUserScript) -> Result<UserScriptRecord>;
    async fn update_user_script(
        &self,
        id: i64,
        body: &UpsertUserScript,
    ) -> Result<UserScriptRecord>;
    async fn delete_user_script(&self, id: i64) -> Result<bool>;
}

// ---------------------------------------------------------------------------
// ProxySettingsStore — persisted security (auth / authz) overrides
// ---------------------------------------------------------------------------

/// Key-value-style API for security overrides; Postgres backs `security_config` only.
///
/// Keys: `"security_config"` only. Routing lives in [`RoutingConfigStore`] / `routing_rules`.
///
/// When Postgres persistence is configured, QueryFlux reads `security_config` at startup
/// to override the YAML config — same pattern as cluster/group configs.
#[async_trait]
pub trait ProxySettingsStore: Send + Sync {
    async fn get_proxy_setting(&self, key: &str) -> Result<Option<serde_json::Value>>;
    async fn set_proxy_setting(&self, key: &str, value: serde_json::Value) -> Result<()>;
    async fn delete_proxy_setting(&self, key: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// RoutingConfigStore — routing fallback + one JSON row per router
// ---------------------------------------------------------------------------

/// Persisted routing configuration (replaces the old `routing_config` JSON blob).
///
/// - [`Self::load_routing_config`] returns [`None`] when `routing_persist_active` is false
///   (never saved from the admin UI / not migrated from legacy), so YAML remains authoritative.
/// - [`Self::replace_routing_config`] writes one `routing_rules` row per router in order.
#[derive(Debug, Clone)]
pub struct LoadedRoutingConfig {
    pub routing_fallback: String,
    pub routing_fallback_group_id: Option<i64>,
    pub routers: Vec<serde_json::Value>,
}

#[async_trait]
pub trait RoutingConfigStore: Send + Sync {
    /// `None` = do not override YAML routing (fresh DB or never persisted).
    async fn load_routing_config(&self) -> Result<Option<LoadedRoutingConfig>>;

    /// Replaces all router rows and sets the fallback. Marks persistence active.
    async fn replace_routing_config(
        &self,
        routing_fallback: &str,
        routing_fallback_group_id: Option<i64>,
        routers: &[serde_json::Value],
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// AdminStore — combined super-trait used by the admin frontend
// ---------------------------------------------------------------------------

/// Combined interface required by the admin REST API.
///
/// Any persistence backend that wants to fully power the Studio admin UI must
/// implement both `QueryHistoryStore` and `ClusterConfigStore`.  Using a
/// supertrait here means `AdminFrontend` only needs one `Arc<dyn AdminStore>`
/// and the compiler enforces that every method group is present.
pub trait AdminStore:
    QueryHistoryStore
    + ClusterConfigStore
    + ScriptLibraryStore
    + ProxySettingsStore
    + RoutingConfigStore
    + Send
    + Sync
{
}

/// Blanket implementation: any type that satisfies both component traits
/// automatically satisfies `AdminStore`, so implementors only need the two.
impl<
        T: QueryHistoryStore
            + ClusterConfigStore
            + ScriptLibraryStore
            + ProxySettingsStore
            + RoutingConfigStore
            + Send
            + Sync,
    > AdminStore for T
{
}

// ---------------------------------------------------------------------------
// BackendStore — full contract for a complete persistence backend
// ---------------------------------------------------------------------------

/// The complete interface a persistence backend must satisfy to replace Postgres.
///
/// Covers all responsibilities:
/// - `Persistence`         — in-flight query state (executing + queued)
/// - `MetricsStore`        — writing completed query records and cluster snapshots
/// - `QueryHistoryStore`   — reading analytics for the admin UI
/// - `ClusterConfigStore`  — CRUD for cluster / cluster-group configuration
/// - `ProxySettingsStore`  — persisted security / auth overrides (`security_config`)
/// - `RoutingConfigStore`  — routing fallback + `routing_rules` rows (slices + `target_group_id`)
///
/// The blanket impl below means you only need to implement the component traits
/// and you automatically satisfy `BackendStore` — no extra code required.
pub trait BackendStore:
    Persistence
    + MetricsStore
    + QueryHistoryStore
    + ClusterConfigStore
    + ScriptLibraryStore
    + ProxySettingsStore
    + RoutingConfigStore
    + Send
    + Sync
{
}

impl<
        T: Persistence
            + MetricsStore
            + QueryHistoryStore
            + ClusterConfigStore
            + ScriptLibraryStore
            + ProxySettingsStore
            + RoutingConfigStore
            + Send
            + Sync,
    > BackendStore for T
{
}
