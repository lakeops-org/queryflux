pub mod cache_store;
pub mod cluster_config;
pub mod in_memory;
pub mod metrics_store;
pub mod postgres;
pub mod query_history;
pub mod routing_json;
pub mod routing_slices;
pub mod script_library;

use std::collections::HashMap;

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
pub use cache_store::{CacheEntryMeta, CacheEntryRef, CacheStore};
pub use metrics_store::{ClusterSnapshot, GuardAction, MetricsStore, QueryRecord};
pub use query_history::{AgentSummary, ConversationSummary, QuerySummary};
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

    /// Bump `last_accessed` to now for a queued query, keeping it alive in the
    /// fairness gate's activity window. Must be called on every client poll.
    async fn touch_queued_last_accessed(&self, id: &ProxyQueryId) -> Result<()>;

    /// Delete all queued queries whose `last_accessed` is older than `cutoff`.
    async fn delete_queued_not_accessed_since(&self, cutoff: DateTime<Utc>) -> Result<u64>;

    /// Number of queued queries for `cluster_group` that are still actively
    /// polling (`last_accessed >= active_after`) and were enqueued strictly
    /// before `enqueued_before` (`None` treats the caller as the newest
    /// possible, so every active waiter counts).
    ///
    /// Powers the admission fairness gate: a query may only take a freed slot
    /// when the group's free capacity exceeds the number of older waiters.
    /// The activity window is what prevents head-of-line blocking — a client
    /// that stopped polling drops out of the count within seconds instead of
    /// holding the queue hostage until stale-queue cleanup.
    async fn count_active_queued_before(
        &self,
        cluster_group: &str,
        enqueued_before: Option<DateTime<Utc>>,
        active_after: DateTime<Utc>,
    ) -> Result<u64>;
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

    /// Delete all query records created before `older_than` (history retention).
    /// Returns the number of records deleted.
    async fn purge_old_query_records(&self, older_than: DateTime<Utc>) -> Result<u64>;
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

    /// Ordered translation-fixup script bodies per cluster group name, resolved
    /// from each group's `translation_script_ids` (for `LiveConfig` reloads).
    async fn load_group_translation_bodies(&self) -> Result<HashMap<String, Vec<String>>>;
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
// ConfigRevisionStore — distributed config change notification
// ---------------------------------------------------------------------------

/// Tracks a monotonically increasing config revision so multiple QueryFlux
/// replicas can detect when shared configuration has changed.
///
/// Every admin write that mutates persisted config (clusters, groups, routing,
/// scripts, guardrails, security) must call [`Self::bump_revision`] so that
/// other instances can detect the change via polling or push notification.
///
/// Backends that support push (e.g. Postgres `LISTEN/NOTIFY`, Redis pub/sub)
/// implement [`Self::subscribe_revisions`] to return a live stream. Backends
/// that cannot push (e.g. in-memory) return `None` and callers fall back to
/// periodic polling via [`Self::current_revision`].
#[async_trait]
pub trait ConfigRevisionStore: Send + Sync {
    /// Read the current global config revision.
    async fn current_revision(&self) -> Result<u64>;

    /// Atomically increment the revision and return the new value.
    /// Must be called inside (or immediately after) every admin write.
    async fn bump_revision(&self) -> Result<u64>;

    /// Return a receiver that yields the new revision each time it changes.
    /// `None` means push is not supported; callers should poll
    /// [`Self::current_revision`] on a timer instead.
    async fn subscribe_revisions(&self) -> Result<Option<tokio::sync::mpsc::Receiver<u64>>>;
}

// ---------------------------------------------------------------------------
// CapacityStore — global cluster capacity coordination
// ---------------------------------------------------------------------------

/// Coordinates cluster capacity across multiple QueryFlux replicas.
///
/// In single-instance mode the in-memory implementation delegates to local
/// atomics (no overhead). In distributed mode the Postgres implementation
/// uses row-level locking or atomic updates so that `max_running_queries`
/// is enforced globally, not per-replica.
#[async_trait]
pub trait CapacityStore: Send + Sync {
    /// Atomically acquire a capacity slot for `cluster_name` if the current
    /// global lease count is below `max_running_queries`. Returns `true` if
    /// the slot was granted.
    ///
    /// The caller passes the effective limit (cluster override or inherited
    /// group limit, as resolved in its hot-reloaded config) rather than the
    /// store re-deriving it — the local `ClusterState` is the one place that
    /// inheritance is already applied. Pass `u64::MAX` for unlimited.
    ///
    /// `instance_id` identifies the calling replica (for stale-lease expiry).
    /// `query_id` ties the slot to a specific query (for release).
    async fn try_acquire(
        &self,
        cluster_name: &str,
        max_running_queries: u64,
        instance_id: &str,
        query_id: &str,
    ) -> Result<bool>;

    /// Release a previously acquired capacity slot.
    async fn release(&self, cluster_name: &str, query_id: &str) -> Result<()>;

    /// Renew the heartbeat on every lease held by `instance_id` so that
    /// [`Self::expire_stale`] does not reclaim slots of long-running queries
    /// on live replicas. Each replica must call this on a timer well inside
    /// the expiry cutoff. Returns the number of leases renewed.
    async fn heartbeat(&self, instance_id: &str) -> Result<u64>;

    /// Reclaim slots whose owning instance has not heartbeated since `cutoff`.
    async fn expire_stale(&self, cutoff: DateTime<Utc>) -> Result<u64>;

    /// Current number of active (non-expired) slots for a cluster.
    async fn active_count(&self, cluster_name: &str) -> Result<u64>;

    /// Release all capacity leases held by `instance_id`. Called during graceful
    /// shutdown so the departing replica's slots are immediately available to
    /// other replicas instead of waiting for the stale-lease expiry sweep.
    async fn release_all_for_instance(&self, instance_id: &str) -> Result<u64>;
}

// ---------------------------------------------------------------------------
// QueueCoordinator — single-owner queued query claiming
// ---------------------------------------------------------------------------

/// Prevents multiple replicas from dequeuing and executing the same queued
/// query. A replica calls [`Self::try_claim`] to atomically take ownership;
/// other replicas see the query as claimed and skip it.
///
/// In single-instance mode the in-memory implementation always grants the
/// claim (there is no contention).
#[async_trait]
pub trait QueueCoordinator: Send + Sync {
    /// Atomically claim an unclaimed queued query for this instance.
    /// Returns the query if the claim succeeded, `None` if it was already
    /// claimed by another instance or does not exist.
    ///
    /// A claim older than `stale_before` is treated as abandoned (the claiming
    /// replica crashed or never finished dispatch) and may be taken over.
    /// Claims are only held for the duration of a single dispatch attempt, so
    /// callers should pass a cutoff of now minus a small multiple of the
    /// expected dispatch time.
    async fn try_claim(
        &self,
        query_id: &str,
        instance_id: &str,
        stale_before: DateTime<Utc>,
    ) -> Result<Option<QueuedQuery>>;

    /// Release a claim without executing (e.g. capacity still unavailable
    /// or the claiming instance is shutting down).
    async fn release_claim(&self, query_id: &str) -> Result<()>;

    /// List queued queries that are not currently claimed by any instance.
    /// Claims older than `stale_before` count as unclaimed.
    async fn list_unclaimed(&self, stale_before: DateTime<Utc>) -> Result<Vec<QueuedQuery>>;
}

// ---------------------------------------------------------------------------
// SweepCoordinator — single-owner background sweeps
// ---------------------------------------------------------------------------

/// Elects a single owner for a named background sweep (e.g. zombie-query
/// eviction) so it runs on one replica per cycle instead of all of them.
///
/// Implementations must guarantee a crashed owner cannot hold the lock
/// forever (Postgres: session advisory lock released with the connection;
/// Redis-style backends: a lock key with TTL). In single-instance mode the
/// in-memory implementation always grants ownership.
#[async_trait]
pub trait SweepCoordinator: Send + Sync {
    /// Try to become the single owner of the named sweep for this cycle.
    /// Returns `None` when another replica currently owns it.
    async fn try_sweep_lock(&self, name: &str) -> Result<Option<Box<dyn SweepGuard>>>;
}

/// Ownership handle for a sweep; call [`Self::release`] when the sweep ends.
///
/// Callers should treat `release` as the primary path and drop-without-release
/// as best-effort only — `Drop` is synchronous, so implementations cannot run
/// the async release there. Each backend needs its own drop story: the
/// Postgres guard spawns the unlock onto the current runtime when one exists
/// and otherwise relies on the session lock dying with its connection; a
/// Redis-style guard would lean on a TTL'd lock key expiring. Whatever the
/// mechanism, an unreleased guard must not hold the sweep forever.
#[async_trait]
pub trait SweepGuard: Send {
    async fn release(self: Box<Self>);
}

// ---------------------------------------------------------------------------
// BackendCapabilities — startup wiring decisions
// ---------------------------------------------------------------------------

/// Capability flags consulted when wiring the proxy at startup, so behavior
/// is keyed on what a backend can do rather than on which backend it is.
pub trait BackendCapabilities: Send + Sync {
    /// Whether this backend can coordinate multiple replicas: global capacity
    /// leases, single-owner queue claims, and cross-replica config revision
    /// notifications. Drives distributed-mode detection.
    fn supports_distributed_coordination(&self) -> bool;
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
    + ConfigRevisionStore
    + Send
    + Sync
{
}

/// Blanket implementation: any type that satisfies all component traits
/// automatically satisfies `AdminStore`, so implementors only need the components.
impl<
        T: QueryHistoryStore
            + ClusterConfigStore
            + ScriptLibraryStore
            + ProxySettingsStore
            + RoutingConfigStore
            + ConfigRevisionStore
            + Send
            + Sync,
    > AdminStore for T
{
}

// ---------------------------------------------------------------------------
// BackendStore — durable persistence contract (single- or multi-replica)
// ---------------------------------------------------------------------------

/// The interface a persistence backend must satisfy to replace Postgres for
/// query state, metrics, and the admin API (e.g. a future Redis-backed store).
///
/// Covers the core responsibilities:
/// - `Persistence`  — in-flight query state (executing + queued)
/// - `MetricsStore` — writing completed query records and cluster snapshots
/// - `AdminStore`   — admin API surface (history analytics, cluster/group
///   config CRUD, script library, proxy settings, routing, config revisions)
///
/// `AdminStore` is a supertrait (rather than its components) so that an
/// `Arc<dyn BackendStore>` upcasts to every narrower trait object the wiring
/// hands out (`Arc<dyn AdminStore>`, `Arc<dyn ProxySettingsStore>`, ...).
///
/// Multi-replica coordination (`CapacityStore`, `QueueCoordinator`,
/// `SweepCoordinator`, `BackendCapabilities`) lives on
/// [`DistributedBackendStore`]. Backends that only serve a single instance, or
/// that delegate coordination elsewhere, implement `BackendStore` alone.
///
/// **Migration:** if your backend previously implemented the distributed traits
/// only because `BackendStore` required them, drop those impls and keep
/// `BackendStore`. To power distributed mode, additionally implement the four
/// coordination traits (or their in-memory no-op variants for testing); the
/// blanket impl on [`DistributedBackendStore`] picks them up automatically.
/// Startup code type-erases distributed backends as
/// `Option<Arc<dyn DistributedBackendStore>>` separately from `BackendStore`.
pub trait BackendStore: Persistence + MetricsStore + AdminStore + CacheStore + Send + Sync {}

impl<T: Persistence + MetricsStore + AdminStore + CacheStore + Send + Sync> BackendStore for T {}

// ---------------------------------------------------------------------------
// DistributedBackendStore — multi-replica coordination (optional layer)
// ---------------------------------------------------------------------------

/// Extension of [`BackendStore`] for backends that can coordinate multiple
/// QueryFlux replicas (global capacity leases, single-owner queue claims,
/// sweep locks, and distributed-mode capability flags).
///
/// Wired only when present: startup holds `Option<Arc<dyn DistributedBackendStore>>`
/// alongside `Option<Arc<dyn BackendStore>>`. Postgres satisfies both; a
/// minimal custom backend can implement `BackendStore` alone and leave
/// distributed mode disabled.
///
/// The blanket impl means you only implement the component traits — no extra
/// `DistributedBackendStore` methods.
pub trait DistributedBackendStore:
    BackendStore
    + CapacityStore
    + QueueCoordinator
    + SweepCoordinator
    + BackendCapabilities
    + Send
    + Sync
{
}

impl<
        T: BackendStore
            + CapacityStore
            + QueueCoordinator
            + SweepCoordinator
            + BackendCapabilities
            + Send
            + Sync,
    > DistributedBackendStore for T
{
}
