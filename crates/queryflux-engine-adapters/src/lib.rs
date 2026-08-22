pub mod adbc;
pub mod athena;
pub mod clickhouse;
pub mod duckdb;
pub mod mysql_native;
pub mod starrocks;
pub mod trino;
pub mod wire_auth;

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use queryflux_core::{
    config::ClusterConfig,
    engine_registry::EngineDescriptor,
    error::Result,
    native_result::NativeResultChunk,
    query::{BackendQueryId, ClusterGroupName, ClusterName, FrontendProtocol},
};

/// Implemented by each engine's typed config struct.
///
/// Parsing succeeds only when all required fields are present and valid for that engine —
/// construction IS validation. Engine-specific constraints (required fields, allowed auth types)
/// live here rather than in core.
pub trait EngineConfigParseable: Sized {
    /// Parse and validate from a DB config JSON blob.
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> Result<Self>;
    /// Parse and validate from a YAML-loaded [`ClusterConfig`].
    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> Result<Self>;
}

/// A stream of Arrow RecordBatches — the universal output type for all adapters.
pub type ArrowStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Slot an adapter fills with the engine-side query id as soon as it is known
/// — typically *before* the blocking wait — so dispatch can `cancel_query` if
/// the client disconnects mid-flight.
///
/// Cheap to clone (shared mutex). Publish is last-write-wins; `get` returns
/// the most recently published id, or `None` if the adapter never assigned one.
#[derive(Clone, Default)]
pub struct BackendQueryIdSlot {
    inner: std::sync::Arc<std::sync::Mutex<Option<BackendQueryId>>>,
}

impl BackendQueryIdSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the engine-side id. Safe to call from the execute task; dispatch
    /// reads it from a `Drop` guard on another task.
    pub fn publish(&self, id: impl Into<String>) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(BackendQueryId(id.into()));
    }

    pub fn get(&self) -> Option<BackendQueryId> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Returned by `SyncAdapter::execute_as_arrow`.
///
/// Carries both the result stream and a post-completion stats channel.
/// Drive `stream` to exhaustion before reading `stats` — adapters send stats
/// into the oneshot only after all batches have been produced.
///
/// `stats` resolves to `None` when the engine does not expose structured
/// execution statistics (CPU time, bytes scanned, etc.).
pub struct SyncExecution {
    /// Arrow RecordBatch stream — drive to completion before reading stats.
    pub stream: ArrowStream,
    /// Engine-reported execution stats. Sent by the adapter once the stream ends.
    pub stats: tokio::sync::oneshot::Receiver<Option<queryflux_core::query::QueryEngineStats>>,
}

/// What wire format this adapter natively produces, determined by its connection type.
///
/// The adapter declares one value based on how it is configured (which driver/pool it uses).
/// Dispatch compares this against the incoming frontend protocol — a match means the
/// Arrow intermediate can be skipped entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionFormat {
    /// Arrow `RecordBatch` stream — ADBC, DuckDB, in-process engines.
    Arrow,
    /// MySQL wire protocol via `mysql_async` pool — StarRocks, ClickHouse MySQL iface.
    MysqlWire,
    /// PostgreSQL wire protocol via `tokio_postgres` pool.
    PostgresWire,
    /// Trino HTTP JSON (opaque bytes, async submit-poll).
    TrinoHttp,
    /// ClickHouse HTTP (opaque bytes).
    ClickHouseHttp,
}

impl ConnectionFormat {
    /// Returns `true` if this backend format directly satisfies the given frontend protocol
    /// without any Arrow conversion.
    pub fn matches_frontend(&self, protocol: &FrontendProtocol) -> bool {
        matches!(
            (self, protocol),
            (ConnectionFormat::MysqlWire, FrontendProtocol::MySqlWire)
                | (
                    ConnectionFormat::PostgresWire,
                    FrontendProtocol::PostgresWire
                )
                | (ConnectionFormat::TrinoHttp, FrontendProtocol::TrinoHttp)
                | (
                    ConnectionFormat::ClickHouseHttp,
                    FrontendProtocol::ClickHouseHttp
                )
        )
    }
}

#[cfg(test)]
mod connection_format_tests {
    use super::*;
    use queryflux_core::query::FrontendProtocol;

    // ── matches (native path taken) ───────────────────────────────────────────

    #[test]
    fn mysql_wire_matches_mysql_wire() {
        assert!(ConnectionFormat::MysqlWire.matches_frontend(&FrontendProtocol::MySqlWire));
    }

    #[test]
    fn postgres_wire_matches_postgres_wire() {
        assert!(ConnectionFormat::PostgresWire.matches_frontend(&FrontendProtocol::PostgresWire));
    }

    #[test]
    fn arrow_does_not_match_flight_sql() {
        assert!(!ConnectionFormat::Arrow.matches_frontend(&FrontendProtocol::FlightSql));
    }

    #[test]
    fn trino_http_matches_trino_http() {
        assert!(ConnectionFormat::TrinoHttp.matches_frontend(&FrontendProtocol::TrinoHttp));
    }

    #[test]
    fn clickhouse_http_matches_clickhouse_http() {
        assert!(
            ConnectionFormat::ClickHouseHttp.matches_frontend(&FrontendProtocol::ClickHouseHttp)
        );
    }

    // ── no match (Arrow fallback taken) ──────────────────────────────────────

    #[test]
    fn mysql_wire_does_not_match_postgres_wire() {
        assert!(!ConnectionFormat::MysqlWire.matches_frontend(&FrontendProtocol::PostgresWire));
    }

    #[test]
    fn mysql_wire_does_not_match_flight_sql() {
        assert!(!ConnectionFormat::MysqlWire.matches_frontend(&FrontendProtocol::FlightSql));
    }

    #[test]
    fn mysql_wire_does_not_match_trino_http() {
        assert!(!ConnectionFormat::MysqlWire.matches_frontend(&FrontendProtocol::TrinoHttp));
    }

    #[test]
    fn arrow_does_not_match_mysql_wire() {
        assert!(!ConnectionFormat::Arrow.matches_frontend(&FrontendProtocol::MySqlWire));
    }

    #[test]
    fn arrow_does_not_match_postgres_wire() {
        assert!(!ConnectionFormat::Arrow.matches_frontend(&FrontendProtocol::PostgresWire));
    }

    #[test]
    fn trino_http_does_not_match_mysql_wire() {
        assert!(!ConnectionFormat::TrinoHttp.matches_frontend(&FrontendProtocol::MySqlWire));
    }

    #[test]
    fn postgres_wire_does_not_match_mysql_wire() {
        assert!(!ConnectionFormat::PostgresWire.matches_frontend(&FrontendProtocol::MySqlWire));
    }
}

#[cfg(test)]
mod backend_query_id_slot_tests {
    use super::BackendQueryIdSlot;

    #[test]
    fn empty_slot_returns_none() {
        assert!(BackendQueryIdSlot::new().get().is_none());
    }

    #[test]
    fn publish_is_visible_on_clones() {
        let slot = BackendQueryIdSlot::new();
        let clone = slot.clone();
        slot.publish("ch-query-1");
        assert_eq!(clone.get().unwrap().0, "ch-query-1");
    }

    #[test]
    fn last_publish_wins() {
        let slot = BackendQueryIdSlot::new();
        slot.publish("first");
        slot.publish("second");
        assert_eq!(slot.get().unwrap().0, "second");
    }
}

/// Returned by `SyncAdapter::execute_native` — a stream of protocol-agnostic row chunks.
pub struct NativeExecution {
    pub stream: Pin<Box<dyn Stream<Item = Result<NativeResultChunk>> + Send>>,
    pub stats: tokio::sync::oneshot::Receiver<Option<queryflux_core::query::QueryEngineStats>>,
}

/// Sync engines: execute to completion, stream Arrow results.
/// Used by DuckDB (embedded + HTTP), StarRocks, ClickHouse, and ADBC.
#[async_trait]
pub trait SyncAdapter: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &queryflux_core::session::SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
        params: &queryflux_core::params::QueryParams,
        id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution>;
    fn engine_type(&self) -> queryflux_core::query::EngineType;
    /// Target dialect for SQL translation (may differ from `engine_type().dialect()`, e.g. Flight SQL + arbitrary sqlglot backend).
    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        self.engine_type().dialect()
    }
    /// The wire format this adapter natively produces based on its connection type.
    ///
    /// Default: `Arrow` — the universal fallback path. Adapters that use a non-Arrow
    /// driver (e.g. `mysql_async`) override this to enable the zero-serialization path.
    fn connection_format(&self) -> ConnectionFormat {
        ConnectionFormat::Arrow
    }

    /// Execute a query and stream results as `NativeResultChunk`s, bypassing Arrow.
    ///
    /// Returns `true` when this adapter handles parameter binding natively (e.g. via ADBC
    /// prepared statements).  When `false` (the default) dispatch interpolates params into
    /// the SQL before calling `execute_as_arrow` / `execute_native`.
    fn supports_native_params(&self) -> bool {
        false
    }

    /// Only called by dispatch when `connection_format().matches_frontend(protocol)` is true.
    /// Default returns `Err` — adapters that override `connection_format` must also override this.
    #[allow(clippy::too_many_arguments)]
    async fn execute_native(
        &self,
        _protocol: &FrontendProtocol,
        _sql: &str,
        _session: &queryflux_core::session::SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &queryflux_core::tags::QueryTags,
        _params: &queryflux_core::params::QueryParams,
        _id_slot: &BackendQueryIdSlot,
    ) -> Result<NativeExecution> {
        Err(queryflux_core::error::QueryFluxError::Engine(
            "execute_native not implemented for this adapter".to_string(),
        ))
    }

    /// Best-effort: stop an in-flight engine query identified by `backend_id`.
    ///
    /// Dispatch calls this when the client disconnects or the request is
    /// cancelled. The default is a no-op — override when the engine has a kill
    /// API (ClickHouse `KILL QUERY`, StarRocks `KILL QUERY`, DuckDB interrupt).
    /// "Query already finished" must be treated as success.
    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> bool;
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }
    async fn execute_custom_health_check(&self, _sql: &str) -> bool {
        self.health_check().await
    }
    async fn execute_custom_reconcile_query(&self, _sql: &str) -> Option<u64> {
        None
    }
    async fn list_catalogs(&self) -> Result<Vec<String>>;
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;
    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<queryflux_core::catalog::TableSchema>>;
}

/// Async engines: submit-and-poll; lifecycle spans multiple HTTP requests.
/// Used by Trino and Athena.
#[async_trait]
pub trait AsyncAdapter: Send + Sync {
    async fn submit_query(
        &self,
        sql: &str,
        session: &queryflux_core::session::SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
        params: &queryflux_core::params::QueryParams,
    ) -> Result<queryflux_core::query::QueryExecution>;
    async fn poll_query(
        &self,
        backend_id: &queryflux_core::query::BackendQueryId,
        poll_token: Option<&str>,
        wire_auth: Option<&queryflux_core::query::StoredWireAuth>,
    ) -> Result<queryflux_core::query::QueryPollResult>;
    async fn cancel_query(
        &self,
        backend_id: &queryflux_core::query::BackendQueryId,
        wire_auth: Option<&queryflux_core::query::StoredWireAuth>,
    ) -> Result<()>;
    fn engine_type(&self) -> queryflux_core::query::EngineType;
    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        self.engine_type().dialect()
    }
    fn base_url(&self) -> &str {
        ""
    }
    /// Execute a query synchronously by driving the internal submit+poll loop to completion.
    ///
    /// Enables MySQL/Postgres wire protocol clients to query async engines.
    /// Returns `Err(SyncEngineRequired)` by default — engines that support this path override it.
    #[allow(clippy::too_many_arguments)]
    async fn execute_as_arrow(
        &self,
        _sql: &str,
        _session: &queryflux_core::session::SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &queryflux_core::tags::QueryTags,
        _params: &queryflux_core::params::QueryParams,
        _id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        Err(queryflux_core::error::QueryFluxError::SyncEngineRequired(
            "this engine only supports the async (HTTP submit-poll) protocol".to_string(),
        ))
    }
    fn supports_native_params(&self) -> bool {
        false
    }
    /// The wire format this adapter natively produces.
    ///
    /// Async adapters that use HTTP passthrough (e.g. Trino → `TrinoHttp`,
    /// ClickHouse → `ClickHouseHttp`) override this so dispatch can document
    /// and validate the native path. Default: `Arrow`.
    fn connection_format(&self) -> ConnectionFormat {
        ConnectionFormat::Arrow
    }
    async fn health_check(&self) -> bool;
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }
    async fn list_catalogs(&self) -> Result<Vec<String>>;
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;
    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<queryflux_core::catalog::TableSchema>>;
}

/// Type-safe adapter discriminant — replaces the `supports_async()` runtime flag.
///
/// Dispatch matches on this to route queries to the correct execution path:
/// `Sync` → `execute_to_sink`, `Async` → `dispatch_query`.
#[derive(Clone)]
pub enum AdapterKind {
    Sync(Arc<dyn SyncAdapter>),
    Async(Arc<dyn AsyncAdapter>),
}

impl AdapterKind {
    pub fn engine_type(&self) -> queryflux_core::query::EngineType {
        match self {
            Self::Sync(a) => a.engine_type(),
            Self::Async(a) => a.engine_type(),
        }
    }

    pub fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        match self {
            Self::Sync(a) => a.translation_target_dialect(),
            Self::Async(a) => a.translation_target_dialect(),
        }
    }

    pub async fn health_check(&self) -> bool {
        match self {
            Self::Sync(a) => a.health_check().await,
            Self::Async(a) => a.health_check().await,
        }
    }

    pub async fn fetch_running_query_count(&self) -> Option<u64> {
        match self {
            Self::Sync(a) => a.fetch_running_query_count().await,
            Self::Async(a) => a.fetch_running_query_count().await,
        }
    }

    /// Execute a custom SQL query and return success/failure. Used for custom health checks.
    pub async fn execute_custom_health_check(&self, sql: &str) -> bool {
        match self {
            Self::Sync(a) => a.execute_custom_health_check(sql).await,
            Self::Async(a) => {
                // Async adapters (Trino/Athena) don't have a simple SQL execution path
                // for custom health checks. Fall back to the default health_check().
                let _ = sql;
                a.health_check().await
            }
        }
    }

    /// Execute a custom SQL query and read a single u64 from the first column of the first row.
    /// Used for custom reconciliation queries.
    pub async fn execute_custom_reconcile_query(&self, sql: &str) -> Option<u64> {
        match self {
            Self::Sync(a) => a.execute_custom_reconcile_query(sql).await,
            Self::Async(_) => None,
        }
    }

    pub fn as_sync(&self) -> Option<Arc<dyn SyncAdapter>> {
        match self {
            Self::Sync(a) => Some(a.clone()),
            Self::Async(_) => None,
        }
    }

    pub fn as_async(&self) -> Option<Arc<dyn AsyncAdapter>> {
        match self {
            Self::Async(a) => Some(a.clone()),
            Self::Sync(_) => None,
        }
    }

    /// `wire_auth` is only meaningful for the `Async` branch (Trino) — sync adapters cancel
    /// via their own connection/session and don't carry a separately-resolved wire credential.
    pub async fn cancel_query(
        &self,
        backend_id: &BackendQueryId,
        wire_auth: Option<&queryflux_core::query::StoredWireAuth>,
    ) -> Result<()> {
        match self {
            Self::Sync(a) => a.cancel_query(backend_id).await,
            Self::Async(a) => a.cancel_query(backend_id, wire_auth).await,
        }
    }
}

/// Factory for constructing engine adapters from raw configuration.
///
/// Each engine provides a zero-sized factory struct implementing this trait.
/// This formalizes the contract that every adapter must support construction
/// from a DB config JSON blob and expose its descriptor metadata.
#[async_trait]
pub trait EngineAdapterFactory: Send + Sync {
    /// The engine key string matching the DB `engine_key` column (e.g. `"trino"`, `"duckDb"`).
    fn engine_key(&self) -> &'static str;

    /// Field-level schema used by the admin API and Studio UI.
    fn descriptor(&self) -> EngineDescriptor;

    /// Build an adapter instance from a raw DB config JSON blob.
    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind>;
}
