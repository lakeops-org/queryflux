use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Driver, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use queryflux_core::{
    catalog::TableSchema,
    config::ClusterConfig,
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::QueryTags,
};
use r2d2_adbc::AdbcConnectionManager;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::{AdapterKind, BackendQueryIdSlot, EngineAdapterFactory, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

mod bigquery;
mod databricks;
mod introspection;
mod redshift;
mod snowflake;
mod sql_helpers;
#[cfg(test)]
mod test_fixtures;

use introspection::AdbcIntrospection;

const DEFAULT_POOL_SIZE: u32 = 4;

const SUPPORTED_DRIVERS: &[&str] = &[
    "trino",
    "duckdb",
    "starrocks",
    "clickhouse",
    "mysql",
    "postgresql",
    "sqlite",
    "flightsql",
    "snowflake",
    "bigquery",
    "databricks",
    "mssql",
    "redshift",
    "exasol",
    "singlestore",
];

/// Maps a driver name to the EngineType used for SQL dialect rewriting.
fn driver_to_engine_type(driver: &str) -> EngineType {
    match driver {
        "trino" => EngineType::Trino,
        "duckdb" => EngineType::DuckDb,
        "starrocks" => EngineType::StarRocks,
        "clickhouse" => EngineType::ClickHouse,
        "mysql" => EngineType::MySql,
        "postgresql" => EngineType::Postgres,
        "sqlite" => EngineType::Sqlite,
        "snowflake" => EngineType::Snowflake,
        "bigquery" => EngineType::BigQuery,
        "databricks" => EngineType::Databricks,
        "mssql" => EngineType::MsSql,
        "redshift" => EngineType::Redshift,
        "exasol" => EngineType::Exasol,
        "singlestore" => EngineType::SingleStore,
        _ => EngineType::Adbc,
    }
}

fn build_introspection(
    driver: &str,
    cluster_name: &ClusterName,
    uri: &str,
    db_kwargs: &[(String, String)],
    pool: AdbcPool,
) -> Option<Box<dyn AdbcIntrospection>> {
    if driver == "databricks" {
        return databricks::try_from_adbc_config(cluster_name, uri, db_kwargs)
            .map(|i| Box::new(i) as Box<dyn AdbcIntrospection>);
    }
    if driver == "snowflake" {
        return snowflake::try_from_adbc_config(cluster_name, uri, db_kwargs, pool)
            .map(|i| Box::new(i) as Box<dyn AdbcIntrospection>);
    }
    if driver == "bigquery" {
        return bigquery::try_from_adbc_config(cluster_name, uri, db_kwargs, pool)
            .map(|i| Box::new(i) as Box<dyn AdbcIntrospection>);
    }
    if driver == "redshift" {
        return redshift::try_from_adbc_config(cluster_name, uri, db_kwargs, pool)
            .map(|i| Box::new(i) as Box<dyn AdbcIntrospection>);
    }
    None
}

/// First numeric cell of the first row (for `COUNT(*)`-style reconcile queries).
fn batch_first_cell_as_u64(batch: &RecordBatch) -> Option<u64> {
    if batch.num_columns() == 0 || batch.num_rows() == 0 {
        return None;
    }
    use arrow::array::{
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
    };
    let col = batch.column(0);
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return (!a.is_null(0)).then(|| a.value(0));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return (!a.is_null(0)).then(|| a.value(0).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return (!a.is_null(0)).then(|| a.value(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return (!a.is_null(0)).then(|| a.value(0).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        return (!a.is_null(0)).then(|| a.value(0).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
        return (!a.is_null(0)).then(|| a.value(0).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return (!a.is_null(0)).then(|| a.value(0).parse().ok()).flatten();
    }
    None
}

fn parse_engine_type_override(value: &str) -> Option<EngineType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trino" => Some(EngineType::Trino),
        "duckdb" => Some(EngineType::DuckDb),
        "starrocks" => Some(EngineType::StarRocks),
        "clickhouse" => Some(EngineType::ClickHouse),
        "adbc" => Some(EngineType::Adbc),
        "postgres" | "postgresql" => Some(EngineType::Postgres),
        "mysql" => Some(EngineType::MySql),
        "sqlite" => Some(EngineType::Sqlite),
        "snowflake" => Some(EngineType::Snowflake),
        "bigquery" => Some(EngineType::BigQuery),
        "databricks" => Some(EngineType::Databricks),
        "mssql" => Some(EngineType::MsSql),
        "redshift" => Some(EngineType::Redshift),
        "exasol" => Some(EngineType::Exasol),
        "singlestore" => Some(EngineType::SingleStore),
        _ => None,
    }
}

/// Parsed and validated configuration for an ADBC cluster.
pub struct AdbcConfig {
    pub driver: String,
    pub uri: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db_kwargs: Vec<(String, String)>,
    /// When `driver` is `flightsql`, sqlglot `write` dialect for translation (any supported name).
    /// JSON key `flightSqlClusterDialect`; legacy `flightSqlEngine` is still accepted when parsing.
    pub flight_sql_cluster_dialect: Option<String>,
    pub pool_size: u32,
}

impl AdbcConfig {
    pub fn engine_type(&self) -> EngineType {
        if self.driver == "flightsql" {
            if let Some(raw) = &self.flight_sql_cluster_dialect {
                let t = raw.trim();
                if !t.is_empty() {
                    if let Some(engine) = parse_engine_type_override(t) {
                        return engine;
                    }
                    return EngineType::Adbc;
                }
            }
        }
        driver_to_engine_type(&self.driver)
    }

    /// Translation target for sqlglot when using the Flight SQL driver.
    pub fn flight_sql_translation_dialect(&self) -> queryflux_core::query::SqlDialect {
        use queryflux_core::query::SqlDialect;
        if self.driver != "flightsql" {
            return self.engine_type().dialect();
        }
        let Some(raw) = &self.flight_sql_cluster_dialect else {
            return self.engine_type().dialect();
        };
        let t = raw.trim();
        if t.is_empty() {
            return self.engine_type().dialect();
        }
        if let Some(engine) = parse_engine_type_override(t) {
            return engine.dialect();
        }
        SqlDialect::Sqlglot(t.to_lowercase())
    }
}

/// Map a `dbKwargs` JSON value to the string passed to the ADBC driver. Scalars are preserved;
/// arrays and objects are rejected so misconfiguration is visible instead of silently dropped.
fn db_kwarg_value_to_string(
    cluster_name: &str,
    key: &str,
    v: &serde_json::Value,
) -> Result<String> {
    match v {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Ok(v.to_string()),
        serde_json::Value::Null => Ok("null".to_string()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name}': dbKwargs['{key}'] must be a string, number, boolean, or null (arrays and objects are not supported)"
            )),
        ),
    }
}

impl crate::EngineConfigParseable for AdbcConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        let driver = json
            .get("driver")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': missing required field 'driver'"
                ))
            })?
            .to_string();

        let uri = json
            .get("uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': missing required field 'uri'"
                ))
            })?
            .to_string();

        let username = json
            .get("username")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let password = json
            .get("password")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let db_kwargs = match json.get("dbKwargs") {
            Some(serde_json::Value::Object(map)) => {
                let mut out = Vec::with_capacity(map.len());
                for (k, v) in map {
                    let s = db_kwarg_value_to_string(cluster_name, k, v)?;
                    out.push((k.clone(), s));
                }
                out
            }
            _ => Vec::new(),
        };

        let flight_sql_cluster_dialect = json
            .get("flightSqlClusterDialect")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                json.get("flightSqlEngine")
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            });

        let pool_size = json
            .get("poolSize")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(u32::MAX as u64) as u32)
            .unwrap_or(DEFAULT_POOL_SIZE)
            .max(1);

        Ok(Self {
            driver,
            uri,
            username,
            password,
            db_kwargs,
            flight_sql_cluster_dialect,
            pool_size,
        })
    }

    fn from_cluster_config(_cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        Err(QueryFluxError::Engine(format!(
            "cluster '{cluster_name}': ADBC clusters must be created via the admin API (no YAML ClusterConfig support)"
        )))
    }
}

pub(crate) type AdbcPool = r2d2::Pool<AdbcConnectionManager<ManagedDatabase>>;

/// Small per-user pool, built on demand for `tokenExchange` clusters. Kept separate from
/// the static `pool` (Type 1 / `serviceAccount`) because its `ManagedDatabase` bakes in a
/// per-user OAuth token at connection-option time — there is no way to swap credentials on
/// a checked-out connection from a shared pool, so a distinct user needs a distinct
/// `ManagedDatabase`. Small size + idle eviction keep this from growing unbounded across a
/// long-running process; see `identity_pool_for_token`.
struct IdentityPoolEntry {
    pool: AdbcPool,
    last_used: std::time::Instant,
}

/// Max connections per per-identity sub-pool. Deliberately small — this exists to amortize
/// the OAuth-token-scoped connection setup cost across the handful of queries a user runs
/// in quick succession, not to serve as a general-purpose pool.
const IDENTITY_POOL_MAX_SIZE: u32 = 2;

/// Evict a per-identity sub-pool after this long without use. Roughly matches how often the
/// resolver's own token cache refreshes (tokens are typically short-lived), so a pool rarely
/// outlives the token it was built for by much.
const IDENTITY_POOL_IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Bound on building a per-identity `ManagedDatabase` (the driver FFI call that can involve
/// real OAuth token validation) — see `AdbcAdapter::identity_pool_for_token`.
const IDENTITY_POOL_BUILD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Cap on concurrent per-identity pool builds (across all tokens) in flight at once.
/// `tokio::time::timeout` gives up on *waiting* for a `spawn_blocking` task, but does not
/// cancel it — the underlying OS thread keeps running the driver FFI call until it returns
/// (or hangs) regardless. Without a cap, a burst of distinct identities hitting a slow or
/// unreachable OAuth endpoint at the same time could each leave a zombie build running,
/// piling up on the shared tokio blocking thread pool. Combined with single-flighting
/// same-token builds (below), this bounds worst-case blocking-thread usage from this path.
const IDENTITY_POOL_MAX_CONCURRENT_BUILDS: usize = 8;

/// Per-driver OAuth connection-option keys for `tokenExchange`. Only `snowflake` is wired in
/// this release — must stay in sync with `ADBC_TOKEN_EXCHANGE_DRIVERS` in
/// `queryflux_core::config`, which is what actually gates which clusters can reach this code
/// path (startup validation rejects `tokenExchange` for any other ADBC driver).
fn oauth_token_options(
    driver: &str,
    token: &str,
) -> Result<Vec<(OptionDatabase, adbc_core::options::OptionValue)>> {
    match driver {
        "snowflake" => Ok(vec![
            (
                OptionDatabase::Other("adbc.snowflake.sql.auth_type".to_string()),
                "auth_oauth".into(),
            ),
            (
                OptionDatabase::Other("adbc.snowflake.sql.client_option.auth_token".to_string()),
                token.to_string().into(),
            ),
        ]),
        other => Err(QueryFluxError::Engine(format!(
            "ADBC driver '{other}' has no tokenExchange connection-option wiring in this release"
        ))),
    }
}

/// ADBC adapter — wraps any ADBC-compatible shared library driver.
///
/// The driver is loaded once at construction via `load_from_name` (manifest-based, searches
/// user/system ADBC driver directories); the shared
/// library remains loaded for the lifetime of the pool via Arc reference counting.
pub struct AdbcAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pool: AdbcPool,
    /// Kept (not dropped after `new()`) so per-identity `ManagedDatabase`s can be built on
    /// demand — cheap to clone (`Arc` inside), see `adbc_driver_manager::ManagedDriver`.
    driver: ManagedDriver,
    /// Raw driver key (e.g. `"snowflake"`) — distinct from `engine_type`, which can be
    /// overridden for `flightsql` via `flightSqlClusterDialect` and would then no longer
    /// identify which OAuth option keys to use.
    driver_name: String,
    base_uri: String,
    base_db_kwargs: Vec<(String, String)>,
    identity_pools: Arc<DashMap<String, IdentityPoolEntry>>,
    /// Per-token single-flight locks — serializes concurrent cache-miss builds for the same
    /// token so a burst of queries from one identity builds exactly one pool instead of
    /// racing to build (and discard all but the last of) several. See
    /// `identity_pool_for_token`.
    identity_pool_build_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Caps total concurrent per-identity pool builds across all tokens. See
    /// `IDENTITY_POOL_MAX_CONCURRENT_BUILDS`.
    identity_pool_build_semaphore: Arc<tokio::sync::Semaphore>,
    engine_type: EngineType,
    translation_dialect: queryflux_core::query::SqlDialect,
    /// Optional driver-specific introspection (Databricks REST, SaaS reconcile SQL).
    /// When present, default `health_check` / `fetch_running_query_count` delegate here.
    introspection: Option<Box<dyn AdbcIntrospection>>,
}

impl AdbcAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: AdbcConfig,
    ) -> Result<Self> {
        let engine_type = config.engine_type();
        let translation_dialect = config.flight_sql_translation_dialect();
        let driver_name = config.driver.clone();
        let base_uri = config.uri.clone();
        let base_db_kwargs = config.db_kwargs.clone();

        let mut driver = ManagedDriver::load_from_name(
            &driver_name,
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': failed to load ADBC driver '{}': {e}",
                cluster_name.0, driver_name
            ))
        })?;

        let mut opts: Vec<(OptionDatabase, adbc_core::options::OptionValue)> =
            vec![(OptionDatabase::Uri, base_uri.clone().into())];

        if let Some(username) = config.username {
            opts.push((OptionDatabase::Username, username.into()));
        }
        if let Some(password) = config.password {
            opts.push((OptionDatabase::Password, password.into()));
        }
        for (k, v) in &base_db_kwargs {
            opts.push((OptionDatabase::Other(k.clone()), v.clone().into()));
        }

        let database = driver.new_database_with_opts(opts).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': failed to create ADBC database: {e}",
                cluster_name.0
            ))
        })?;
        // `driver` is kept (not dropped) on the adapter so per-identity `ManagedDatabase`s
        // can be built later for `tokenExchange` — see `identity_pool_for_token`. Cloning it
        // is cheap (Arc-backed), and the shared library stays loaded via that Arc either way.

        let manager = AdbcConnectionManager::new(database);
        let pool = r2d2::Pool::builder()
            .max_size(config.pool_size)
            .build(manager)
            .map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': failed to create ADBC connection pool: {e}",
                    cluster_name.0
                ))
            })?;

        let introspection = build_introspection(
            &driver_name,
            &cluster_name,
            &base_uri,
            &base_db_kwargs,
            pool.clone(),
        );

        Ok(Self {
            cluster_name,
            group_name,
            pool,
            driver,
            driver_name,
            base_uri,
            base_db_kwargs,
            identity_pools: Arc::new(DashMap::new()),
            identity_pool_build_locks: Arc::new(DashMap::new()),
            identity_pool_build_semaphore: Arc::new(tokio::sync::Semaphore::new(
                IDENTITY_POOL_MAX_CONCURRENT_BUILDS,
            )),
            engine_type,
            translation_dialect,
            introspection,
        })
    }

    /// Return the small per-identity pool for `token`, building (and caching) it on first
    /// use.
    ///
    /// The cache-hit path is a cheap `DashMap` lookup, safe to call directly from async
    /// context. The cache-miss path calls the driver FFI (`new_database_with_opts`) — a
    /// genuinely blocking call (Snowflake's driver validates the OAuth token, which can
    /// mean real network I/O), so it runs inside `spawn_blocking` rather than directly on
    /// the async executor — an earlier version of this comment claimed it "rides along" on
    /// `execute_as_arrow`'s own `spawn_blocking`, which was wrong: this is called *before*
    /// that, so without its own `spawn_blocking` it would have blocked whatever tokio
    /// worker thread happened to be running the query. Also bounded by
    /// [`IDENTITY_POOL_BUILD_TIMEOUT`] — left unbounded, a slow or unreachable Snowflake
    /// OAuth endpoint would hang the query indefinitely instead of failing it.
    ///
    /// Concurrent cache misses for the *same* token are single-flighted through
    /// `identity_pool_build_locks`: only the first caller actually builds; the rest wait on
    /// the per-token lock and then hit the now-populated cache. Without this, a burst of
    /// queries from one identity would each build (and all but one immediately discard) a
    /// real Snowflake connection — wasted OAuth validation calls, not just wasted CPU.
    /// `identity_pool_build_semaphore` additionally caps concurrent builds *across* distinct
    /// tokens, since a timed-out build's `spawn_blocking` task keeps running rather than
    /// being cancelled (see `IDENTITY_POOL_MAX_CONCURRENT_BUILDS`).
    ///
    /// Sweeps idle entries at the *start* of every call, before the cache-hit check —
    /// sweeping only on the (rarer, in steady state) miss path would let an expired pool
    /// stay reachable indefinitely as long as it kept getting cache hits.
    async fn identity_pool_for_token(&self, token: &str) -> Result<AdbcPool> {
        let now = std::time::Instant::now();
        self.identity_pools
            .retain(|_, e| now.duration_since(e.last_used) < IDENTITY_POOL_IDLE_TTL);

        if let Some(mut entry) = self.identity_pools.get_mut(token) {
            entry.last_used = now;
            return Ok(entry.pool.clone());
        }

        let lock = self
            .identity_pool_build_locks
            .entry(token.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _build_guard = lock.lock().await;

        // Re-check: another waiter may have just finished building this token's pool while
        // we were waiting for the lock.
        if let Some(mut entry) = self.identity_pools.get_mut(token) {
            entry.last_used = std::time::Instant::now();
            self.identity_pool_build_locks.remove(token);
            return Ok(entry.pool.clone());
        }

        let permit = self
            .identity_pool_build_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| {
                QueryFluxError::Engine(format!("identity pool build semaphore closed: {e}"))
            })?;

        let driver_name = self.driver_name.clone();
        let mut driver = self.driver.clone();
        let base_uri = self.base_uri.clone();
        let base_db_kwargs = self.base_db_kwargs.clone();
        let cluster_name = self.cluster_name.0.clone();
        let token_owned = token.to_string();

        let build = tokio::task::spawn_blocking(move || -> Result<AdbcPool> {
            let _permit = permit;
            let opts = oauth_token_options(&driver_name, &token_owned)?;
            let mut opts_with_uri = vec![(OptionDatabase::Uri, base_uri.into())];
            for (k, v) in &base_db_kwargs {
                opts_with_uri.push((OptionDatabase::Other(k.clone()), v.clone().into()));
            }
            opts_with_uri.extend(opts);

            let database = driver.new_database_with_opts(opts_with_uri).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': failed to create per-identity ADBC database: {e}"
                ))
            })?;
            let manager = AdbcConnectionManager::new(database);
            r2d2::Pool::builder()
                .max_size(IDENTITY_POOL_MAX_SIZE)
                .build(manager)
                .map_err(|e| {
                    QueryFluxError::Engine(format!(
                        "cluster '{cluster_name}': failed to create per-identity ADBC \
                         connection pool: {e}"
                    ))
                })
        });

        let result = tokio::time::timeout(IDENTITY_POOL_BUILD_TIMEOUT, build)
            .await
            .map_err(|_| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': building a per-identity ADBC connection timed out after \
                     {}s (the OAuth token validation may be slow or the identity backend \
                     unreachable)",
                    self.cluster_name.0,
                    IDENTITY_POOL_BUILD_TIMEOUT.as_secs()
                ))
            })
            .and_then(|joined| {
                joined.map_err(|e| {
                    QueryFluxError::Engine(format!(
                        "cluster '{}': per-identity ADBC connection task panicked: {e}",
                        self.cluster_name.0
                    ))
                })
            })
            .and_then(|built| built);

        // Populate the cache (on success) *before* releasing the single-flight lock, so a
        // brand-new caller that arrives right after the lock is released is guaranteed to
        // see the cache hit rather than racing to start a redundant build. Release the lock
        // regardless of outcome — a build failure must not wedge every subsequent attempt
        // for this token behind a lock nobody will ever release again (the `Arc<Mutex>`
        // itself is dropped along with the map entry once every clone — including whichever
        // waiters are still parked on `.lock().await` — has released it).
        if let Ok(pool) = &result {
            self.identity_pools.insert(
                token.to_string(),
                IdentityPoolEntry {
                    pool: pool.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
        }
        self.identity_pool_build_locks.remove(token);

        result
    }

    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "adbc",
            display_name: "ADBC",
            description: "Generic ADBC adapter — connect to any engine via an installed ADBC driver.",
            hex: "6366F1",
            connection_type: ConnectionType::Driver,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::Basic],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "driver",
                    label: "Driver",
                    description: "ADBC driver name (from `dbc install <driver>`) or path to shared library.",
                    field_type: FieldType::Select {
                        options: SUPPORTED_DRIVERS.to_vec(),
                    },
                    required: true,
                    example: Some("trino"),
                },
                ConfigField {
                    key: "uri",
                    label: "URI",
                    description: "Driver-specific connection URI.",
                    field_type: FieldType::Text,
                    required: true,
                    example: Some("http://trino-host:8080"),
                },
                ConfigField {
                    key: "username",
                    label: "Username",
                    description: "Authentication username.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("admin"),
                },
                ConfigField {
                    key: "password",
                    label: "Password",
                    description: "Authentication password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "dbKwargs",
                    label: "Driver Options",
                    description: "Additional driver-specific key/value options (JSON object).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("{}"),
                },
                ConfigField {
                    key: "flightSqlClusterDialect",
                    label: "Cluster SQL dialect (Flight SQL)",
                    description: "When driver is flightsql: which SQL dialect this cluster speaks, for translation. Flight SQL is only the wire protocol.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("starrocks"),
                },
                ConfigField {
                    key: "poolSize",
                    label: "Pool Size",
                    description: "Maximum number of pooled connections. Defaults to 4.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("4"),
                },
            ],
        }
    }
}

/// Build an Arrow RecordBatch encoding positional query parameters for ADBC's `stmt.bind()`.
///
/// ADBC uses a RecordBatch with one column per `?` placeholder and one row per execution.
/// Column names are positional ("p1", "p2", …); the driver ignores names and binds by position.
fn params_to_record_batch(params: &queryflux_core::params::QueryParams) -> Result<RecordBatch> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, NullArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use queryflux_core::params::QueryParam;

    let mut fields = Vec::with_capacity(params.len());
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(params.len());

    for (i, param) in params.iter().enumerate() {
        let name = format!("p{}", i + 1);
        match param {
            QueryParam::Text(s)
            | QueryParam::Date(s)
            | QueryParam::Timestamp(s)
            | QueryParam::Time(s) => {
                fields.push(Field::new(&name, DataType::Utf8, false));
                columns.push(Arc::new(StringArray::from(vec![s.as_str()])));
            }
            QueryParam::Numeric(s) => {
                if let Ok(n) = s.parse::<i64>() {
                    fields.push(Field::new(&name, DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from(vec![n])));
                } else if let Ok(f) = s.parse::<f64>() {
                    fields.push(Field::new(&name, DataType::Float64, false));
                    columns.push(Arc::new(Float64Array::from(vec![f])));
                } else {
                    fields.push(Field::new(&name, DataType::Utf8, false));
                    columns.push(Arc::new(StringArray::from(vec![s.as_str()])));
                }
            }
            QueryParam::Boolean(b) => {
                fields.push(Field::new(&name, DataType::Boolean, false));
                columns.push(Arc::new(BooleanArray::from(vec![*b])));
            }
            QueryParam::Null => {
                fields.push(Field::new(&name, DataType::Null, true));
                columns.push(Arc::new(NullArray::new(1)));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: failed to build param batch: {e}")))
}

pub(crate) fn collect_batches(
    reader: impl Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>,
) -> std::result::Result<Vec<RecordBatch>, QueryFluxError> {
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: failed to read results: {e}")))
}

#[async_trait]
impl SyncAdapter for AdbcAdapter {
    fn supports_native_params(&self) -> bool {
        true
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
        params: &queryflux_core::params::QueryParams,
        _id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        // ADBC `Statement::cancel` requires the live statement (`&mut self`)
        // on the blocking thread; there is no cross-thread cancel handle.
        // Leave the slot unset so dispatch does not record a fake backend id.
        // `cancel_query` stays the default no-op. Dropping the result stream
        // stops *reading* batches.
        tracing::debug!(
            cluster = %self.cluster_name,
            attempt_id = %uuid::Uuid::new_v4(),
            "Executing ADBC query"
        );
        // `Bearer` is the only non-serviceAccount `QueryCredentials` that can reach an ADBC
        // adapter — startup validation (`query_auth_supported`) rejects `passthrough`/
        // `impersonate` for every ADBC driver, so `tokenExchange` (resolved to `Bearer`) is
        // the only other case to handle here. `identity_pool_for_token` is a cheap DashMap
        // lookup on cache hits; the miss path runs the blocking driver FFI call inside its
        // own `spawn_blocking` with a timeout, not inline here.
        let pool = match credentials {
            queryflux_auth::QueryCredentials::Bearer { token } => {
                self.identity_pool_for_token(token).await?
            }
            _ => self.pool.clone(),
        };
        let sql = sql.to_string();
        let param_batch = if params.is_empty() {
            None
        } else {
            Some(params_to_record_batch(params)?)
        };

        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch>>(32);
        let (stats_tx, stats_rx) = oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    let _ = batch_tx.blocking_send(Err(QueryFluxError::Engine(format!(
                        "ADBC: failed to get connection from pool: {e}"
                    ))));
                    return;
                }
            };
            let mut stmt = match conn.new_statement() {
                Ok(s) => s,
                Err(e) => {
                    let _ = batch_tx.blocking_send(Err(QueryFluxError::Engine(format!(
                        "ADBC: failed to create statement: {e}"
                    ))));
                    return;
                }
            };
            if let Err(e) = stmt.set_sql_query(&sql) {
                let _ = batch_tx.blocking_send(Err(QueryFluxError::Engine(format!(
                    "ADBC: failed to set SQL query: {e}"
                ))));
                return;
            }
            if let Some(batch) = param_batch {
                if let Err(e) = stmt.bind(batch) {
                    let _ = batch_tx.blocking_send(Err(QueryFluxError::Engine(format!(
                        "ADBC: failed to bind parameters: {e}"
                    ))));
                    return;
                }
            }
            let reader = match stmt.execute() {
                Ok(r) => r,
                Err(e) => {
                    let _ = batch_tx.blocking_send(Err(QueryFluxError::Engine(format!(
                        "ADBC: query execution failed: {e}"
                    ))));
                    return;
                }
            };
            for batch in reader {
                let result = batch.map_err(|e| {
                    QueryFluxError::Engine(format!("ADBC: failed to read results: {e}"))
                });
                if batch_tx.blocking_send(result).is_err() {
                    return; // consumer dropped, stop reading
                }
            }
            // Send stats only after all batches have been produced.
            let _ = stats_tx.send(None); // ADBC has no standard stats API
        });

        Ok(SyncExecution {
            stream: Box::pin(ReceiverStream::new(batch_rx)),
            stats: stats_rx,
        })
    }

    fn engine_type(&self) -> EngineType {
        self.engine_type.clone()
    }

    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        self.translation_dialect.clone()
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        match &self.engine_type {
            EngineType::Trino => {
                let pool = self.pool.clone();
                let sql = "SELECT count(*) - 1 FROM system.runtime.queries WHERE state = 'RUNNING'"
                    .to_string();
                tokio::task::spawn_blocking(move || {
                    let mut conn = pool.get().ok()?;
                    let mut stmt = conn.new_statement().ok()?;
                    stmt.set_sql_query(&sql).ok()?;
                    let reader = stmt.execute().ok()?;
                    let batches = collect_batches(reader).ok()?;
                    batches.iter().find_map(batch_first_cell_as_u64)
                })
                .await
                .ok()?
            }
            EngineType::StarRocks => {
                let pool = self.pool.clone();
                let sql =
                    "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND = 'Query'"
                        .to_string();
                tokio::task::spawn_blocking(move || {
                    let mut conn = pool.get().ok()?;
                    let mut stmt = conn.new_statement().ok()?;
                    stmt.set_sql_query(&sql).ok()?;
                    let reader = stmt.execute().ok()?;
                    let batches = collect_batches(reader).ok()?;
                    batches.iter().find_map(batch_first_cell_as_u64)
                })
                .await
                .ok()?
            }
            EngineType::ClickHouse => {
                let pool = self.pool.clone();
                let sql = "SELECT count() FROM system.processes".to_string();
                tokio::task::spawn_blocking(move || {
                    let mut conn = pool.get().ok()?;
                    let mut stmt = conn.new_statement().ok()?;
                    stmt.set_sql_query(&sql).ok()?;
                    let reader = stmt.execute().ok()?;
                    let batches = collect_batches(reader).ok()?;
                    batches.iter().find_map(batch_first_cell_as_u64)
                })
                .await
                .ok()?
            }
            _ => {
                if let Some(ref intro) = self.introspection {
                    return intro.fetch_running_query_count().await;
                }
                None
            }
        }
    }

    async fn health_check(&self) -> bool {
        if let Some(ref intro) = self.introspection {
            return intro.health_check().await;
        }
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().ok()?;
            let mut stmt = conn.new_statement().ok()?;
            stmt.set_sql_query("SELECT 1").ok()?;
            stmt.execute().ok()?;
            Some(())
        })
        .await
        .ok()
        .flatten()
        .is_some()
    }

    async fn execute_custom_health_check(&self, sql: &str) -> bool {
        let pool = self.pool.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().ok()?;
            let mut stmt = conn.new_statement().ok()?;
            stmt.set_sql_query(&sql).ok()?;
            stmt.execute().ok()?;
            Some(())
        })
        .await
        .ok()
        .flatten()
        .is_some()
    }

    async fn execute_custom_reconcile_query(&self, sql: &str) -> Option<u64> {
        let pool = self.pool.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().ok()?;
            let mut stmt = conn.new_statement().ok()?;
            stmt.set_sql_query(&sql).ok()?;
            let reader = stmt.execute().ok()?;
            let batches = collect_batches(reader).ok()?;
            batches.iter().find_map(|batch| {
                sql_helpers::cell_u64(batch, "running", 0)
                    .or_else(|| batch_first_cell_as_u64(batch))
            })
        })
        .await
        .ok()?
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: pool error: {e}"))
            })?;
            let mut stmt = conn.new_statement().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: statement error: {e}"))
            })?;
            stmt.set_sql_query("SELECT catalog_name FROM information_schema.schemata GROUP BY catalog_name ORDER BY catalog_name")
                .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut catalogs = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            catalogs.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(catalogs)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let catalog = catalog.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let catalog = catalog.replace('\'', "''");
            let mut conn = pool.get().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let mut stmt = conn.new_statement().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            stmt.set_sql_query(format!(
                "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{catalog}' ORDER BY schema_name"
            ))
            .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut schemas = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let arr = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            schemas.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(schemas)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let catalog = catalog.to_string();
        let database = database.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let catalog = catalog.replace('\'', "''");
            let database = database.replace('\'', "''");
            let mut conn = pool.get().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let mut stmt = conn.new_statement().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            stmt.set_sql_query(format!(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{catalog}' AND table_schema = '{database}' ORDER BY table_name"
            ))
            .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut tables = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let arr = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            tables.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(tables)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result
    }

    async fn describe_table(
        &self,
        _catalog: &str,
        _database: &str,
        _table: &str,
    ) -> Result<Option<TableSchema>> {
        // Best-effort: not all ADBC drivers expose information_schema column types uniformly.
        Ok(None)
    }
}

pub struct AdbcFactory;

#[async_trait]
impl EngineAdapterFactory for AdbcFactory {
    fn engine_key(&self) -> &'static str {
        "adbc"
    }

    fn descriptor(&self) -> EngineDescriptor {
        AdbcAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = AdbcConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(AdbcAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::{oauth_token_options, AdbcConfig};
    use crate::EngineConfigParseable;
    use queryflux_core::query::{EngineType, SqlDialect};

    #[test]
    fn oauth_token_options_snowflake_sets_auth_type_and_token() {
        let opts = oauth_token_options("snowflake", "the-token").expect("snowflake is wired");
        let keys: Vec<String> = opts.iter().map(|(k, _)| format!("{k:?}")).collect();
        assert!(
            keys.iter()
                .any(|k| k.contains("adbc.snowflake.sql.auth_type")),
            "missing auth_type option, got: {keys:?}"
        );
        assert!(
            keys.iter()
                .any(|k| k.contains("adbc.snowflake.sql.client_option.auth_token")),
            "missing auth_token option, got: {keys:?}"
        );
    }

    #[test]
    fn oauth_token_options_rejects_unwired_drivers() {
        for driver in ["postgresql", "mysql", "flightsql", "databricks", "unknown"] {
            assert!(
                oauth_token_options(driver, "token").is_err(),
                "driver '{driver}' should not be wired for tokenExchange yet"
            );
        }
    }

    #[test]
    fn trino_driver_maps_to_trino_engine_type() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "poolSize": 2
        });
        let cfg = AdbcConfig::from_json(&json, "cluster-a").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Trino);
        assert_eq!(cfg.driver, "trino");
        assert_eq!(cfg.uri, "http://localhost:8080");
        assert_eq!(cfg.pool_size, 2);
    }

    #[test]
    fn trino_config_accepts_db_kwargs() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://trino:8080",
            "dbKwargs": { "session_properties": "query_max_memory=1GB" }
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Trino);
        assert_eq!(cfg.db_kwargs.len(), 1);
        assert_eq!(cfg.db_kwargs[0].0, "session_properties");
    }

    #[test]
    fn missing_driver_field_errors() {
        let json = serde_json::json!({ "uri": "http://localhost:8080" });
        match AdbcConfig::from_json(&json, "x") {
            Err(e) => assert!(e.to_string().contains("driver"), "unexpected: {e}"),
            Ok(_) => panic!("expected parse error when driver is missing"),
        }
    }

    #[test]
    fn missing_uri_field_errors() {
        let json = serde_json::json!({ "driver": "trino" });
        match AdbcConfig::from_json(&json, "c") {
            Err(e) => assert!(e.to_string().contains("uri"), "unexpected: {e}"),
            Ok(_) => panic!("expected parse error when uri is missing"),
        }
    }

    #[test]
    fn default_pool_size_when_omitted() {
        let json = serde_json::json!({
            "driver": "duckdb",
            "uri": "duckdb:///tmp/x.db"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.pool_size, 4);
    }

    #[test]
    fn pool_size_zero_clamps_to_one() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "poolSize": 0
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.pool_size, 1);
    }

    #[test]
    fn duckdb_driver_maps_to_duckdb_engine_type() {
        let json = serde_json::json!({
            "driver": "duckdb",
            "uri": "duckdb:///tmp/q.db"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::DuckDb);
    }

    #[test]
    fn starrocks_and_clickhouse_map_to_engine_types() {
        let sr = serde_json::json!({
            "driver": "starrocks",
            "uri": "mysql://sr:9030"
        });
        assert_eq!(
            AdbcConfig::from_json(&sr, "c")
                .expect("parse")
                .engine_type(),
            EngineType::StarRocks
        );
        let ch = serde_json::json!({
            "driver": "clickhouse",
            "uri": "http://localhost:8123"
        });
        assert_eq!(
            AdbcConfig::from_json(&ch, "c")
                .expect("parse")
                .engine_type(),
            EngineType::ClickHouse
        );
    }

    #[test]
    fn unknown_driver_maps_to_adbc_engine_type() {
        let json = serde_json::json!({
            "driver": "snowflake",
            "uri": "snowflake://acct/db"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Snowflake);
    }

    #[test]
    fn mysql_driver_maps_to_mysql_engine_type() {
        let json = serde_json::json!({
            "driver": "mysql",
            "uri": "mysql://localhost:3306/db"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::MySql);
    }

    #[test]
    fn flightsql_without_override_maps_to_adbc() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Adbc);
        assert!(cfg.flight_sql_cluster_dialect.is_none());
    }

    #[test]
    fn flightsql_with_trino_cluster_dialect_maps_to_trino() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337",
            "flightSqlClusterDialect": "trino"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Trino);
    }

    #[test]
    fn flightsql_legacy_flight_sql_engine_key_still_parsed() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337",
            "flightSqlEngine": "trino"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Trino);
    }

    #[test]
    fn flight_sql_cluster_dialect_is_case_insensitive() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337",
            "flightSqlClusterDialect": "StarRocks"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::StarRocks);
    }

    #[test]
    fn flight_sql_cluster_dialect_new_key_wins_over_legacy() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337",
            "flightSqlClusterDialect": "trino",
            "flightSqlEngine": "starrocks"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Trino);
    }

    #[test]
    fn flight_sql_arbitrary_dialect_maps_engine_to_adbc_but_translates_via_sqlglot() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://localhost:31337",
            "flightSqlClusterDialect": "hive"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::Adbc);
        assert_eq!(
            cfg.flight_sql_translation_dialect(),
            SqlDialect::Sqlglot("hive".to_string())
        );
    }

    #[test]
    fn username_and_password_parsed() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "username": "alice",
            "password": "secret"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.username.as_deref(), Some("alice"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
    }

    #[test]
    fn db_kwargs_serializes_scalar_values() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "dbKwargs": {
                "a": "ok",
                "n": 42,
                "flag": true,
                "empty": null
            }
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.db_kwargs.len(), 4);
        let m: std::collections::HashMap<_, _> = cfg.db_kwargs.into_iter().collect();
        assert_eq!(m.get("a").map(String::as_str), Some("ok"));
        assert_eq!(m.get("n").map(String::as_str), Some("42"));
        assert_eq!(m.get("flag").map(String::as_str), Some("true"));
        assert_eq!(m.get("empty").map(String::as_str), Some("null"));
    }

    #[test]
    fn db_kwargs_rejects_nested_array() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "dbKwargs": { "bad": [1, 2] }
        });
        match AdbcConfig::from_json(&json, "c") {
            Err(e) => assert!(e.to_string().contains("dbKwargs['bad']"), "unexpected: {e}"),
            Ok(_) => panic!("expected error for array dbKwargs value"),
        }
    }

    #[test]
    fn non_object_db_kwargs_yields_empty() {
        let json = serde_json::json!({
            "driver": "trino",
            "uri": "http://localhost:8080",
            "dbKwargs": "not-an-object"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert!(cfg.db_kwargs.is_empty());
    }

    #[test]
    fn adbc_descriptor_reports_adbc_engine_key() {
        let d = super::AdbcAdapter::descriptor();
        assert_eq!(d.engine_key, "adbc");
        assert!(d.implemented);
    }

    #[test]
    fn flightsql_starrocks_engine_type_for_reconcile_sql() {
        let json = serde_json::json!({
            "driver": "flightsql",
            "uri": "grpc://h:9000",
            "flightSqlClusterDialect": "starrocks"
        });
        let cfg = AdbcConfig::from_json(&json, "c").expect("parse");
        assert_eq!(cfg.engine_type(), EngineType::StarRocks);
    }

    // ── params_to_record_batch ────────────────────────────────────────────────

    use super::params_to_record_batch;
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray};
    use arrow::datatypes::DataType;
    use queryflux_core::params::QueryParam;

    #[test]
    fn text_param_produces_utf8_column() {
        let params = vec![QueryParam::Text("hello".into())];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(0).name(), "p1");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
    }

    #[test]
    fn integer_numeric_produces_int64_column() {
        let params = vec![QueryParam::Numeric("42".into())];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn float_numeric_produces_float64_column() {
        let params = vec![QueryParam::Numeric("2.5".into())];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn non_parseable_numeric_falls_back_to_utf8() {
        let params = vec![QueryParam::Numeric("bad".into())];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn boolean_param_produces_boolean_column() {
        let params = vec![QueryParam::Boolean(true)];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }

    #[test]
    fn null_param_produces_null_column() {
        let params = vec![QueryParam::Null];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Null);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn temporal_params_produce_utf8_columns() {
        let params = vec![
            QueryParam::Date("2025-01-15".into()),
            QueryParam::Timestamp("2025-01-15 12:00:00".into()),
            QueryParam::Time("08:30:00".into()),
        ];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.num_columns(), 3);
        for i in 0..3 {
            assert_eq!(batch.schema().field(i).data_type(), &DataType::Utf8);
        }
    }

    #[test]
    fn multiple_params_get_positional_column_names() {
        let params = vec![
            QueryParam::Text("a".into()),
            QueryParam::Numeric("1".into()),
            QueryParam::Boolean(false),
        ];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.schema().field(0).name(), "p1");
        assert_eq!(batch.schema().field(1).name(), "p2");
        assert_eq!(batch.schema().field(2).name(), "p3");
    }

    #[test]
    fn batch_always_has_exactly_one_row() {
        let params = vec![
            QueryParam::Text("x".into()),
            QueryParam::Numeric("5".into()),
        ];
        let batch = params_to_record_batch(&params).expect("build");
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn batch_first_cell_as_u64_parses_numeric_types() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use super::batch_first_cell_as_u64;
        use crate::adbc::test_fixtures::count_batch;

        assert_eq!(batch_first_cell_as_u64(&count_batch(9)), Some(9));

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![4_i64]))]).unwrap();
        assert_eq!(batch_first_cell_as_u64(&batch), Some(4));
    }

    #[test]
    fn batch_first_cell_as_u64_empty_batch_returns_none() {
        use std::sync::Arc;

        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;

        use super::batch_first_cell_as_u64;

        assert_eq!(
            batch_first_cell_as_u64(&RecordBatch::new_empty(Arc::new(Schema::empty()))),
            None
        );
    }
}
