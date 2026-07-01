/// Test harness: in-process QueryFlux Trino HTTP server on a random port.
///
/// Backends are optional and discovered via connectivity / env:
///   TRINO_URL         — default http://localhost:18081
///   STARROCKS_URL     — default mysql://root@localhost:9030 (matches docker-compose.test.yml)
///
/// Lakekeeper / Iceberg (optional):
///   LAKEKEEPER_URL, MINIO_ENDPOINT — StarRocks external catalog DDL only.
///
/// At least one of Trino or StarRocks must be reachable or [`TestHarness::new`] fails.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::Router;
use queryflux_auth::{
    AllowAllAuthorization, AuthProvider, AuthorizationChecker, BackendIdentityResolver,
    NoneAuthProvider,
};
use queryflux_cluster_manager::{
    cluster_state::ClusterState, simple::SimpleClusterGroupManager, strategy::strategy_from_config,
};
use queryflux_core::config::SnowflakeHttpFrontendConfig;
use queryflux_core::{
    error::Result as QfResult,
    query::{ClusterGroupName, ClusterName, EngineType},
};
use queryflux_engine_adapters::{
    duckdb::{DuckDbAdapter, DuckDbConfig},
    starrocks::StarRocksAdapter,
    trino::TrinoAdapter,
    AdapterKind,
};
use queryflux_frontend::{
    snowflake::SnowflakeFrontend,
    state::LiveConfig,
    trino_http::{state::AppState, TrinoHttpFrontend},
};
use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
use queryflux_persistence::in_memory::InMemoryPersistence;
use queryflux_routing::{
    chain::RouterChain,
    implementations::{header::HeaderRouter, protocol_based::ProtocolBasedRouter},
    RouterTrait,
};
use queryflux_translation::TranslationService;
use tokio::net::TcpListener;

struct CapturingMetrics {
    records: Arc<Mutex<Vec<QueryRecord>>>,
}

#[async_trait]
impl MetricsStore for CapturingMetrics {
    async fn record_query(&self, r: QueryRecord) -> QfResult<()> {
        self.records.lock().expect("lock records").push(r);
        Ok(())
    }

    async fn record_cluster_snapshot(&self, _s: ClusterSnapshot) -> QfResult<()> {
        Ok(())
    }
}

pub const GROUP_TRINO: &str = "trino";
pub const GROUP_STARROCKS: &str = "starrocks";
/// Always available — in-process embedded DuckDB (in-memory, no external dependency).
pub const GROUP_DUCKDB: &str = "duckdb";
/// Set when Lakekeeper port is reachable (Iceberg tables seeded by e2e tests via Trino).
pub const GROUP_LAKEKEEPER: &str = "lakekeeper";

pub struct TestHarness {
    pub port: u16,
    pub groups: Vec<String>,
    records: Arc<Mutex<Vec<QueryRecord>>>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestHarness {
    pub async fn new() -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("error")
            .try_init();

        type GroupEntry = (
            Vec<Arc<ClusterState>>,
            Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        );
        let mut group_states: HashMap<ClusterGroupName, GroupEntry> = HashMap::new();
        let mut adapters: HashMap<String, AdapterKind> = HashMap::new();
        let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
        let mut group_order: Vec<String> = Vec::new();
        let mut available_groups: Vec<String> = Vec::new();
        let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();
        let mut header_map: HashMap<String, ClusterGroupName> = HashMap::new();

        // --- Trino ---
        let trino_url =
            std::env::var("TRINO_URL").unwrap_or_else(|_| "http://localhost:18081".to_string());
        let trino_available = is_trino_ready(&trino_url).await;
        if trino_available {
            let group = ClusterGroupName(GROUP_TRINO.to_string());
            let cluster = ClusterName("trino-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::Trino,
                Some(trino_url.clone()),
                20,
                true,
            ));
            let adapter = Arc::new(TrinoAdapter::new(
                cluster.clone(),
                group.clone(),
                queryflux_engine_adapters::trino::TrinoConfig {
                    endpoint: trino_url,
                    tls_skip_verify: false,
                    auth: None,
                },
            ));

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_TRINO.to_string(), vec![cluster.0.clone()]);
            group_order.push(GROUP_TRINO.to_string());
            adapters.insert(cluster.0.clone(), AdapterKind::Async(adapter));
            available_groups.push(GROUP_TRINO.to_string());
            header_map.insert(GROUP_TRINO.to_string(), group);
        }

        // --- StarRocks ---
        let sr_url = std::env::var("STARROCKS_URL")
            .unwrap_or_else(|_| "mysql://root@localhost:9030".to_string());
        let sr_available = is_starrocks_ready(&sr_url).await;
        let sr_adapter = if sr_available {
            let group = ClusterGroupName(GROUP_STARROCKS.to_string());
            let cluster = ClusterName("starrocks-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::StarRocks,
                Some(sr_url.clone()),
                8,
                true,
            ));
            let adapter = Arc::new(
                StarRocksAdapter::new(
                    cluster.clone(),
                    group.clone(),
                    queryflux_engine_adapters::starrocks::StarRocksConfig {
                        endpoint: sr_url,
                        auth: None,
                        pool_size: 2,
                    },
                )
                .map_err(|e| anyhow!("StarRocks adapter: {e}"))?,
            );

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_STARROCKS.to_string(), vec![cluster.0.clone()]);
            group_order.push(GROUP_STARROCKS.to_string());
            available_groups.push(GROUP_STARROCKS.to_string());
            header_map.insert(GROUP_STARROCKS.to_string(), group);
            Some((cluster, adapter))
        } else {
            None
        };

        // --- Lakekeeper + StarRocks Iceberg catalog ---
        let lakekeeper_url = std::env::var("LAKEKEEPER_URL")
            .unwrap_or_else(|_| "http://localhost:18181".to_string());
        if is_lakekeeper_ready(&lakekeeper_url).await {
            if let Some((_, sr)) = &sr_adapter {
                let sr_setup = "CREATE EXTERNAL CATALOG IF NOT EXISTS lakekeeper \
                     PROPERTIES ( \
                       \"type\" = \"iceberg\", \
                       \"iceberg.catalog.type\" = \"rest\", \
                       \"iceberg.catalog.uri\" = \"http://lakekeeper:8181/catalog\", \
                       \"iceberg.catalog.warehouse\" = \"demo\", \
                       \"aws.s3.region\" = \"local\", \
                       \"aws.s3.enable_path_style_access\" = \"true\", \
                       \"aws.s3.endpoint\" = \"http://minio:9000\", \
                       \"aws.s3.access_key\" = \"minio-root-user\", \
                       \"aws.s3.secret_key\" = \"minio-root-password\" \
                     )";
                sr.execute_ddl(sr_setup).await.ok();
            }
            available_groups.push(GROUP_LAKEKEEPER.to_string());
        }

        if let Some((cluster, sr)) = sr_adapter {
            adapters.insert(cluster.0.clone(), AdapterKind::Sync(sr));
        }

        // --- DuckDB (always available — embedded, in-memory, no external dependency) ---
        {
            let group = ClusterGroupName(GROUP_DUCKDB.to_string());
            let cluster = ClusterName("duckdb-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::DuckDb,
                None,
                4,
                true,
            ));
            let adapter = Arc::new(
                DuckDbAdapter::new(
                    cluster.clone(),
                    group.clone(),
                    DuckDbConfig {
                        database_path: None,
                        motherduck_token: None,
                    },
                )
                .map_err(|e| anyhow!("DuckDB adapter: {e}"))?,
            );
            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_DUCKDB.to_string(), vec![cluster.0.clone()]);
            group_order.push(GROUP_DUCKDB.to_string());
            adapters.insert(cluster.0.clone(), AdapterKind::Sync(adapter));
            available_groups.push(GROUP_DUCKDB.to_string());
            header_map.insert(GROUP_DUCKDB.to_string(), group);
        }

        if group_states.len() == 1 {
            // Only DuckDB: warn but don't fail — query-params tests can still run.
            eprintln!(
                "WARNING: No external backends reachable (Trino :18081, StarRocks :9030). \
                 Only DuckDB group available. Start docker compose for full e2e coverage."
            );
        }

        let fallback = pick_fallback_group(&group_order);
        // Snowflake HTTP/SQL-API requests always target DuckDB in the test harness.
        // This prevents Snowflake e2e tests (query_params_tests, etc.) from being routed
        // to the Trino fallback when Trino happens to be reachable but requires auth.
        let duckdb_group = ClusterGroupName(GROUP_DUCKDB.to_string());
        routers.push(Box::new(ProtocolBasedRouter {
            trino_http: None,
            postgres_wire: None,
            mysql_wire: None,
            clickhouse_http: None,
            flight_sql: None,
            snowflake_http: Some(duckdb_group.clone()),
            snowflake_sql_api: Some(duckdb_group),
        }));
        // Route compatibility:
        // - `X-Qf-Group` is our internal E2E routing header (legacy tests).
        // - `X-Trino-Client-Tags` is set by real Trino clients like `trino-rust-client`.
        //   We route on it so e2e tests can behave like real-world Trino traffic.
        let header_map_qf = header_map.clone();
        routers.push(Box::new(HeaderRouter::new(
            "x-qf-group".to_string(),
            header_map_qf,
        )));
        routers.push(Box::new(HeaderRouter::new(
            "x-trino-client-tags".to_string(),
            header_map,
        )));

        let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));
        let translation = Arc::new(TranslationService::disabled());
        let router_chain = RouterChain::new(routers, fallback);

        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        let port = tmp.local_addr()?.port();
        drop(tmp);

        let live_config = LiveConfig {
            router_chain,
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager,
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members,
            group_order,
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization) as Arc<dyn AuthorizationChecker>,
        };
        let records = Arc::new(Mutex::new(Vec::<QueryRecord>::new()));
        let state = Arc::new(AppState {
            external_address: format!("http://127.0.0.1:{port}"),
            live: Arc::new(tokio::sync::RwLock::new(live_config)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation,
            metrics: Arc::new(CapturingMetrics {
                records: records.clone(),
            }),
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            capacity_store: None,
            queue_coordinator: None,
            instance_id: "test-harness".to_string(),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build shared http client"),
            result_cache: Arc::new(queryflux_cache::noop::NoopResultCache),
        });

        let trino_fe = TrinoHttpFrontend::new(state.clone(), port, None);
        let snowflake_fe = SnowflakeFrontend::new(
            state,
            SnowflakeHttpFrontendConfig {
                enabled: true,
                port,
                max_connections: None,
                session_affinity_acknowledged: true,
                session_max_age_secs: 86400,
                session_idle_timeout_secs: 14400,
            },
        );
        // Serve both Trino HTTP (/v1/statement) and Snowflake (wire v1 + SQL API v2)
        // on the same port so query-params e2e tests can use the Snowflake protocol with bindings.
        let router: Router = trino_fe.router().merge(snowflake_fe.router());
        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            port,
            groups: available_groups,
            records,
            _shutdown_tx: shutdown_tx,
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn has_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }

    pub fn clear_records(&self) {
        self.records.lock().expect("lock records").clear();
    }

    pub async fn wait_for_record<F>(&self, predicate: F) -> Option<QueryRecord>
    where
        F: Fn(&QueryRecord) -> bool,
    {
        // `AppState::record_query` schedules `MetricsStore::record_query` via `tokio::spawn`.
        // On slow shared runners the task can land after several hundred ms; keep a generous window.
        for _ in 0..150 {
            if let Some(record) = self
                .records
                .lock()
                .expect("lock records")
                .iter()
                .find(|r| predicate(r))
                .cloned()
            {
                return Some(record);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

fn pick_fallback_group(group_order: &[String]) -> ClusterGroupName {
    // Prefer external engines for the Trino-HTTP fallback path; fall back to DuckDB
    // when no external backend is reachable (e.g. query-params-only CI run).
    for preferred in [GROUP_TRINO, GROUP_STARROCKS, GROUP_DUCKDB] {
        if group_order.iter().any(|g| g == preferred) {
            return ClusterGroupName(preferred.to_string());
        }
    }
    ClusterGroupName(group_order[0].clone())
}

async fn port_is_open(host: &str, port: u16) -> bool {
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect((host, port)),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

async fn is_trino_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(8080);
    port_is_open(host, port).await
}

async fn is_starrocks_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(9030);
    port_is_open(host, port).await
}

async fn is_lakekeeper_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(8181);
    port_is_open(host, port).await
}

// ---------------------------------------------------------------------------
// WireTestHarness — Snowflake HTTP wire v1 + SQL API v2 on a DuckDB backend.
//
// Uses short session TTLs so expiry tests run fast without real-time sleeps.
// ---------------------------------------------------------------------------

pub struct WireTestHarness {
    pub port: u16,
    pub session_idle_timeout_secs: u64,
    pub session_max_age_secs: u64,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl WireTestHarness {
    /// Create a wire harness. `session_max_age_secs` / `session_idle_timeout_secs`
    /// are passed through to the session store so TTL tests can use small values.
    pub async fn new(session_max_age_secs: u64, session_idle_timeout_secs: u64) -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("error")
            .try_init();

        // --- DuckDB only ---
        let group = ClusterGroupName(GROUP_DUCKDB.to_string());
        let cluster = ClusterName("duckdb-wire-1".to_string());
        let cs = Arc::new(ClusterState::new(
            cluster.clone(),
            group.clone(),
            None,
            None,
            EngineType::DuckDb,
            None,
            4,
            true,
        ));
        let adapter = Arc::new(
            DuckDbAdapter::new(
                cluster.clone(),
                group.clone(),
                DuckDbConfig {
                    database_path: None,
                    motherduck_token: None,
                },
            )
            .map_err(|e| anyhow!("DuckDB adapter: {e}"))?,
        );

        let mut group_states: HashMap<ClusterGroupName, _> = HashMap::new();
        group_states.insert(group.clone(), (vec![cs], strategy_from_config(None)));
        let mut adapters: HashMap<String, AdapterKind> = HashMap::new();
        adapters.insert(cluster.0.clone(), AdapterKind::Sync(adapter));

        let duckdb_group = group.clone();
        let router = Box::new(ProtocolBasedRouter {
            trino_http: None,
            postgres_wire: None,
            mysql_wire: None,
            clickhouse_http: None,
            flight_sql: None,
            snowflake_http: Some(duckdb_group.clone()),
            snowflake_sql_api: Some(duckdb_group),
        });

        let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));
        let translation = Arc::new(TranslationService::disabled());
        let router_chain = RouterChain::new(vec![router], group.clone());

        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        let port = tmp.local_addr()?.port();
        drop(tmp);

        let live_config = LiveConfig {
            router_chain,
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager,
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members: HashMap::from([(GROUP_DUCKDB.to_string(), vec![cluster.0.clone()])]),
            group_order: vec![GROUP_DUCKDB.to_string()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization) as Arc<dyn AuthorizationChecker>,
        };

        let state = Arc::new(AppState {
            external_address: format!("http://127.0.0.1:{port}"),
            live: Arc::new(tokio::sync::RwLock::new(live_config)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation,
            metrics: Arc::new(NullMetrics),
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            capacity_store: None,
            queue_coordinator: None,
            instance_id: "wire-test-harness".to_string(),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build http client"),
            result_cache: Arc::new(queryflux_cache::noop::NoopResultCache),
        });

        let snowflake_fe = SnowflakeFrontend::new(
            state,
            SnowflakeHttpFrontendConfig {
                enabled: true,
                port,
                max_connections: None,
                session_affinity_acknowledged: true,
                session_max_age_secs,
                session_idle_timeout_secs,
            },
        );

        let router = snowflake_fe.router();
        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            port,
            session_idle_timeout_secs,
            session_max_age_secs,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Create a wire harness backed by StarRocks instead of DuckDB.
    /// Returns `Ok(None)` when StarRocks is not reachable (tests can skip cleanly).
    pub async fn new_starrocks(
        session_max_age_secs: u64,
        session_idle_timeout_secs: u64,
    ) -> Result<Option<Self>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("error")
            .try_init();

        let sr_url = std::env::var("STARROCKS_URL")
            .unwrap_or_else(|_| "mysql://root@localhost:9030".to_string());
        if !is_starrocks_ready(&sr_url).await {
            return Ok(None);
        }

        let group = ClusterGroupName(GROUP_STARROCKS.to_string());
        let cluster = ClusterName("starrocks-wire-1".to_string());
        let cs = Arc::new(ClusterState::new(
            cluster.clone(),
            group.clone(),
            None,
            None,
            EngineType::StarRocks,
            Some(sr_url.clone()),
            8,
            true,
        ));
        let adapter = Arc::new(
            StarRocksAdapter::new(
                cluster.clone(),
                group.clone(),
                queryflux_engine_adapters::starrocks::StarRocksConfig {
                    endpoint: sr_url,
                    auth: None,
                    pool_size: 2,
                },
            )
            .map_err(|e| anyhow!("StarRocks adapter: {e}"))?,
        );

        let mut group_states: HashMap<ClusterGroupName, _> = HashMap::new();
        group_states.insert(group.clone(), (vec![cs], strategy_from_config(None)));
        let mut adapters: HashMap<String, AdapterKind> = HashMap::new();
        adapters.insert(cluster.0.clone(), AdapterKind::Sync(adapter));

        let sr_group = group.clone();
        let router = Box::new(ProtocolBasedRouter {
            trino_http: None,
            postgres_wire: None,
            mysql_wire: None,
            clickhouse_http: None,
            flight_sql: None,
            snowflake_http: Some(sr_group.clone()),
            snowflake_sql_api: Some(sr_group),
        });

        let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));
        let translation = Arc::new(TranslationService::disabled());
        let router_chain = RouterChain::new(vec![router], group.clone());

        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        let port = tmp.local_addr()?.port();
        drop(tmp);

        let live_config = LiveConfig {
            router_chain,
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager,
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members: HashMap::from([(GROUP_STARROCKS.to_string(), vec![cluster.0.clone()])]),
            group_order: vec![GROUP_STARROCKS.to_string()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization) as Arc<dyn AuthorizationChecker>,
        };

        let state = Arc::new(AppState {
            external_address: format!("http://127.0.0.1:{port}"),
            live: Arc::new(tokio::sync::RwLock::new(live_config)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation,
            metrics: Arc::new(NullMetrics),
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            capacity_store: None,
            queue_coordinator: None,
            instance_id: "wire-starrocks-harness".to_string(),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build http client"),
            result_cache: Arc::new(queryflux_cache::noop::NoopResultCache),
        });

        let snowflake_fe = SnowflakeFrontend::new(
            state,
            SnowflakeHttpFrontendConfig {
                enabled: true,
                port,
                max_connections: None,
                session_affinity_acknowledged: true,
                session_max_age_secs,
                session_idle_timeout_secs,
            },
        );

        let router = snowflake_fe.router();
        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Some(Self {
            port,
            session_idle_timeout_secs,
            session_max_age_secs,
            _shutdown_tx: shutdown_tx,
        }))
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

// Minimal no-op metrics sink for WireTestHarness (avoids the Mutex overhead of CapturingMetrics).
struct NullMetrics;

#[async_trait]
impl MetricsStore for NullMetrics {
    async fn record_query(&self, _r: QueryRecord) -> QfResult<()> {
        Ok(())
    }

    async fn record_cluster_snapshot(&self, _s: ClusterSnapshot) -> QfResult<()> {
        Ok(())
    }
}
