//! Real dispatch + filesystem OpenDAL cache + embedded DuckDB regressions.
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use queryflux_auth::{
    AllowAllAuthorization, AuthContext, AuthProvider, AuthorizationChecker,
    BackendIdentityResolver, NoneAuthProvider, QueryAction, QueryAuthz,
};
use queryflux_cache::{opendal_cache::OpenDalResultCache, CacheKey, QueryResultCache};
use queryflux_cluster_manager::{cluster_state::ClusterState, simple::SimpleClusterGroupManager};
use queryflux_core::{
    config::{CacheBackendConfig, GroupCacheConfig},
    error::Result,
    query::{ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStats},
    session::SessionContext,
};
use queryflux_engine_adapters::{
    duckdb::{DuckDbAdapter, DuckDbConfig, DEFAULT_MAX_RESULT_BUFFER_BYTES},
    AdapterKind,
};
use queryflux_frontend::{
    dispatch::{execute_to_sink, ResultSink},
    state::{AppState, LiveConfig},
};
use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
use queryflux_persistence::{cache_store::CacheStore, in_memory::InMemoryPersistence};
use queryflux_routing::chain::RouterChain;
use queryflux_translation::TranslationService;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};
use tokio::sync::RwLock;

#[derive(Default)]
struct Metrics {
    hits: AtomicUsize,
}
#[async_trait]
impl MetricsStore for Metrics {
    /// Discard query history so cache assertions depend only on synchronous hit counters.
    async fn record_query(&self, _: QueryRecord) -> Result<()> {
        Ok(())
    }
    /// Ignore cluster snapshots, which are unrelated to result-cache behavior.
    async fn record_cluster_snapshot(&self, _: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
    /// Count successful cache replays independently of query-history persistence.
    fn on_cache_hit(&self, _: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
struct Sink {
    batches: Vec<RecordBatch>,
    completed: bool,
}
#[async_trait]
impl ResultSink for Sink {
    /// Accept schema callbacks; result assertions inspect the collected Arrow batches.
    async fn on_schema(&mut self, _: &Schema) -> Result<()> {
        Ok(())
    }
    /// Retain each batch so tests can compare cached results with engine output.
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.batches.push(batch.clone());
        Ok(())
    }
    /// Record terminal success so fixture requests cannot silently finish incomplete.
    async fn on_complete(&mut self, _: &QueryStats) -> Result<()> {
        self.completed = true;
        Ok(())
    }
    /// Propagate engine errors for assertions that invalid SQL still reaches DuckDB.
    async fn on_error(&mut self, message: &str) -> Result<()> {
        Err(queryflux_core::error::QueryFluxError::Engine(
            message.into(),
        ))
    }
}

struct Fixture {
    state: Arc<AppState>,
    store: Arc<InMemoryPersistence>,
    root: std::path::PathBuf,
    session: SessionContext,
    metrics: Arc<Metrics>,
}
impl Drop for Fixture {
    /// Remove this fixture's cache files even when a test exits through a failed assertion.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    /// Create an isolated filesystem cache, metadata store, and in-memory DuckDB database.
    async fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("queryflux-cache-write-{}", uuid::Uuid::new_v4()));
        let store = Arc::new(InMemoryPersistence::new());
        let cfg = CacheBackendConfig {
            scheme: "fs".into(),
            compression: Default::default(),
            options: HashMap::from([("root".into(), root.to_str().unwrap().into())]),
            cleanup_interval_secs: 300,
        };
        let cache = Arc::new(OpenDalResultCache::new(&cfg, store.clone()).unwrap());
        let metrics = Arc::new(Metrics::default());
        Self {
            state: test_state(cache, metrics.clone()).await,
            metrics,
            store,
            root,
            session: SessionContext {
                database: Some("main".into()),
                ..Default::default()
            },
        }
    }
    /// Build the same cache key dispatch uses for this fixture's identity and database.
    fn key(&self, sql: &str) -> CacheKey {
        CacheKey::new(sql, "duckdb", &self.session, "alice", &[])
    }
    /// Dispatch one separate request, returning its results or propagating execution errors.
    async fn try_run(&self, sql: &str) -> Result<Sink> {
        let mut sink = Sink::default();
        execute_to_sink(
            &self.state,
            sql.into(),
            vec![],
            self.session.clone(),
            FrontendProtocol::Mcp,
            ClusterGroupName("duckdb".into()),
            &mut sink,
            &AuthContext {
                user: "alice".into(),
                ..Default::default()
            },
        )
        .await?;
        assert!(sink.completed);
        Ok(sink)
    }
    /// Execute a request that must succeed and return its collected results.
    async fn run(&self, sql: &str) -> Sink {
        self.try_run(sql).await.unwrap()
    }
    /// Read the number of successful cache replays observed by this fixture.
    fn hits(&self) -> usize {
        self.metrics.hits.load(Ordering::SeqCst)
    }
    /// Execute a query whose first result cell must be a non-null Arrow Int64 value.
    async fn scalar(&self, sql: &str) -> i64 {
        self.run(sql).await.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
    /// Store a sentinel result directly to model an entry created before eligibility checks.
    async fn seed(&self, sql: &str) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "poison",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![999]))])
                .unwrap();
        let mut writer = self
            .state
            .result_cache
            .writer(&self.key(sql), 300)
            .await
            .unwrap();
        writer.write_schema(&schema).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(true).await.unwrap();
        assert!(self.cached(sql).await, "fixture must seed a live entry");
    }
    /// Check whether unexpired cache metadata exists for this exact request.
    async fn cached(&self, sql: &str) -> bool {
        self.store
            .cache_get_valid(&self.key(sql).hex)
            .await
            .unwrap()
            .is_some()
    }
}

/// Reproduce the reported duplicate insert and require two rows with no write caching.
#[tokio::test]
async fn repeated_insert_returning_executes_twice() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE cache_write_repro (id INTEGER)").await;
    let sql = "INSERT INTO cache_write_repro VALUES (1) RETURNING id";
    f.run(sql).await;
    let stored = f.cached(sql).await;
    f.run(sql).await;
    let result = f.run("SELECT COUNT(*) FROM cache_write_repro").await;
    let count = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "insert entry stored: {stored}; cache hits: {}; count after two inserts: {count}",
        f.metrics.hits.load(Ordering::SeqCst)
    );
    assert_eq!(count, 2);
    assert!(!stored, "writes must not be stored");
    assert_eq!(f.hits(), 0);
}
/// Wire real dispatch to one DuckDB connection and an enabled 300-second result cache.
async fn test_state(cache: Arc<dyn QueryResultCache>, metrics: Arc<Metrics>) -> Arc<AppState> {
    let group_name = ClusterGroupName("duckdb".into());
    let cluster_name = ClusterName("duckdb-1".into());
    let adapter = AdapterKind::Sync(Arc::new(
        DuckDbAdapter::new(
            cluster_name.clone(),
            group_name.clone(),
            DuckDbConfig {
                database_path: None,
                motherduck_token: None,
                pool_size: 1,
                max_result_buffer_bytes: DEFAULT_MAX_RESULT_BUFFER_BYTES,
            },
        )
        .unwrap(),
    ));
    let cluster_state = Arc::new(ClusterState::new(
        cluster_name.clone(),
        group_name.clone(),
        None,
        None,
        EngineType::DuckDb,
        None,
        10,
        true,
    ));
    let mut adapters = HashMap::new();
    adapters.insert(cluster_name.0.clone(), adapter);
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
        access_control_guard: None,
        cluster_manager: Arc::new(SimpleClusterGroupManager::new(groups)),
        adapters,
        health_check_targets: vec![],
        custom_health_queries: HashMap::new(),
        custom_reconcile_queries: HashMap::new(),
        cluster_configs: HashMap::new(),
        group_members,
        group_order: vec![group_name.0.clone()],
        group_translation_scripts: HashMap::new(),
        group_default_tags: HashMap::new(),
        group_max_queued_queries: HashMap::new(),
        group_capacity_wait_timeout_secs: HashMap::new(),
        group_cache_settings: HashMap::from([(
            "duckdb".into(),
            GroupCacheConfig {
                enabled: true,
                ttl_secs: 300,
                max_entry_size_mb: None,
            },
        )]),
        auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
        authorization: Arc::new(AllowAllAuthorization::default()) as Arc<dyn AuthorizationChecker>,
        catalog: Arc::new(queryflux_core::catalog::NullCatalogProvider),
    };
    Arc::new(AppState {
        external_address: "http://127.0.0.1:8080".into(),
        live: Arc::new(RwLock::new(live)),
        persistence: Arc::new(InMemoryPersistence::new()),
        translation: Arc::new(TranslationService::disabled()),
        metrics,
        identity_resolver: Arc::new(BackendIdentityResolver::new()),
        capacity_store: None,
        queue_coordinator: None,
        instance_id: "test".into(),
        http_client: reqwest::Client::new(),
        result_cache: cache,
    })
}

/// Require DDL and DML to execute even when matching sentinel cache entries exist.
#[tokio::test]
async fn writes_do_not_replay_existing_entries() {
    let f = Fixture::new().await;
    let create = "CREATE TABLE t (id BIGINT)";
    f.seed(create).await;
    f.run(create).await;
    for sql in [
        "-- a comment\nINSERT INTO t VALUES (1) RETURNING id",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET id = id + 1 RETURNING id",
    ] {
        f.seed(sql).await;
        f.run(sql).await;
        f.run(sql).await;
    }
    assert_eq!(f.scalar("SELECT COUNT(*) FROM t WHERE id = 3").await, 4);
    let delete = "DELETE FROM t WHERE id = 3 RETURNING id";
    f.seed(delete).await;
    let deleted = f.run(delete).await;
    assert_eq!(
        deleted
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(f.scalar("SELECT COUNT(*) FROM t").await, 0);
    assert_eq!(f.hits(), 0);
}

/// Verify repeated updates and deletes change database state instead of replaying RETURNING rows.
#[tokio::test]
async fn repeated_update_and_delete_returning_execute() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE t (id BIGINT)").await;
    f.run("INSERT INTO t VALUES (1), (2)").await;
    let update = "UPDATE t SET id = id + 1 RETURNING id";
    f.run(update).await;
    f.run(update).await;
    assert_eq!(f.scalar("SELECT MAX(id) FROM t").await, 4);
    assert!(!f.cached(update).await);
    let delete = "DELETE FROM t WHERE id = (SELECT MIN(id) FROM t) RETURNING id";
    f.run(delete).await;
    f.run(delete).await;
    assert_eq!(f.scalar("SELECT COUNT(*) FROM t").await, 0);
    assert!(!f.cached(delete).await);
    assert_eq!(f.hits(), 0);
}

/// Verify supported reads store results and replay identical Arrow batches on repetition.
#[tokio::test]
async fn deterministic_selects_still_hit_cache() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE t (id INTEGER)").await;
    f.run("INSERT INTO t VALUES (1)").await;
    for sql in [
        "SELECT 42",
        "/* comment */ SELECT COUNT(*) FROM t",
        "WITH r AS (SELECT id FROM t) SELECT * FROM r",
        "SELECT id FROM t WHERE 'apple' LIKE 'a%'",
        "SELECT id FROM t WHERE 'APPLE' ILIKE 'a%'",
        "SELECT id FROM t WHERE 'a_b' LIKE 'a!_%' ESCAPE '!'",
    ] {
        let before = f.hits();
        let first = f.run(sql).await;
        assert!(f.cached(sql).await);
        assert_eq!(f.hits(), before);
        let second = f.run(sql).await;
        assert_eq!(first.batches, second.batches);
        assert_eq!(f.hits(), before + 1, "{sql}");
    }
}

/// Both configuration scopes can turn a deterministic read into a sequence advance.
async fn assert_translation_fixups_bypass_cache(global: bool) {
    for hinted in [false, true] {
        for seeded in [false, true] {
            let mut f = Fixture::new().await;
            f.run("CREATE SEQUENCE cache_fixup_seq START 1").await;
            let script =
                "def transform(sql, src, dst):\n    return \"SELECT nextval('cache_fixup_seq')\"";
            let scripts = if global { vec![script.into()] } else { vec![] };
            Arc::get_mut(&mut f.state).unwrap().translation =
                Arc::new(TranslationService::new_sqlglot(scripts).unwrap());
            f.session.extra.insert("dialect".into(), "duckdb".into());
            if !global {
                f.state
                    .live
                    .write()
                    .await
                    .group_translation_scripts
                    .insert("duckdb".into(), vec![script.into()]);
            }
            if hinted {
                f.state.live.write().await.group_cache_settings.clear();
                f.session
                    .extra
                    .insert("x-queryflux-cache".into(), "true".into());
            }
            let sql = "SELECT 1";
            if seeded {
                f.seed(sql).await;
            }
            assert_eq!(
                f.scalar(sql).await,
                1,
                "global={global}, hinted={hinted}, seeded={seeded}"
            );
            assert_eq!(
                f.scalar(sql).await,
                2,
                "each request must advance the sequence"
            );
            assert_eq!(f.hits(), 0);
            if !seeded {
                assert!(!f.cached(sql).await, "fixup results must not be stored");
            }
        }
    }
}

/// Group fixups bypass fresh and seeded entries, including when hints enable caching.
#[tokio::test]
async fn group_translation_fixups_bypass_cache() {
    assert_translation_fixups_bypass_cache(false).await;
}

/// Global fixups obey the same cache bypass as group-specific scripts.
#[tokio::test]
async fn global_translation_fixups_bypass_cache() {
    assert_translation_fixups_bypass_cache(true).await;
}

/// Fixups on another group must not disable this group's deterministic read cache.
#[tokio::test]
async fn unrelated_group_fixups_preserve_cache_hits() {
    let mut f = Fixture::new().await;
    Arc::get_mut(&mut f.state).unwrap().translation =
        Arc::new(TranslationService::new_sqlglot(vec![]).unwrap());
    f.session.extra.insert("dialect".into(), "duckdb".into());
    f.state.live.write().await.group_translation_scripts.insert(
        "another-group".into(),
        vec!["def transform(sql, src, dst): return sql".into()],
    );
    let sql = "SELECT CAST(42 AS BIGINT)";
    assert_eq!(f.scalar(sql).await, 42);
    assert!(f.cached(sql).await);
    assert_eq!(f.scalar(sql).await, 42);
    assert_eq!(f.hits(), 1);
}

/// Reload scripts exactly after dispatch snapshots configuration, without timing races.
struct ReloadFixupsOnAuthorize {
    live: Weak<RwLock<LiveConfig>>,
    scripts: Vec<String>,
}

#[async_trait]
impl AuthorizationChecker for ReloadFixupsOnAuthorize {
    /// Apply one reload before allowing the request to continue to cache lookup/setup.
    async fn check(&self, _: &AuthContext, group: &str) -> bool {
        let live = self.live.upgrade().unwrap();
        let mut live = live.write().await;
        live.group_translation_scripts
            .insert(group.into(), self.scripts.clone());
        live.authorization = Arc::new(AllowAllAuthorization::default());
        true
    }

    /// Query ownership checks are unrelated to this execution-only fixture.
    async fn check_query(&self, _: &AuthContext, _: QueryAction, _: &QueryAuthz) -> bool {
        true
    }
}

/// Adding/removing scripts mid-request must not mix cache eligibility and execution states.
#[tokio::test]
async fn group_fixup_reload_preserves_request_snapshot() {
    for initially_configured in [false, true] {
        let mut f = Fixture::new().await;
        Arc::get_mut(&mut f.state).unwrap().translation =
            Arc::new(TranslationService::new_sqlglot(vec![]).unwrap());
        f.session.extra.insert("dialect".into(), "duckdb".into());
        let scripts =
            vec!["def transform(sql, src, dst): return 'SELECT CAST(99 AS BIGINT)'".into()];
        {
            let mut live = f.state.live.write().await;
            if initially_configured {
                live.group_translation_scripts
                    .insert("duckdb".into(), scripts.clone());
            }
            live.authorization = Arc::new(ReloadFixupsOnAuthorize {
                live: Arc::downgrade(&f.state.live),
                scripts: if initially_configured {
                    vec![]
                } else {
                    scripts
                },
            });
        }
        let sql = "SELECT CAST(42 AS BIGINT)";
        let first = f.scalar(sql).await;
        assert_eq!(f.hits(), 0);
        assert_eq!(f.cached(sql).await, !initially_configured);
        // Once scripts are removed, no script-produced result may be replayed.
        f.state.live.write().await.group_translation_scripts.clear();
        assert_eq!(f.scalar(sql).await, 42, "reload must not poison the cache");
        assert_eq!(f.scalar(sql).await, 42);
        assert_eq!(f.hits(), if initially_configured { 1 } else { 2 });
        assert_eq!(first, if initially_configured { 99 } else { 42 });
    }
}

/// MCP without a declared dialect skips scripts and can still store/replay cached reads.
#[tokio::test]
async fn mcp_without_dialect_preserves_cache_with_fixups() {
    for global in [false, true] {
        for hinted in [false, true] {
            for seeded in [false, true] {
                let mut f = Fixture::new().await;
                let scripts =
                    vec!["def transform(sql, src, dst): return 'SELECT CAST(99 AS BIGINT)'".into()];
                Arc::get_mut(&mut f.state).unwrap().translation = Arc::new(
                    TranslationService::new_sqlglot(if global { scripts.clone() } else { vec![] })
                        .unwrap(),
                );
                if !global {
                    f.state
                        .live
                        .write()
                        .await
                        .group_translation_scripts
                        .insert("duckdb".into(), scripts);
                }
                if hinted {
                    f.state.live.write().await.group_cache_settings.clear();
                    f.session
                        .extra
                        .insert("x-queryflux-cache".into(), "true".into());
                }
                let sql = "SELECT CAST(42 AS BIGINT)";
                if seeded {
                    f.seed(sql).await;
                }
                let expected = if seeded { 999 } else { 42 };
                assert_eq!(f.scalar(sql).await, expected);
                assert!(f.cached(sql).await);
                assert_eq!(f.scalar(sql).await, expected);
                assert_eq!(f.hits(), if seeded { 2 } else { 1 });
            }
        }
    }
}

/// Reject caching for mixed batches regardless of whether the write comes first or last.
#[tokio::test]
async fn mixed_read_write_batches_bypass_lookup_and_storage() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE t (id INTEGER)").await;
    for sql in [
        "SELECT 0; INSERT INTO t VALUES (1) RETURNING id",
        "INSERT INTO t VALUES (1); SELECT 0",
    ] {
        f.run(sql).await;
        assert!(!f.cached(sql).await);
        f.seed(sql).await;
        f.run(sql).await;
    }
    assert_eq!(f.scalar("SELECT COUNT(*) FROM t").await, 4);
    assert_eq!(f.hits(), 0);
}

/// Preserve engine errors for invalid SQL despite a matching cached result.
#[tokio::test]
async fn parse_failures_do_not_replay_existing_entries() {
    let f = Fixture::new().await;
    let sql = "SELECT (";
    f.seed(sql).await;
    let err = f
        .try_run(sql)
        .await
        .expect_err("DuckDB must receive the invalid SQL");
    assert!(err.to_string().contains("DuckDB prepare failed"), "{err}");
    assert_eq!(f.hits(), 0);
    // The next valid request still executes and caches normally.
    f.run("SELECT 42").await;
    f.run("SELECT 42").await;
    assert_eq!(f.hits(), 1);
}

/// Require sequence-advancing SELECT functions to execute on every request.
#[tokio::test]
async fn unsupported_select_functions_execute_normally() {
    let f = Fixture::new().await;
    f.run("CREATE SEQUENCE counter START 1").await;
    let sql = "SELECT nextval('counter')";
    assert_eq!(f.scalar(sql).await, 1);
    assert!(!f.cached(sql).await);
    f.seed(sql).await;
    assert_eq!(f.scalar(sql).await, 2);
    assert_eq!(f.hits(), 0);
}

/// Verify sampling bypasses caching without asserting differences between random results.
#[tokio::test]
async fn random_sampling_bypasses_lookup_and_storage() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE t (id INTEGER)").await;
    f.run("INSERT INTO t VALUES (1), (2), (3)").await;
    let sql = "SELECT * FROM t USING SAMPLE 50 PERCENT";
    f.run(sql).await;
    assert!(!f.cached(sql).await);
    f.seed(sql).await;
    f.run(sql).await;
    assert_eq!(f.hits(), 0);
}

/// Catch sequence side effects inside ESCAPE expressions that the generic AST walk skips.
#[tokio::test]
async fn pattern_escape_side_effects_bypass_lookup_and_storage() {
    let f = Fixture::new().await;
    f.run("CREATE SEQUENCE counter START 1").await;
    let sql = "SELECT 'a' LIKE 'a' ESCAPE CAST(nextval('counter') AS VARCHAR)";
    f.run(sql).await;
    assert!(!f.cached(sql).await);
    f.seed(sql).await;
    f.run(sql).await;
    assert_eq!(f.scalar("SELECT currval('counter')").await, 2);
    assert_eq!(f.hits(), 0);
}

/// Check that header, tag, and comment hints enable read caching but cannot cache writes.
#[tokio::test]
async fn hints_cannot_enable_caching_for_writes() {
    for hint in ["header", "tag", "comment"] {
        let mut f = Fixture::new().await;
        f.state.live.write().await.group_cache_settings.clear();
        f.run("CREATE TABLE t (id INTEGER)").await;
        match hint {
            "header" => {
                f.session
                    .extra
                    .insert("x-queryflux-cache".into(), "true".into());
            }
            "tag" => {
                f.session.tags.insert("queryflux:cache".into(), None);
            }
            _ => {}
        }
        let prefix = if hint == "comment" {
            "/* queryflux:cache:ttl=300 */ "
        } else {
            ""
        };
        let sql = format!("{prefix}INSERT INTO t VALUES (1) RETURNING id");
        f.run(&sql).await;
        assert!(!f.cached(&sql).await, "{hint}");
        f.seed(&sql).await;
        f.run(&sql).await;
        assert_eq!(f.scalar("SELECT COUNT(*) FROM t").await, 2, "{hint}");
        assert_eq!(f.hits(), 0, "{hint}");
        let read = format!("{prefix}SELECT 42");
        f.run(&read).await;
        assert!(
            f.cached(&read).await,
            "hint must actually enable caching: {hint}"
        );
        f.run(&read).await;
        assert_eq!(f.hits(), 1, "{hint}");
    }
}
