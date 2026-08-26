//! Embedding smoke tests: exercise the `QueryFlux::builder()` surface end to end —
//! a mock `SyncAdapter` registered via `.with_adapter()`, an extra `Guard`, an extra
//! `RouterTrait`, and a `QueryHook` — without starting any real network listener.
//!
//! `execute_to_sink` is called directly against the built `AppState`, the same way a
//! frontend handler would, so these tests exercise the real dispatch path (guards,
//! hooks, translation skip, adapter call) rather than a purpose-built test seam.

use std::io::Write;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_auth::{AuthContext, QueryCredentials};
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_core::params::QueryParams;
use queryflux_core::query::{
    BackendQueryId, ClusterGroupName, EngineType, FrontendProtocol, QueryStats,
};
use queryflux_core::session::SessionContext;
use queryflux_core::tags::QueryTags;
use queryflux_engine_adapters::{AdapterKind, BackendQueryIdSlot, SyncAdapter, SyncExecution};
use queryflux_frontend::dispatch::{execute_to_sink, ResultSink};
use queryflux_frontend::hook::{HookContext, HookOutcome, QueryHook};
use queryflux_guardrails::built_in::Guard;
use queryflux_guardrails::{GuardContext, GuardLayer, GuardResult};
use queryflux_routing::{RouterTrait, RoutingDecision};

/// Minimal YAML: in-memory persistence, no YAML-defined clusters (the mock adapter is
/// registered in code via `.with_adapter`), one group whose sole member is that adapter,
/// and a fast reload interval so the "extras survive reload" test doesn't need to wait.
fn test_config_yaml() -> String {
    r#"
queryflux:
  externalAddress: http://localhost:0
  frontends:
    trinoHttp:
      enabled: false
      port: 0
  persistence:
    type: inMemory
  adminApi:
    port: 0
  configReloadIntervalSecs: 1

clusterGroups:
  mock:
    enabled: true
    maxRunningQueries: 10
    members: [mock-1]
    strategy:
      type: roundRobin

routers: []
routingFallback: mock

translation:
  errorOnUnsupported: false
"#
    .to_string()
}

fn write_temp_config() -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("create temp config file");
    f.write_all(test_config_yaml().as_bytes())
        .expect("write temp config");
    f
}

/// A trivial in-memory `SyncAdapter`: always returns one row, unless `sql` is exactly
/// `"FAIL"`, in which case it returns an error (used to exercise hook `on_error`).
/// Records every SQL string it's asked to run, so tests can assert whether a guard
/// denial actually stopped the query before it reached the adapter.
#[derive(Default)]
struct MockAdapter {
    calls: Mutex<Vec<String>>,
}

impl MockAdapter {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl SyncAdapter for MockAdapter {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        _credentials: &QueryCredentials,
        _tags: &QueryTags,
        _params: &QueryParams,
        _hints: queryflux_core::sql_classify::ExecutionHints,
        _id_slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        self.calls.lock().unwrap().push(sql.to_string());
        if sql == "FAIL" {
            return Err(QueryFluxError::Engine("mock adapter failure".to_string()));
        }

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let stream = futures::stream::once(async move { Ok(batch) });
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(None);
        Ok(SyncExecution {
            stream: Box::pin(stream),
            stats: rx,
            affected_rows: None,
        })
    }

    fn engine_type(&self) -> EngineType {
        // Exercises the open EngineType::Custom variant: this adapter isn't one of the
        // built-in engines, and core never needs to know that.
        EngineType::Custom("mock".to_string())
    }

    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> bool {
        true
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn describe_table(
        &self,
        _catalog: &str,
        _database: &str,
        _table: &str,
    ) -> Result<Option<queryflux_core::catalog::TableSchema>> {
        Ok(None)
    }
}

/// Denies any query whose translated SQL contains "DROP" (case-insensitive).
struct NoDropGuard;

#[async_trait]
impl Guard for NoDropGuard {
    fn name(&self) -> &'static str {
        "no_drop"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        if ctx.translated_sql.to_uppercase().contains("DROP") {
            GuardResult::deny("DROP is not permitted in this deployment", "NO_DROP")
        } else {
            GuardResult::allow()
        }
    }
}

/// Always routes to the `mock` group — stands in for a real routing policy (e.g.
/// geo-based or cost-based) an embedder would implement.
struct AlwaysMockRouter;

#[async_trait]
impl RouterTrait for AlwaysMockRouter {
    fn type_name(&self) -> &'static str {
        "AlwaysMock"
    }
    async fn route(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _protocol: &FrontendProtocol,
        _auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        Ok(RoutingDecision::Route(ClusterGroupName("mock".to_string())))
    }
}

/// Records which lifecycle points fired, for assertions.
#[derive(Default)]
struct RecordingHook {
    before_execute: Mutex<Vec<String>>,
    errors: Mutex<Vec<String>>,
}

#[async_trait]
impl QueryHook for RecordingHook {
    async fn before_execute(&self, ctx: &HookContext<'_>) -> HookOutcome {
        self.before_execute.lock().unwrap().push(ctx.sql.clone());
        HookOutcome::Continue
    }

    async fn on_error(&self, ctx: &HookContext<'_>, err: &QueryFluxError) {
        self.errors
            .lock()
            .unwrap()
            .push(format!("{}: {err}", ctx.sql));
    }
}

/// Captures what a dispatched query produced, without needing a real frontend.
#[derive(Default, Debug)]
struct TestSink {
    rows: usize,
    error: Option<String>,
    completed: bool,
}

#[async_trait]
impl ResultSink for TestSink {
    async fn on_schema(&mut self, _schema: &arrow::datatypes::Schema) -> Result<()> {
        Ok(())
    }
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows();
        Ok(())
    }
    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        self.completed = true;
        Ok(())
    }
    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.error = Some(message.to_string());
        Ok(())
    }
}

fn test_auth() -> AuthContext {
    AuthContext {
        user: "embed-test".to_string(),
        groups: vec![],
        roles: vec![],
        raw_token: None,
        ..Default::default()
    }
}

async fn dispatch(app_state: &Arc<queryflux_frontend::state::AppState>, sql: &str) -> TestSink {
    let mut sink = TestSink::default();
    let _ = execute_to_sink(
        app_state,
        sql.to_string(),
        vec![],
        SessionContext::default(),
        FrontendProtocol::TrinoHttp,
        ClusterGroupName("mock".to_string()),
        &mut sink,
        &test_auth(),
    )
    .await;
    sink
}

#[tokio::test]
async fn extra_guard_denies_drop_before_adapter_runs() {
    let config = write_temp_config();
    let adapter = Arc::new(MockAdapter::default());

    let qf = queryflux::QueryFlux::builder()
        .config_path(config.path().to_str().unwrap())
        .with_builtin_plugins()
        .with_adapter("mock-1", AdapterKind::Sync(adapter.clone()))
        .router_prepend(Box::new(AlwaysMockRouter))
        .guard(Box::new(NoDropGuard))
        .build()
        .await
        .expect("build");

    let sink = dispatch(qf.app_state(), "DROP TABLE t").await;
    assert!(sink.error.is_some(), "DROP must be denied: {sink:?}");
    assert!(
        adapter.calls().is_empty(),
        "guard must stop the query before the adapter ever runs"
    );

    // A normal query still reaches the adapter — the guard is SQL-specific, not blanket.
    let sink = dispatch(qf.app_state(), "SELECT 1").await;
    assert_eq!(sink.error, None, "non-DROP query must not be denied");
    assert_eq!(sink.rows, 1);
    assert_eq!(adapter.calls(), vec!["SELECT 1".to_string()]);
}

#[tokio::test]
async fn hook_records_before_execute_and_on_error() {
    let config = write_temp_config();
    let adapter = Arc::new(MockAdapter::default());
    let hook = Arc::new(RecordingHook::default());

    let qf = queryflux::QueryFlux::builder()
        .config_path(config.path().to_str().unwrap())
        .with_builtin_plugins()
        .with_adapter("mock-1", AdapterKind::Sync(adapter.clone()))
        .router_prepend(Box::new(AlwaysMockRouter))
        .hook(hook.clone())
        .build()
        .await
        .expect("build");

    let sink = dispatch(qf.app_state(), "SELECT 1").await;
    assert_eq!(sink.error, None);
    assert_eq!(
        hook.before_execute.lock().unwrap().as_slice(),
        ["SELECT 1".to_string()]
    );
    assert!(hook.errors.lock().unwrap().is_empty());

    let sink = dispatch(qf.app_state(), "FAIL").await;
    assert!(
        sink.error.is_some(),
        "adapter failure must surface as an error"
    );
    let errors = hook.errors.lock().unwrap();
    assert_eq!(
        errors.len(),
        1,
        "on_error must fire exactly once: {errors:?}"
    );
    assert!(errors[0].contains("mock adapter failure"));
}

#[tokio::test]
async fn live_config_reload_keeps_registered_extras() {
    let config = write_temp_config();
    let adapter = Arc::new(MockAdapter::default());

    let qf = queryflux::QueryFlux::builder()
        .config_path(config.path().to_str().unwrap())
        .with_builtin_plugins()
        .with_adapter("mock-1", AdapterKind::Sync(adapter.clone()))
        .router_prepend(Box::new(AlwaysMockRouter))
        .guard(Box::new(NoDropGuard))
        .build()
        .await
        .expect("build");

    // Sanity: the guard works right after build.
    let sink = dispatch(qf.app_state(), "DROP TABLE t").await;
    assert!(sink.error.is_some());

    // configReloadIntervalSecs: 1 in the test config — wait past at least one tick of
    // the background reload loop, which rebuilds LiveConfig (including guard_chain)
    // from scratch every time. The registered guard must still be there afterward.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let sink = dispatch(qf.app_state(), "DROP TABLE t").await;
    assert!(
        sink.error.is_some(),
        "extra guard must survive a LiveConfig reload, not just the initial build"
    );

    // The with_adapter cluster (not backed by any YAML/DB row) must also still be
    // routable after reload.
    let sink = dispatch(qf.app_state(), "SELECT 1").await;
    assert_eq!(sink.error, None);
    assert!(adapter.calls().contains(&"SELECT 1".to_string()));
}
