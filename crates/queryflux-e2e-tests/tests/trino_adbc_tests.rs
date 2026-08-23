/// Exercise the Trino ADBC driver path (`driver: trino` in QueryFlux admin config).
///
/// Requires:
/// - Trino reachable at `TRINO_ADBC_URI` (default `http://127.0.0.1:18081`, same port as `TRINO_URL` in docker-compose.test.yml).
/// - The Trino ADBC shared library installed where `adbc_driver_manager` can find it (e.g. `dbc install trino`).
///
/// If the driver is not installed, tests skip with a message instead of failing.
///
/// Run with the same flow as other e2e tests: `make test-e2e` or
/// `cargo test -p queryflux-e2e-tests --test trino_adbc_tests -- --include-ignored`.
use futures::StreamExt;
use queryflux_auth::QueryCredentials;
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use queryflux_core::session::SessionContext;
use queryflux_core::tags::QueryTags;
use queryflux_engine_adapters::adbc::{AdbcAdapter, AdbcConfig};
use queryflux_engine_adapters::{EngineConfigParseable, SyncAdapter};

fn trino_adbc_uri() -> String {
    std::env::var("TRINO_ADBC_URI").unwrap_or_else(|_| "http://127.0.0.1:18081".to_string())
}

fn maybe_trino_adbc_adapter() -> Option<AdbcAdapter> {
    let uri = trino_adbc_uri();
    let config = AdbcConfig {
        driver: "trino".to_string(),
        uri,
        username: None,
        password: None,
        db_kwargs: Vec::new(),
        flight_sql_cluster_dialect: None,
        pool_size: 2,
    };
    match AdbcAdapter::new(
        ClusterName("trino-adbc-e2e".to_string()),
        ClusterGroupName("adbc".to_string()),
        config,
    ) {
        Ok(a) => Some(a),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("failed to load ADBC driver") {
                eprintln!("SKIP trino ADBC: install the Trino ADBC driver (e.g. bash scripts/install-adbc-trino-driver.sh or `dbc install trino`). Detail: {msg}");
                return None;
            }
            panic!("unexpected adapter error: {e}");
        }
    }
}

/// Adapter with a live Trino reachable at `TRINO_ADBC_URI` (driver loaded and `SELECT 1` succeeds).
async fn trino_adbc_adapter_ready() -> Option<AdbcAdapter> {
    let adapter = maybe_trino_adbc_adapter()?;
    if !adapter.health_check().await {
        eprintln!(
            "SKIP: Trino ADBC health check failed — is Trino up at {}?",
            trino_adbc_uri()
        );
        return None;
    }
    Some(adapter)
}

fn empty_trino_session() -> SessionContext {
    SessionContext::default()
}

async fn count_arrow_rows(adapter: &AdbcAdapter, sql: &str) -> usize {
    let session = empty_trino_session();
    let creds = QueryCredentials::ServiceAccount;
    let tags = QueryTags::new();
    let mut exec = adapter
        .execute_as_arrow(
            sql,
            &session,
            &creds,
            &tags,
            &vec![],
            queryflux_core::sql_classify::ExecutionHints::default(),
            &queryflux_engine_adapters::BackendQueryIdSlot::new(),
        )
        .await
        .expect("execute_as_arrow");
    let mut total = 0usize;
    while let Some(batch_res) = exec.stream.next().await {
        let batch = batch_res.expect("record batch");
        total += batch.num_rows();
    }
    total
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_engine_type_is_trino() {
    let Some(adapter) = maybe_trino_adbc_adapter() else {
        return;
    };
    assert_eq!(adapter.engine_type(), EngineType::Trino);
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_health_check() {
    let Some(adapter) = maybe_trino_adbc_adapter() else {
        return;
    };
    if !adapter.health_check().await {
        eprintln!(
            "SKIP: Trino ADBC health check failed — start Trino (e.g. docker compose -f docker/docker-compose.test.yml) at {}",
            trino_adbc_uri()
        );
        return;
    }
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_select_one_returns_arrow() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };
    let n = count_arrow_rows(&adapter, "SELECT 1 AS n").await;
    assert!(n >= 1, "expected at least one row from SELECT 1");
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_select_multi_row() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };
    let n = count_arrow_rows(&adapter, "SELECT v FROM (VALUES (1), (2), (3)) t(v)").await;
    assert_eq!(n, 3, "expected three rows from VALUES");
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_list_catalogs_includes_system() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };
    let catalogs = adapter.list_catalogs().await.expect("list_catalogs");
    assert!(
        catalogs.iter().any(|c| c == "system"),
        "expected catalog 'system', got {catalogs:?}"
    );
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_list_databases_includes_information_schema() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };
    let schemas = adapter
        .list_databases("system")
        .await
        .expect("list_databases");
    assert!(
        schemas.iter().any(|s| s == "information_schema"),
        "expected schema information_schema under system, got {schemas:?}"
    );
}

#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_list_tables_in_information_schema() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };
    let tables = adapter
        .list_tables("system", "information_schema")
        .await
        .expect("list_tables");
    assert!(
        tables.iter().any(|t| t == "tables" || t == "schemata"),
        "expected information_schema.tables or .schemata, got {tables:?}"
    );
}

/// Same as production admin JSON: parse [`AdbcConfig`] then build the adapter.
#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_from_admin_json_config() {
    let uri = trino_adbc_uri();
    let json = serde_json::json!({
        "driver": "trino",
        "uri": uri,
        "poolSize": 2,
    });
    let cfg = AdbcConfig::from_json(&json, "e2e-admin-json").expect("from_json");
    assert_eq!(cfg.engine_type(), EngineType::Trino);

    let adapter = match AdbcAdapter::new(
        ClusterName("trino-adbc-json".to_string()),
        ClusterGroupName("adbc".to_string()),
        cfg,
    ) {
        Ok(a) => a,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("failed to load ADBC driver") {
                eprintln!("SKIP trino ADBC (from_json): {msg}");
                return;
            }
            panic!("unexpected adapter error: {e}");
        }
    };

    if !adapter.health_check().await {
        eprintln!(
            "SKIP: Trino not reachable for from_json test at {}",
            trino_adbc_uri()
        );
        return;
    }

    let n = count_arrow_rows(&adapter, "SELECT 1").await;
    assert!(n >= 1);
}

/// Regression for lakeops-org/queryflux#97: non-read SQL must use ADBC
/// `execute_update`, returning an empty Arrow stream and `affected_rows`.
#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_ddl_uses_execute_update_empty_stream() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };

    let session = empty_trino_session();
    let creds = QueryCredentials::ServiceAccount;
    let tags = QueryTags::new();

    // Force the update path explicitly (same path dispatch takes for classified DDL).
    let hints = queryflux_core::sql_classify::ExecutionHints {
        is_read_like: Some(false),
    };

    let table = format!(
        "memory.default.qf_wire_ok_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let create_sql = format!("CREATE TABLE {table} (id BIGINT)");
    let mut exec = adapter
        .execute_as_arrow(
            &create_sql,
            &session,
            &creds,
            &tags,
            &vec![],
            hints,
            &queryflux_engine_adapters::BackendQueryIdSlot::new(),
        )
        .await
        .expect("CREATE via execute_update path");

    assert!(
        exec.affected_rows.is_some(),
        "DDL must surface affected_rows from execute_update"
    );

    let mut batches = 0usize;
    while let Some(batch_res) = exec.stream.next().await {
        let batch = batch_res.expect("batch");
        batches += 1;
        assert_eq!(
            batch.num_columns(),
            0,
            "DDL update path must not produce a result-set schema"
        );
    }
    assert_eq!(
        batches, 0,
        "DDL execute_update path must yield an empty Arrow stream"
    );

    // Classification without an explicit hint should also route CREATE to update.
    let drop_sql = format!("DROP TABLE IF EXISTS {table}");
    let mut drop_exec = adapter
        .execute_as_arrow(
            &drop_sql,
            &session,
            &creds,
            &tags,
            &vec![],
            queryflux_core::sql_classify::ExecutionHints::default(),
            &queryflux_engine_adapters::BackendQueryIdSlot::new(),
        )
        .await
        .expect("DROP via classified execute_update path");
    assert!(
        drop_exec.affected_rows.is_some(),
        "classified DDL must set affected_rows"
    );
    assert!(
        drop_exec.stream.next().await.is_none(),
        "DROP must also produce an empty stream"
    );
}

/// SELECT still uses the result-set path (execute), not execute_update.
#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_select_does_not_set_affected_rows() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };

    let session = empty_trino_session();
    let creds = QueryCredentials::ServiceAccount;
    let tags = QueryTags::new();
    let hints = queryflux_core::sql_classify::ExecutionHints {
        is_read_like: Some(true),
    };

    let mut exec = adapter
        .execute_as_arrow(
            "SELECT 1 AS n",
            &session,
            &creds,
            &tags,
            &vec![],
            hints,
            &queryflux_engine_adapters::BackendQueryIdSlot::new(),
        )
        .await
        .expect("SELECT");

    assert!(
        exec.affected_rows.is_none(),
        "SELECT must not set affected_rows"
    );
    let mut rows = 0usize;
    while let Some(batch_res) = exec.stream.next().await {
        rows += batch_res.expect("batch").num_rows();
    }
    assert!(rows >= 1);
}

/// Regression: a SELECT matching zero rows must still surface its real schema
/// (not an empty stream that dispatch would misframe as a DDL/DML OK response).
#[tokio::test]
#[ignore = "requires Trino + Trino ADBC driver — run with: make test-e2e"]
async fn trino_adbc_select_zero_rows_still_yields_schema() {
    let Some(adapter) = trino_adbc_adapter_ready().await else {
        return;
    };

    let session = empty_trino_session();
    let creds = QueryCredentials::ServiceAccount;
    let tags = QueryTags::new();
    let hints = queryflux_core::sql_classify::ExecutionHints {
        is_read_like: Some(true),
    };

    let mut exec = adapter
        .execute_as_arrow(
            "SELECT 1 AS n WHERE 1 = 0",
            &session,
            &creds,
            &tags,
            &vec![],
            hints,
            &queryflux_engine_adapters::BackendQueryIdSlot::new(),
        )
        .await
        .expect("SELECT");

    assert!(
        exec.affected_rows.is_none(),
        "SELECT must not set affected_rows"
    );
    let batch = exec
        .stream
        .next()
        .await
        .expect("empty SELECT must still yield one batch carrying the real schema")
        .expect("batch");
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema().fields().len(), 1, "schema must have column n");
    assert_eq!(batch.schema().field(0).name(), "n");
    assert!(exec.stream.next().await.is_none(), "only one (empty) batch expected");
}
