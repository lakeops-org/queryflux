//! End-to-end tests for the OPA-backed data access-control guard, through a real
//! Postgres-wire frontend backed by an in-process DuckDB instance and a tiny in-process
//! OPA stub (a real HTTP server, not a mock of `PolicyDecisionProvider`).
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_tests`

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::{extract::State, routing::post, Json, Router};
use queryflux_access_control::{build_controller, AccessControlConfig, NoopMetrics};
use queryflux_core::access_config::{OnMissingSchema, OpaProviderConfig, ProviderKind};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use queryflux_frontend::access_control_guard::OpaAccessGuard;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_postgres::SimpleQueryMessage;

/// Shared, mutable decision table the stub OPA server consults per request.
#[derive(Default)]
struct StubState {
    deny_tables: HashSet<String>,
    row_filters: HashMap<String, String>,
}

async fn opa_handler(
    State(state): State<Arc<Mutex<StubState>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let resources = body["input"]["action"]["resources"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let st = state.lock().unwrap();
    let mut out = Vec::new();
    for r in resources {
        let table = r["table"].as_str().unwrap_or("").to_string();
        let mut entry = json!({ "table": table, "allow": true });
        if st.deny_tables.contains(&table) {
            entry["allow"] = json!(false);
            entry["reason"] = json!("denied by stub policy");
        } else if let Some(filter) = st.row_filters.get(&table) {
            entry["rowFilters"] = json!([{ "expression": filter }]);
        }
        out.push(entry);
    }
    Json(json!({ "result": { "resources": out } }))
}

/// Start the stub OPA server and return its base URL + a handle to mutate decisions.
async fn start_opa_stub() -> (String, Arc<Mutex<StubState>>) {
    let state = Arc::new(Mutex::new(StubState::default()));
    let app = Router::new()
        .route("/v1/data/queryflux/access", post(opa_handler))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind opa stub");
    let addr = listener.local_addr().expect("stub addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}"), state)
}

/// Build a real `OpaAccessGuard` pointed at the stub, evaluating every SELECT.
fn build_guard(opa_url: &str) -> Arc<OpaAccessGuard> {
    let cfg = AccessControlConfig {
        provider: ProviderKind::Opa,
        opa: Some(OpaProviderConfig {
            url: opa_url.to_string(),
            decision_path: "/v1/data/queryflux/access".to_string(),
            timeout_ms: 2_000,
            bearer_token: None,
            client_credentials: None,
        }),
        operations: vec!["table.select".to_string()],
        on_missing_schema: OnMissingSchema::Evaluate,
        fail_open: false,
        cache_ttl_ms: 0, // disable caching so every test query hits the stub fresh
        cache_capacity: 100,
        session_param_keys: vec![],
        groups: HashMap::new(),
    };
    let controller =
        build_controller(&cfg, Arc::new(NoopMetrics)).expect("build access controller");
    Arc::new(OpaAccessGuard::new(
        Arc::new(controller),
        cfg.session_param_keys.clone(),
        cfg.on_missing_schema,
    ))
}

async fn pg_connect(port: u16) -> tokio_postgres::Client {
    let url = format!("postgresql://testuser@127.0.0.1:{port}/postgres");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("postgres wire connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn pg_run(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let messages = client
        .simple_query(sql)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut rows = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let mut vals = Vec::new();
            for i in 0..row.len() {
                vals.push(row.get(i).unwrap_or("").to_string());
            }
            rows.push(vals);
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn denied_table_is_rejected_and_audited() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .deny_tables
        .insert("secret".to_string());

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    let err = pg_run(&client, "SELECT * FROM secret")
        .await
        .expect_err("denied table must error");
    assert!(
        err.to_lowercase().contains("denied") || err.to_lowercase().contains("secret"),
        "unexpected error: {err}"
    );

    let record = h
        .wait_for_record(|r| r.sql_preview.contains("secret"))
        .await
        .expect("denied query should be recorded");
    assert_eq!(format!("{:?}", record.status), "Denied");
    assert!(record.was_guard_blocked);
    assert!(
        record
            .guard_actions
            .iter()
            .any(|a| a.guard == "opa_access" && a.action == "deny"),
        "expected an opa_access deny action, got: {:?}",
        record.guard_actions
    );
}

#[tokio::test]
async fn allowed_table_with_row_filter_only_returns_matching_rows() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .row_filters
        .insert("orders".to_string(), "amount > 100".to_string());

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    // Single-connection DuckDB pool (see `new_with_access_control`) — state persists
    // across these statements.
    pg_run(&client, "CREATE TABLE orders (id INTEGER, amount INTEGER)")
        .await
        .expect("create table");
    pg_run(
        &client,
        "INSERT INTO orders VALUES (1, 50), (2, 150), (3, 200)",
    )
    .await
    .expect("insert rows");

    let rows = pg_run(&client, "SELECT id FROM orders ORDER BY id")
        .await
        .expect("select should succeed");
    let ids: Vec<i32> = rows
        .iter()
        .map(|r| r[0].parse::<i32>().expect("id is an int"))
        .collect();
    assert_eq!(
        ids,
        vec![2, 3],
        "row filter amount > 100 must exclude order 1 (amount=50)"
    );

    let record = h
        .wait_for_record(|r| {
            r.sql_preview
                .to_lowercase()
                .contains("select id from orders")
        })
        .await
        .expect("allowed query should be recorded");
    assert_eq!(format!("{:?}", record.status), "Success");
    assert!(
        record
            .guard_actions
            .iter()
            .any(|a| a.guard == "opa_access" && a.action == "rewrite"),
        "expected an opa_access rewrite action, got: {:?}",
        record.guard_actions
    );
}

#[tokio::test]
async fn opa_unreachable_fails_closed_by_default() {
    // Point at a port nothing is listening on.
    let guard = build_guard("http://127.0.0.1:1");
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    let err = pg_run(&client, "SELECT * FROM anything")
        .await
        .expect_err("unreachable OPA must deny by default (fail-closed)");
    assert!(!err.is_empty());

    let record = h
        .wait_for_record(|r| r.sql_preview.contains("anything"))
        .await
        .expect("denied query should be recorded");
    assert_eq!(format!("{:?}", record.status), "Denied");
}

/// A query the analyzer can't parse has no resources to put in a request, so it can't be
/// judged at all. Even under the default `onMissingSchema: evaluate` it must be denied —
/// allowing it would let anything the parser can't read skip access control entirely. A valid
/// query that simply references no table (`SELECT 1`) is unaffected.
#[tokio::test]
async fn unanalyzable_query_is_denied_even_when_on_missing_schema_is_evaluate() {
    let (opa_url, _stub) = start_opa_stub().await;
    let guard = build_guard(&opa_url); // default: onMissingSchema = evaluate
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    let err = pg_run(&client, "SELECT * FROM orders WHERE (((")
        .await
        .expect_err("an unanalyzable query must be denied");
    assert!(err.contains("could not analyze"), "unexpected error: {err}");

    let record = h
        .wait_for_record(|r| r.sql_preview.contains("WHERE ((("))
        .await
        .expect("denied query should be recorded");
    assert_eq!(format!("{:?}", record.status), "Denied");
    assert!(record.was_guard_blocked);

    pg_run(&client, "SELECT 1")
        .await
        .expect("a table-less query is still allowed");
}
