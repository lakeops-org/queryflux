//! Statements that used to slip past the guard, and the administrative ones added after DDL:
//! `COPY`, `EXPLAIN [ANALYZE]`, `SELECT … INTO`, writes nested in a query, functions and
//! procedures, `GRANT`/`REVOKE`, roles, session settings and `CALL`.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_admin_tests`

use queryflux_core::access_model::Operation;
use queryflux_e2e_tests::access_control::{
    build_guard, build_guard_with_operations, customers_schema, orders_schema, payroll_schema,
    pg_connect, pg_run, seed_customers, seed_orders, start_opa_stub, MapCatalog, StubState,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

async fn harness_with_ops(opa_url: &str, ops: &[&str]) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(
        Some(build_guard_with_operations(opa_url, ops)),
        catalog,
    )
    .await
    .expect("harness")
}

async fn default_harness(opa_url: &str) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(Some(build_guard(opa_url)), catalog)
        .await
        .expect("harness")
}

fn tmp(name: &str) -> String {
    let p = std::env::temp_dir().join(format!("qf_{}_{}", std::process::id(), name));
    let _ = std::fs::remove_file(&p);
    p.to_string_lossy().to_string()
}

/// `(kind, name)` of every resource in `action`.
fn kinds(action: &Value) -> Vec<(String, String)> {
    action["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["kind"].as_str().unwrap().into(),
                r["name"].as_str().unwrap().into(),
            )
        })
        .collect()
}

fn pair(kind: &str, name: &str) -> (String, String) {
    (kind.to_string(), name.to_string())
}

fn recorded(stub: &Arc<Mutex<StubState>>, op: &str) -> Vec<Value> {
    stub.lock().unwrap().actions_for(op)
}

// ---- reads hidden inside other statements (default `operations: [table.select]`) ----

#[tokio::test]
async fn copy_export_of_a_denied_table_is_rejected_and_writes_nothing() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().deny("customers");

    for (i, sql) in [
        "COPY (SELECT * FROM customers) TO '{}'",
        "COPY customers TO '{}'",
    ]
    .iter()
    .enumerate()
    {
        let file = tmp(&format!("denied_{i}.csv"));
        let err = pg_run(&client, &sql.replace("{}", &file))
            .await
            .expect_err(sql);
        assert!(err.contains("customers"), "unexpected error: {err}");
        assert!(
            !std::path::Path::new(&file).exists(),
            "{sql}: nothing may be exported"
        );
    }
}

#[tokio::test]
async fn copy_export_applies_the_row_filter() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().filter("customers", "region = 'EU'");

    let file = tmp("filtered.csv");
    pg_run(
        &client,
        &format!("COPY (SELECT name, region FROM customers) TO '{file}'"),
    )
    .await
    .expect("export");
    let out = std::fs::read_to_string(&file).expect("exported file");
    assert!(
        out.contains("Ana") && out.contains("Cam"),
        "EU rows expected: {out}"
    );
    assert!(
        !out.contains("Ben"),
        "the US row must not be exported: {out}"
    );
}

#[tokio::test]
async fn explain_and_explain_analyze_check_the_statement_they_wrap() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().deny("customers");

    for sql in [
        "EXPLAIN SELECT * FROM customers",
        "EXPLAIN ANALYZE SELECT * FROM customers",
        "explain (analyze) select * from customers",
    ] {
        let err = pg_run(&client, sql).await.expect_err(sql);
        assert!(err.contains("customers"), "{sql}: unexpected error: {err}");
    }
    // An allowed statement can still be explained.
    stub.lock().unwrap().clear_filters();
    stub.lock().unwrap().deny_tables.clear();
    pg_run(&client, "EXPLAIN SELECT * FROM customers")
        .await
        .expect("explain of an allowed table");
}

#[tokio::test]
async fn explain_that_cannot_be_unwrapped_fails_closed() {
    let (opa_url, _stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    let err = pg_run(&client, "EXPLAIN mystery SELECT 1")
        .await
        .expect_err("unrecognized");
    assert!(
        err.contains("could not analyze the EXPLAIN"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn a_write_nested_in_a_query_is_refused() {
    let (opa_url, _stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    let err = pg_run(
        &client,
        "WITH d AS (DELETE FROM orders RETURNING *) SELECT * FROM d",
    )
    .await
    .expect_err("nested write");
    assert!(err.contains("nested"), "unexpected error: {err}");
    let n = pg_run(&client, "SELECT COUNT(*) FROM orders")
        .await
        .unwrap();
    assert_eq!(n[0][0], "4", "nothing may have been deleted");
}

// ---- COPY FROM / SELECT INTO / functions ----

#[tokio::test]
async fn copy_from_is_a_write_to_its_target() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(&opa_url, &["table.insert"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().deny("customers");
    stub.lock().unwrap().requests.clear(); // drop the seeding INSERTs

    let file = tmp("in.csv");
    let err = pg_run(&client, &format!("COPY customers (id, name) FROM '{file}'"))
        .await
        .expect_err("denied target");
    assert!(err.contains("customers"), "unexpected error: {err}");
    let inserts = recorded(&stub, "table.insert");
    assert_eq!(kinds(&inserts[0]), vec![pair("table", "customers")]);
    assert_eq!(inserts[0]["resources"][0]["columns"], json!(["id", "name"]));
}

#[tokio::test]
async fn select_into_checks_the_source_and_the_created_table() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(&opa_url, &["table.select", "table.create"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().requests.clear();

    // The engine has no SELECT INTO; the point is what the guard asked before it got there.
    let _ = pg_run(&client, "SELECT * INTO newt FROM customers").await;
    assert_eq!(
        kinds(&recorded(&stub, "table.create")[0]),
        vec![pair("table", "newt")]
    );
    assert_eq!(
        kinds(&recorded(&stub, "table.select")[0]),
        vec![pair("table", "customers")]
    );

    stub.lock().unwrap().deny_for("table.create", "blocked");
    let err = pg_run(&client, "SELECT * INTO blocked FROM customers")
        .await
        .expect_err("denied");
    assert!(err.contains("blocked"), "unexpected error: {err}");
}

#[tokio::test]
async fn function_and_procedure_ddl_are_evaluated() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(
        &opa_url,
        &[
            "function.create",
            "function.drop",
            "procedure.create",
            "procedure.drop",
        ],
    )
    .await;
    let client = pg_connect(h.postgres_port).await;

    let _ = pg_run(
        &client,
        "CREATE FUNCTION s.f(a int) RETURNS int AS 'select 1' LANGUAGE sql",
    )
    .await;
    let _ = pg_run(&client, "DROP PROCEDURE p").await;
    let create = &recorded(&stub, "function.create")[0];
    assert_eq!(kinds(create), vec![pair("function", "f")]);
    assert_eq!(create["resources"][0]["schema"], "s");
    assert_eq!(
        kinds(&recorded(&stub, "procedure.drop")[0]),
        vec![pair("procedure", "p")]
    );

    stub.lock().unwrap().deny("f");
    pg_run(&client, "DROP FUNCTION f")
        .await
        .expect_err("denied function drop");
}

// ---- GRANT / REVOKE / roles ----

#[tokio::test]
async fn grant_carries_privileges_grantees_and_the_securable() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(&opa_url, &["grant.grant", "grant.revoke"]).await;
    let client = pg_connect(h.postgres_port).await;

    let _ = pg_run(
        &client,
        "GRANT SELECT (a), INSERT ON TABLE orders TO alice, ROLE bob WITH GRANT OPTION",
    )
    .await;
    let grants = recorded(&stub, "grant.grant");
    assert_eq!(kinds(&grants[0]), vec![pair("table", "orders")]);
    assert_eq!(grants[0]["resources"][0]["columns"], json!(["a"]));
    assert_eq!(
        grants[0]["grant"]["privileges"],
        json!(["INSERT", "SELECT"])
    );
    assert_eq!(grants[0]["grant"]["grantees"], json!(["alice", "role:bob"]));
    assert_eq!(grants[0]["grant"]["withGrantOption"], true);

    let _ = pg_run(&client, "GRANT ALL ON SCHEMA analytics TO analyst").await;
    assert_eq!(
        kinds(&recorded(&stub, "grant.grant")[1]),
        vec![pair("schema", "analytics")]
    );

    stub.lock().unwrap().deny("orders");
    let err = pg_run(&client, "REVOKE SELECT ON orders FROM alice")
        .await
        .expect_err("denied revoke");
    assert!(err.contains("orders"), "unexpected error: {err}");
    assert_eq!(recorded(&stub, "grant.revoke").len(), 1);
}

#[tokio::test]
async fn role_statements_are_evaluated() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(
        &opa_url,
        &[
            "role.create",
            "role.drop",
            "role.grant",
            "role.revoke",
            "role.set",
        ],
    )
    .await;
    let client = pg_connect(h.postgres_port).await;

    let _ = pg_run(&client, "CREATE ROLE analyst").await;
    let _ = pg_run(&client, "DROP ROLE a, b").await;
    let _ = pg_run(&client, "GRANT admin TO alice, bob").await;
    let _ = pg_run(&client, "REVOKE admin FROM alice").await;
    // `SET ROLE` is answered by the Postgres-wire frontend and never dispatched, so
    // `role.set` is covered where the statement is seen (`USE ROLE`, extraction tests).

    assert_eq!(
        kinds(&recorded(&stub, "role.create")[0]),
        vec![pair("role", "analyst")]
    );
    assert_eq!(
        kinds(&recorded(&stub, "role.drop")[0]),
        vec![pair("role", "a"), pair("role", "b")]
    );
    let grant = &recorded(&stub, "role.grant")[0];
    assert_eq!(kinds(grant), vec![pair("role", "admin")]);
    assert_eq!(grant["grant"]["grantees"], json!(["alice", "bob"]));
    assert_eq!(recorded(&stub, "role.revoke").len(), 1);

    stub.lock().unwrap().deny("admin");
    pg_run(&client, "GRANT admin TO carol")
        .await
        .expect_err("denied role grant");
}

// ---- CALL and session ----

#[tokio::test]
async fn call_is_evaluated_as_a_procedure_call() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(&opa_url, &["procedure.call"]).await;
    let client = pg_connect(h.postgres_port).await;

    let _ = pg_run(&client, "CALL system.runtime.kill_query('q1')").await;
    let call = &recorded(&stub, "procedure.call")[0];
    assert_eq!(kinds(call), vec![pair("procedure", "kill_query")]);
    assert_eq!(
        (
            call["resources"][0]["catalog"].as_str(),
            call["resources"][0]["schema"].as_str()
        ),
        (Some("system"), Some("runtime"))
    );

    stub.lock().unwrap().deny("kill_query");
    let err = pg_run(&client, "CALL system.runtime.kill_query('q2')")
        .await
        .expect_err("denied");
    assert!(err.contains("kill_query"), "unexpected error: {err}");
}

/// `USE` changes how bare names resolve and reaches the guard. `SET` does not: the
/// Postgres-wire frontend answers every `SET` itself (a no-op that is never dispatched), so
/// `session.set` only applies on frontends that forward it.
#[tokio::test]
async fn use_reaches_the_guard_but_set_is_answered_by_the_frontend() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with_ops(&opa_url, &["session.set", "session.use"]).await;
    let client = pg_connect(h.postgres_port).await;
    stub.lock().unwrap().requests.clear();

    let _ = pg_run(&client, "SET search_path = analytics, public").await;
    assert!(
        stub.lock().unwrap().requests.is_empty(),
        "the frontend swallows SET"
    );

    let _ = pg_run(&client, "USE analytics").await;
    let uses = recorded(&stub, "session.use");
    assert_eq!(uses.len(), 1);
    assert_eq!(kinds(&uses[0]), vec![pair("schema", "analytics")]);

    stub.lock().unwrap().deny("analytics");
    let err = pg_run(&client, "USE analytics")
        .await
        .expect_err("denied USE");
    assert!(err.contains("analytics"), "unexpected error: {err}");
}

/// None of the administrative operations is evaluated unless enabled.
#[tokio::test]
async fn administrative_statements_are_not_evaluated_by_default() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = default_harness(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    stub.lock().unwrap().requests.clear();

    for sql in [
        "GRANT SELECT ON orders TO alice",
        "CREATE ROLE r",
        "SET search_path = analytics",
        "CALL proc(1)",
        "CREATE FUNCTION f() RETURNS int AS 'select 1' LANGUAGE sql",
    ] {
        let _ = pg_run(&client, sql).await;
    }
    assert!(
        stub.lock().unwrap().requests.is_empty(),
        "no provider call: {:?}",
        stub.lock().unwrap().requests
    );
    let _ = Operation::table_select(); // keep the import honest
}
