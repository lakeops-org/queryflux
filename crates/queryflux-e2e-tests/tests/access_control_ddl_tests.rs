//! DDL authorization: CREATE/DROP/ALTER of tables, views, schemas and catalogs reach the
//! provider under `table.*`, `view.*`, `schema.*` and `catalog.*`, each target carrying its
//! resource `kind` and name. Reads inside a DDL statement (`CTAS`, `CREATE VIEW … AS`) are
//! still `table.select`; `CREATE OR REPLACE` also needs the matching drop.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_ddl_tests`

use queryflux_e2e_tests::access_control::{
    build_guard, build_guard_with_operations, customers_schema, orders_schema, payroll_schema,
    pg_connect, pg_run, seed_orders, start_opa_stub, MapCatalog,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use serde_json::Value;

async fn harness(opa_url: &str, ops: &[&str]) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(
        Some(build_guard_with_operations(opa_url, ops)),
        catalog,
    )
    .await
    .expect("harness")
}

/// `(kind, name)` of every resource in `action`.
fn kinds(action: &Value) -> Vec<(String, String)> {
    action["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["kind"].as_str().unwrap().to_string(),
                r["name"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn pair(kind: &str, name: &str) -> (String, String) {
    (kind.to_string(), name.to_string())
}

/// Under the default `operations: [table.select]` no DDL statement reaches the provider.
#[tokio::test]
async fn ddl_is_not_evaluated_by_default() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = ProtocolWireHarness::new_with_access_control_and_catalog(
        Some(build_guard(&opa_url)),
        MapCatalog::new(vec![orders_schema()]),
    )
    .await
    .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    stub.lock().unwrap().deny("analytics");
    stub.lock().unwrap().deny("t");

    pg_run(&client, "CREATE SCHEMA analytics")
        .await
        .expect("create schema");
    pg_run(&client, "CREATE TABLE analytics.t (id INTEGER)")
        .await
        .expect("create table");
    pg_run(&client, "DROP SCHEMA analytics CASCADE")
        .await
        .expect("drop schema");
    assert!(
        stub.lock().unwrap().requests.is_empty(),
        "no DDL call by default"
    );
}

#[tokio::test]
async fn schema_ddl_is_sent_as_a_schema_resource() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["schema.create", "schema.drop"]).await;
    let client = pg_connect(h.postgres_port).await;

    pg_run(&client, "CREATE SCHEMA analytics")
        .await
        .expect("create schema");
    pg_run(&client, "DROP SCHEMA analytics")
        .await
        .expect("drop schema");

    let st = stub.lock().unwrap();
    let create = st.actions_for("schema.create");
    assert_eq!(kinds(&create[0]), vec![pair("schema", "analytics")]);
    assert_eq!(create[0]["resources"][0]["schema"], "analytics");
    assert!(
        create[0]["resources"][0].get("table").is_none(),
        "a schema has no table"
    );
    assert_eq!(
        kinds(&st.actions_for("schema.drop")[0]),
        vec![pair("schema", "analytics")]
    );
}

#[tokio::test]
async fn denied_schema_drop_is_rejected_and_the_schema_survives() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["schema.drop"]).await;
    let client = pg_connect(h.postgres_port).await;
    pg_run(&client, "CREATE SCHEMA analytics")
        .await
        .expect("create schema");
    pg_run(&client, "CREATE TABLE analytics.t (id INTEGER)")
        .await
        .expect("create table");
    stub.lock().unwrap().deny("analytics");

    let err = pg_run(&client, "DROP SCHEMA analytics CASCADE")
        .await
        .expect_err("denied");
    assert!(err.contains("analytics"), "unexpected error: {err}");
    pg_run(&client, "SELECT COUNT(*) FROM analytics.t")
        .await
        .expect("the schema and its table must still exist");
}

#[tokio::test]
async fn table_ddl_reports_kind_name_and_defined_columns() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.create", "table.alter", "table.drop"]).await;
    let client = pg_connect(h.postgres_port).await;

    pg_run(&client, "CREATE TABLE t (b INTEGER, a INTEGER)")
        .await
        .expect("create");
    pg_run(&client, "ALTER TABLE t ADD COLUMN c INTEGER")
        .await
        .expect("alter");
    pg_run(&client, "DROP TABLE t").await.expect("drop");

    let st = stub.lock().unwrap();
    let create = &st.actions_for("table.create")[0];
    assert_eq!(kinds(create), vec![pair("table", "t")]);
    assert_eq!(
        create["resources"][0]["columns"],
        serde_json::json!(["a", "b"])
    );
    assert_eq!(
        kinds(&st.actions_for("table.alter")[0]),
        vec![pair("table", "t")]
    );
    assert_eq!(
        kinds(&st.actions_for("table.drop")[0]),
        vec![pair("table", "t")]
    );
}

#[tokio::test]
async fn denied_table_ddl_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.create", "table.alter", "table.drop"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");
    stub.lock().unwrap().deny("blocked");

    for sql in [
        "CREATE TABLE blocked (id INTEGER)",
        "ALTER TABLE orders ADD COLUMN c INTEGER",
        "DROP TABLE orders",
    ] {
        pg_run(&client, sql).await.expect_err(sql);
    }
    stub.lock().unwrap().clear_filters();
    pg_run(&client, "SELECT COUNT(*) FROM blocked")
        .await
        .expect_err("never created");
    let cols = pg_run(&client, "SELECT c FROM orders").await;
    assert!(cols.is_err(), "the column must not have been added");
}

/// A view's reads are `table.select`; the view itself is a `view` resource.
#[tokio::test]
async fn create_view_checks_the_view_and_the_tables_it_reads() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "view.create", "view.drop"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    pg_run(
        &client,
        "CREATE VIEW big AS SELECT * FROM orders WHERE amount > 100",
    )
    .await
    .expect("create view");
    pg_run(&client, "DROP VIEW big").await.expect("drop view");

    let st = stub.lock().unwrap();
    assert_eq!(
        kinds(&st.actions_for("view.create")[0]),
        vec![pair("view", "big")]
    );
    assert_eq!(
        kinds(&st.actions_for("table.select")[0]),
        vec![pair("table", "orders")]
    );
    assert_eq!(
        kinds(&st.actions_for("view.drop")[0]),
        vec![pair("view", "big")]
    );
}

/// The CTAS target is a `table.create`; what it reads is still `table.select` — denying
/// either stops the copy.
#[tokio::test]
async fn ctas_checks_its_target_and_its_source() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.create"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    pg_run(&client, "CREATE TABLE copy1 AS SELECT * FROM orders")
        .await
        .expect("ctas");
    {
        let st = stub.lock().unwrap();
        assert_eq!(st.requests.len(), 2, "one call per operation kind");
        assert_eq!(
            kinds(&st.actions_for("table.create")[0]),
            vec![pair("table", "copy1")]
        );
        assert_eq!(
            kinds(&st.actions_for("table.select")[0]),
            vec![pair("table", "orders")]
        );
    }

    stub.lock().unwrap().deny_for("table.create", "copy2");
    pg_run(&client, "CREATE TABLE copy2 AS SELECT * FROM orders")
        .await
        .expect_err("a denied target must stop the CTAS");
    pg_run(&client, "SELECT COUNT(*) FROM copy2")
        .await
        .expect_err("never created");
}

/// `CREATE OR REPLACE` destroys the existing object, so it also needs the drop.
#[tokio::test]
async fn create_or_replace_also_requires_the_drop() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.create", "table.drop"]).await;
    let client = pg_connect(h.postgres_port).await;
    pg_run(&client, "CREATE TABLE t (id INTEGER)")
        .await
        .expect("create");
    pg_run(&client, "INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    stub.lock().unwrap().requests.clear();

    pg_run(&client, "CREATE OR REPLACE TABLE t AS SELECT 2 AS id")
        .await
        .expect("replace when both are allowed");
    {
        let st = stub.lock().unwrap();
        assert_eq!(st.actions_for("table.create").len(), 1);
        assert_eq!(
            st.actions_for("table.drop").len(),
            1,
            "the replaced table is dropped too"
        );
    }

    // Allowed to create but not to drop → the replace is refused and `t` is untouched.
    stub.lock().unwrap().deny_for("table.drop", "t");
    pg_run(&client, "CREATE OR REPLACE TABLE t AS SELECT 3 AS id")
        .await
        .expect_err("replace needs the drop");
    let rows = pg_run(&client, "SELECT id FROM t").await.unwrap();
    assert_eq!(rows[0][0], "2", "the existing table must be untouched");
}

#[tokio::test]
async fn catalog_ddl_is_a_catalog_resource() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["catalog.drop"]).await;
    let client = pg_connect(h.postgres_port).await;
    stub.lock().unwrap().deny("warehouse");

    let err = pg_run(&client, "DROP DATABASE warehouse")
        .await
        .expect_err("denied");
    assert!(err.contains("warehouse"), "unexpected error: {err}");
    let st = stub.lock().unwrap();
    assert_eq!(
        kinds(&st.actions_for("catalog.drop")[0]),
        vec![pair("catalog", "warehouse")]
    );
}

/// A row filter or mask returned for DDL can't be applied — denied, like any write that isn't
/// UPDATE/DELETE/MERGE.
#[tokio::test]
async fn filters_on_ddl_targets_fail_closed() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.create"]).await;
    let client = pg_connect(h.postgres_port).await;
    stub.lock().unwrap().filter("t", "id > 0");

    let err = pg_run(&client, "CREATE TABLE t (id INTEGER)")
        .await
        .expect_err("filter on DDL");
    assert!(
        err.contains("only supported for table.select, table.update, table.delete and table.merge"),
        "unexpected error: {err}"
    );
}

/// Statements that aren't modeled (indexes, grants, session settings) stay unevaluated even
/// with every DDL operation enabled — the docs list what is and isn't covered.
#[tokio::test]
async fn unmodeled_statements_are_not_evaluated() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(
        &opa_url,
        &[
            "table.select",
            "table.create",
            "table.drop",
            "table.alter",
            "view.create",
            "view.drop",
            "schema.create",
            "schema.drop",
            "catalog.create",
            "catalog.drop",
        ],
    )
    .await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();
    stub.lock().unwrap().deny("orders");

    let _ = pg_run(&client, "CREATE INDEX i ON orders (id)").await;
    let _ = pg_run(&client, "SET threads = 1").await;
    assert!(
        stub.lock().unwrap().requests.is_empty(),
        "no provider call for unmodeled statements"
    );
}
