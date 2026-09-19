//! What the provider is actually asked for INSERT/UPDATE/DELETE/MERGE/TRUNCATE: one call
//! per operation kind (reads as `table.select`, the write target under the statement's own
//! operation), the columns a write names, and nothing at all for operations that aren't
//! enabled.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_dml_tests`

use queryflux_e2e_tests::access_control::{
    build_guard, build_guard_with_operations, customers_schema, orders_schema, payroll_schema,
    pg_connect, pg_run, seed_customers, seed_orders, start_opa_stub, MapCatalog,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use queryflux_frontend::access_control_guard::OpaAccessGuard;
use serde_json::{json, Value};
use std::sync::Arc;

async fn harness_with(guard: Arc<OpaAccessGuard>) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(Some(guard), catalog)
        .await
        .expect("harness")
}

/// `(table, columns)` of every resource in `action`.
fn resources(action: &Value) -> Vec<(String, Value)> {
    action["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .map(|r| {
            (
                r["table"].as_str().unwrap().to_string(),
                r["columns"].clone(),
            )
        })
        .collect()
}

#[tokio::test]
async fn update_makes_one_write_call_and_one_read_call() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(
        &opa_url,
        &["table.select", "table.update"],
    ))
    .await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    pg_run(
        &client,
        "UPDATE orders SET amount = 0 WHERE customer_id IN (SELECT id FROM customers)",
    )
    .await
    .expect("update");

    let st = stub.lock().unwrap();
    assert_eq!(
        st.requests.len(),
        2,
        "one call per operation kind: {:?}",
        st.requests
    );
    let writes = st.actions_for("table.update");
    assert_eq!(writes.len(), 1);
    assert_eq!(
        resources(&writes[0]),
        vec![("orders".to_string(), json!(["amount"]))],
        "the write target carries the SET columns, not the columns the WHERE reads"
    );
    let reads = st.actions_for("table.select");
    assert_eq!(reads.len(), 1);
    let read_tables: Vec<_> = resources(&reads[0]).into_iter().map(|(t, _)| t).collect();
    assert_eq!(read_tables, vec!["customers".to_string()]);
}

#[tokio::test]
async fn insert_reports_its_column_list_or_all_columns() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(&opa_url, &["table.insert"])).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    pg_run(&client, "INSERT INTO orders (region, id) VALUES ('EU', 50)")
        .await
        .expect("insert with column list");
    pg_run(&client, "INSERT INTO orders VALUES (51, 1, 5, 'EU')")
        .await
        .expect("insert without column list");

    let st = stub.lock().unwrap();
    let inserts = st.actions_for("table.insert");
    assert_eq!(inserts.len(), 2, "no read call: nothing is read");
    assert_eq!(
        resources(&inserts[0]),
        vec![("orders".into(), json!(["id", "region"]))]
    );
    assert_eq!(
        resources(&inserts[1]),
        vec![("orders".into(), Value::Null)],
        "no column list means every column"
    );
}

#[tokio::test]
async fn delete_reports_all_columns() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(&opa_url, &["table.delete"])).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    pg_run(&client, "DELETE FROM orders WHERE id = 10")
        .await
        .expect("delete");

    let st = stub.lock().unwrap();
    let deletes = st.actions_for("table.delete");
    assert_eq!(deletes.len(), 1);
    assert_eq!(resources(&deletes[0]), vec![("orders".into(), Value::Null)]);
}

/// MERGE is one write to its target plus reads of its USING source. Whether the embedded
/// DuckDB can run MERGE is irrelevant: the guard decides before the engine sees it.
#[tokio::test]
async fn merge_asks_for_the_target_and_the_source_separately() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(
        &opa_url,
        &["table.select", "table.merge"],
    ))
    .await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    stub.lock().unwrap().requests.clear();

    let _ = pg_run(
        &client,
        "MERGE INTO orders USING customers c ON orders.customer_id = c.id \
         WHEN MATCHED THEN UPDATE SET amount = 0",
    )
    .await;

    let st = stub.lock().unwrap();
    let merges = st.actions_for("table.merge");
    assert_eq!(merges.len(), 1, "{:?}", st.requests);
    assert_eq!(resources(&merges[0]), vec![("orders".into(), Value::Null)]);
    let reads = st.actions_for("table.select");
    assert_eq!(reads.len(), 1);
    assert_eq!(resources(&reads[0])[0].0, "customers");
}

#[tokio::test]
async fn merge_into_a_denied_target_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(&opa_url, &["table.merge"])).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");

    let err = pg_run(
        &client,
        "MERGE INTO orders USING customers c ON orders.customer_id = c.id \
         WHEN MATCHED THEN UPDATE SET amount = 0",
    )
    .await
    .expect_err("denied target");
    assert!(err.contains("orders"), "unexpected error: {err}");
}

#[tokio::test]
async fn truncate_is_denied_when_enabled_and_never_asked_otherwise() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(&opa_url, &["table.truncate"])).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");

    let err = pg_run(&client, "TRUNCATE TABLE orders")
        .await
        .expect_err("denied");
    assert!(err.contains("orders"), "unexpected error: {err}");

    // Not enabled → the provider is never consulted for the TRUNCATE.
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");
    stub.lock().unwrap().requests.clear();
    let _ = pg_run(&client, "TRUNCATE TABLE orders").await;
    assert!(
        stub.lock()
            .unwrap()
            .actions_for("table.truncate")
            .is_empty(),
        "table.truncate is not enabled, so no call for it"
    );
    assert!(
        stub.lock().unwrap().requests.is_empty(),
        "and no other call either"
    );
}
