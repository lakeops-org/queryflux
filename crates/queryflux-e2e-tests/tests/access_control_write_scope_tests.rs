//! Row scoping on writes: the policy's row filter is ANDed into an `UPDATE`/`DELETE`'s
//! `WHERE`, and into a `MERGE`'s `WHEN MATCHED` / `WHEN NOT MATCHED BY SOURCE` clauses (the
//! branches that act on a target row that already exists), so a principal can only modify
//! rows the policy lets them target. `INSERT` and `TRUNCATE` have no existing row for a
//! filter to restrict — nor does `MERGE`'s `WHEN NOT MATCHED` branch, which inserts one — so
//! a filter (and any column mask) returned for those is denied instead.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_write_scope_tests`

use queryflux_core::access_model::MaskType;
use queryflux_e2e_tests::access_control::{
    build_guard_with_operations, customers_schema, mask, orders_schema, payroll_schema, pg_connect,
    pg_run, seed_customers, seed_orders, start_opa_stub, MapCatalog, StubState,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use std::sync::{Arc, Mutex};

async fn harness(opa_url: &str, ops: &[&str]) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(
        Some(build_guard_with_operations(opa_url, ops)),
        catalog,
    )
    .await
    .expect("harness")
}

/// `id:amount` of every order, read with the policy's filters removed. Seed data:
/// 10:50 EU, 11:150 EU, 12:200 US, 13:80 EU.
async fn orders(client: &tokio_postgres::Client, stub: &Arc<Mutex<StubState>>) -> Vec<String> {
    stub.lock().unwrap().clear_filters();
    pg_run(client, "SELECT id, amount FROM orders ORDER BY id")
        .await
        .expect("read orders")
        .into_iter()
        .map(|r| format!("{}:{}", r[0], r[1]))
        .collect()
}

#[tokio::test]
async fn update_only_touches_rows_the_filter_allows() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.update"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(&client, "UPDATE orders SET amount = 0")
        .await
        .expect("update");

    assert_eq!(
        orders(&client, &stub).await,
        ["10:0", "11:0", "12:200", "13:0"],
        "the US row is outside the caller's scope and must be untouched"
    );
    let record = h
        .wait_for_record(|r| r.sql_preview.to_lowercase().contains("update orders"))
        .await
        .expect("update recorded");
    assert!(
        record
            .guard_actions
            .iter()
            .any(|a| a.guard == "opa_access" && a.action == "rewrite"),
        "the scoping must be audited as a rewrite: {:?}",
        record.guard_actions
    );
}

#[tokio::test]
async fn delete_only_removes_rows_the_filter_allows() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.delete"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(&client, "DELETE FROM orders").await.expect("delete");

    assert_eq!(orders(&client, &stub).await, ["12:200"]);
}

/// A filter is ANDed into `MERGE`'s `WHEN MATCHED` clause: the US row matches the `ON` join
/// like every other row, but the filter keeps it out of scope, so it must be untouched.
#[tokio::test]
async fn merge_only_touches_matched_rows_the_filter_allows() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.merge"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(
        &client,
        "MERGE INTO orders USING (VALUES (10, 0), (11, 0), (12, 0), (13, 0)) \
         AS src(id, new_amount) ON orders.id = src.id \
         WHEN MATCHED THEN UPDATE SET amount = src.new_amount",
    )
    .await
    .expect("merge");

    assert_eq!(
        orders(&client, &stub).await,
        ["10:0", "11:0", "12:200", "13:0"],
        "the US row is outside the caller's scope and must be untouched"
    );
    let record = h
        .wait_for_record(|r| r.sql_preview.to_lowercase().contains("merge into orders"))
        .await
        .expect("merge recorded");
    assert!(
        record
            .guard_actions
            .iter()
            .any(|a| a.guard == "opa_access" && a.action == "rewrite"),
        "the scoping must be audited as a rewrite: {:?}",
        record.guard_actions
    );
}

/// The policy must constrain the caller's whole `WHERE`, including an `OR`.
#[tokio::test]
async fn filter_constrains_the_statements_own_or() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.delete"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(&client, "DELETE FROM orders WHERE id = 10 OR id = 12")
        .await
        .expect("delete");

    assert_eq!(
        orders(&client, &stub).await,
        ["11:150", "12:200", "13:80"],
        "12 matches the caller's WHERE but is US, so it must survive"
    );
}

#[tokio::test]
async fn aliased_target_is_scoped() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.update"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(&client, "UPDATE orders o SET amount = 1 WHERE o.id > 0")
        .await
        .expect("update");

    assert_eq!(
        orders(&client, &stub).await,
        ["10:1", "11:1", "12:200", "13:1"]
    );
}

/// `customers` also has a `region` column: an unqualified filter would be ambiguous.
#[tokio::test]
async fn filter_is_not_ambiguous_against_a_joined_table() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.delete"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    stub.lock()
        .unwrap()
        .filter_for("table.delete", "orders", "region = 'EU'");

    pg_run(
        &client,
        "DELETE FROM orders USING customers c WHERE orders.customer_id = c.id",
    )
    .await
    .expect("delete using");

    assert_eq!(orders(&client, &stub).await, ["12:200"]);
}

/// The same table read and written in one statement gets each operation's own filter.
#[tokio::test]
async fn read_and_write_filters_on_the_same_table_are_independent() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.delete"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    {
        let mut st = stub.lock().unwrap();
        st.filter_for("table.select", "orders", "amount > 100");
        st.filter_for("table.delete", "orders", "region = 'EU'");
    }

    // The subquery reads only amount > 100 (11, 12); the delete only reaches EU (10, 11, 13).
    pg_run(
        &client,
        "DELETE FROM orders WHERE id IN (SELECT id FROM orders)",
    )
    .await
    .expect("delete");

    assert_eq!(orders(&client, &stub).await, ["10:50", "12:200", "13:80"]);
}

/// INSERT/TRUNCATE have no existing row for a filter to restrict, so a returned filter is
/// denied rather than silently dropped.
#[tokio::test]
async fn filters_on_insert_and_truncate_fail_closed() {
    for (op, sql) in [
        (
            "table.insert",
            "INSERT INTO orders VALUES (99, 1, 10, 'EU')",
        ),
        ("table.truncate", "TRUNCATE TABLE orders"),
    ] {
        let (opa_url, stub) = start_opa_stub().await;
        let h = harness(&opa_url, &["table.select", op]).await;
        let client = pg_connect(h.postgres_port).await;
        seed_orders(&client).await;
        stub.lock().unwrap().filter("orders", "region = 'EU'");

        let err = pg_run(&client, sql)
            .await
            .expect_err("filter on a write with no WHERE");
        assert!(
            err.contains(
                "only supported for table.select, table.update, table.delete and table.merge"
            ),
            "{op}: unexpected error: {err}"
        );
        assert_eq!(
            orders(&client, &stub).await.len(),
            4,
            "{op}: nothing may have changed"
        );
    }
}

#[tokio::test]
async fn column_masks_on_a_write_fail_closed() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.update"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock()
        .unwrap()
        .mask("orders", mask("amount", MaskType::Null));

    let err = pg_run(&client, "UPDATE orders SET amount = 1")
        .await
        .expect_err("a mask on a write must fail closed");
    assert!(
        err.contains("apply to reads only"),
        "unexpected error: {err}"
    );
    // The verification read would be masked too — drop the mask first.
    stub.lock().unwrap().column_masks.clear();
    assert_eq!(
        orders(&client, &stub).await,
        ["10:50", "11:150", "12:200", "13:80"]
    );
}

/// Scoping decides which rows a write may *target*, not what it may write (Postgres RLS
/// `USING` without `WITH CHECK`). A caller can therefore move a row out of their own scope;
/// a policy that must prevent it should deny `table.update` on the scoping column instead.
/// This test pins that boundary so it can't change unnoticed.
#[tokio::test]
async fn update_can_move_a_row_out_of_the_callers_scope() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness(&opa_url, &["table.select", "table.update"]).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    pg_run(&client, "UPDATE orders SET region = 'US' WHERE id = 10")
        .await
        .expect("update");

    stub.lock().unwrap().clear_filters();
    let rows = pg_run(&client, "SELECT region FROM orders WHERE id = 10")
        .await
        .unwrap();
    assert_eq!(rows[0][0], "US", "row 10 left the caller's EU scope");
}
