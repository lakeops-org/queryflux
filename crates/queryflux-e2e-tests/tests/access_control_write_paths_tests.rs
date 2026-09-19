//! Policies must hold for reads that happen *inside* write statements. Under the default
//! `operations: [table.select]`, `INSERT … SELECT`, `CREATE TABLE … AS SELECT` and
//! `UPDATE … WHERE x IN (SELECT …)` still read tables the policy protects, so deny, row
//! filters and column masks apply to those reads; the write target itself is only checked
//! when its operation (`table.insert`, `table.delete`, …) is enabled.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_write_paths_tests`

use queryflux_core::access_model::MaskType;
use queryflux_e2e_tests::access_control::{
    build_guard, build_guard_with_operations, customers_schema, mask, orders_schema,
    payroll_schema, pg_connect, pg_run, seed_customers, seed_orders, start_opa_stub, MapCatalog,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use std::sync::Arc;

async fn harness_with(
    guard: Arc<queryflux_frontend::access_control_guard::OpaAccessGuard>,
) -> ProtocolWireHarness {
    let catalog = MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()]);
    ProtocolWireHarness::new_with_access_control_and_catalog(Some(guard), catalog)
        .await
        .expect("harness")
}

async fn count(client: &tokio_postgres::Client, table: &str) -> String {
    pg_run(client, &format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count")[0][0]
        .clone()
}

const MINE: &str = "CREATE TABLE mine (id INTEGER, name VARCHAR, region VARCHAR, ssn VARCHAR, \
                    email VARCHAR, hired DATE)";
const COPY_CUSTOMERS: &str =
    "INSERT INTO mine SELECT id, name, region, ssn, email, hired FROM customers";

#[tokio::test]
async fn plain_select_of_a_denied_table_is_still_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().deny("customers");

    assert!(pg_run(&client, "SELECT * FROM customers").await.is_err());
}

#[tokio::test]
async fn insert_select_from_a_denied_table_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    pg_run(&client, MINE).await.expect("create mine");
    stub.lock().unwrap().deny("customers");

    let err = pg_run(&client, COPY_CUSTOMERS)
        .await
        .expect_err("reading a denied table via INSERT … SELECT must be rejected");
    assert!(err.contains("customers"), "unexpected error: {err}");
    assert_eq!(
        count(&client, "mine").await,
        "0",
        "no rows may have been copied"
    );
}

#[tokio::test]
async fn ctas_from_a_denied_table_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock().unwrap().deny("customers");

    pg_run(&client, "CREATE TABLE stolen AS SELECT * FROM customers")
        .await
        .expect_err("reading a denied table via CTAS must be rejected");
    pg_run(&client, "SELECT COUNT(*) FROM stolen")
        .await
        .expect_err("the CTAS target must not have been created");
}

#[tokio::test]
async fn update_with_a_subquery_on_a_denied_table_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("customers");

    pg_run(
        &client,
        "UPDATE orders SET amount = 0 WHERE customer_id IN (SELECT id FROM customers)",
    )
    .await
    .expect_err("a subquery read of a denied table inside UPDATE must be rejected");
    let rows = pg_run(&client, "SELECT COUNT(*) FROM orders WHERE amount = 0")
        .await
        .unwrap();
    assert_eq!(rows[0][0], "0", "no order may have been updated");
}

#[tokio::test]
async fn ctas_and_insert_select_apply_the_row_filter_to_their_source() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "amount > 100");

    pg_run(&client, "CREATE TABLE orders_copy AS SELECT * FROM orders")
        .await
        .expect("ctas");
    assert_eq!(
        count(&client, "orders_copy").await,
        "2",
        "CTAS must copy only filtered rows"
    );

    pg_run(
        &client,
        "CREATE TABLE orders_ins (id INTEGER, customer_id INTEGER, amount INTEGER, region VARCHAR)",
    )
    .await
    .expect("create");
    pg_run(
        &client,
        "INSERT INTO orders_ins SELECT id, customer_id, amount, region FROM orders",
    )
    .await
    .expect("insert select");
    assert_eq!(
        count(&client, "orders_ins").await,
        "2",
        "INSERT … SELECT must copy only filtered rows"
    );
}

#[tokio::test]
async fn ctas_applies_the_column_mask_to_its_source() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    pg_run(
        &client,
        "CREATE TABLE masked AS SELECT id, ssn FROM customers",
    )
    .await
    .expect("ctas");
    let rows = pg_run(&client, "SELECT ssn FROM masked ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows[0][0], "****3333",
        "the copy must hold masked values, not raw ones"
    );
}

/// Statements that only *name* a table (`DROP`, `DESCRIBE`) don't read it, so a table the
/// policy denies can still be dropped/described — unchanged by evaluating embedded reads.
#[tokio::test]
async fn drop_of_a_denied_table_is_not_treated_as_a_read() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");

    pg_run(&client, "DROP TABLE orders")
        .await
        .expect("DROP embeds no read of the table");
}

/// Writes are not authorized unless their operation is enabled: the target of an
/// `INSERT` is not checked under the default `operations: [table.select]`.
#[tokio::test]
async fn write_target_is_not_checked_by_default() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard(&opa_url)).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().deny("orders");

    pg_run(&client, "INSERT INTO orders VALUES (99, 1, 10, 'EU')")
        .await
        .expect("INSERT target is not evaluated when table.insert is not enabled");
}

#[tokio::test]
async fn enabled_write_operation_allows_or_denies_the_target_table() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(
        &opa_url,
        &["table.select", "table.delete"],
    ))
    .await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    pg_run(&client, "DELETE FROM orders WHERE id = 10")
        .await
        .expect("allowed target");
    stub.lock().unwrap().deny("orders");
    let err = pg_run(&client, "DELETE FROM orders WHERE id = 11")
        .await
        .expect_err("denied target");
    assert!(err.contains("orders"), "unexpected error: {err}");
}

/// A filter/mask returned for a write target cannot be spliced at a read site, so it is
/// denied with a clear reason instead of emitting invalid SQL or silently dropping it.
#[tokio::test]
async fn row_filter_on_a_write_target_fails_closed() {
    let (opa_url, stub) = start_opa_stub().await;
    let h = harness_with(build_guard_with_operations(
        &opa_url,
        &["table.select", "table.delete"],
    ))
    .await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;
    stub.lock().unwrap().filter("orders", "amount > 100");

    let err = pg_run(&client, "DELETE FROM orders WHERE id = 10")
        .await
        .expect_err("a filter on a write target must fail closed");
    assert!(
        err.contains("only supported for table.select"),
        "unexpected error: {err}"
    );
    // Reads go through the policy too — drop the filter to see every row.
    stub.lock().unwrap().row_filters.clear();
    assert_eq!(
        count(&client, "orders").await,
        "4",
        "nothing may have been deleted"
    );
}
