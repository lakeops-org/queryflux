//! End-to-end tests for the OPA-backed data access-control guard, through a real
//! Postgres-wire frontend backed by an in-process DuckDB instance and a tiny in-process
//! OPA stub (a real HTTP server, not a mock of `PolicyDecisionProvider`).
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_tests`

use queryflux_e2e_tests::access_control::{build_guard, pg_connect, pg_run, start_opa_stub};
use queryflux_e2e_tests::harness::ProtocolWireHarness;

#[tokio::test]
async fn denied_table_is_rejected_and_audited() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().deny("secret");

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
    stub.lock().unwrap().filter("orders", "amount > 100");

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

/// A fixup script that monkey-patches the translated AST from a `Select` into a `Delete`
/// referencing the same table — simulating a buggy operator-authored translation fixup
/// script mutating the query after our own scan-site rewrite. Proves the post-rewrite
/// invariant assert in `dispatch.rs` (Phase 4 step 4a) denies rather than silently letting
/// a read become a write.
const SELECT_TO_DELETE_FIXUP: &str = r#"
import sqlglot
import sqlglot.expressions as exp

def transform(sql: str, src: str, dst: str) -> str:
    # Only mangle SELECTs — leave CREATE TABLE / INSERT (used to seed the test table)
    # completely alone, so the only thing this simulates is a fixup bug that corrupts
    # a read query specifically.
    ast = sqlglot.parse_one(sql, dialect=dst)
    if not isinstance(ast, exp.Select):
        return sql
    tables = list(ast.find_all(exp.Table))
    if not tables:
        return sql
    delete = exp.Delete(this=tables[0].copy())
    return delete.sql(dialect=dst)
"#;

#[tokio::test]
async fn fixup_script_turning_read_into_write_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    // A row filter is configured too, but it no longer changes what arms this rejection:
    // `run_fixup_scripts` now rejects any read-to-write statement-kind change
    // unconditionally, inside dialect translation, whether or not access control also
    // rewrote the query. See `queryflux_translation::run_fixup_scripts`.
    stub.lock().unwrap().filter("orders", "amount > 0");

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control_and_fixups(
        Some(guard),
        vec![SELECT_TO_DELETE_FIXUP.to_string()],
    )
    .await
    .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    pg_run(&client, "CREATE TABLE orders (id INTEGER, amount INTEGER)")
        .await
        .expect("create table");

    let err = pg_run(&client, "SELECT id FROM orders")
        .await
        .expect_err("a fixup script turning the read into a write must be rejected");
    assert!(!err.is_empty());

    let record = h
        .wait_for_record(|r| {
            r.sql_preview
                .to_lowercase()
                .contains("select id from orders")
        })
        .await
        .expect("rejected query should be recorded");
    // This is a translation-layer rejection (fixup scripts run inside `maybe_translate`),
    // not an access-control denial — hence `Failed`/no guard block, unlike the
    // access-control-guard denials asserted elsewhere in this file.
    assert_eq!(format!("{:?}", record.status), "Failed");
    assert!(!record.was_guard_blocked);
    assert!(
        record
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("changed the statement kind"),
        "expected a statement-kind-change rejection reason, got: {:?}",
        record.error_message
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

/// PostgreSQL's simple-query protocol allows a semicolon-separated batch in a single
/// message, and the wire frontend forwards it unsplit. `classify_operation` and
/// `extract_resources` each only look at the first statement, so a batch like
/// `SELECT 1; DELETE FROM orders` must not be let through on the strength of its harmless
/// first statement — the whole batch must be rejected, and none of it must reach the engine.
#[tokio::test]
async fn multi_statement_batch_is_rejected_not_judged_by_its_first_statement() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("orders", "amount > 0");

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;

    pg_run(&client, "CREATE TABLE orders (id INTEGER, amount INTEGER)")
        .await
        .expect("create table");
    pg_run(&client, "INSERT INTO orders VALUES (1, 50), (2, 150)")
        .await
        .expect("insert rows");

    let err = pg_run(&client, "SELECT 1; DELETE FROM orders")
        .await
        .expect_err("a multi-statement batch must be rejected");
    assert!(err.contains("multi-statement"), "unexpected error: {err}");

    // The DELETE must never have reached the engine, whatever the error path looked like.
    let rows = pg_run(&client, "SELECT id FROM orders ORDER BY id")
        .await
        .expect("select should succeed");
    assert_eq!(rows.len(), 2, "DELETE must not have executed: {rows:?}");

    let record = h
        .wait_for_record(|r| r.sql_preview.to_lowercase().contains("delete from orders"))
        .await
        .expect("rejected query should be recorded");
    assert_eq!(format!("{:?}", record.status), "Denied");
    assert!(record.was_guard_blocked);
}
