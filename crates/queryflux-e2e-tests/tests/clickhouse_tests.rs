/// ClickHouse tests — require a running ClickHouse instance.
///
/// All tests are marked `#[ignore]` and run with: make test-e2e
/// or: cargo test -p queryflux-e2e-tests --test clickhouse_tests -- --include-ignored
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_CLICKHOUSE, GROUP_TRINO},
    trino_client::TrinoClient,
};
use serde_json::json;

static HARNESS: OnceLock<TestHarness> = OnceLock::new();

fn harness() -> &'static TestHarness {
    HARNESS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            let h = rt.block_on(TestHarness::new()).expect("TestHarness::new");
            tx.send(h).expect("send harness");
            rt.block_on(std::future::pending::<()>());
        });
        rx.recv().expect("recv harness")
    })
}

fn client() -> TrinoClient {
    TrinoClient::new(&harness().base_url())
}

macro_rules! require_group {
    ($group:expr) => {
        if !harness().has_group($group) {
            eprintln!("SKIP: engine group '{}' not available", $group);
            return;
        }
    };
}

// ---------------------------------------------------------------------------
// Basic ClickHouse
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_select_literal() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_CLICKHOUSE)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], json!(2));
}

#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_select_multi_row() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on(
            "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3",
            GROUP_CLICKHOUSE,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

/// `system.numbers` exists only in ClickHouse — proves the query really ran there.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_system_numbers() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on(
            "SELECT number FROM system.numbers LIMIT 5",
            GROUP_CLICKHOUSE,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0][0], json!(0));
    assert_eq!(r.rows[4][0], json!(4));
}

#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_syntax_error_returns_error() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on("THIS IS NOT VALID SQL FOR CLICKHOUSE", GROUP_CLICKHOUSE)
        .await
        .expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_empty_result() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on("SELECT 1 AS n WHERE 1 = 0", GROUP_CLICKHOUSE)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 0);
}

/// Run a statement on the ClickHouse group and assert it succeeded.
async fn exec_ok(c: &TrinoClient, sql: &str) -> queryflux_e2e_tests::trino_client::QueryResult {
    let r = c
        .execute_on(sql, GROUP_CLICKHOUSE)
        .await
        .expect("request succeeded");
    assert!(r.error.is_none(), "'{sql}' failed: {:?}", r.error);
    r
}

/// DDL and INSERT return an empty ArrowStream body; a full create → insert →
/// select → drop cycle proves those paths and data round-tripping.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_ddl_insert_select_roundtrip() {
    require_group!(GROUP_CLICKHOUSE);
    let c = client();

    c.execute_on("DROP TABLE IF EXISTS qf_e2e_ch_roundtrip", GROUP_CLICKHOUSE)
        .await
        .expect("drop if exists");
    exec_ok(
        &c,
        "CREATE TABLE qf_e2e_ch_roundtrip (id UInt32, name String) ENGINE = Memory",
    )
    .await;
    exec_ok(
        &c,
        "INSERT INTO qf_e2e_ch_roundtrip VALUES (1, 'alpha'), (2, 'beta')",
    )
    .await;

    let r = exec_ok(&c, "SELECT id, name FROM qf_e2e_ch_roundtrip ORDER BY id").await;
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], json!("alpha"));
    assert_eq!(r.rows[1][1], json!("beta"));

    exec_ok(&c, "DROP TABLE qf_e2e_ch_roundtrip").await;
}

// ---------------------------------------------------------------------------
// Session context propagation
// ---------------------------------------------------------------------------

/// `X-Trino-User` from the Trino HTTP frontend must end up in the QueryRecord.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_session_user_recorded_in_metrics() {
    require_group!(GROUP_CLICKHOUSE);
    harness().clear_records();
    let r = client()
        .execute(
            "SELECT 1",
            &[("x-trino-user", "alice"), ("x-qf-group", GROUP_CLICKHOUSE)],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);

    let record = harness()
        .wait_for_record(|r| {
            r.user.as_deref() == Some("alice") && r.cluster_group.0 == GROUP_CLICKHOUSE
        })
        .await;
    assert!(
        record.is_some(),
        "expected QueryRecord with user=alice on clickhouse"
    );
}

/// The ClickHouse adapter passes `session.database()` as the HTTP `database`
/// parameter. `one` resolves only inside the `system` database, so success
/// here proves the parameter was applied.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_database_hint_scopes_queries() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute(
            "SELECT dummy FROM one",
            &[
                ("x-trino-user", "test"),
                ("x-trino-catalog", "system"),
                ("x-qf-group", GROUP_CLICKHOUSE),
            ],
        )
        .await
        .expect("query");
    assert!(
        r.error.is_none(),
        "expected database=system to scope the query, got: {:?}",
        r.error
    );
    assert_eq!(r.rows.len(), 1, "system.one always contains one row");
}

/// An invalid database hint must bubble up as a query error (ClickHouse
/// rejects an unknown `database` parameter), not a panic or silent mismatch.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_invalid_database_hint_returns_error() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute(
            "SELECT 1",
            &[
                ("x-trino-user", "test"),
                ("x-trino-catalog", "nonexistent_db_xyz_qf_test"),
                ("x-qf-group", GROUP_CLICKHOUSE),
            ],
        )
        .await
        .expect("request succeeded");
    assert!(
        r.error.is_some(),
        "expected an error for an unknown database, got rows: {:?}",
        r.rows
    );
}

/// Omitting the catalog header must not break queries — no `database`
/// parameter is sent and ClickHouse uses the connection default.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_no_database_hint_still_executes() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute(
            "SELECT 42 AS n",
            &[("x-trino-user", "test"), ("x-qf-group", GROUP_CLICKHOUSE)],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows[0][0], json!(42));
}

// ---------------------------------------------------------------------------
// Cross-engine routing
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino + ClickHouse — run with: make test-e2e"]
async fn routing_same_sql_trino_and_clickhouse() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_CLICKHOUSE);
    let c = client();
    let sql = "SELECT 1 + 1 AS result";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let ch = c
        .execute_on(sql, GROUP_CLICKHOUSE)
        .await
        .expect("clickhouse");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(ch.error.is_none(), "clickhouse error: {:?}", ch.error);
    assert_eq!(trino.rows.len(), 1);
    assert_eq!(ch.rows.len(), 1);
    assert_eq!(trino.rows[0][0], ch.rows[0][0]);
}
