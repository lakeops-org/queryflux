/// ClickHouse tests — require a running ClickHouse instance.
///
/// All tests are marked `#[ignore]` and run with: make test-e2e
/// or: cargo test -p queryflux-e2e-tests --test clickhouse_tests -- --include-ignored
use std::sync::OnceLock;

use queryflux_core::query::QueryStatus;
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

/// A query that fails AFTER ClickHouse sent HTTP 200 (mid-stream) must
/// surface the server's exception text — proving `find_exception_frame`
/// parses the real server's `__exception__` framing, not just fixtures.
///
/// The throw fires in the second output block (100_000 > one 65_536-row
/// block), after the server has flushed the first block and committed to
/// HTTP 200 — verified against 26.3.17.56 (the CI image) and 26.8. The
/// `mid-stream` assertion keeps the row count honest: if the failure ever
/// arrives before streaming starts it degrades to a plain HTTP error and
/// this test fails, instead of silently passing without exercising the
/// framing parser.
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_mid_stream_failure_surfaces_exception_message() {
    require_group!(GROUP_CLICKHOUSE);
    let r = client()
        .execute_on(
            "SELECT throwIf(number = 100000) FROM system.numbers LIMIT 200000 \
             SETTINGS max_block_size = 65536",
            GROUP_CLICKHOUSE,
        )
        .await
        .expect("request succeeded");
    let err = r
        .error
        .expect("expected mid-stream failure to surface as an error");
    assert!(
        err.contains("mid-stream"),
        "expected the exception-frame path (adapter says 'failed mid-stream'), got: {err}"
    );
    assert!(
        err.contains("throwIf") || err.contains("DB::Exception"),
        "expected the ClickHouse exception text from the __exception__ frame, got: {err}"
    );
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
///
/// NOTE: the harness runs `TranslationService::disabled()`, so this SQL is
/// native ClickHouse dialect (`UInt32`, `ENGINE = Memory`) sent as-is — the
/// Trino→ClickHouse sqlglot translation path is NOT exercised here. Real
/// clients go through translation, whose DDL coverage is a known limitation
/// (see the PR #105 description).
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

/// Client disconnect must `KILL QUERY` an in-flight ClickHouse query (#111).
#[tokio::test]
#[ignore = "requires ClickHouse — run with: make test-e2e"]
async fn clickhouse_client_disconnect_kills_backend_query() {
    require_group!(GROUP_CLICKHOUSE);
    harness().clear_records();
    let marker = format!(
        "qf-cancel-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let url = format!("{}/v1/statement", harness().base_url());
    // `sleep(N)` is capped at 3s (`function_sleep_max_microseconds_per_block`).
    // An unbounded `system.numbers` count stays in-flight until KILL QUERY.
    let sql = format!("SELECT count() FROM system.numbers /* {marker} */");

    let http = reqwest::Client::new();
    let inflight = tokio::spawn({
        let http = http.clone();
        let url = url.clone();
        let sql = sql.clone();
        async move {
            let _ = http
                .post(url)
                .header("X-Trino-User", "test")
                .header("X-Qf-Group", GROUP_CLICKHOUSE)
                .body(sql)
                .send()
                .await;
        }
    });

    // Wait until ClickHouse is actually running the sleep query.
    let mut started = false;
    for _ in 0..50 {
        let r = client()
            .execute_on(
                &format!(
                    "SELECT count() FROM system.processes \
                     WHERE query LIKE '%/* {marker} */%' AND query NOT LIKE '%system.processes%'"
                ),
                GROUP_CLICKHOUSE,
            )
            .await
            .expect("processlist");
        if r.error.is_none() && r.rows.first().and_then(|row| row.first()) == Some(&json!(1)) {
            started = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(started, "long query never appeared in system.processes");

    inflight.abort();
    let _ = inflight.await;

    let mut gone = false;
    for _ in 0..50 {
        let r = client()
            .execute_on(
                &format!(
                    "SELECT count() FROM system.processes \
                     WHERE query LIKE '%/* {marker} */%' AND query NOT LIKE '%system.processes%'"
                ),
                GROUP_CLICKHOUSE,
            )
            .await
            .expect("processlist");
        if r.error.is_none() && r.rows.first().and_then(|row| row.first()) == Some(&json!(0)) {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        gone,
        "long query still running after client disconnect — KILL QUERY did not land"
    );

    let record = harness()
        .wait_for_record(|r| {
            r.cluster_group.0 == GROUP_CLICKHOUSE
                && r.sql_preview.contains(&marker)
                && r.status == QueryStatus::Cancelled
        })
        .await;
    if record.is_none() {
        let dump: Vec<String> = harness()
            .snapshot_records()
            .into_iter()
            .map(|r| {
                format!(
                    "status={:?} group={} preview={:?} err={:?}",
                    r.status, r.cluster_group.0, r.sql_preview, r.error_message
                )
            })
            .collect();
        panic!(
            "expected QueryRecord with status=Cancelled for the disconnected query; records: {dump:?}"
        );
    }
}
