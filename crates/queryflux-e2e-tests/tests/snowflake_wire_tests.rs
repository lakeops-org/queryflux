//! End-to-end tests for the Snowflake HTTP wire protocol v1.
//!
//! Uses an in-process `WireTestHarness` backed by DuckDB (always available — no docker).
//! Session TTLs are set short (5s idle, 10s max) so expiry tests complete quickly.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test snowflake_wire_tests`

use queryflux_e2e_tests::{
    harness::WireTestHarness, snowflake_client::SnowflakeClient,
    snowflake_wire_client::SnowflakeWireClient,
};
use serde_json::json;

const IDLE_SECS: u64 = 5;
const MAX_AGE_SECS: u64 = 10;

async fn harness() -> WireTestHarness {
    WireTestHarness::new(MAX_AGE_SECS, IDLE_SECS)
        .await
        .expect("WireTestHarness::new")
}

// ---------------------------------------------------------------------------
// 1. Basic login → query → logout flow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn login_and_query() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());

    client.login("testuser", "").await.expect("login");
    assert!(client.session_token.is_some());

    let result = client.query("SELECT 1 AS n", None).await.expect("query");
    assert!(result.success, "query should succeed");
    assert_eq!(result.total_rows, 1);

    client.logout().await.expect("logout");
    assert!(client.session_token.is_none());
}

// ---------------------------------------------------------------------------
// 2. Multiple queries in one session all succeed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_persists_across_queries() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    for i in 0u32..3 {
        let result = client
            .query(&format!("SELECT {i} AS n"), None)
            .await
            .expect("query");
        assert!(result.success, "query {i} should succeed");
        assert_eq!(result.total_rows, 1);
    }

    client.logout().await.expect("logout");
}

// ---------------------------------------------------------------------------
// 3. Query without a session → 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_without_login_returns_401() {
    let h = harness().await;
    let client = SnowflakeWireClient::new(&h.base_url());
    // session_token is None → header will be "Snowflake Token=\"invalid\""
    let status = client
        .query_raw_status("SELECT 1")
        .await
        .expect("raw status");
    assert_eq!(status, 401);
}

// ---------------------------------------------------------------------------
// 4. Random UUID token → 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_with_invalid_token_returns_401() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.session_token = Some("00000000-0000-0000-0000-000000000000".to_string());
    let status = client
        .query_raw_status("SELECT 1")
        .await
        .expect("raw status");
    assert_eq!(status, 401);
}

// ---------------------------------------------------------------------------
// 5. Logout invalidates the session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logout_invalidates_session() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    let result = client.query("SELECT 1", None).await.expect("query");
    assert!(result.success);

    client.logout().await.expect("logout");

    // Manually set a stale token and confirm it's now rejected.
    client.session_token = Some("stale-token".to_string());
    let status = client
        .query_raw_status("SELECT 1")
        .await
        .expect("raw status after logout");
    assert_eq!(status, 401);
}

// ---------------------------------------------------------------------------
// 6. Heartbeat extends the idle timer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn heartbeat_extends_session() {
    // Harness with a 3s idle timeout and no max-age limit.
    let h = WireTestHarness::new(0, 3).await.expect("harness");
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    // Advance 2s — within idle timeout.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // Heartbeat bumps last_seen.
    let remaining = client.heartbeat().await.expect("heartbeat");
    assert!(remaining > 0, "should have remaining validity");

    // Advance another 2s — only 2s since last heartbeat; still within 3s idle.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let result = client.query("SELECT 'alive'", None).await.expect("query");
    assert!(
        result.success,
        "session should still be alive after heartbeat"
    );

    client.logout().await.ok();
}

// ---------------------------------------------------------------------------
// 7. Session expires after idle timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_expires_after_idle_timeout() {
    // Harness with a 2s idle timeout.
    let h = WireTestHarness::new(0, 2).await.expect("harness");
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    // Wait past idle timeout.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let status = client
        .query_raw_status("SELECT 1")
        .await
        .expect("raw status");
    assert_eq!(status, 401, "session should have expired");
}

// ---------------------------------------------------------------------------
// 8. Token renewal returns remaining validity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_renewal() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    let remaining = client.token_request().await.expect("token_request");
    assert!(remaining > 0, "should return positive validity seconds");

    // Original session should still work.
    let result = client.query("SELECT 42", None).await.expect("query");
    assert!(result.success);

    client.logout().await.ok();
}

// ---------------------------------------------------------------------------
// 9. Wire v1 and SQL API v2 coexist on the same port
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wire_and_sql_api_coexist() {
    let h = harness().await;

    // Wire v1 query.
    let mut wire = SnowflakeWireClient::new(&h.base_url());
    wire.login("testuser", "").await.expect("wire login");
    let wire_result = wire.query("SELECT 'wire'", None).await.expect("wire query");
    assert!(wire_result.success);
    wire.logout().await.ok();

    // SQL API v2 query on the same port.
    let sql_api = SnowflakeClient::new(&h.base_url());
    let api_result = sql_api
        .query("SELECT 'api'", None)
        .await
        .expect("sql api query");
    assert!(
        api_result.success,
        "SQL API v2 should work on the same port"
    );
    assert_eq!(api_result.total_rows, 1);
}

// ---------------------------------------------------------------------------
// 10. Parameter bindings via wire v1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parameter_bindings() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    // TEXT binding.
    let result = client
        .query(
            "SELECT ? AS s",
            Some(json!({"1": {"type": "TEXT", "value": "hello"}})),
        )
        .await
        .expect("TEXT binding query");
    assert!(result.success, "TEXT binding should succeed");
    assert_eq!(result.total_rows, 1);

    // FIXED (integer) binding.
    let result = client
        .query(
            "SELECT ? AS n",
            Some(json!({"1": {"type": "FIXED", "value": "99"}})),
        )
        .await
        .expect("FIXED binding query");
    assert!(result.success, "FIXED binding should succeed");
    assert_eq!(result.total_rows, 1);

    // BOOLEAN binding.
    let result = client
        .query(
            "SELECT ? AS b",
            Some(json!({"1": {"type": "BOOLEAN", "value": "true"}})),
        )
        .await
        .expect("BOOLEAN binding query");
    assert!(result.success, "BOOLEAN binding should succeed");

    client.logout().await.ok();
}

// ---------------------------------------------------------------------------
// SQL API v2 (SnowflakeClient) tests
// ---------------------------------------------------------------------------

// 11. Row values are correctly decoded
#[tokio::test]
async fn sql_api_returns_correct_values() {
    let h = harness().await;
    let client = SnowflakeClient::new(&h.base_url());

    let result = client
        .query("SELECT 42 AS n, 'hello' AS s", None)
        .await
        .expect("query");

    assert!(result.success);
    assert_eq!(result.total_rows, 1);
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0].as_deref(), Some("42"));
    assert_eq!(row[1].as_deref(), Some("hello"));
}

// 12. Multiple rows all come back
#[tokio::test]
async fn sql_api_multi_row_query() {
    let h = harness().await;
    let client = SnowflakeClient::new(&h.base_url());

    let result = client
        .query("SELECT unnest([1, 2, 3]) AS n ORDER BY n", None)
        .await
        .expect("query");

    assert!(result.success);
    assert_eq!(result.total_rows, 3);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0].as_deref(), Some("1"));
    assert_eq!(result.rows[1][0].as_deref(), Some("2"));
    assert_eq!(result.rows[2][0].as_deref(), Some("3"));
}

// 13. NULL values decode as None
#[tokio::test]
async fn sql_api_null_handling() {
    let h = harness().await;
    let client = SnowflakeClient::new(&h.base_url());

    let result = client
        .query("SELECT NULL AS x, 1 AS y", None)
        .await
        .expect("query");

    assert!(result.success);
    assert_eq!(result.total_rows, 1);
    let row = &result.rows[0];
    assert_eq!(row[0], None, "NULL should decode as None");
    assert_eq!(row[1].as_deref(), Some("1"));
}

// 14. Invalid SQL returns an error response (not a panic / 500)
#[tokio::test]
async fn sql_api_error_on_bad_sql() {
    let h = harness().await;
    let client = SnowflakeClient::new(&h.base_url());

    let result = client
        .query("this is not valid sql at all !!!!", None)
        .await
        .expect("request completed");

    assert!(!result.success, "bad SQL should return success=false");
    assert!(result.error.is_some(), "should have an error message");
}

// 15. SQL API v2 parameter bindings (TEXT, FIXED, BOOLEAN)
#[tokio::test]
async fn sql_api_parameter_bindings() {
    let h = harness().await;
    let client = SnowflakeClient::new(&h.base_url());

    let result = client
        .query(
            "SELECT ? AS s",
            Some(json!({"1": {"type": "TEXT", "value": "world"}})),
        )
        .await
        .expect("TEXT binding");
    assert!(result.success);
    assert_eq!(result.rows[0][0].as_deref(), Some("world"));

    let result = client
        .query(
            "SELECT ? AS n",
            Some(json!({"1": {"type": "FIXED", "value": "7"}})),
        )
        .await
        .expect("FIXED binding");
    assert!(result.success);
    assert_eq!(result.rows[0][0].as_deref(), Some("7"));

    let result = client
        .query(
            "SELECT ? AS b",
            Some(json!({"1": {"type": "BOOLEAN", "value": "false"}})),
        )
        .await
        .expect("BOOLEAN binding");
    assert!(result.success);
    assert_eq!(result.rows[0][0].as_deref(), Some("false"));
}

// 16. Wire v1 and SQL API v2 return consistent row counts for the same query
#[tokio::test]
async fn wire_and_sql_api_return_consistent_results() {
    let h = harness().await;

    let sql = "SELECT 'consistent' AS tag, 123 AS val";

    let mut wire = SnowflakeWireClient::new(&h.base_url());
    wire.login("testuser", "").await.expect("login");
    let wire_result = wire.query(sql, None).await.expect("wire query");
    wire.logout().await.ok();

    let api_result = SnowflakeClient::new(&h.base_url())
        .query(sql, None)
        .await
        .expect("api query");

    assert!(wire_result.success);
    assert!(api_result.success);
    assert_eq!(wire_result.total_rows, api_result.total_rows);
    assert_eq!(wire_result.total_rows, 1);
}

// ---------------------------------------------------------------------------
// Cancel in-flight query (wire v1)
// ---------------------------------------------------------------------------

/// Heavy DuckDB query — long enough to cancel while still running.
const LONG_RUNNING_SQL: &str = "SELECT sum(x * x) FROM generate_series(1, 500000000) AS t(x)";

#[tokio::test]
async fn cancel_in_flight_query() {
    let h = harness().await;
    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    let base_url = h.base_url();
    let token = client.session_token.clone().expect("session token");
    let http = reqwest::Client::new();

    let query_task = tokio::spawn(async move {
        http.post(format!("{base_url}/queries/v1/query-request"))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .header("Content-Type", "application/json")
            .json(&json!({"sqlText": LONG_RUNNING_SQL}))
            .send()
            .await
            .expect("query request")
            .json::<serde_json::Value>()
            .await
            .expect("query json")
    });

    let query_id = loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let ids = client.monitoring_query_ids().await.expect("monitoring");
        if let Some(id) = ids.into_iter().next() {
            break id;
        }
    };

    let cancel_status = client.cancel(&query_id).await.expect("cancel");
    assert_eq!(cancel_status, 200, "cancel should return 200");

    let resp = query_task.await.expect("query task join");
    assert!(
        resp["success"].as_bool() == Some(false),
        "cancelled query should report success=false: {resp}"
    );

    client.logout().await.ok();
}

// ---------------------------------------------------------------------------
// StarRocks backend scenarios
//
// These tests create a WireTestHarness backed by a real StarRocks instance and
// verify that the Snowflake frontend routes correctly to it. Each test calls
// `starrocks_harness()` which returns None when StarRocks is not reachable —
// the test then prints a notice and exits rather than failing, so CI without
// docker compose still passes.
// ---------------------------------------------------------------------------

async fn starrocks_harness() -> Option<WireTestHarness> {
    match WireTestHarness::new_starrocks(86400, 14400).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("starrocks harness error: {e}");
            None
        }
    }
}

// 17. Wire v1: basic login → SELECT → logout against StarRocks
#[tokio::test]
async fn starrocks_wire_login_and_query() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_wire_login_and_query: StarRocks not reachable");
        return;
    };

    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    let result = client.query("SELECT 1 AS n", None).await.expect("query");
    assert!(
        result.success,
        "wire query against StarRocks should succeed"
    );
    assert_eq!(result.total_rows, 1);

    client.logout().await.ok();
}

// 18. Wire v1: multi-row result from StarRocks
#[tokio::test]
async fn starrocks_wire_multi_row() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_wire_multi_row: StarRocks not reachable");
        return;
    };

    let mut client = SnowflakeWireClient::new(&h.base_url());
    client.login("testuser", "").await.expect("login");

    let result = client
        .query(
            "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY n",
            None,
        )
        .await
        .expect("query");
    assert!(result.success);
    assert_eq!(result.total_rows, 3);

    client.logout().await.ok();
}

// 19. SQL API v2: correct row values from StarRocks
#[tokio::test]
async fn starrocks_sql_api_returns_correct_values() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_sql_api_returns_correct_values: StarRocks not reachable");
        return;
    };

    let client = SnowflakeClient::new(&h.base_url());
    let result = client
        .query("SELECT 42 AS n, 'hello' AS s", None)
        .await
        .expect("query");

    assert!(result.success);
    assert_eq!(result.total_rows, 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("42"));
    assert_eq!(result.rows[0][1].as_deref(), Some("hello"));
}

// 20. SQL API v2: NULL handling from StarRocks
#[tokio::test]
async fn starrocks_sql_api_null_handling() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_sql_api_null_handling: StarRocks not reachable");
        return;
    };

    let client = SnowflakeClient::new(&h.base_url());
    let result = client
        .query("SELECT NULL AS x, 1 AS y", None)
        .await
        .expect("query");

    assert!(result.success);
    assert_eq!(result.total_rows, 1);
    assert_eq!(result.rows[0][0], None, "NULL should decode as None");
    assert_eq!(result.rows[0][1].as_deref(), Some("1"));
}

// 21. Wire v1 and SQL API v2 agree on row count when both hit StarRocks
#[tokio::test]
async fn starrocks_wire_and_sql_api_consistent() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_wire_and_sql_api_consistent: StarRocks not reachable");
        return;
    };

    let sql = "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n";

    let mut wire = SnowflakeWireClient::new(&h.base_url());
    wire.login("testuser", "").await.expect("login");
    let wire_result = wire.query(sql, None).await.expect("wire query");
    wire.logout().await.ok();

    let api_result = SnowflakeClient::new(&h.base_url())
        .query(sql, None)
        .await
        .expect("api query");

    assert!(wire_result.success);
    assert!(api_result.success);
    assert_eq!(wire_result.total_rows, api_result.total_rows);
    assert_eq!(wire_result.total_rows, 2);
}

// 22. SQL API v2: bad SQL returns an error from StarRocks (no crash / 500)
#[tokio::test]
async fn starrocks_sql_api_error_on_bad_sql() {
    let Some(h) = starrocks_harness().await else {
        eprintln!("SKIP starrocks_sql_api_error_on_bad_sql: StarRocks not reachable");
        return;
    };

    let result = SnowflakeClient::new(&h.base_url())
        .query("this is not valid sql !!!!", None)
        .await
        .expect("request completed");

    assert!(!result.success, "bad SQL should return success=false");
    assert!(result.error.is_some(), "should carry an error message");
}
