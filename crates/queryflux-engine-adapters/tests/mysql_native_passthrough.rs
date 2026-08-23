//! Integration tests for `mysql_native::open_passthrough_connection` against a real
//! MySQL-protocol server.
//!
//! These prove the actual claim behind StarRocks `passthrough`: that a connection opened
//! directly with the target user's credentials gets that user's real, distinct privileges
//! enforced by the server. (An earlier design used `COM_CHANGE_USER` on a pooled
//! connection instead — a live-StarRocks test caught a real `mysql_async` protocol bug in
//! that path when it had to switch auth plugins mid-flight; see the doc comment on
//! `open_passthrough_connection` for the full story. This file's tests were rewritten
//! accordingly.)
//!
//! Requires a real MySQL server. All tests are `#[ignore]`d and skip gracefully if
//! unreachable. Run with:
//!   docker run -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=rootpass mysql:8.0
//!   mysql -h127.0.0.1 -P13306 -uroot -prootpass <<'SQL'
//!     CREATE USER 'svc'@'%' IDENTIFIED BY 'svc-password';
//!     GRANT ALL PRIVILEGES ON *.* TO 'svc'@'%';
//!     CREATE USER 'alice'@'%' IDENTIFIED BY 'alice-password';
//!     GRANT SELECT ON *.* TO 'alice'@'%';
//!   SQL
//!   MYSQL_TEST_URL=mysql://root:rootpass@127.0.0.1:13306 \
//!     cargo test -p queryflux-engine-adapters --test mysql_native_passthrough -- --ignored

use std::collections::HashMap;

use mysql_async::{prelude::Queryable, Opts, OptsBuilder};
use queryflux_auth::QueryCredentials;
use queryflux_core::session::SessionContext;

fn mysql_test_url() -> String {
    std::env::var("MYSQL_TEST_URL")
        .unwrap_or_else(|_| "mysql://root:rootpass@127.0.0.1:13306".to_string())
}

/// `base_opts` mirrors what `StarRocksAdapter` stores: no user/pass baked in, so
/// `open_passthrough_connection` can clone it and set its own.
fn base_opts() -> Opts {
    Opts::from(OptsBuilder::from_opts(
        Opts::from_url(&mysql_test_url()).expect("valid test URL"),
    ))
}

/// Reachability probe — connects as svc directly (not through `open_passthrough_connection`)
/// just to decide whether to skip the suite.
async fn mysql_reachable() -> bool {
    let opts = Opts::from(
        OptsBuilder::from_opts(base_opts())
            .user(Some("svc"))
            .pass(Some("svc-password")),
    );
    match mysql_async::Conn::new(opts).await {
        Ok(_) => true,
        Err(e) => {
            eprintln!("SKIP: mysql_native_passthrough tests — MySQL not reachable or svc user not set up: {e}");
            false
        }
    }
}

fn passthrough_session(username: &str, password: &str) -> SessionContext {
    let mut extra = HashMap::new();
    extra.insert("passthrough_username".to_string(), username.to_string());
    extra.insert("passthrough_password".to_string(), password.to_string());
    SessionContext {
        extra,
        ..Default::default()
    }
}

async fn current_user(conn: &mut mysql_async::Conn) -> String {
    conn.query_first::<String, _>("SELECT CURRENT_USER()")
        .await
        .expect("SELECT CURRENT_USER() failed")
        .expect("no row returned")
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn passthrough_connection_authenticates_as_the_target_identity() {
    if !mysql_reachable().await {
        return;
    }

    let mut conn = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("open_passthrough_connection should succeed")
    .expect("Passthrough credentials must yield Some(conn)");

    assert!(
        current_user(&mut conn).await.starts_with("alice@"),
        "the opened connection must be authenticated as alice, not the service account"
    );
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn passthrough_connection_privileges_are_actually_enforced_not_just_the_display_name() {
    if !mysql_reachable().await {
        return;
    }

    let mut conn = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("open_passthrough_connection should succeed")
    .expect("Passthrough credentials must yield Some(conn)");

    // alice (SELECT-only) must not be able to create a database — if this succeeds, the
    // connection isn't really running with alice's privileges.
    let result = conn
        .query_drop("CREATE DATABASE IF NOT EXISTS alice_should_not_create_this")
        .await;
    assert!(
        result.is_err(),
        "alice has SELECT-only privileges; CREATE DATABASE must be rejected by the server"
    );

    conn.query_drop("SELECT 1")
        .await
        .expect("alice should still be able to run a plain SELECT");
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn passthrough_connection_fails_closed_on_wrong_password() {
    if !mysql_reachable().await {
        return;
    }

    let result = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "definitely-the-wrong-password"),
    )
    .await;

    assert!(
        result.is_err(),
        "a wrong password must fail to open a connection, not silently authenticate"
    );
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn service_account_credentials_never_open_a_passthrough_connection() {
    if !mysql_reachable().await {
        return;
    }

    // A session that happens to carry passthrough_* keys (e.g. left over from a prior
    // passthrough query reusing the same SessionContext) must NOT open a connection when
    // credentials resolved to ServiceAccount for this particular query — the caller is
    // expected to fall back to the shared pool instead.
    let result = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::ServiceAccount,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("ServiceAccount must never error");

    assert!(
        result.is_none(),
        "ServiceAccount credentials must resolve to None, not open a connection as alice"
    );
}
