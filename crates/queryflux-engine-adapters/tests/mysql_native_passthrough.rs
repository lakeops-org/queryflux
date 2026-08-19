//! Integration tests for `mysql_native::apply_passthrough_identity` (COM_CHANGE_USER)
//! against a real MySQL-protocol server.
//!
//! These prove the actual, previously-unverified claim behind StarRocks `passthrough`:
//! that `Conn::change_user()` re-authenticates an existing connection as a different user
//! — with that user's real, distinct privileges enforced by the server — rather than just
//! updating what `CURRENT_USER()` reports. StarRocks-specific behavior (the
//! `authentication_ldap_simple` / `mysql_clear_password` plugin negotiation, TLS) isn't
//! covered here — that needs a StarRocks+LDAP+TLS environment this crate doesn't provision.
//! Plain MySQL already proves the generic mechanism: `change_user` + `continue_auth`
//! correctly renegotiate authentication for a different identity, and privileges genuinely
//! change with it.
//!
//! Requires a real MySQL server. All tests are `#[ignore]`d and skip gracefully if
//! unreachable. Run with:
//!   docker run -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=rootpass mysql:8.0
//!   mysql -h127.0.0.1 -P13306 -uroot -prootpass <<'SQL'
//!     CREATE USER 'svc'@'%' IDENTIFIED BY 'svc-password';
//!     GRANT ALL PRIVILEGES ON *.* TO 'svc'@'%';
//!     CREATE USER 'alice'@'%' IDENTIFIED BY 'alice-password';
//!     GRANT SELECT ON *.* TO 'alice'@'%';
//!     CREATE DATABASE IF NOT EXISTS passthrough_test;
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

fn opts_for(user: &str, pass: &str) -> Opts {
    Opts::from(
        OptsBuilder::from_opts(Opts::from_url(&mysql_test_url()).expect("valid test URL"))
            .user(Some(user))
            .pass(Some(pass)),
    )
}

/// `svc` connects with root's own credentials — root created svc/alice with known
/// passwords, so we connect back in as svc directly rather than reusing the root conn.
async fn svc_conn() -> Option<mysql_async::Conn> {
    match mysql_async::Conn::new(opts_for("svc", "svc-password")).await {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("SKIP: mysql_native_passthrough tests — MySQL not reachable or svc user not set up: {e}");
            None
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
async fn com_change_user_reauthenticates_as_the_target_identity() {
    let Some(mut conn) = svc_conn().await else {
        return;
    };
    assert!(
        current_user(&mut conn).await.starts_with("svc@"),
        "baseline: connection should start out authenticated as svc"
    );

    queryflux_engine_adapters::mysql_native::apply_passthrough_identity(
        &mut conn,
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("COM_CHANGE_USER to alice should succeed");

    assert!(
        current_user(&mut conn).await.starts_with("alice@"),
        "after COM_CHANGE_USER, the connection must report alice, not svc"
    );
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn com_change_user_actually_changes_enforced_privileges_not_just_the_display_name() {
    // This is the actual point of the feature: the backend must enforce alice's real
    // grants, not just report her name while still running with svc's privileges.
    let Some(mut conn) = svc_conn().await else {
        return;
    };

    // svc (ALL PRIVILEGES) can create a database.
    conn.query_drop("CREATE DATABASE IF NOT EXISTS svc_only_db")
        .await
        .expect("svc should be able to create a database");

    queryflux_engine_adapters::mysql_native::apply_passthrough_identity(
        &mut conn,
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("COM_CHANGE_USER to alice should succeed");

    // alice (SELECT-only) must not be able to — if this succeeds, the connection is
    // still effectively running as svc and the whole mechanism is a lie.
    let result = conn
        .query_drop("CREATE DATABASE IF NOT EXISTS alice_should_not_create_this")
        .await;
    assert!(
        result.is_err(),
        "alice has SELECT-only privileges; CREATE DATABASE must be rejected by the server"
    );

    // alice's actual allowed action still works on the same, re-authenticated connection.
    conn.query_drop("SELECT 1")
        .await
        .expect("alice should still be able to run a plain SELECT");
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn com_change_user_fails_closed_on_wrong_password() {
    let Some(mut conn) = svc_conn().await else {
        return;
    };

    let result = queryflux_engine_adapters::mysql_native::apply_passthrough_identity(
        &mut conn,
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", "definitely-the-wrong-password"),
    )
    .await;

    assert!(
        result.is_err(),
        "COM_CHANGE_USER with the wrong password must fail, not silently authenticate"
    );
}

#[tokio::test]
#[ignore = "requires a real MySQL server — see file header"]
async fn service_account_credentials_never_trigger_change_user() {
    let Some(mut conn) = svc_conn().await else {
        return;
    };
    let before = current_user(&mut conn).await;

    // A session that happens to carry passthrough_* keys (e.g. left over from a prior
    // passthrough query reusing the same SessionContext) must NOT trigger a user switch
    // when credentials resolved to ServiceAccount for this particular query.
    queryflux_engine_adapters::mysql_native::apply_passthrough_identity(
        &mut conn,
        &QueryCredentials::ServiceAccount,
        &passthrough_session("alice", "alice-password"),
    )
    .await
    .expect("ServiceAccount must be a no-op, never an error");

    assert_eq!(
        current_user(&mut conn).await,
        before,
        "ServiceAccount credentials must never change the connection's identity"
    );
}
