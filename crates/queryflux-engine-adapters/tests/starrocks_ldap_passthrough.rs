//! Integration tests for `mysql_native::open_passthrough_connection` against a **real
//! StarRocks server with LDAP authentication configured** — the specific plugin
//! negotiation (`authentication_ldap_simple` → `mysql_clear_password`) that
//! `mysql_native_passthrough.rs` (plain MySQL) doesn't exercise.
//!
//! This is the test suite that caught a real bug: the original design used
//! `COM_CHANGE_USER` on a pooled connection (the mechanism ProxySQL itself uses). Against
//! real StarRocks, `change_user` to an LDAP user reported `Ok(())` but silently left the
//! connection's packet sequence counter desynchronized — every query afterward failed with
//! `packet out of order`. Root cause: `COM_CHANGE_USER` needing to switch auth plugins
//! mid-flight (the pooled connection's own plugin vs. the target user's
//! `mysql_clear_password`) hits a bug in `mysql_async`'s auth-switch handling for that
//! path specifically — a plain-password `change_user` (no plugin switch needed) and a
//! *fresh* connection authenticated directly as the LDAP user both worked fine in
//! isolation. `open_passthrough_connection` uses the fresh-connection approach.
//!
//! TLS is intentionally **not** covered here: StarRocks FE SSL requires a JKS keystore
//! generated and mounted at container build/start time, which needs its own provisioning
//! step this test file doesn't attempt. `StarRocksAdapter::new`'s own unit tests
//! (`starrocks::tests::passthrough_*`) already verify the TLS *gate* — that construction
//! is refused without `?require_ssl=true` — using client-side `Opts` parsing alone, which
//! doesn't need a live server.
//!
//! Requires a real StarRocks server with LDAP auth already configured. All tests are
//! `#[ignore]`d and skip gracefully if unreachable. Setup:
//!
//!   docker network create qf-ldap-test
//!   docker run -d --name qf-test-ldap --network qf-ldap-test \
//!     -e LDAP_ORGANISATION="QueryFlux Test" -e LDAP_DOMAIN="qftest.local" \
//!     -e LDAP_ADMIN_PASSWORD="admin-password" osixia/openldap:1.5.0
//!   # ldapadd a uid=alice,ou=people,dc=qftest,dc=local entry with userPassword: alice-ldap-password
//!
//!   docker run -d --name qf-test-starrocks --network qf-ldap-test \
//!     -p 19030:9030 -p 18030:8030 --shm-size=2g starrocks/allin1-ubuntu:latest
//!   # wait for http://127.0.0.1:18030/api/health, then via the container's own mysql client
//!   # (host mysql clients newer than 8.0 may be missing the mysql_native_password plugin) —
//!   # run each ADMIN SET as its own -e call; batching them in one connection silently
//!   # dropped all but the last when tested:
//!   docker exec qf-test-starrocks mysql -h127.0.0.1 -P9030 -uroot -e '
//!     ADMIN SET FRONTEND CONFIG ("authentication_ldap_simple_server_host" = "<ldap container IP>");'
//!   docker exec qf-test-starrocks mysql -h127.0.0.1 -P9030 -uroot -e '
//!     ADMIN SET FRONTEND CONFIG ("authentication_ldap_simple_bind_base_dn" = "ou=people,dc=qftest,dc=local");'
//!   docker exec qf-test-starrocks mysql -h127.0.0.1 -P9030 -uroot -e '
//!     ADMIN SET FRONTEND CONFIG ("authentication_ldap_simple_bind_root_dn" = "cn=admin,dc=qftest,dc=local");'
//!   docker exec qf-test-starrocks mysql -h127.0.0.1 -P9030 -uroot -e '
//!     ADMIN SET FRONTEND CONFIG ("authentication_ldap_simple_bind_root_pwd" = "admin-password");'
//!   docker exec qf-test-starrocks mysql -h127.0.0.1 -P9030 -uroot -e "
//!     CREATE USER 'alice' IDENTIFIED WITH authentication_ldap_simple AS 'uid=alice,ou=people,dc=qftest,dc=local';
//!     GRANT SELECT ON *.* TO 'alice'@'%';
//!   "
//!
//!   STARROCKS_LDAP_TEST_URL=mysql://root@127.0.0.1:19030 \
//!     cargo test -p queryflux-engine-adapters --test starrocks_ldap_passthrough -- --ignored

use std::collections::HashMap;

use mysql_async::{prelude::Queryable, Opts, OptsBuilder};
use queryflux_auth::QueryCredentials;
use queryflux_core::session::SessionContext;

const ALICE_PASSWORD: &str = "alice-ldap-password";

fn starrocks_test_url() -> String {
    std::env::var("STARROCKS_LDAP_TEST_URL")
        .unwrap_or_else(|_| "mysql://root@127.0.0.1:19030".to_string())
}

/// `enable_cleartext_plugin` must be set here — `open_passthrough_connection` clones this
/// and only overrides `user`/`pass` — matching what `StarRocksAdapter::new` does for a
/// `passthrough`-configured cluster.
fn base_opts() -> Opts {
    Opts::from(
        OptsBuilder::from_opts(Opts::from_url(&starrocks_test_url()).expect("valid test URL"))
            .prefer_socket(false)
            .enable_cleartext_plugin(true),
    )
}

async fn starrocks_reachable() -> bool {
    match mysql_async::Conn::new(base_opts()).await {
        Ok(_) => true,
        Err(e) => {
            eprintln!(
                "SKIP: starrocks_ldap_passthrough tests — StarRocks not reachable or not \
                 configured for LDAP: {e}"
            );
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
#[ignore = "requires a real StarRocks server with LDAP configured — see file header"]
async fn passthrough_connection_authenticates_via_ldap_on_real_starrocks() {
    if !starrocks_reachable().await {
        return;
    }

    let mut conn = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", ALICE_PASSWORD),
    )
    .await
    .expect("opening an LDAP-backed passthrough connection should succeed")
    .expect("Passthrough credentials must yield Some(conn)");

    let user = current_user(&mut conn).await;
    assert!(
        user.starts_with("'alice'@"),
        "StarRocks must report alice (LDAP-authenticated), got {user}"
    );
}

#[tokio::test]
#[ignore = "requires a real StarRocks server with LDAP configured — see file header"]
async fn ldap_backed_user_privileges_are_actually_enforced_by_starrocks() {
    if !starrocks_reachable().await {
        return;
    }

    let mut conn = queryflux_engine_adapters::mysql_native::open_passthrough_connection(
        &base_opts(),
        &QueryCredentials::Passthrough,
        &passthrough_session("alice", ALICE_PASSWORD),
    )
    .await
    .expect("opening an LDAP-backed passthrough connection should succeed")
    .expect("Passthrough credentials must yield Some(conn)");

    // alice is SELECT-only; StarRocks itself (not QueryFlux) must reject this.
    let result = conn
        .query_drop("CREATE DATABASE ldap_alice_should_not_create_this")
        .await;
    assert!(
        result.is_err(),
        "LDAP-backed alice has SELECT-only privileges; CREATE DATABASE must be rejected \
         by StarRocks"
    );

    conn.query_drop("SELECT 1")
        .await
        .expect("alice should still be able to run a plain SELECT");
}

#[tokio::test]
#[ignore = "requires a real StarRocks server with LDAP configured — see file header"]
async fn passthrough_connection_fails_closed_on_wrong_ldap_password() {
    if !starrocks_reachable().await {
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
        "a wrong LDAP password must fail to open a connection, not silently authenticate"
    );
}
