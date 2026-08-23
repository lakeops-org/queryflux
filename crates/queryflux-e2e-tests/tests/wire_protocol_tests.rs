//! Smoke e2e tests for MySQL wire, Postgres wire, and Flight SQL frontends.
//!
//! Uses an in-process `ProtocolWireHarness` backed by DuckDB (no docker).
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test wire_protocol_tests`

use queryflux_e2e_tests::{
    harness::ProtocolWireHarness,
    wire_protocol_clients::{flight_sql_select_one, mysql_select_one, postgres_select_one},
};

const HOST: &str = "127.0.0.1";

#[tokio::test]
async fn mysql_wire_select_one() {
    let h = ProtocolWireHarness::new()
        .await
        .expect("ProtocolWireHarness::new");
    let n = mysql_select_one(HOST, h.mysql_port, "testuser")
        .await
        .expect("mysql SELECT 1");
    assert_eq!(n, 1);
}

#[tokio::test]
async fn postgres_wire_select_one() {
    let h = ProtocolWireHarness::new()
        .await
        .expect("ProtocolWireHarness::new");
    let n = postgres_select_one(HOST, h.postgres_port, "testuser")
        .await
        .expect("postgres SELECT 1");
    assert_eq!(n, 1);
}

#[tokio::test]
async fn flight_sql_wire_select_one() {
    let h = ProtocolWireHarness::new()
        .await
        .expect("ProtocolWireHarness::new");
    let rows = flight_sql_select_one(HOST, h.flight_port)
        .await
        .expect("flight sql SELECT 1");
    assert_eq!(rows, 1);
}
