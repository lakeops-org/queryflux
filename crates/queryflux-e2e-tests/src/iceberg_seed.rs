//! Create Iceberg tables in Lakekeeper via **direct** Trino (not QueryFlux), using small
//! synthetic rows — no Trino `tpch` connector or compose `data-loader` service.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Url;
use tokio::sync::OnceCell;
use trino_rust_client::client::{Client as TrinoClient, ClientBuilder};

static ICEBERG_E2E_READY: OnceCell<()> = OnceCell::const_new();

const SEED_TIMEOUT: Duration = Duration::from_secs(600);

/// Idempotent for a given process: (re)creates catalog `lakekeeper` and schema `e2e` with fixed rows.
pub async fn ensure_iceberg_e2e_data() -> Result<()> {
    ICEBERG_E2E_READY
        .get_or_init(|| async {
            seed_inner()
                .await
                .expect("Iceberg e2e seed failed (see stderr / TRINO_URL)");
        })
        .await;
    Ok(())
}

fn direct_trino_client() -> Result<TrinoClient> {
    let trino_url =
        std::env::var("TRINO_URL").unwrap_or_else(|_| "http://localhost:18081".to_string());
    let u = Url::parse(&trino_url).context("TRINO_URL parse")?;
    let host = u.host_str().unwrap_or("127.0.0.1").to_string();
    let port = u.port().unwrap_or(8080);
    let secure = u.scheme() == "https";

    ClientBuilder::new("e2e-seed", host)
        .port(port)
        .secure(secure)
        .client_request_timeout(SEED_TIMEOUT)
        .build()
        .context("trino ClientBuilder")
}

async fn exec(client: &TrinoClient, sql: &str) -> Result<()> {
    client
        .execute(sql.to_string())
        .await
        .with_context(|| format!("Trino execute failed: {}", truncate(sql, 200)))?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

async fn seed_inner() -> Result<()> {
    let client = direct_trino_client()?;

    let stmts: &[&str] = &[
        "DROP CATALOG IF EXISTS lakekeeper",
        r#"CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    "s3.endpoint" = 'http://rustfs:9000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'rustfs-root-user',
    "s3.aws-secret-key" = 'rustfs-root-password'
)"#,
        "CREATE SCHEMA IF NOT EXISTS lakekeeper.e2e",
        "DROP TABLE IF EXISTS lakekeeper.e2e.lineitem",
        "DROP TABLE IF EXISTS lakekeeper.e2e.orders",
        "DROP TABLE IF EXISTS lakekeeper.e2e.customer",
        "DROP TABLE IF EXISTS lakekeeper.e2e.nation",
        r#"CREATE TABLE lakekeeper.e2e.nation AS
SELECT * FROM (
  VALUES
    (BIGINT '0', VARCHAR 'n0', BIGINT '0', VARCHAR ''),
    (BIGINT '1', VARCHAR 'n1', BIGINT '0', VARCHAR ''),
    (BIGINT '2', VARCHAR 'n2', BIGINT '0', VARCHAR '')
) AS t(n_nationkey, n_name, n_regionkey, n_comment)"#,
        r#"CREATE TABLE lakekeeper.e2e.customer AS
SELECT * FROM (
  VALUES
    (BIGINT '10', VARCHAR 'c10', BIGINT '1', DOUBLE '0'),
    (BIGINT '20', VARCHAR 'c20', BIGINT '0', DOUBLE '0')
) AS t(c_custkey, c_name, c_nationkey, c_acctbal)"#,
        r#"CREATE TABLE lakekeeper.e2e.orders AS
SELECT * FROM (
  VALUES
    (BIGINT '100', BIGINT '10', DOUBLE '99.5', DATE '2020-01-01', VARCHAR '1-URGENT', VARCHAR 'clerk', INTEGER '0', VARCHAR '', VARCHAR 'O'),
    (BIGINT '101', BIGINT '20', DOUBLE '50', DATE '2020-01-02', VARCHAR '1-URGENT', VARCHAR 'clerk', INTEGER '0', VARCHAR '', VARCHAR 'O'),
    (BIGINT '102', BIGINT '10', DOUBLE '25', DATE '2020-01-03', VARCHAR '1-URGENT', VARCHAR 'clerk', INTEGER '0', VARCHAR '', VARCHAR 'F'),
    (BIGINT '103', BIGINT '20', DOUBLE '10', DATE '2020-01-04', VARCHAR '1-URGENT', VARCHAR 'clerk', INTEGER '0', VARCHAR '', VARCHAR 'O')
) AS t(o_orderkey, o_custkey, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderstatus)"#,
        r#"CREATE TABLE lakekeeper.e2e.lineitem AS
SELECT * FROM (
  VALUES
    (BIGINT '100', BIGINT '1', BIGINT '1', INTEGER '1', DOUBLE '5', DOUBLE '100', DOUBLE '0', DOUBLE '0', VARCHAR 'R', VARCHAR 'O',
     DATE '2020-01-10', DATE '2020-01-11', DATE '2020-01-12', VARCHAR 'DELIVER', VARCHAR 'MAIL', VARCHAR ''),
    (BIGINT '101', BIGINT '2', BIGINT '1', INTEGER '1', DOUBLE '3', DOUBLE '60', DOUBLE '0', DOUBLE '0', VARCHAR 'N', VARCHAR 'O',
     DATE '2020-01-10', DATE '2020-01-11', DATE '2020-01-12', VARCHAR 'DELIVER', VARCHAR 'MAIL', VARCHAR ''),
    (BIGINT '103', BIGINT '3', BIGINT '1', INTEGER '1', DOUBLE '7', DOUBLE '70', DOUBLE '0', DOUBLE '0', VARCHAR 'R', VARCHAR 'O',
     DATE '2020-01-10', DATE '2020-01-11', DATE '2020-01-12', VARCHAR 'DELIVER', VARCHAR 'TRUCK', VARCHAR '')
) AS t(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
     l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)"#,
    ];

    for sql in stmts {
        exec(&client, sql).await?;
    }

    Ok(())
}
