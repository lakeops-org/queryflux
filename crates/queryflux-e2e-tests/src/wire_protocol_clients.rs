//! Thin clients for MySQL wire, Postgres wire, and Flight SQL e2e smoke tests.

use anyhow::{Context, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use tokio_postgres::SimpleQueryMessage;
use tonic::transport::Channel;

/// Run `SELECT 1` over the MySQL wire protocol.
pub async fn mysql_select_one(host: &str, port: u16, user: &str) -> Result<i64> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(user.to_string()))
        .pass(None::<String>)
        .db_name(Some("default".to_string()));

    let mut conn = Conn::new(opts).await.context("mysql wire connect")?;
    let row: Option<i64> = conn
        .query_first("SELECT 1 AS n")
        .await
        .context("mysql wire query")?;
    row.context("mysql wire: expected one row")
}

/// Run `SELECT 1` over the Postgres wire protocol.
pub async fn postgres_select_one(host: &str, port: u16, user: &str) -> Result<i32> {
    let url = format!("postgresql://{user}@{host}:{port}/postgres");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .context("postgres wire connect")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let messages = client
        .simple_query("SELECT 1 AS n")
        .await
        .context("postgres wire query")?;
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let val = row
                .get(0)
                .context("postgres wire: missing column")?
                .parse::<i32>()
                .context("postgres wire: parse int")?;
            return Ok(val);
        }
    }
    anyhow::bail!("postgres wire: expected one row")
}

/// Run `SELECT 1` over Arrow Flight SQL (GetFlightInfo + DoGet).
pub async fn flight_sql_select_one(host: &str, port: u16) -> Result<i64> {
    let endpoint = format!("http://{host}:{port}");
    let channel = Channel::from_shared(endpoint)
        .context("flight sql endpoint")?
        .connect()
        .await
        .context("flight sql connect")?;

    let mut client = FlightSqlServiceClient::new(channel);
    let flight_info = client
        .execute("SELECT 1 AS n".to_string(), None)
        .await
        .context("flight sql execute")?;

    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .context("flight sql: missing ticket")?;

    let mut stream = client.do_get(ticket).await.context("flight sql do_get")?;

    let mut total = 0i64;
    while let Some(batch) = stream.next().await {
        let batch = batch.context("flight sql batch")?;
        total += batch.num_rows() as i64;
    }
    Ok(total)
}
