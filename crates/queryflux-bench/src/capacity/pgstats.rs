//! Postgres load during a scenario: transactions from `pg_stat_database`, statements from
//! `pg_stat_statements` when the extension is installed.
//!
//! `pg_stat_database` counters are flushed by each backend at most about once a second, and
//! idle backends can hold them for several seconds more. Both snapshots are therefore taken
//! while load is still running (start and end of the measured window), so the lag is
//! similar at both ends and roughly cancels. Prefer runs of 10 s or more.

use anyhow::Context;
use sqlx::{Connection, PgConnection};
use tokio::sync::Mutex;

pub struct PgStatsProbe {
    conn: Mutex<PgConnection>,
    statements: bool,
    pub note: Option<String>,
    pub server_version: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PgSnapshot {
    pub xacts: i64,
    pub statements: Option<i64>,
}

const XACTS: &str = "SELECT (xact_commit + xact_rollback)::bigint FROM pg_stat_database \
                     WHERE datname = current_database()";
const STATEMENTS: &str = "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_statements \
                          WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())";

impl PgStatsProbe {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let mut conn = PgConnection::connect(database_url)
            .await
            .context("connect for pg_stat sampling")?;
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&mut conn)
            .await
            .unwrap_or_default();
        let (statements, note) = match sqlx::query_scalar::<_, i64>(STATEMENTS)
            .fetch_one(&mut conn)
            .await
        {
            Ok(_) => (true, None),
            Err(e) => (
                false,
                Some(format!(
                    "statements/s skipped: pg_stat_statements unavailable ({})",
                    first_line(&e.to_string())
                )),
            ),
        };
        Ok(Self {
            conn: Mutex::new(conn),
            statements,
            note,
            server_version,
        })
    }

    pub async fn snapshot(&self) -> anyhow::Result<PgSnapshot> {
        let mut conn = self.conn.lock().await;
        let xacts: i64 = sqlx::query_scalar(XACTS)
            .fetch_one(&mut *conn)
            .await
            .context("pg_stat_database")?;
        let statements = if self.statements {
            Some(
                sqlx::query_scalar::<_, i64>(STATEMENTS)
                    .fetch_one(&mut *conn)
                    .await
                    .context("pg_stat_statements")?,
            )
        } else {
            None
        };
        Ok(PgSnapshot { xacts, statements })
    }
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}
