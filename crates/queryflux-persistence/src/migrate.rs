//! Schema migration runners for durable persistence backends.
//!
//! Postgres uses [Refinery](https://github.com/rust-db/refinery) with embedded SQL
//! migrations and a `refinery_schema_history` ledger, run over a sqlx connection (see
//! [`SqlxMigrations`]). Future Redis / ClickHouse backends can implement
//! [`SchemaMigrator`] with their own version ledger.

use async_trait::async_trait;
use queryflux_core::{
    config::PersistenceConfig,
    error::{QueryFluxError, Result},
};
use refinery::Migration;
use refinery_core::traits::r#async::{AsyncQuery, AsyncTransaction};
use sqlx::postgres::PgPoolOptions;
use sqlx::Connection;

use crate::postgres::PostgresStore;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations/postgres");
}

/// Applies pending schema migrations for a persistence backend.
#[async_trait]
pub trait SchemaMigrator: Send + Sync {
    async fn migrate(&self) -> Result<()>;
}

#[async_trait]
impl SchemaMigrator for PostgresStore {
    async fn migrate(&self) -> Result<()> {
        PostgresStore::migrate(self).await
    }
}

/// Fail closed when a DB still has sqlx's ledger but no Refinery history.
///
/// Replaying `V1__…` on such a database would fail on existing tables. There is
/// intentionally no automatic import (clean break); wipe/recreate the DB instead.
async fn reject_legacy_sqlx_migration_ledger(database_url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .map_err(|e| {
            QueryFluxError::Persistence(format!("Failed to connect for migration check: {e}"))
        })?;

    let has_sqlx: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = '_sqlx_migrations'
        )",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| QueryFluxError::Persistence(format!("Failed to inspect migration tables: {e}")))?;

    if !has_sqlx {
        return Ok(());
    }

    let has_refinery: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = 'refinery_schema_history'
        )",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| QueryFluxError::Persistence(format!("Failed to inspect migration tables: {e}")))?;

    if has_refinery {
        return Ok(());
    }

    Err(QueryFluxError::Persistence(
        "legacy `_sqlx_migrations` table found without `refinery_schema_history`. \
         Recreate the Postgres database (or wipe the volume) before running migrations; \
         there is no automatic sqlx→Refinery upgrade path"
            .into(),
    ))
}

/// Refinery's migration ledger over a sqlx connection.
///
/// Refinery's own Postgres connection rebuilds the URL from host, user and database, so
/// it drops `options` (the pinned schema's `search_path`) and every other parameter. A
/// sqlx connection made from the same URL as the pools keeps them, so migrations land in
/// the schema the pools read.
struct SqlxMigrations(sqlx::PgConnection);

#[async_trait]
impl AsyncTransaction for SqlxMigrations {
    type Error = sqlx::Error;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> std::result::Result<usize, Self::Error> {
        // Owned, so no borrow of the caller's iterator is held across an await. Unprepared
        // `&str` runs over the simple query protocol, so a migration file's many
        // statements run in one call.
        use sqlx::Executor;
        let queries: Vec<String> = queries.map(str::to_string).collect();
        let conn = &mut self.0;
        conn.execute("BEGIN").await?;
        for query in &queries {
            if let Err(e) = conn.execute(query.as_str()).await {
                let _ = conn.execute("ROLLBACK").await;
                return Err(e);
            }
        }
        conn.execute("COMMIT").await?;
        Ok(queries.len())
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for SqlxMigrations {
    async fn query(&mut self, query: &str) -> std::result::Result<Vec<Migration>, Self::Error> {
        use sqlx::Row;
        let rows = sqlx::query(query).fetch_all(&mut self.0).await?;
        rows.into_iter()
            .map(|row| {
                let applied_on: String = row.try_get(2)?;
                let checksum: String = row.try_get(3)?;
                let decode = |e: String| sqlx::Error::Decode(e.into());
                Ok(Migration::applied(
                    row.try_get(0)?,
                    row.try_get(1)?,
                    time::OffsetDateTime::parse(
                        &applied_on,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .map_err(|e| decode(format!("applied_on {applied_on:?}: {e}")))?,
                    checksum
                        .parse()
                        .map_err(|e| decode(format!("checksum {checksum:?}: {e}")))?,
                ))
            })
            .collect()
    }
}

impl refinery::AsyncMigrate for SqlxMigrations {}

/// Run Refinery migrations against a Postgres connection URL.
///
/// With a pinned `schema` (already in `database_url` as its `search_path`), creates the
/// schema when missing. Holds a session-level advisory lock for the duration so
/// concurrent migrators (replicas starting together, parallel tests) don't race on
/// Refinery's ledger; the lock is per schema, so tenants sharing a database migrate in
/// parallel.
pub(crate) async fn run_postgres_refinery_migrations(
    database_url: &str,
    schema: Option<&str>,
) -> Result<()> {
    let err = |what: &str, e: sqlx::Error| QueryFluxError::Persistence(format!("{what}: {e}"));
    let mut conn = sqlx::PgConnection::connect(database_url)
        .await
        .map_err(|e| err("Failed to connect for migrations", e))?;
    if let Some(schema) = schema {
        // `schema` is a validated lowercase identifier (`pinned_schema`). Two replicas
        // creating it at once: one fails on the catalog's unique index, and the schema
        // exists either way.
        if let Err(e) = sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(&mut conn)
            .await
        {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
                    .bind(schema)
                    .fetch_one(&mut conn)
                    .await
                    .map_err(|e| err("Failed to check the schema", e))?;
            if !exists {
                return Err(err(&format!("Failed to create schema {schema}"), e));
            }
        }
    }
    sqlx::query(
        "SELECT pg_advisory_lock(hashtext(CASE WHEN current_schema() = 'public' \
         THEN 'queryflux:migrate' ELSE current_schema() || ':queryflux:migrate' END)::bigint)",
    )
    .execute(&mut conn)
    .await
    .map_err(|e| err("Failed to take migration lock", e))?;
    let mut migrations = SqlxMigrations(conn);
    let result = run_migrations_locked(database_url, &mut migrations).await;
    // Closing the session releases the advisory lock.
    let _ = migrations.0.close().await;
    result
}

async fn run_migrations_locked(database_url: &str, migrations: &mut SqlxMigrations) -> Result<()> {
    reject_legacy_sqlx_migration_ledger(database_url).await?;
    embedded::migrations::runner()
        .run_async(migrations)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Migration failed: {e}")))?;
    Ok(())
}

/// Connect using `persistence` config and run all pending schema migrations.
///
/// Intended for `queryflux migrate`. Rejects in-memory and Redis configs.
pub async fn run_persistence_migrations(persistence: &PersistenceConfig) -> Result<()> {
    match persistence {
        PersistenceConfig::Postgres { conn } => {
            let url = conn.connection_url().map_err(QueryFluxError::Persistence)?;
            let schema = conn.pinned_schema().map_err(QueryFluxError::Persistence)?;
            run_postgres_refinery_migrations(&url, schema).await
        }
        PersistenceConfig::InMemory => Err(QueryFluxError::Persistence(
            "migrations require persistence.type = postgres (got inMemory)".into(),
        )),
        PersistenceConfig::Redis { .. } => Err(QueryFluxError::Persistence(
            "migrations require persistence.type = postgres (got redis)".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrate_rejects_in_memory() {
        let err = run_persistence_migrations(&PersistenceConfig::InMemory)
            .await
            .expect_err("inMemory must fail");
        assert!(
            err.to_string().contains("inMemory"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn migrate_rejects_redis_without_exposing_url() {
        let err = run_persistence_migrations(&PersistenceConfig::Redis {
            url: "redis://:super-secret@localhost:6379/0".into(),
        })
        .await
        .expect_err("redis must fail");
        let msg = err.to_string();
        assert!(msg.contains("redis"), "unexpected error: {msg}");
        assert!(
            !msg.contains("super-secret") && !msg.contains("redis://"),
            "error must not leak Redis URL/credentials: {msg}"
        );
    }
}
