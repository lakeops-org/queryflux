//! Schema migration runners for durable persistence backends.
//!
//! Postgres uses [Refinery](https://github.com/rust-db/refinery) with embedded SQL
//! migrations and a `refinery_schema_history` ledger. Future Redis / ClickHouse
//! backends can implement [`SchemaMigrator`] with their own version ledger.

use std::str::FromStr;

use async_trait::async_trait;
use queryflux_core::{
    config::PersistenceConfig,
    error::{QueryFluxError, Result},
};
use refinery::config::Config;
use sqlx::postgres::PgPoolOptions;

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

/// Run Refinery migrations against a Postgres connection URL.
pub(crate) async fn run_postgres_refinery_migrations(database_url: &str) -> Result<()> {
    reject_legacy_sqlx_migration_ledger(database_url).await?;

    let mut config = Config::from_str(database_url).map_err(|e| {
        QueryFluxError::Persistence(format!("Invalid database URL for migrations: {e}"))
    })?;
    embedded::migrations::runner()
        .run_async(&mut config)
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
            run_postgres_refinery_migrations(&url).await
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
