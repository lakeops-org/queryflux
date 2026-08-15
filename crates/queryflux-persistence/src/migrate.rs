//! Schema migration runners for durable persistence backends.
//!
//! Postgres uses embedded sqlx migrations today. Future Redis / ClickHouse
//! backends can implement [`SchemaMigrator`] with their own version ledger.

use async_trait::async_trait;
use queryflux_core::{
    config::PersistenceConfig,
    error::{QueryFluxError, Result},
};

use crate::postgres::PostgresStore;

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

/// Connect using `persistence` config and run all pending schema migrations.
///
/// Intended for `queryflux migrate`. Rejects in-memory and Redis configs.
pub async fn run_persistence_migrations(persistence: &PersistenceConfig) -> Result<()> {
    match persistence {
        PersistenceConfig::Postgres { conn } => {
            let url = conn.connection_url().map_err(QueryFluxError::Persistence)?;
            let store = PostgresStore::connect_from_config(&url, conn).await?;
            SchemaMigrator::migrate(&store).await
        }
        PersistenceConfig::InMemory => Err(QueryFluxError::Persistence(
            "migrations require persistence.type = postgres (got inMemory)".into(),
        )),
        PersistenceConfig::Redis { url } => Err(QueryFluxError::Persistence(format!(
            "migrations require persistence.type = postgres (got redis, url: {url})"
        ))),
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
    async fn migrate_rejects_redis() {
        let err = run_persistence_migrations(&PersistenceConfig::Redis {
            url: "redis://localhost".into(),
        })
        .await
        .expect_err("redis must fail");
        assert!(err.to_string().contains("redis"), "unexpected error: {err}");
    }
}
