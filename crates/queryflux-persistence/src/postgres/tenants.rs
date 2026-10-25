//! The tenants a multi-tenant control plane hosts (`controlPlane.tenancy`): one row per
//! tenant, naming the schema that holds its deployment.

use queryflux_core::config::PostgresPersistenceConfig;
use queryflux_core::error::{QueryFluxError, Result};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Tenant {
    pub id: String,
    pub schema: String,
}

/// What [`TenantRegistry::put`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Unchanged,
    /// The tenant exists with another schema, or the schema belongs to another tenant.
    Conflict(String),
}

pub struct TenantRegistry {
    pool: PgPool,
}

impl TenantRegistry {
    /// Connect with the control plane's own persistence settings (its schema, if pinned)
    /// and create the table if it is missing.
    pub async fn connect(conn: &PostgresPersistenceConfig) -> Result<Self> {
        let url = conn.connection_url().map_err(QueryFluxError::Persistence)?;
        let pool = super::build_pg_pool(
            &url,
            2,
            conn.acquire_timeout_secs,
            conn.statement_timeout_secs,
        )
        .await?;
        // ponytail: one table, created in place rather than through the migration set
        // that every tenant schema runs.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS queryflux_tenants (
                id TEXT PRIMARY KEY,
                schema_name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .map_err(err)?;
        Ok(Self { pool })
    }

    pub async fn list(&self) -> Result<Vec<Tenant>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT id, schema_name FROM queryflux_tenants ORDER BY id")
                .fetch_all(&self.pool)
                .await
                .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|(id, schema)| Tenant { id, schema })
            .collect())
    }

    /// Register `tenant`. A tenant's schema never changes: that would leave its data
    /// behind.
    pub async fn put(&self, tenant: &Tenant) -> Result<PutOutcome> {
        let inserted = sqlx::query(
            "INSERT INTO queryflux_tenants (id, schema_name) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(&tenant.id)
        .bind(&tenant.schema)
        .execute(&self.pool)
        .await
        .map_err(err)?
        .rows_affected();
        if inserted == 1 {
            return Ok(PutOutcome::Created);
        }
        let owner: Option<(String, String)> = sqlx::query_as(
            "SELECT id, schema_name FROM queryflux_tenants WHERE id = $1 OR schema_name = $2
             LIMIT 1",
        )
        .bind(&tenant.id)
        .bind(&tenant.schema)
        .fetch_optional(&self.pool)
        .await
        .map_err(err)?;
        Ok(match owner {
            Some((id, schema)) if id == tenant.id && schema == tenant.schema => {
                PutOutcome::Unchanged
            }
            Some((id, schema)) if id == tenant.id => {
                PutOutcome::Conflict(format!("tenant '{id}' already uses schema '{schema}'"))
            }
            Some((id, schema)) => {
                PutOutcome::Conflict(format!("schema '{schema}' belongs to tenant '{id}'"))
            }
            // Deleted between the two statements.
            None => PutOutcome::Conflict("tenant changed concurrently, retry".into()),
        })
    }

    /// Unregister `id`. Its schema and data stay; dropping them is the caller's call.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let deleted = sqlx::query("DELETE FROM queryflux_tenants WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(err)?
            .rows_affected();
        Ok(deleted == 1)
    }
}

fn err(e: sqlx::Error) -> QueryFluxError {
    QueryFluxError::Persistence(format!("tenant registry: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn pg_a_tenant_keeps_its_schema_and_a_schema_its_tenant() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let conn = PostgresPersistenceConfig {
            url: Some(url),
            schema: Some("qf_tenant_registry_test".into()),
            ..Default::default()
        };
        crate::migrate::run_postgres_refinery_migrations(
            &conn.connection_url().unwrap(),
            Some("qf_tenant_registry_test"),
        )
        .await
        .unwrap();
        let reg = TenantRegistry::connect(&conn).await.unwrap();
        for t in reg.list().await.unwrap() {
            reg.delete(&t.id).await.unwrap();
        }
        let t = |id: &str, schema: &str| Tenant {
            id: id.into(),
            schema: schema.into(),
        };
        assert_eq!(reg.put(&t("a", "s_a")).await.unwrap(), PutOutcome::Created);
        assert_eq!(
            reg.put(&t("a", "s_a")).await.unwrap(),
            PutOutcome::Unchanged
        );
        assert!(matches!(
            reg.put(&t("a", "s_x")).await.unwrap(),
            PutOutcome::Conflict(_)
        ));
        assert!(matches!(
            reg.put(&t("b", "s_a")).await.unwrap(),
            PutOutcome::Conflict(_)
        ));
        assert_eq!(reg.list().await.unwrap(), vec![t("a", "s_a")]);
        assert!(reg.delete("a").await.unwrap());
        assert!(!reg.delete("a").await.unwrap());
    }
}
