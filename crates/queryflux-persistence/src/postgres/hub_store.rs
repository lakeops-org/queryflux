//! [`HubStore`] on Postgres (`hub_sites`, `hub_enrollment_tokens`, `hub_keys`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::{QueryFluxError, Result};

use super::PostgresStore;
use crate::hub_store::{HubKey, HubSite, HubStore};

fn err(what: &str) -> impl FnOnce(sqlx::Error) -> QueryFluxError + '_ {
    move |e| QueryFluxError::Persistence(format!("{what}: {e}"))
}

#[async_trait]
impl HubStore for PostgresStore {
    async fn upsert_hub_site(&self, name: &str, clusters: Option<Vec<String>>) -> Result<()> {
        sqlx::query(
            "INSERT INTO hub_sites (name, clusters) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET clusters = EXCLUDED.clusters",
        )
        .bind(name)
        .bind(clusters.map(sqlx::types::Json))
        .execute(&self.admin_pool)
        .await
        .map_err(err("upsert_hub_site"))?;
        Ok(())
    }

    async fn delete_hub_site(&self, name: &str) -> Result<bool> {
        let done = sqlx::query("DELETE FROM hub_sites WHERE name = $1")
            .bind(name)
            .execute(&self.admin_pool)
            .await
            .map_err(err("delete_hub_site"))?;
        Ok(done.rows_affected() > 0)
    }

    async fn list_hub_sites(&self) -> Result<Vec<HubSite>> {
        type Row = (
            String,
            Option<sqlx::types::Json<Vec<String>>>,
            DateTime<Utc>,
        );
        let rows: Vec<Row> =
            sqlx::query_as("SELECT name, clusters, created_at FROM hub_sites ORDER BY name")
                .fetch_all(&self.admin_pool)
                .await
                .map_err(err("list_hub_sites"))?;
        Ok(rows
            .into_iter()
            .map(|(name, clusters, created_at)| HubSite {
                name,
                clusters: clusters.map(|c| c.0),
                created_at,
            })
            .collect())
    }

    async fn create_enrollment_token(
        &self,
        site: &str,
        token_sha256: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool> {
        let done = sqlx::query(
            "INSERT INTO hub_enrollment_tokens (token_sha256, site, expires_at)
             SELECT $1, name, $3 FROM hub_sites WHERE name = $2",
        )
        .bind(token_sha256)
        .bind(site)
        .bind(expires_at)
        .execute(&self.admin_pool)
        .await
        .map_err(err("create_enrollment_token"))?;
        Ok(done.rows_affected() > 0)
    }

    async fn enroll_hub(&self, token_sha256: &str, hub: &HubKey) -> Result<bool> {
        // The DELETE … RETURNING uses the token up and the INSERT runs only on a row it
        // returned, in one statement: two hubs racing on one token can't both enroll.
        let done = sqlx::query(
            "WITH used AS (
                 DELETE FROM hub_enrollment_tokens
                  WHERE token_sha256 = $1 AND site = $2 AND expires_at > now()
              RETURNING site)
             INSERT INTO hub_keys (id, site, public_key, host, enrolled_at)
             SELECT $3, site, $4, $5, $6 FROM used",
        )
        .bind(token_sha256)
        .bind(&hub.site)
        .bind(&hub.id)
        .bind(&hub.public_key)
        .bind(&hub.host)
        .bind(hub.enrolled_at)
        .execute(&self.admin_pool)
        .await
        .map_err(err("enroll_hub"))?;
        Ok(done.rows_affected() > 0)
    }

    async fn list_hub_keys(&self) -> Result<Vec<HubKey>> {
        type Row = (String, String, String, Option<String>, DateTime<Utc>);
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, site, public_key, host, enrolled_at FROM hub_keys ORDER BY site, id",
        )
        .fetch_all(&self.admin_pool)
        .await
        .map_err(err("list_hub_keys"))?;
        Ok(rows
            .into_iter()
            .map(|(id, site, public_key, host, enrolled_at)| HubKey {
                id,
                site,
                public_key,
                host,
                enrolled_at,
            })
            .collect())
    }

    async fn delete_hub_key(&self, site: &str, id: &str) -> Result<bool> {
        let done = sqlx::query("DELETE FROM hub_keys WHERE site = $1 AND id = $2")
            .bind(site)
            .bind(id)
            .execute(&self.admin_pool)
            .await
            .map_err(err("delete_hub_key"))?;
        Ok(done.rows_affected() > 0)
    }
}
