use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::{QueryFluxError, Result};

use super::PostgresStore;
use crate::admin_access::{AdminAccessStore, AdminAuditEntry, AdminToken};

type TokenRow = (
    String,
    String,
    String,
    Vec<String>,
    String,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

const TOKEN_COLUMNS: &str =
    "id, name, token_hash, scopes, created_by, created_at, last_used_at, revoked_at";

fn token(r: TokenRow) -> AdminToken {
    AdminToken {
        id: r.0,
        name: r.1,
        token_hash: r.2,
        scopes: r.3,
        created_by: r.4,
        created_at: r.5,
        last_used_at: r.6,
        revoked_at: r.7,
    }
}

fn err(what: &str) -> impl FnOnce(sqlx::Error) -> QueryFluxError + '_ {
    move |e| QueryFluxError::Persistence(format!("{what}: {e}"))
}

#[async_trait]
impl AdminAccessStore for PostgresStore {
    async fn create_admin_token(&self, t: &AdminToken) -> Result<()> {
        sqlx::query(
            "INSERT INTO admin_tokens (id, name, token_hash, scopes, created_by)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&t.id)
        .bind(&t.name)
        .bind(&t.token_hash)
        .bind(&t.scopes)
        .bind(&t.created_by)
        .execute(&self.admin_pool)
        .await
        .map_err(err("create_admin_token"))?;
        Ok(())
    }

    async fn list_admin_tokens(&self) -> Result<Vec<AdminToken>> {
        let rows: Vec<TokenRow> = sqlx::query_as(&format!(
            "SELECT {TOKEN_COLUMNS} FROM admin_tokens ORDER BY created_at"
        ))
        .fetch_all(&self.admin_pool)
        .await
        .map_err(err("list_admin_tokens"))?;
        Ok(rows.into_iter().map(token).collect())
    }

    async fn revoke_admin_token(&self, id: &str) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE admin_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(id)
        .execute(&self.admin_pool)
        .await
        .map_err(err("revoke_admin_token"))?
        .rows_affected();
        Ok(n == 1)
    }

    async fn use_admin_token(&self, token_hash: &str) -> Result<Option<AdminToken>> {
        // `last_used_at` moves at most once a minute, so steady use doesn't write per request.
        sqlx::query(
            "UPDATE admin_tokens SET last_used_at = now()
             WHERE token_hash = $1 AND revoked_at IS NULL
               AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')",
        )
        .bind(token_hash)
        .execute(&self.admin_pool)
        .await
        .map_err(err("use_admin_token"))?;
        let row: Option<TokenRow> = sqlx::query_as(&format!(
            "SELECT {TOKEN_COLUMNS} FROM admin_tokens
             WHERE token_hash = $1 AND revoked_at IS NULL"
        ))
        .bind(token_hash)
        .fetch_optional(&self.admin_pool)
        .await
        .map_err(err("use_admin_token"))?;
        Ok(row.map(token))
    }

    async fn record_admin_audit(&self, e: &AdminAuditEntry) -> Result<()> {
        sqlx::query(
            "INSERT INTO admin_audit (principal, actor, method, path, status)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&e.principal)
        .bind(&e.actor)
        .bind(&e.method)
        .bind(&e.path)
        .bind(e.status as i16)
        .execute(&self.admin_pool)
        .await
        .map_err(err("record_admin_audit"))?;
        Ok(())
    }

    async fn list_admin_audit(
        &self,
        limit: i64,
        before: Option<i64>,
    ) -> Result<Vec<AdminAuditEntry>> {
        let rows: Vec<(
            i64,
            DateTime<Utc>,
            String,
            Option<String>,
            String,
            String,
            i16,
        )> = sqlx::query_as(
            "SELECT id, at, principal, actor, method, path, status FROM admin_audit
                 WHERE $2::BIGINT IS NULL OR id < $2
                 ORDER BY id DESC LIMIT $1",
        )
        .bind(limit)
        .bind(before)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(err("list_admin_audit"))?;
        Ok(rows
            .into_iter()
            .map(|r| AdminAuditEntry {
                id: r.0,
                at: r.1,
                principal: r.2,
                actor: r.3,
                method: r.4,
                path: r.5,
                status: r.6 as u16,
            })
            .collect())
    }
}
