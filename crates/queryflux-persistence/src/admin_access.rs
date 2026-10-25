//! Admin API bearer tokens and the admin API's audit log.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::Result;
use serde::Serialize;

/// An admin API token. The token itself is shown once, at creation; only its SHA-256 is
/// kept.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminToken {
    pub id: String,
    pub name: String,
    #[serde(skip)]
    pub token_hash: String,
    pub scopes: Vec<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// One admin API write.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminAuditEntry {
    pub id: i64,
    pub at: DateTime<Utc>,
    /// Who authenticated: `basic:<user>`, `token:<id>` or `platform`.
    pub principal: String,
    /// On whose behalf a platform caller acted (`X-QueryFlux-Actor`).
    pub actor: Option<String>,
    pub method: String,
    pub path: String,
    pub status: u16,
}

#[async_trait]
pub trait AdminAccessStore: Send + Sync {
    async fn create_admin_token(&self, token: &AdminToken) -> Result<()>;
    async fn list_admin_tokens(&self) -> Result<Vec<AdminToken>>;
    /// `false` when there is no such unrevoked token.
    async fn revoke_admin_token(&self, id: &str) -> Result<bool>;
    /// The unrevoked token with this hash; marks it used.
    async fn use_admin_token(&self, token_hash: &str) -> Result<Option<AdminToken>>;
    /// `id` and `at` are assigned by the store.
    async fn record_admin_audit(&self, entry: &AdminAuditEntry) -> Result<()>;
    /// Newest first, older than `before` (an entry id) when given.
    async fn list_admin_audit(
        &self,
        limit: i64,
        before: Option<i64>,
    ) -> Result<Vec<AdminAuditEntry>>;
}

#[async_trait]
impl AdminAccessStore for crate::in_memory::InMemoryPersistence {
    async fn create_admin_token(&self, token: &AdminToken) -> Result<()> {
        self.admin_tokens.lock().unwrap().push(token.clone());
        Ok(())
    }

    async fn list_admin_tokens(&self) -> Result<Vec<AdminToken>> {
        Ok(self.admin_tokens.lock().unwrap().clone())
    }

    async fn revoke_admin_token(&self, id: &str) -> Result<bool> {
        let mut tokens = self.admin_tokens.lock().unwrap();
        Ok(
            match tokens
                .iter_mut()
                .find(|t| t.id == id && t.revoked_at.is_none())
            {
                Some(t) => {
                    t.revoked_at = Some(Utc::now());
                    true
                }
                None => false,
            },
        )
    }

    async fn use_admin_token(&self, token_hash: &str) -> Result<Option<AdminToken>> {
        let mut tokens = self.admin_tokens.lock().unwrap();
        Ok(tokens
            .iter_mut()
            .find(|t| t.token_hash == token_hash && t.revoked_at.is_none())
            .map(|t| {
                t.last_used_at = Some(Utc::now());
                t.clone()
            }))
    }

    async fn record_admin_audit(&self, entry: &AdminAuditEntry) -> Result<()> {
        let mut log = self.admin_audit.lock().unwrap();
        let id = log.len() as i64 + 1;
        log.push(AdminAuditEntry {
            id,
            at: Utc::now(),
            ..entry.clone()
        });
        Ok(())
    }

    async fn list_admin_audit(
        &self,
        limit: i64,
        before: Option<i64>,
    ) -> Result<Vec<AdminAuditEntry>> {
        let log = self.admin_audit.lock().unwrap();
        Ok(log
            .iter()
            .rev()
            .filter(|e| before.is_none_or(|b| e.id < b))
            .take(limit.max(0) as usize)
            .cloned()
            .collect())
    }
}
