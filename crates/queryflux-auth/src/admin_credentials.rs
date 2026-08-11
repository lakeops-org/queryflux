//! Admin API credential management.
//!
//! # Credential priority (highest → lowest)
//!
//! 1. **Database override** — a bcrypt hash stored under the `"admin_credentials"`
//!    key in `ProxySettingsStore`. Written on the first successful password change
//!    via the web UI. Once present, YAML/env credentials are ignored.
//! 2. **Bootstrap credentials** — plain-text username/password from the YAML config
//!    or `QUERYFLUX_ADMIN_USER` / `QUERYFLUX_ADMIN_PASSWORD` environment variables.
//!    Used until the operator changes the password via the web UI.
//!
//! # Persistence note
//! Password changes require a `ProxySettingsStore`. With Postgres the bcrypt hash
//! is written to `proxy_settings` under `"admin_credentials"` and survives restart.
//! In-memory persistence keeps the override only for the process lifetime.

use std::sync::Arc;

use queryflux_core::error::{QueryFluxError, Result};
use queryflux_persistence::ProxySettingsStore;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const SETTINGS_KEY: &str = "admin_credentials";
const BCRYPT_COST: u32 = 12;

/// Stored in `proxy_settings` under key `"admin_credentials"`.
#[derive(Debug, Serialize, Deserialize)]
struct StoredCredentials {
    username: String,
    password_hash: String,
}

/// Manages admin API credentials with DB-override semantics.
///
/// Thread-safe; wrap in `Arc` and share across the Axum state.
pub struct AdminCredentialsManager {
    bootstrap_username: String,
    bootstrap_password: String,
    store: Option<Arc<dyn ProxySettingsStore>>,
}

impl AdminCredentialsManager {
    pub fn new(
        bootstrap_username: String,
        bootstrap_password: String,
        store: Option<Arc<dyn ProxySettingsStore>>,
    ) -> Self {
        Self {
            bootstrap_username,
            bootstrap_password,
            store,
        }
    }

    /// Returns `true` when the DB contains an overriding credential record.
    pub async fn has_db_override(&self) -> bool {
        let Some(store) = &self.store else {
            return false;
        };
        matches!(store.get_proxy_setting(SETTINGS_KEY).await, Ok(Some(_)))
    }

    /// Validate `username` + `password` against the active credentials.
    ///
    /// - If a DB record exists: checks bcrypt hash, ignores bootstrap creds.
    /// - Otherwise: plain-text comparison against bootstrap creds.
    pub async fn verify(&self, username: &str, password: &str) -> bool {
        if let Some(stored) = self.load_db_credentials().await {
            if username != stored.username {
                return false;
            }
            return tokio::task::spawn_blocking({
                let hash = stored.password_hash.clone();
                let pw = password.to_string();
                move || bcrypt::verify(&pw, &hash).unwrap_or(false)
            })
            .await
            .unwrap_or(false);
        }

        // Fall back to bootstrap plain-text comparison.
        username == self.bootstrap_username && password == self.bootstrap_password
    }

    /// Change the admin password.
    ///
    /// Validates `current_password` first, then stores a bcrypt hash of
    /// `new_password` in the DB. After this call `has_db_override()` returns `true`
    /// and the bootstrap credentials are no longer used.
    ///
    /// Returns an error if:
    /// - `current_password` is wrong.
    /// - `new_password` is shorter than 8 characters.
    /// - No DB store is configured (in-memory only — change applies until restart).
    pub async fn change_password(&self, current_password: &str, new_password: &str) -> Result<()> {
        // Resolve the username that will be stored (DB username if override exists,
        // otherwise bootstrap username).
        let username = self
            .load_db_credentials()
            .await
            .map(|s| s.username)
            .unwrap_or_else(|| self.bootstrap_username.clone());

        if !self.verify(&username, current_password).await {
            return Err(QueryFluxError::Auth(
                "current password is incorrect".to_string(),
            ));
        }

        if new_password.len() < 8 {
            return Err(QueryFluxError::Auth(
                "new password must be at least 8 characters".to_string(),
            ));
        }

        let hash = tokio::task::spawn_blocking({
            let pw = new_password.to_string();
            move || bcrypt::hash(&pw, BCRYPT_COST)
        })
        .await
        .map_err(|e| QueryFluxError::Auth(format!("bcrypt task panicked: {e}")))?
        .map_err(|e| QueryFluxError::Auth(format!("bcrypt hash failed: {e}")))?;

        let stored = StoredCredentials {
            username: username.clone(),
            password_hash: hash,
        };
        let value = serde_json::to_value(&stored)
            .map_err(|e| QueryFluxError::Auth(format!("serialize credentials: {e}")))?;

        let Some(store) = &self.store else {
            warn!(
                "No settings store configured — admin password change refused. \
                 Configure Postgres persistence to make password changes permanent."
            );
            return Err(QueryFluxError::Auth(
                "admin password cannot be persisted: no settings store configured".to_string(),
            ));
        };
        store.set_proxy_setting(SETTINGS_KEY, value).await?;
        info!(username, "Admin password changed and stored in DB");

        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    async fn load_db_credentials(&self) -> Option<StoredCredentials> {
        let store = self.store.as_ref()?;
        let value = store.get_proxy_setting(SETTINGS_KEY).await.ok()??;
        serde_json::from_value(value).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_persistence::in_memory::InMemoryPersistence;

    fn mgr(store: Option<Arc<dyn ProxySettingsStore>>) -> AdminCredentialsManager {
        AdminCredentialsManager::new("admin".into(), "admin".into(), store)
    }

    #[tokio::test]
    async fn bootstrap_accepts_default_until_override() {
        let m = mgr(Some(Arc::new(InMemoryPersistence::new())));
        assert!(m.verify("admin", "admin").await);
        assert!(!m.has_db_override().await);
    }

    #[tokio::test]
    async fn change_password_persists_and_rejects_bootstrap() {
        let store = Arc::new(InMemoryPersistence::new());
        let m = mgr(Some(store.clone()));
        m.change_password("admin", "new-secret").await.unwrap();
        assert!(m.has_db_override().await);
        assert!(m.verify("admin", "new-secret").await);
        assert!(!m.verify("admin", "admin").await);

        let again = mgr(Some(store));
        assert!(again.has_db_override().await);
        assert!(again.verify("admin", "new-secret").await);
    }

    #[tokio::test]
    async fn change_password_without_store_fails() {
        let m = mgr(None);
        let err = m.change_password("admin", "new-secret").await.unwrap_err();
        assert!(err.to_string().contains("no settings store"));
        assert!(m.verify("admin", "admin").await);
    }

    #[tokio::test]
    async fn wrong_current_password_rejected() {
        let m = mgr(Some(Arc::new(InMemoryPersistence::new())));
        assert!(m.change_password("nope", "new-secret").await.is_err());
        assert!(m.verify("admin", "admin").await);
    }
}
