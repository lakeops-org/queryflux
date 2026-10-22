//! Hub enrollment (#255): sites, one-time enrollment tokens and enrolled hubs' keys.
//!
//! An admin creates a site and an enrollment token for it. A hub presents the token
//! once, with a public key it generated; from then on it signs short-lived tokens with
//! that key (`queryflux_core::hub_token`). Tokens are stored as their SHA-256 only.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HubSite {
    pub name: String,
    /// The clusters this site gets; `None`: every cluster.
    pub clusters: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HubKey {
    /// The hub's id: the `kid` of every token it signs.
    pub id: String,
    pub site: String,
    /// Ed25519 public key, base64url.
    pub public_key: String,
    /// The host the hub reported when it enrolled.
    pub host: Option<String>,
    pub enrolled_at: DateTime<Utc>,
}

#[async_trait]
pub trait HubStore: Send + Sync {
    /// Create the site, or set its clusters if it exists.
    async fn upsert_hub_site(&self, name: &str, clusters: Option<Vec<String>>) -> Result<()>;
    /// Delete the site with its tokens and hubs. `false` if it did not exist.
    async fn delete_hub_site(&self, name: &str) -> Result<bool>;
    async fn list_hub_sites(&self) -> Result<Vec<HubSite>>;
    /// Store an enrollment token (its SHA-256) for an existing site. `false` if the site
    /// does not exist.
    async fn create_enrollment_token(
        &self,
        site: &str,
        token_sha256: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool>;
    /// Use up an enrollment token for `site` and record `hub`'s key, in one step. `false`
    /// (and nothing recorded) unless the token exists, is for `site` and has not expired.
    async fn enroll_hub(&self, token_sha256: &str, hub: &HubKey) -> Result<bool>;
    async fn list_hub_keys(&self) -> Result<Vec<HubKey>>;
    /// Revoke one hub. `false` if it did not exist.
    async fn delete_hub_key(&self, site: &str, id: &str) -> Result<bool>;
}

struct Token {
    sha256: String,
    site: String,
    expires_at: DateTime<Utc>,
}

/// For tests and Postgres-less development: enrolled hubs are forgotten on restart.
#[derive(Default)]
pub struct InMemoryHubStore {
    state: Mutex<(Vec<HubSite>, Vec<Token>, Vec<HubKey>)>,
}

impl InMemoryHubStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl HubStore for InMemoryHubStore {
    async fn upsert_hub_site(&self, name: &str, clusters: Option<Vec<String>>) -> Result<()> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match st.0.iter_mut().find(|s| s.name == name) {
            Some(site) => site.clusters = clusters,
            None => st.0.push(HubSite {
                name: name.into(),
                clusters,
                created_at: Utc::now(),
            }),
        }
        Ok(())
    }

    async fn delete_hub_site(&self, name: &str) -> Result<bool> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let before = st.0.len();
        st.0.retain(|s| s.name != name);
        st.1.retain(|t| t.site != name);
        st.2.retain(|k| k.site != name);
        Ok(st.0.len() != before)
    }

    async fn list_hub_sites(&self) -> Result<Vec<HubSite>> {
        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut sites = st.0.clone();
        sites.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(sites)
    }

    async fn create_enrollment_token(
        &self,
        site: &str,
        token_sha256: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !st.0.iter().any(|s| s.name == site) {
            return Ok(false);
        }
        st.1.push(Token {
            sha256: token_sha256.into(),
            site: site.into(),
            expires_at,
        });
        Ok(true)
    }

    async fn enroll_hub(&self, token_sha256: &str, hub: &HubKey) -> Result<bool> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let Some(i) =
            st.1.iter()
                .position(|t| t.sha256 == token_sha256 && t.site == hub.site && t.expires_at > now)
        else {
            return Ok(false);
        };
        st.1.remove(i);
        st.2.push(hub.clone());
        Ok(true)
    }

    async fn list_hub_keys(&self) -> Result<Vec<HubKey>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .2
            .clone())
    }

    async fn delete_hub_key(&self, site: &str, id: &str) -> Result<bool> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let before = st.2.len();
        st.2.retain(|k| !(k.site == site && k.id == id));
        Ok(st.2.len() != before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub(id: &str, site: &str) -> HubKey {
        HubKey {
            id: id.into(),
            site: site.into(),
            public_key: "pk".into(),
            host: None,
            enrolled_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_token_enrolls_one_hub_of_its_own_site_before_it_expires() {
        let store = InMemoryHubStore::new();
        let later = Utc::now() + chrono::Duration::hours(1);
        assert!(!store
            .create_enrollment_token("dc1", "t1", later)
            .await
            .unwrap());
        store.upsert_hub_site("dc1", None).await.unwrap();
        store.upsert_hub_site("dc2", None).await.unwrap();
        assert!(store
            .create_enrollment_token("dc1", "t1", later)
            .await
            .unwrap());

        assert!(!store.enroll_hub("t1", &hub("h0", "dc2")).await.unwrap());
        assert!(store.enroll_hub("t1", &hub("h1", "dc1")).await.unwrap());
        assert!(!store.enroll_hub("t1", &hub("h2", "dc1")).await.unwrap());

        let past = Utc::now() - chrono::Duration::seconds(1);
        store
            .create_enrollment_token("dc1", "t2", past)
            .await
            .unwrap();
        assert!(!store.enroll_hub("t2", &hub("h3", "dc1")).await.unwrap());

        let ids: Vec<_> = store
            .list_hub_keys()
            .await
            .unwrap()
            .into_iter()
            .map(|k| k.id)
            .collect();
        assert_eq!(ids, ["h1"]);
        assert!(store.delete_hub_site("dc1").await.unwrap());
        assert!(store.list_hub_keys().await.unwrap().is_empty());
    }
}
