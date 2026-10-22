//! The control plane's view of enrolled hubs (#255): which site each hub key belongs to,
//! and which clusters each site gets.
//!
//! Every worker-API call from a hub resolves its token here, so the store is cached:
//! reloaded at most every [`MAX_AGE`], at once on this replica after an admin change, and
//! early (at most every [`MIN_RELOAD_GAP`]) when a token names a hub the cache does not
//! know yet, such as one that just enrolled through another replica. A revoked hub is
//! therefore refused within [`MAX_AGE`] on every replica.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use queryflux_core::hub_token;
use queryflux_core::hub_token::VerifyingKey;
use queryflux_persistence::hub_store::HubStore;
use tokio::sync::{Mutex, RwLock};

const MAX_AGE: Duration = Duration::from_secs(10);
const MIN_RELOAD_GAP: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Cache {
    /// Hub id → (site, public key).
    keys: HashMap<String, (String, VerifyingKey)>,
    /// Site → the clusters it is limited to.
    sites: HashMap<String, Option<Vec<String>>>,
    loaded: Option<Instant>,
}

pub struct HubDirectory {
    store: Arc<dyn HubStore>,
    cache: RwLock<Cache>,
    reloading: Mutex<()>,
}

impl HubDirectory {
    pub fn new(store: Arc<dyn HubStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            cache: RwLock::new(Cache::default()),
            reloading: Mutex::new(()),
        })
    }

    pub fn store(&self) -> &Arc<dyn HubStore> {
        &self.store
    }

    /// Reload now (after an admin change).
    pub async fn reload(&self) {
        let _one = self.reloading.lock().await;
        self.load().await;
    }

    /// Reload if the cache is older than `max_age`.
    async fn fresh(&self, max_age: Duration) {
        let stale = |c: &Cache| c.loaded.is_none_or(|t| t.elapsed() >= max_age);
        if !stale(&*self.cache.read().await) {
            return;
        }
        let _one = self.reloading.lock().await;
        if stale(&*self.cache.read().await) {
            self.load().await;
        }
    }

    async fn load(&self) {
        let loaded = async {
            let sites = self.store.list_hub_sites().await?;
            let keys = self.store.list_hub_keys().await?;
            Ok::<_, queryflux_core::error::QueryFluxError>((sites, keys))
        }
        .await;
        let mut cache = self.cache.write().await;
        // A failed read keeps what was loaded and waits a full MAX_AGE to retry, so an
        // unreachable store isn't read on every request.
        cache.loaded = Some(Instant::now());
        match loaded {
            Ok((sites, keys)) => {
                cache.sites = sites.into_iter().map(|s| (s.name, s.clusters)).collect();
                cache.keys = keys
                    .into_iter()
                    .filter_map(|k| {
                        let public = hub_token::decode_public_key(&k.public_key)?;
                        Some((k.id, (k.site, public)))
                    })
                    .collect();
            }
            Err(e) => tracing::warn!("Could not load enrolled hubs: {e}"),
        }
    }

    /// The site of the hub that signed `token`, if it is a valid, current hub token.
    pub async fn site_of(&self, token: &str) -> Option<String> {
        let id = hub_token::key_id(token)?;
        self.fresh(MAX_AGE).await;
        let mut entry = self.cache.read().await.keys.get(&id).cloned();
        if entry.is_none() {
            self.fresh(MIN_RELOAD_GAP).await;
            entry = self.cache.read().await.keys.get(&id).cloned();
        }
        let (site, key) = entry?;
        let now = chrono::Utc::now().timestamp();
        match hub_token::verify(token, &key, now) {
            Ok(()) => Some(site),
            Err(why) => {
                tracing::debug!(hub = %id, "Hub token refused: {why}");
                None
            }
        }
    }

    /// The clusters `site` is limited to, if any.
    pub async fn clusters(&self, site: &str) -> Option<Vec<String>> {
        self.fresh(MAX_AGE).await;
        self.cache.read().await.sites.get(site).cloned().flatten()
    }

    /// Every site limited to some clusters.
    pub async fn limited_sites(&self) -> Vec<(String, Vec<String>)> {
        self.fresh(MAX_AGE).await;
        self.cache
            .read()
            .await
            .sites
            .iter()
            .filter_map(|(site, clusters)| Some((site.clone(), clusters.clone()?)))
            .collect()
    }
}
