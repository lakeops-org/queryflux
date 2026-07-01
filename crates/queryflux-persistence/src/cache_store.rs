use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::error::Result;
use serde::{Deserialize, Serialize};

/// Metadata for a single cached query result entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntryMeta {
    pub cache_key: String,
    pub group_name: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
}

/// Lightweight reference returned by delete operations (key + group for path derivation).
#[derive(Debug, Clone)]
pub struct CacheEntryRef {
    pub cache_key: String,
    pub group_name: String,
}

impl CacheEntryRef {
    pub fn storage_path(&self) -> String {
        format!("{}/{}.arrow", self.group_name, self.cache_key)
    }
}

/// Persistence interface for cache entry metadata.
///
/// Implementations: Postgres (`cache_entries` table) and in-memory (`DashMap`).
#[async_trait]
pub trait CacheStore: Send + Sync {
    /// Look up a non-expired entry by cache key.
    async fn cache_get_valid(&self, cache_key: &str) -> Result<Option<CacheEntryMeta>>;

    /// Insert or update a cache entry.
    async fn cache_upsert(&self, entry: &CacheEntryMeta) -> Result<()>;

    /// Delete all expired entries. Returns refs for file cleanup.
    async fn cache_delete_expired(&self) -> Result<Vec<CacheEntryRef>>;

    /// Delete all entries for a group. Returns refs for file cleanup.
    async fn cache_delete_by_group(&self, group: &str) -> Result<Vec<CacheEntryRef>>;

    /// Delete all cache entries. Returns refs for file cleanup.
    async fn cache_delete_all(&self) -> Result<Vec<CacheEntryRef>>;
}
