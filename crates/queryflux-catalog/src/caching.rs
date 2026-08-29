//! TTL + capacity-bounded cache wrapping any `CatalogProvider`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use queryflux_core::catalog::{CatalogProvider, TableSchema};
use queryflux_core::error::Result;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum CacheKey {
    ListCatalogs,
    ListDatabases(String),
    ListTables(String, String),
    GetTableSchema(String, String, String),
}

#[derive(Debug, Clone)]
enum CacheValue {
    Catalogs(Vec<String>),
    Databases(Vec<String>),
    Tables(Vec<String>),
    Schema(Option<TableSchema>),
}

struct Entry {
    value: CacheValue,
    inserted_at: Instant,
}

struct Inner {
    entries: HashMap<CacheKey, Entry>,
    /// Insertion order, for FIFO eviction once `max_entries` is exceeded. A full
    /// LRU (touch-on-read reordering) isn't worth the extra bookkeeping here —
    /// catalog lookups are cheap/infrequent relative to query traffic.
    order: VecDeque<CacheKey>,
}

/// Wraps a `CatalogProvider`, caching each method's results independently by its
/// arguments — built by `maybe_cached` in `lib.rs` for any provider configured
/// with a `cache: { ttlSeconds, maxEntries }` field. Only `Ok` results are
/// cached — an error is never pinned for `ttl_seconds`, so a transient catalog
/// outage self-heals on the very next call instead of being cached as a failure.
pub struct CachingCatalogProvider {
    delegate: Arc<dyn CatalogProvider>,
    ttl: Duration,
    max_entries: usize,
    inner: Mutex<Inner>,
}

impl CachingCatalogProvider {
    pub fn new(delegate: Arc<dyn CatalogProvider>, ttl_seconds: u64, max_entries: usize) -> Self {
        Self {
            delegate,
            ttl: Duration::from_secs(ttl_seconds),
            max_entries,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    async fn get_cached(&self, key: &CacheKey) -> Option<CacheValue> {
        let inner = self.inner.lock().await;
        let entry = inner.entries.get(key)?;
        if entry.inserted_at.elapsed() > self.ttl {
            return None;
        }
        Some(entry.value.clone())
    }

    async fn insert(&self, key: CacheKey, value: CacheValue) {
        let mut inner = self.inner.lock().await;
        if !inner.entries.contains_key(&key) {
            inner.order.push_back(key.clone());
        }
        inner.entries.insert(
            key,
            Entry {
                value,
                inserted_at: Instant::now(),
            },
        );
        while inner.entries.len() > self.max_entries {
            match inner.order.pop_front() {
                Some(oldest) => {
                    inner.entries.remove(&oldest);
                }
                None => break,
            }
        }
    }
}

#[async_trait]
impl CatalogProvider for CachingCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let key = CacheKey::ListCatalogs;
        if let Some(CacheValue::Catalogs(v)) = self.get_cached(&key).await {
            return Ok(v);
        }
        let v = self.delegate.list_catalogs().await?;
        self.insert(key, CacheValue::Catalogs(v.clone())).await;
        Ok(v)
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let key = CacheKey::ListDatabases(catalog.to_string());
        if let Some(CacheValue::Databases(v)) = self.get_cached(&key).await {
            return Ok(v);
        }
        let v = self.delegate.list_databases(catalog).await?;
        self.insert(key, CacheValue::Databases(v.clone())).await;
        Ok(v)
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let key = CacheKey::ListTables(catalog.to_string(), database.to_string());
        if let Some(CacheValue::Tables(v)) = self.get_cached(&key).await {
            return Ok(v);
        }
        let v = self.delegate.list_tables(catalog, database).await?;
        self.insert(key, CacheValue::Tables(v.clone())).await;
        Ok(v)
    }

    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let key =
            CacheKey::GetTableSchema(catalog.to_string(), database.to_string(), table.to_string());
        if let Some(CacheValue::Schema(v)) = self.get_cached(&key).await {
            return Ok(v);
        }
        let v = self
            .delegate
            .get_table_schema(catalog, database, table)
            .await?;
        self.insert(key, CacheValue::Schema(v.clone())).await;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::error::QueryFluxError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts calls and lets a test script errors/results per call.
    struct CountingProvider {
        calls: AtomicUsize,
        fail_next: std::sync::atomic::AtomicBool,
    }

    impl CountingProvider {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_next: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl CatalogProvider for CountingProvider {
        async fn list_catalogs(&self) -> Result<Vec<String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(QueryFluxError::Catalog("boom".to_string()));
            }
            Ok(vec!["hive".to_string()])
        }
        async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }
        async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }
        async fn get_table_schema(
            &self,
            _catalog: &str,
            _database: &str,
            _table: &str,
        ) -> Result<Option<TableSchema>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn hits_are_served_from_cache() {
        let inner = Arc::new(CountingProvider::new());
        let cache = CachingCatalogProvider::new(inner.clone(), 300, 10);

        assert_eq!(cache.list_catalogs().await.unwrap(), vec!["hive"]);
        assert_eq!(cache.list_catalogs().await.unwrap(), vec!["hive"]);
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            1,
            "second call should hit cache"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn entries_expire_after_ttl() {
        let inner = Arc::new(CountingProvider::new());
        let cache = CachingCatalogProvider::new(inner.clone(), 60, 10);

        cache.list_catalogs().await.unwrap();
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(30)).await;
        cache.list_catalogs().await.unwrap();
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1, "still within TTL");

        tokio::time::advance(Duration::from_secs(31)).await;
        cache.list_catalogs().await.unwrap();
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            2,
            "TTL expired, should refetch"
        );
    }

    #[tokio::test]
    async fn errors_are_never_cached() {
        let inner = Arc::new(CountingProvider::new());
        inner.fail_next.store(true, Ordering::SeqCst);
        let cache = CachingCatalogProvider::new(inner.clone(), 300, 10);

        assert!(cache.list_catalogs().await.is_err());
        assert_eq!(cache.list_catalogs().await.unwrap(), vec!["hive"]);
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            2,
            "error must not be cached"
        );
    }
}
