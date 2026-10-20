//! Small bounded LRU used to skip repeated per-query SQL work (translation, access-control
//! table extraction, fingerprints).
//!
//! Callers key it on every input that changes the result and store only deterministic
//! results. Each cache counts hits, misses and evictions twice: on the instance (for
//! tests and introspection) and in a process-wide total per cache name, which
//! `/metrics` exports (see [`totals`]).

use std::collections::BTreeMap;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use lru::LruCache;

/// SQL longer than this is never cached: generated statements can be huge, and the entry
/// count alone would not bound memory for them.
pub const MAX_CACHED_SQL_BYTES: usize = 64 * 1024;

/// Hit, miss and eviction counters.
#[derive(Debug, Default)]
pub struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// A point-in-time copy of [`CacheStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl CacheStats {
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

static TOTALS: LazyLock<Mutex<BTreeMap<&'static str, Arc<CacheStats>>>> =
    LazyLock::new(Default::default);

fn totals_for(name: &'static str) -> Arc<CacheStats> {
    TOTALS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(name)
        .or_default()
        .clone()
}

/// Process-wide counters per cache name, summed over every instance with that name.
pub fn totals() -> Vec<(&'static str, CacheStatsSnapshot)> {
    TOTALS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(name, stats)| (*name, stats.snapshot()))
        .collect()
}

/// Thread-safe bounded LRU. A capacity of 0 disables it: lookups miss without counting and
/// inserts are dropped. Store cheap-to-clone values (wrap large ones in `Arc`).
pub struct BoundedCache<K, V> {
    name: &'static str,
    inner: Mutex<Option<LruCache<K, V>>>,
    stats: CacheStats,
    totals: Arc<CacheStats>,
}

impl<K: Hash + Eq, V: Clone> BoundedCache<K, V> {
    pub fn new(name: &'static str, capacity: usize) -> Self {
        Self {
            name,
            inner: Mutex::new(NonZeroUsize::new(capacity).map(LruCache::new)),
            stats: CacheStats::default(),
            totals: totals_for(name),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<LruCache<K, V>>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn is_enabled(&self) -> bool {
        self.lock().is_some()
    }

    pub fn capacity(&self) -> usize {
        self.lock().as_ref().map_or(0, |c| c.cap().get())
    }

    pub fn len(&self) -> usize {
        self.lock().as_ref().map_or(0, |c| c.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        self.stats.snapshot()
    }

    fn count(&self, f: impl Fn(&CacheStats) -> &AtomicU64, n: u64) {
        f(&self.stats).fetch_add(n, Ordering::Relaxed);
        f(&self.totals).fetch_add(n, Ordering::Relaxed);
    }

    /// Look `key` up, counting a hit or a miss. Always `None` when disabled.
    pub fn get(&self, key: &K) -> Option<V> {
        let found = {
            let mut guard = self.lock();
            let cache = guard.as_mut()?;
            cache.get(key).cloned()
        };
        if found.is_some() {
            self.count(|s| &s.hits, 1);
        } else {
            self.count(|s| &s.misses, 1);
        }
        found
    }

    /// Insert, evicting the least recently used entry when full. No-op when disabled.
    pub fn insert(&self, key: K, value: V) {
        let evicted = {
            let mut guard = self.lock();
            let Some(cache) = guard.as_mut() else {
                return;
            };
            let full = cache.len() == cache.cap().get() && !cache.contains(&key);
            cache.push(key, value);
            full
        };
        if evicted {
            self.count(|s| &s.evictions, 1);
        }
    }

    /// Change the capacity; 0 disables and drops every entry. Entries pushed out by a
    /// smaller capacity count as evictions.
    pub fn set_capacity(&self, capacity: usize) {
        let mut guard = self.lock();
        let dropped = match (guard.as_mut(), NonZeroUsize::new(capacity)) {
            (Some(cache), Some(cap)) => {
                let before = cache.len();
                cache.resize(cap);
                before - cache.len()
            }
            (Some(cache), None) => {
                let before = cache.len();
                *guard = None;
                before
            }
            (None, Some(cap)) => {
                *guard = Some(LruCache::new(cap));
                0
            }
            (None, None) => 0,
        };
        drop(guard);
        if dropped > 0 {
            self.count(|s| &s.evictions, dropped as u64);
        }
    }

    /// Drop every entry (not counted as evictions).
    pub fn clear(&self) {
        if let Some(cache) = self.lock().as_mut() {
            cache.clear();
        }
    }
}

/// Stable 128-bit content hash for key components that are too large to keep verbatim
/// (fixup scripts, schema context). Feed it with [`ContentHasher::write_str`] etc.
pub struct ContentHasher(xxhash_rust::xxh3::Xxh3Default);

impl Default for ContentHasher {
    fn default() -> Self {
        Self(xxhash_rust::xxh3::Xxh3Default::new())
    }
}

impl ContentHasher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Length-prefixed, so `("ab", "c")` and `("a", "bc")` hash differently.
    pub fn write_str(&mut self, s: &str) -> &mut Self {
        self.0.update(&(s.len() as u64).to_le_bytes());
        self.0.update(s.as_bytes());
        self
    }

    pub fn write_u64(&mut self, v: u64) -> &mut Self {
        self.0.update(&v.to_le_bytes());
        self
    }

    pub fn finish(&self) -> u128 {
        self.0.digest128()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_miss_and_eviction_are_counted() {
        let cache: BoundedCache<String, u32> = BoundedCache::new("test_counts", 2);
        assert_eq!(cache.get(&"a".to_string()), None);
        cache.insert("a".into(), 1);
        cache.insert("b".into(), 2);
        assert_eq!(cache.get(&"a".to_string()), Some(1));
        // "b" is now least recently used and is evicted by "c".
        cache.insert("c".into(), 3);
        assert_eq!(cache.get(&"b".to_string()), None);
        assert_eq!(cache.get(&"c".to_string()), Some(3));
        assert_eq!(
            cache.stats(),
            CacheStatsSnapshot {
                hits: 2,
                misses: 2,
                evictions: 1
            }
        );
        // Replacing an existing key is not an eviction.
        cache.insert("c".into(), 4);
        assert_eq!(cache.stats().evictions, 1);
        assert_eq!(cache.get(&"c".to_string()), Some(4));
        assert!(totals()
            .iter()
            .any(|(name, s)| *name == "test_counts" && s.hits >= 3));
    }

    #[test]
    fn zero_capacity_disables_without_counting() {
        let cache: BoundedCache<String, u32> = BoundedCache::new("test_disabled", 0);
        assert!(!cache.is_enabled());
        cache.insert("a".into(), 1);
        assert_eq!(cache.get(&"a".to_string()), None);
        assert_eq!(cache.stats(), CacheStatsSnapshot::default());
    }

    #[test]
    fn set_capacity_resizes_enables_and_disables() {
        let cache: BoundedCache<u32, u32> = BoundedCache::new("test_resize", 0);
        cache.set_capacity(3);
        for i in 0..3 {
            cache.insert(i, i);
        }
        assert_eq!(cache.len(), 3);
        cache.set_capacity(1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.stats().evictions, 2);
        cache.set_capacity(0);
        assert!(!cache.is_enabled());
        assert_eq!(cache.stats().evictions, 3);
        cache.clear();
    }

    #[test]
    fn content_hash_is_length_prefixed() {
        let a = ContentHasher::new().write_str("ab").write_str("c").finish();
        let b = ContentHasher::new().write_str("a").write_str("bc").finish();
        assert_ne!(a, b);
        let c = ContentHasher::new().write_str("ab").write_str("c").finish();
        assert_eq!(a, c);
    }
}
