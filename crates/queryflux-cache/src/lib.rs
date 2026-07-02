pub mod noop;
pub mod opendal_cache;

use std::fmt;

use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_core::config::GroupCacheConfig;
use queryflux_core::params::QueryParam;
use queryflux_core::session::SessionContext;
use xxhash_rust::xxh64::Xxh64;

// ---------------------------------------------------------------------------
// QueryResultCache trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait QueryResultCache: Send + Sync {
    /// Try to serve a cached result. Returns `Some(stats)` on cache hit after
    /// streaming all batches to `sink`, or `None` on miss / expired.
    async fn try_stream_cached(
        &self,
        key: &CacheKey,
        sink: &mut dyn CacheSink,
    ) -> Result<Option<CacheHitStats>>;

    /// Create a writer for storing a new cache entry.
    async fn writer(&self, key: &CacheKey, ttl_secs: u64) -> Result<Box<dyn CacheWriter>>;

    /// Delete all cached entries for a group. Returns number of entries removed.
    async fn invalidate_group(&self, group: &str) -> Result<u64>;

    /// Delete all cached entries. Returns number of entries removed.
    async fn invalidate_all(&self) -> Result<u64>;

    /// Delete expired entries and their backing files. Returns number removed.
    async fn cleanup_expired(&self) -> Result<u64>;
}

/// Subset of ResultSink used for cache replay — avoids coupling to the full
/// dispatch ResultSink which has on_error / on_native_chunk.
#[async_trait]
pub trait CacheSink: Send {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()>;
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

#[async_trait]
pub trait CacheWriter: Send {
    async fn write_schema(&mut self, schema: &Schema) -> Result<()>;
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    /// Finalize the entry. If `success` is false, clean up partial data.
    async fn finalize(&mut self, success: bool) -> Result<()>;
    /// Total bytes written so far.
    fn bytes_written(&self) -> u64;
}

// ---------------------------------------------------------------------------
// CacheKey
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CacheKey {
    pub hex: String,
    pub group: String,
}

impl CacheKey {
    pub fn new(
        sql: &str,
        group: &str,
        session: &SessionContext,
        user: &str,
        params: &[QueryParam],
    ) -> Self {
        let mut hasher = Xxh64::new(0);
        hasher.update(sql.as_bytes());
        hasher.update(group.as_bytes());
        hasher.update(user.as_bytes());
        if let Some(db) = &session.database {
            hasher.update(db.as_bytes());
        }
        for param in params {
            hash_param(&mut hasher, param);
        }
        let digest = hasher.digest();
        Self {
            hex: format!("{:016x}", digest),
            group: group.to_string(),
        }
    }

    /// Derive the OpenDAL storage path from the key.
    pub fn storage_path(&self) -> String {
        format!("{}/{}.arrow", self.group, self.hex)
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.group, self.hex)
    }
}

// ---------------------------------------------------------------------------
// CacheHitStats
// ---------------------------------------------------------------------------

pub struct CacheHitStats {
    pub row_count: u64,
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// CacheHint — per-query opt-in
// ---------------------------------------------------------------------------

/// Present when the client explicitly requests caching for this query.
/// `Option<CacheHint>` is the enable signal — if `Some`, cache this query.
pub struct CacheHint {
    pub ttl_secs: Option<u64>,
}

impl CacheHint {
    /// Convert to a GroupCacheConfig for dispatch logic (group_cache OR hint).
    pub fn to_group_config(&self) -> GroupCacheConfig {
        GroupCacheConfig {
            enabled: true,
            ttl_secs: self.ttl_secs.unwrap_or(300),
            max_entry_size_mb: None,
        }
    }
}

/// Extract a per-query cache hint from headers, tags, or SQL comment.
///
/// Extraction order (first match wins):
/// 1. `x-queryflux-cache` header in `SessionContext.extra`
/// 2. `queryflux:cache` tag key in `SessionContext.tags`
/// 3. SQL comment `/* queryflux:cache */` or `/* queryflux:cache:ttl=N */`
pub fn extract_cache_hint(sql: &str, session: &SessionContext) -> Option<CacheHint> {
    // 1. HTTP header
    if let Some(val) = session.extra.get("x-queryflux-cache") {
        if val.eq_ignore_ascii_case("true") || val == "1" {
            let ttl = session
                .extra
                .get("x-queryflux-cache-ttl")
                .and_then(|v| v.parse::<u64>().ok());
            return Some(CacheHint { ttl_secs: ttl });
        }
    }

    // 2. Query tags
    if let Some(tag_val) = session.tags.get("queryflux:cache") {
        let ttl = tag_val
            .as_ref()
            .and_then(|v| v.strip_prefix("ttl="))
            .and_then(|v| v.parse::<u64>().ok());
        return Some(CacheHint { ttl_secs: ttl });
    }

    // 3. SQL comment (first 200 chars only to avoid false positives in string literals)
    let end = sql.floor_char_boundary(sql.len().min(200));
    let prefix = &sql[..end];
    static CACHE_COMMENT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"/\*\s*queryflux:cache(?::ttl=(\d+))?\s*\*/").unwrap()
    });
    if let Some(caps) = CACHE_COMMENT.captures(prefix) {
        let ttl = caps.get(1).and_then(|m| m.as_str().parse::<u64>().ok());
        return Some(CacheHint { ttl_secs: ttl });
    }

    None
}

// ---------------------------------------------------------------------------
// Determinism check — AST-based via polyglot-sql with regex fallback
// ---------------------------------------------------------------------------

/// Returns `true` if the query is deterministic (safe to cache).
/// Uses polyglot-sql AST walk for accuracy; falls back to regex if parse fails.
pub fn is_deterministic(sql: &str, dialect: &str) -> bool {
    queryflux_fingerprint::is_deterministic(sql, dialect)
}

fn hash_param(hasher: &mut Xxh64, param: &QueryParam) {
    match param {
        QueryParam::Text(v) => {
            hasher.update(b"T:");
            hasher.update(v.as_bytes());
        }
        QueryParam::Numeric(v) => {
            hasher.update(b"N:");
            hasher.update(v.as_bytes());
        }
        QueryParam::Boolean(v) => {
            hasher.update(if *v { b"B:1" } else { b"B:0" });
        }
        QueryParam::Date(v) => {
            hasher.update(b"D:");
            hasher.update(v.as_bytes());
        }
        QueryParam::Timestamp(v) => {
            hasher.update(b"TS:");
            hasher.update(v.as_bytes());
        }
        QueryParam::Time(v) => {
            hasher.update(b"TM:");
            hasher.update(v.as_bytes());
        }
        QueryParam::Null => hasher.update(b"NULL"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_deterministic() {
        let s = SessionContext::default();
        let k1 = CacheKey::new("SELECT 1", "grp", &s, "alice", &[]);
        let k2 = CacheKey::new("SELECT 1", "grp", &s, "alice", &[]);
        assert_eq!(k1.hex, k2.hex);
    }

    #[test]
    fn cache_key_different_group() {
        let s = SessionContext::default();
        let k1 = CacheKey::new("SELECT 1", "grp_a", &s, "alice", &[]);
        let k2 = CacheKey::new("SELECT 1", "grp_b", &s, "alice", &[]);
        assert_ne!(k1.hex, k2.hex);
    }

    #[test]
    fn cache_key_different_database() {
        let s1 = SessionContext {
            database: Some("db1".to_string()),
            ..Default::default()
        };
        let s2 = SessionContext {
            database: Some("db2".to_string()),
            ..Default::default()
        };
        let k1 = CacheKey::new("SELECT 1", "grp", &s1, "alice", &[]);
        let k2 = CacheKey::new("SELECT 1", "grp", &s2, "alice", &[]);
        assert_ne!(k1.hex, k2.hex);
    }

    #[test]
    fn cache_key_different_user() {
        let s = SessionContext::default();
        let k1 = CacheKey::new("SELECT 1", "grp", &s, "alice", &[]);
        let k2 = CacheKey::new("SELECT 1", "grp", &s, "bob", &[]);
        assert_ne!(k1.hex, k2.hex);
    }

    #[test]
    fn cache_key_different_params() {
        let s = SessionContext::default();
        let k1 = CacheKey::new(
            "SELECT ?",
            "grp",
            &s,
            "alice",
            &[QueryParam::Numeric("1".into())],
        );
        let k2 = CacheKey::new(
            "SELECT ?",
            "grp",
            &s,
            "alice",
            &[QueryParam::Numeric("2".into())],
        );
        assert_ne!(k1.hex, k2.hex);
    }

    #[test]
    fn determinism_check_catches_now() {
        assert!(!is_deterministic("SELECT NOW()", "generic"));
        assert!(!is_deterministic("select random()", "generic"));
        assert!(!is_deterministic("SELECT uuid()", "generic"));
        assert!(!is_deterministic("SELECT CURRENT_TIMESTAMP", "generic"));
    }

    #[test]
    fn determinism_check_passes_normal() {
        assert!(is_deterministic(
            "SELECT * FROM orders WHERE id = 1",
            "generic"
        ));
        assert!(is_deterministic("SELECT count(*) FROM users", "generic"));
    }

    #[test]
    fn hint_from_header() {
        let mut s = SessionContext::default();
        s.extra.insert("x-queryflux-cache".into(), "true".into());
        s.extra.insert("x-queryflux-cache-ttl".into(), "600".into());
        let hint = extract_cache_hint("SELECT 1", &s).unwrap();
        assert_eq!(hint.ttl_secs, Some(600));
    }

    #[test]
    fn hint_from_sql_comment_utf8_boundary() {
        let s = SessionContext::default();
        // 200-byte boundary falls inside a multi-byte UTF-8 character without floor_char_boundary.
        let sql = format!("/* queryflux:cache:ttl=99 */ {}", "é".repeat(250));
        let hint = extract_cache_hint(&sql, &s).unwrap();
        assert_eq!(hint.ttl_secs, Some(99));
    }

    #[test]
    fn hint_from_sql_comment() {
        let s = SessionContext::default();
        let hint = extract_cache_hint("/* queryflux:cache:ttl=120 */ SELECT 1", &s).unwrap();
        assert_eq!(hint.ttl_secs, Some(120));
    }

    #[test]
    fn hint_from_sql_comment_no_ttl() {
        let s = SessionContext::default();
        let hint = extract_cache_hint("/* queryflux:cache */ SELECT 1", &s).unwrap();
        assert_eq!(hint.ttl_secs, None);
    }

    #[test]
    fn no_hint() {
        let s = SessionContext::default();
        assert!(extract_cache_hint("SELECT 1", &s).is_none());
    }
}
