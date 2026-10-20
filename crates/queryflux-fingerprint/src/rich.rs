//! Rich fingerprinting using polyglot-sql for AST-based normalization.
//!
//! Called inside `tokio::spawn` after query completion — never on the routing hot path.
//! Uses polyglot-sql to parse and re-serialize SQL for consistent normalization across
//! dialects, then applies regex-based literal replacement.
//!
//! Falls back to `crate::fallback` if polyglot-sql cannot parse the input.

use std::sync::{Arc, LazyLock};

use queryflux_core::bounded_cache::{BoundedCache, CacheStatsSnapshot, MAX_CACHED_SQL_BYTES};
use queryflux_core::config::DEFAULT_SQL_CACHE_ENTRIES;
use tracing::warn;
use xxhash_rust::xxh64::xxh64;

use crate::fallback;

/// The result of rich fingerprinting — everything needed for storage and analytics.
#[derive(Debug, Clone)]
pub struct QueryFingerprint {
    /// Hash of the normalized original SQL (exact match, no literal replacement).
    pub query_hash: u64,
    /// Hash of the parameterized original SQL (literals replaced with `?`).
    pub query_parameterized_hash: u64,
    /// Human-readable parameterized SQL — stored in `query_digest_stats`, not in `query_records`.
    pub digest_text: String,
    /// Hash of the parameterized translated SQL. `None` when no translation occurred.
    pub translated_query_hash: Option<u64>,
    /// Human-readable parameterized translated SQL for `query_digest_stats`. `None` when no translation.
    pub translated_digest_text: Option<String>,
    /// `false` if the query contains non-deterministic functions (NOW, RANDOM, UUID, etc.).
    /// Non-deterministic queries must not be cached.
    pub is_deterministic: bool,
}

/// Compute a rich fingerprint for `original_sql`.
///
/// Returns `None` if polyglot-sql fails to parse the SQL — callers should leave
/// hash/digest fields unpopulated rather than writing unreliable data.
///
/// Repeated statements are served from the process-wide [`fingerprint_cache`].
pub fn rich_fingerprint(
    original_sql: &str,
    translated_sql: Option<&str>,
    src_dialect: &str,
    tgt_dialect: &str,
) -> Option<QueryFingerprint> {
    FINGERPRINT_CACHE.rich_fingerprint(original_sql, translated_sql, src_dialect, tgt_dialect)
}

fn rich_fingerprint_with(
    original_sql: &str,
    translated_sql: Option<&str>,
    src_dialect: &str,
    tgt_dialect: &str,
    one: impl Fn(&str, &str) -> Option<OneFingerprint>,
) -> Option<QueryFingerprint> {
    let (query_hash, query_parameterized_hash, digest_text, is_deterministic) =
        one(original_sql, src_dialect)?;

    let (translated_query_hash, translated_digest_text) = match translated_sql {
        Some(tsql) => match one(tsql, tgt_dialect) {
            Some((_, hash, digest, _)) => (Some(hash), Some(digest)),
            None => (None, None),
        },
        None => (None, None),
    };

    Some(QueryFingerprint {
        query_hash,
        query_parameterized_hash,
        digest_text,
        translated_query_hash,
        translated_digest_text,
        is_deterministic,
    })
}

/// `(query_hash, parameterized_hash, digest_text, is_deterministic)` for one statement.
type OneFingerprint = (u64, u64, String, bool);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FingerprintKey {
    sql: String,
    dialect: String,
}

/// Cache of per-statement fingerprints keyed on the exact SQL text and polyglot dialect,
/// the only inputs. Only successful parses are kept: a failure may be the parse pool's
/// thread overflowing rather than the SQL itself.
pub struct FingerprintCache {
    cache: BoundedCache<FingerprintKey, Arc<OneFingerprint>>,
}

impl FingerprintCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: BoundedCache::new("fingerprint", capacity),
        }
    }

    pub fn set_capacity(&self, capacity: usize) {
        self.cache.set_capacity(capacity);
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        self.cache.stats()
    }

    /// [`rich_fingerprint`] through this cache.
    pub fn rich_fingerprint(
        &self,
        original_sql: &str,
        translated_sql: Option<&str>,
        src_dialect: &str,
        tgt_dialect: &str,
    ) -> Option<QueryFingerprint> {
        rich_fingerprint_with(
            original_sql,
            translated_sql,
            src_dialect,
            tgt_dialect,
            |s, d| self.fingerprint_one(s, d),
        )
    }

    fn fingerprint_one(&self, sql: &str, dialect: &str) -> Option<OneFingerprint> {
        let key = (self.cache.is_enabled() && sql.len() <= MAX_CACHED_SQL_BYTES).then(|| {
            FingerprintKey {
                sql: sql.to_string(),
                dialect: dialect.to_string(),
            }
        });
        if let Some(hit) = key.as_ref().and_then(|k| self.cache.get(k)) {
            return Some((*hit).clone());
        }
        let fp = fingerprint_one(sql, dialect)?;
        if let Some(key) = key {
            self.cache.insert(key, Arc::new(fp.clone()));
        }
        Some(fp)
    }
}

static FINGERPRINT_CACHE: LazyLock<FingerprintCache> =
    LazyLock::new(|| FingerprintCache::new(DEFAULT_SQL_CACHE_ENTRIES));

/// The process-wide fingerprint cache [`rich_fingerprint`] uses. Sized at startup from
/// `translation.cache.fingerprints`.
pub fn fingerprint_cache() -> &'static FingerprintCache {
    &FINGERPRINT_CACHE
}

/// Normalize and fingerprint a single SQL string using polyglot-sql.
/// Returns `None` if parsing fails or the thread overflows — no fallback.
fn fingerprint_one(sql: &str, dialect: &str) -> Option<OneFingerprint> {
    let sql_owned = sql.to_string();
    let dialect_owned = dialect.to_string();

    // polyglot-sql is a recursive-descent parser that overflows the default tokio worker
    // stack for complex SQL. Run it on a pooled thread with a 16 MiB stack.
    let result =
        crate::polyglot_pool::run(move || try_polyglot(&sql_owned, &dialect_owned).ok()).flatten();

    if result.is_none() {
        warn!(
            dialect,
            "polyglot-sql fingerprint failed or overflowed — skipping"
        );
    }
    result
}

/// AST-based determinism check using polyglot-sql.
/// Returns `true` if no known non-deterministic expressions are found.
/// This does not establish read-only behavior or cache eligibility.
/// Falls back to a conservative regex check if polyglot fails to parse.
pub fn is_deterministic(sql: &str, dialect: &str) -> bool {
    let sql_owned = sql.to_string();
    let dialect_owned = dialect.to_string();

    let result =
        crate::polyglot_pool::run(move || try_determinism_check(&sql_owned, &dialect_owned))
            .flatten();

    // If polyglot can't parse it, fall back to regex (conservative).
    // When polyglot succeeds, still require regex pass to catch keyword forms it may miss.
    let regex_ok = !NON_DETERMINISTIC_FALLBACK.is_match(sql);
    result.map(|p| p && regex_ok).unwrap_or(regex_ok)
}

static NON_DETERMINISTIC_FALLBACK: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(
    || {
        regex::Regex::new(
            r"(?i)(?:\b(?:RAND|RANDOM|UUID|NOW|SYSDATE|GETDATE|NEWID)\b\s*\(|\b(?:CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME)\b)"
        ).unwrap()
    },
);

fn try_determinism_check(sql: &str, dialect: &str) -> Option<bool> {
    use polyglot_sql::parse_by_name;
    let statements = parse_by_name(sql, dialect).ok()?;
    if statements.is_empty() {
        return None;
    }
    Some(!statements.iter().any(contains_nondeterministic_expr))
}

fn try_polyglot(
    sql: &str,
    dialect: &str,
) -> Result<(u64, u64, String, bool), Box<dyn std::error::Error>> {
    use polyglot_sql::{generate_by_name, parse_by_name};

    // 1. Parse — this normalizes whitespace, comments, and keyword casing.
    let statements = parse_by_name(sql, dialect)?;
    if statements.is_empty() {
        return Err("empty parse result".into());
    }

    // 2. Non-determinism detection via AST walk.
    let is_deterministic = !statements.iter().any(contains_nondeterministic_expr);

    // 3. Re-serialize for query_hash (deterministic output from generate_by_name; no case folding).
    let normalized_parts: Vec<String> = statements
        .iter()
        .map(|s| generate_by_name(s, dialect))
        .collect::<Result<Vec<_>, _>>()?;
    let normalized = normalized_parts.join("; ");
    let query_hash = xxh64(normalized.as_bytes(), 0);

    // 4. Parameterization: case-fold for stable literal/identifier buckets in digest.
    let normalized_lower = normalized.to_lowercase();
    let digest_text = fallback::parameterize(&normalized_lower);
    let parameterized_hash = xxh64(digest_text.as_bytes(), 0);

    Ok((
        query_hash,
        parameterized_hash,
        digest_text,
        is_deterministic,
    ))
}

/// Walk one polyglot-sql Expression tree (DFS) to detect non-deterministic functions.
fn contains_nondeterministic_expr(expr: &polyglot_sql::expressions::Expression) -> bool {
    use polyglot_sql::expressions::Expression;
    use polyglot_sql::traversal::DfsIter;

    DfsIter::new(expr).any(|e| match e {
        Expression::CurrentDate(_)
        | Expression::CurrentTime(_)
        | Expression::CurrentTimestamp(_)
        | Expression::CurrentTimestampLTZ(_)
        | Expression::CurrentDatetime(_)
        | Expression::Random(_)
        | Expression::Rand(_)
        | Expression::Uuid(_) => true,
        Expression::Function(f) => {
            f.name.eq_ignore_ascii_case("now")
                || f.name.eq_ignore_ascii_case("random")
                || f.name.eq_ignore_ascii_case("uuid")
                || f.name.eq_ignore_ascii_case("sysdate")
                || f.name.eq_ignore_ascii_case("getdate")
        }
        _ => false,
    })
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn same(a: &QueryFingerprint, b: &QueryFingerprint) {
        assert_eq!(a.query_hash, b.query_hash);
        assert_eq!(a.query_parameterized_hash, b.query_parameterized_hash);
        assert_eq!(a.digest_text, b.digest_text);
        assert_eq!(a.translated_query_hash, b.translated_query_hash);
        assert_eq!(a.translated_digest_text, b.translated_digest_text);
        assert_eq!(a.is_deterministic, b.is_deterministic);
    }

    #[test]
    fn hit_returns_the_same_fingerprint() {
        let cache = FingerprintCache::new(16);
        let sql = "SELECT id FROM orders WHERE id = 42";
        let translated = "SELECT id FROM orders WHERE id = 42 LIMIT 10";
        let uncached = rich_fingerprint_with(sql, Some(translated), "trino", "postgres", |s, d| {
            fingerprint_one(s, d)
        })
        .unwrap();
        let first = cache
            .rich_fingerprint(sql, Some(translated), "trino", "postgres")
            .unwrap();
        let second = cache
            .rich_fingerprint(sql, Some(translated), "trino", "postgres")
            .unwrap();
        same(&first, &uncached);
        same(&second, &uncached);
        // Original and translated are cached separately: 2 misses, then 2 hits.
        assert_eq!(cache.stats().misses, 2);
        assert_eq!(cache.stats().hits, 2);
    }

    #[test]
    fn sql_change_misses() {
        let cache = FingerprintCache::new(16);
        let a = cache
            .rich_fingerprint("SELECT 1 FROM t", None, "trino", "trino")
            .unwrap();
        let b = cache
            .rich_fingerprint("SELECT 1 FROM u", None, "trino", "trino")
            .unwrap();
        assert_ne!(a.query_hash, b.query_hash);
        assert_eq!(cache.stats().misses, 2);
        assert_eq!(cache.stats().hits, 0);
    }

    #[test]
    fn dialect_change_misses() {
        let cache = FingerprintCache::new(16);
        cache.rich_fingerprint("SELECT 1 FROM t", None, "trino", "trino");
        cache.rich_fingerprint("SELECT 1 FROM t", None, "postgres", "trino");
        assert_eq!(cache.stats().misses, 2);
        assert_eq!(cache.stats().hits, 0);
    }

    #[test]
    fn parse_failures_are_not_cached() {
        let cache = FingerprintCache::new(16);
        assert!(cache
            .rich_fingerprint("SELEC FROM (((", None, "trino", "trino")
            .is_none());
        assert!(cache
            .rich_fingerprint("SELEC FROM (((", None, "trino", "trino")
            .is_none());
        assert_eq!(cache.stats().hits, 0);
    }

    #[test]
    fn disabled_cache_still_fingerprints() {
        let cache = FingerprintCache::new(0);
        let a = cache
            .rich_fingerprint("SELECT 1 FROM t", None, "trino", "trino")
            .unwrap();
        let b = cache
            .rich_fingerprint("SELECT 1 FROM t", None, "trino", "trino")
            .unwrap();
        same(&a, &b);
        assert_eq!(cache.stats(), CacheStatsSnapshot::default());
    }
}
