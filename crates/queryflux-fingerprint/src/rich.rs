//! Rich fingerprinting using polyglot-sql for AST-based normalization.
//!
//! Called inside `tokio::spawn` after query completion — never on the routing hot path.
//! Uses polyglot-sql to parse and re-serialize SQL for consistent normalization across
//! dialects, then applies regex-based literal replacement.
//!
//! Falls back to `crate::fallback` if polyglot-sql cannot parse the input.

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
pub fn rich_fingerprint(
    original_sql: &str,
    translated_sql: Option<&str>,
    src_dialect: &str,
    tgt_dialect: &str,
) -> Option<QueryFingerprint> {
    let (query_hash, query_parameterized_hash, digest_text, is_deterministic) =
        fingerprint_one(original_sql, src_dialect)?;

    let (translated_query_hash, translated_digest_text) = match translated_sql {
        Some(tsql) => match fingerprint_one(tsql, tgt_dialect) {
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

/// Normalize and fingerprint a single SQL string using polyglot-sql.
/// Returns `None` if parsing fails or the thread overflows — no fallback.
fn fingerprint_one(sql: &str, dialect: &str) -> Option<(u64, u64, String, bool)> {
    let sql_owned = sql.to_string();
    let dialect_owned = dialect.to_string();

    // polyglot-sql is a recursive-descent parser (59K lines, 605 fns) that overflows the
    // default tokio worker stack for complex SQL. Run it on a dedicated thread with an
    // explicit 16MB stack. .ok() inside the closure converts the Result to Option so no
    // non-Send error type crosses the thread boundary.
    let result = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || try_polyglot(&sql_owned, &dialect_owned).ok())
        .ok()
        .and_then(|h| h.join().ok())
        .flatten();

    if result.is_none() {
        warn!(
            dialect,
            "polyglot-sql fingerprint failed or overflowed — skipping"
        );
    }
    result
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
