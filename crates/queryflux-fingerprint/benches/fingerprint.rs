//! Benchmarks for query fingerprinting and SQL classification.
//!
//! Run locally with `cargo bench -p queryflux-fingerprint`, or under CodSpeed with
//! `cargo codspeed build -p queryflux-fingerprint && cargo codspeed run`.

use divan::black_box;
use queryflux_core::query::SqlDialect;
use queryflux_core::sql_classify::{is_read_like_fallback, SqlParseCache};
use queryflux_fingerprint::{fallback, fast_hash, is_deterministic, rich_fingerprint};

fn main() {
    divan::main();
}

const SIMPLE: &str = "SELECT id, name FROM users WHERE id = 42";

const MEDIUM: &str = "/* dashboard: revenue */
SELECT o.customer_id, c.name, SUM(o.amount) AS total, COUNT(*) AS cnt
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.created_at >= '2024-01-01' AND o.status IN ('paid', 'shipped', 'delivered')
  AND o.amount > -10.5 -- ignore refunds
GROUP BY o.customer_id, c.name
HAVING SUM(o.amount) > 1000
ORDER BY total DESC
LIMIT 100";

const COMPLEX: &str = "WITH recent AS (
  SELECT user_id, event_type, ts, payload
  FROM events
  WHERE ts > TIMESTAMP '2024-06-01 00:00:00' AND event_type <> 'heartbeat'
), ranked AS (
  SELECT user_id, event_type, ts,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn,
         LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts
  FROM recent
), sessions AS (
  SELECT user_id,
         SUM(CASE WHEN prev_ts IS NULL OR ts - prev_ts > INTERVAL '30' MINUTE THEN 1 ELSE 0 END)
           OVER (PARTITION BY user_id ORDER BY ts) AS session_id,
         event_type, ts
  FROM ranked
)
SELECT s.user_id, s.session_id, COUNT(*) AS events,
       MIN(s.ts) AS started_at, MAX(s.ts) AS ended_at,
       COALESCE(u.country, 'unknown') AS country
FROM sessions s
LEFT JOIN users u ON u.id = s.user_id AND u.deleted = FALSE
WHERE s.user_id NOT IN (SELECT user_id FROM blocked_users WHERE reason = 'fraud')
  AND u.signup_date BETWEEN DATE '2023-01-01' AND DATE '2024-12-31'
GROUP BY s.user_id, s.session_id, u.country
HAVING COUNT(*) >= 3
ORDER BY events DESC, started_at
LIMIT 500";

const NON_DETERMINISTIC: &str =
    "SELECT id, RANDOM() AS r, NOW() AS t FROM sessions WHERE created_at > CURRENT_DATE - 7";

const INSERT: &str =
    "INSERT INTO audit_log (user_id, action, ts) VALUES (1, 'login', '2024-01-01 00:00:00')";

/// Look up a sample query by its short label (keeps benchmark names readable).
fn sql(name: &str) -> &'static str {
    match name {
        "simple" => SIMPLE,
        "medium" => MEDIUM,
        "complex" => COMPLEX,
        "non_deterministic" => NON_DETERMINISTIC,
        "insert" => INSERT,
        _ => unreachable!("unknown sample query {name}"),
    }
}

const QUERIES: &[&str] = &["simple", "medium", "complex"];

/// Hot-path zero-allocation hash used by the router.
#[divan::bench(args = QUERIES)]
fn fast_hash_query(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| fast_hash(black_box(sql)));
}

/// Hot-path hash over a large batched query (many literal values).
#[divan::bench]
fn fast_hash_large_in_list(bencher: divan::Bencher) {
    let values: Vec<String> = (0..2_000).map(|i| i.to_string()).collect();
    let sql = format!(
        "SELECT * FROM t WHERE id IN ({}) AND name = 'abc'",
        values.join(", ")
    );
    bencher.bench(|| fast_hash(black_box(&sql)));
}

/// Regex-based fallback fingerprinting used when polyglot-sql cannot parse.
#[divan::bench(args = QUERIES)]
fn fallback_fingerprint(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| fallback::fallback_fingerprint(black_box(sql)));
}

/// AST-based rich fingerprint of the original SQL only.
#[divan::bench(args = QUERIES)]
fn rich_fingerprint_original(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| rich_fingerprint(black_box(sql), None, "trino", "trino"));
}

/// AST-based rich fingerprint of original + translated SQL.
#[divan::bench]
fn rich_fingerprint_with_translation() -> Option<queryflux_fingerprint::QueryFingerprint> {
    rich_fingerprint(
        black_box(MEDIUM),
        Some(black_box(MEDIUM)),
        "trino",
        "duckdb",
    )
}

/// Determinism check (cache eligibility signal).
#[divan::bench(args = ["medium", "non_deterministic"])]
fn is_deterministic_check(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| is_deterministic(black_box(sql), "trino"));
}

/// Full read/write classification (parse on the polyglot pool + AST inspection).
#[divan::bench(args = ["medium", "complex", "insert"])]
fn classify_is_read_like(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| SqlParseCache::new(black_box(sql), SqlDialect::Trino).is_read_like());
}

/// Keyword-based classification fallback.
#[divan::bench(args = ["medium", "complex", "insert"])]
fn classify_is_read_like_fallback(bencher: divan::Bencher, query: &str) {
    let sql = sql(query);
    bencher.bench(|| is_read_like_fallback(black_box(sql)));
}
