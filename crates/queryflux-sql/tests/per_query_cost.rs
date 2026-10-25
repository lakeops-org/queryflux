//! Per-query CPU cost of the SQL steps a worker runs for every query.
//!
//! Ignored by default: it is a timing harness, not a correctness test. Run with
//!
//! ```text
//! cargo test --release -p queryflux-sql --test per_query_cost -- --ignored --nocapture
//! ```
//!
//! Each step runs the same SQL repeatedly (the common case at high qps) and reports the
//! median and p90 per call, first with its cache disabled (the cost of a miss), then with
//! it enabled (the cost of a hit).

use std::time::{Duration, Instant};

use queryflux_core::config::SqlCacheConfig;
use queryflux_core::query::SqlDialect;
use queryflux_sql::access::ExtractionCache;
use queryflux_sql::classify::SqlParseCache;
use queryflux_sql::sqlglot::extract_table_refs;
use queryflux_sql::{extract_resources, ColumnMap, SchemaContext, TranslationService};

const ITERS: usize = 200;

const QUERIES: &[(&str, &str)] = &[
    ("point", "SELECT id, name FROM orders WHERE id = 42"),
    (
        "join_agg",
        "SELECT c.region, date_trunc('day', o.created_at) AS d, count(*) AS n, \
         sum(o.amount) AS total FROM hive.sales.orders o JOIN hive.sales.customers c \
         ON o.customer_id = c.id WHERE o.created_at > TIMESTAMP '2026-01-01 00:00:00' \
         AND c.region IN ('eu', 'us') GROUP BY 1, 2 ORDER BY total DESC LIMIT 100",
    ),
    (
        "cte_window",
        "WITH recent AS (SELECT customer_id, amount, created_at, \
         row_number() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn \
         FROM orders WHERE created_at > current_date - INTERVAL '30' DAY), \
         top AS (SELECT customer_id, sum(amount) AS spend FROM recent WHERE rn <= 10 \
         GROUP BY customer_id HAVING sum(amount) > 1000) \
         SELECT t.customer_id, t.spend, c.name, c.region, \
         approx_percentile(r.amount, 0.5) AS median_amount \
         FROM top t JOIN customers c ON c.id = t.customer_id \
         JOIN recent r ON r.customer_id = t.customer_id \
         WHERE c.region NOT IN ('test') AND regexp_like(c.name, '^[A-Z]') \
         GROUP BY t.customer_id, t.spend, c.name, c.region ORDER BY t.spend DESC LIMIT 50",
    ),
];

fn schema() -> SchemaContext {
    let mut orders = ColumnMap::new();
    for (c, t) in [
        ("id", "BIGINT"),
        ("name", "VARCHAR"),
        ("customer_id", "BIGINT"),
        ("amount", "DOUBLE"),
        ("created_at", "TIMESTAMP"),
    ] {
        orders.insert(c.into(), t.into());
    }
    let mut customers = ColumnMap::new();
    for (c, t) in [("id", "BIGINT"), ("name", "VARCHAR"), ("region", "VARCHAR")] {
        customers.insert(c.into(), t.into());
    }
    SchemaContext {
        catalog: None,
        database: None,
        tables: [
            ("orders".to_string(), orders),
            ("customers".to_string(), customers),
        ]
        .into_iter()
        .collect(),
    }
}

struct Stats {
    p50: Duration,
    p90: Duration,
}

fn stats(mut samples: Vec<Duration>) -> Stats {
    samples.sort();
    Stats {
        p50: samples[samples.len() / 2],
        p90: samples[samples.len() * 9 / 10],
    }
}

fn report(step: &str, query: &str, s: Stats) {
    println!(
        "{step:<34} {query:<11} p50 {:>9.1} us   p90 {:>9.1} us",
        s.p50.as_secs_f64() * 1e6,
        s.p90.as_secs_f64() * 1e6
    );
}

fn time_sync(mut f: impl FnMut()) -> Stats {
    f(); // warm up (imports, first parse)
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        f();
        samples.push(t.elapsed());
    }
    stats(samples)
}

async fn time_async<F, Fut>(mut f: F) -> Stats
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    f().await;
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        f().await;
        samples.push(t.elapsed());
    }
    stats(samples)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "timing harness; run explicitly with --ignored --nocapture"]
async fn per_query_cost() {
    let service = TranslationService::new_sqlglot(vec![])
        .expect("sqlglot available")
        .with_cache_config(&SqlCacheConfig {
            translations: 0,
            table_extraction: 0,
            fingerprints: 0,
        });
    let cached_service = TranslationService::new_sqlglot(vec![]).expect("sqlglot available");
    let extraction_cache = ExtractionCache::new(1024);
    let no_fp_cache = queryflux_sql::fingerprint::FingerprintCache::new(0);
    let fp_cache = queryflux_sql::fingerprint::FingerprintCache::new(1024);
    let fixup = "def transform(sql, src, dst):\n    return sql\n".to_string();
    let schema = schema();
    let empty = SchemaContext::default();

    for (name, sql) in QUERIES {
        let s = time_async(|| async {
            let r = service
                .maybe_translate_report(sql, &SqlDialect::Trino, &SqlDialect::Postgres, &empty, &[])
                .await;
            assert!(r.result.is_ok());
        })
        .await;
        report("translate trino->postgres", name, s);

        let s = time_async(|| async {
            let r = service
                .maybe_translate_report(
                    sql,
                    &SqlDialect::Trino,
                    &SqlDialect::Postgres,
                    &schema,
                    &[],
                )
                .await;
            assert!(r.result.is_ok());
        })
        .await;
        report("translate +schema", name, s);

        let s = time_async(|| async {
            let r = service
                .maybe_translate_report(
                    sql,
                    &SqlDialect::Trino,
                    &SqlDialect::Trino,
                    &empty,
                    std::slice::from_ref(&fixup),
                )
                .await;
            assert!(r.result.is_ok());
        })
        .await;
        report("translate same dialect +fixup", name, s);

        let s = time_async(|| async {
            let r = cached_service
                .maybe_translate_report(
                    sql,
                    &SqlDialect::Trino,
                    &SqlDialect::Postgres,
                    &schema,
                    &[],
                )
                .await;
            assert!(r.result.is_ok());
        })
        .await;
        report("translate +schema (cache hit)", name, s);

        let s = time_sync(|| {
            extract_resources(sql, &SqlDialect::Trino, &empty).expect("extract");
        });
        report("access extract_resources", name, s);

        let s = time_sync(|| {
            extract_resources(sql, &SqlDialect::Trino, &schema).expect("extract");
        });
        report("access extract_resources +schema", name, s);

        let s = time_sync(|| {
            extraction_cache
                .extract(sql, &SqlDialect::Trino, &schema)
                .expect("extract");
        });
        report("access extract +schema (cache hit)", name, s);

        let s = time_sync(|| {
            extract_table_refs(sql, "trino").expect("refs");
        });
        report("schema extract_table_refs", name, s);

        let translated = service
            .maybe_translate(sql, &SqlDialect::Trino, &SqlDialect::Postgres, &empty, &[])
            .await
            .unwrap();
        let s = time_sync(|| {
            no_fp_cache
                .rich_fingerprint(sql, Some(&translated), "trino", "postgres")
                .expect("fingerprint");
        });
        report("rich_fingerprint (orig+translated)", name, s);

        let s = time_sync(|| {
            fp_cache
                .rich_fingerprint(sql, Some(&translated), "trino", "postgres")
                .expect("fingerprint");
        });
        report("rich_fingerprint (cache hit)", name, s);

        let s = time_async(|| async {
            let c = SqlParseCache::new(sql.to_string(), SqlDialect::Trino);
            let _ = c.statements_async().await;
        })
        .await;
        report("polyglot classify parse (ref)", name, s);
        println!();
    }
}
