//! The translation cache: hits return exactly what a fresh translation returns, every key
//! component causes a miss, and only deterministic failures are kept.
//!
//! Requires sqlglot on `PYTHONPATH` (same as the other translation tests).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::bounded_cache::CacheStatsSnapshot;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::config::{SqlCacheConfig, TranslationMode};
use queryflux_core::error::Result;
use queryflux_core::query::{SqlDialect, TranslationStatus};
use queryflux_translation::{ColumnMap, SchemaContext, TranslationReport, TranslationService};

const SQL: &str = "SELECT id, approx_distinct(name) FROM orders GROUP BY id";

fn service() -> TranslationService {
    TranslationService::new_sqlglot(vec![]).expect("sqlglot available")
}

fn schema(ty: &str) -> SchemaContext {
    SchemaContext {
        catalog: None,
        database: None,
        tables: [(
            "orders".into(),
            ColumnMap::from([
                ("id".to_string(), ty.to_string()),
                ("name".to_string(), "VARCHAR".to_string()),
            ]),
        )]
        .into_iter()
        .collect(),
    }
}

async fn run(
    service: &TranslationService,
    sql: &str,
    src: SqlDialect,
    tgt: SqlDialect,
    schema: &SchemaContext,
    fixups: &[String],
) -> TranslationReport {
    service
        .maybe_translate_report(sql, &src, &tgt, schema, fixups)
        .await
}

fn render(r: &TranslationReport) -> String {
    format!(
        "{:?} {:?} {:?}",
        r.result.as_ref().map_err(|e| e.to_string()),
        r.outcome.status,
        r.outcome.reason
    )
}

fn stats(hits: u64, misses: u64) -> CacheStatsSnapshot {
    CacheStatsSnapshot {
        hits,
        misses,
        evictions: 0,
    }
}

#[tokio::test]
async fn hit_returns_identical_report() {
    let svc = service();
    let empty = SchemaContext::default();
    let first = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    let second = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(render(&first), render(&second));
    assert_eq!(first.outcome.status, TranslationStatus::Fallback);
    assert_eq!(svc.translation_cache_stats(), stats(1, 1));

    // Same answer as a service without a cache.
    let uncached = service().with_cache_config(&SqlCacheConfig {
        translations: 0,
        ..SqlCacheConfig::default()
    });
    let fresh = run(
        &uncached,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(render(&first), render(&fresh));
}

#[tokio::test]
async fn sql_change_misses() {
    let svc = service();
    let empty = SchemaContext::default();
    let a = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    let b = run(
        &svc,
        "SELECT id FROM customers",
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_ne!(render(&a), render(&b));
    assert_eq!(svc.translation_cache_stats(), stats(0, 2));
}

#[tokio::test]
async fn source_dialect_change_misses() {
    let svc = service();
    let empty = SchemaContext::default();
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    run(
        &svc,
        SQL,
        SqlDialect::MySql,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(svc.translation_cache_stats(), stats(0, 2));
}

#[tokio::test]
async fn target_dialect_change_misses() {
    let svc = service();
    let empty = SchemaContext::default();
    let pg = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    let duck = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &empty,
        &[],
    )
    .await;
    assert_ne!(render(&pg), render(&duck));
    assert_eq!(svc.translation_cache_stats(), stats(0, 2));
}

#[tokio::test]
async fn fixup_script_change_misses() {
    let svc = service();
    let empty = SchemaContext::default();
    let script = |table: &str| {
        format!("def transform(sql, src, dst):\n    return sql.replace('orders', '{table}')\n")
    };
    let a = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[script("orders_a")],
    )
    .await;
    let b = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[script("orders_b")],
    )
    .await;
    assert!(a.result.unwrap().contains("orders_a"));
    assert!(b.result.unwrap().contains("orders_b"));
    // Same scripts in a different order are a different pipeline.
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[script("x"), script("y")],
    )
    .await;
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[script("y"), script("x")],
    )
    .await;
    assert_eq!(svc.translation_cache_stats(), stats(0, 4));
}

#[tokio::test]
async fn global_scripts_are_part_of_the_key() {
    // Two services with different global scripts but the same group scripts never share
    // results; within one service the global scripts are fixed, so this checks the
    // combined pipeline is what gets hashed (global + group) by moving one script between
    // the two lists.
    let script = "def transform(sql, src, dst):\n    return sql + ' -- g'\n".to_string();
    let svc = TranslationService::new_sqlglot(vec![script.clone()]).unwrap();
    let empty = SchemaContext::default();
    let with_global = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    let with_both = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        std::slice::from_ref(&script),
    )
    .await;
    assert_ne!(render(&with_global), render(&with_both));
    assert_eq!(svc.translation_cache_stats(), stats(0, 2));
}

#[tokio::test]
async fn schema_change_misses() {
    let svc = service();
    let none = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &SchemaContext::default(),
        &[],
    )
    .await;
    let bigint = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &schema("BIGINT"),
        &[],
    )
    .await;
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &schema("INT"),
        &[],
    )
    .await;
    assert_eq!(none.outcome.status, TranslationStatus::Fallback);
    assert_eq!(bigint.outcome.status, TranslationStatus::Yes);
    assert_eq!(svc.translation_cache_stats(), stats(0, 3));
}

#[tokio::test]
async fn policy_change_misses() {
    let svc = service();
    let empty = SchemaContext::default();
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    svc.set_policy(TranslationMode::Strict, false);
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    svc.set_policy(TranslationMode::BestEffort, true);
    // errorOnUnsupported is the same sqlglot call as strict mode: a hit.
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    svc.set_policy(TranslationMode::BestEffort, false);
    run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(svc.translation_cache_stats(), stats(2, 2));
}

/// A deterministic parse error is cached, and the cached failure still follows the policy
/// in force for each query: rejected in strict mode, passed through in best-effort mode.
#[tokio::test]
async fn deterministic_errors_are_cached_and_follow_the_policy() {
    let svc = service().with_policy(TranslationMode::Strict, false);
    let empty = SchemaContext::default();
    let bad = "SELECT FROM WHERE";
    let first = run(
        &svc,
        bad,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &empty,
        &[],
    )
    .await;
    let second = run(
        &svc,
        bad,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &empty,
        &[],
    )
    .await;
    assert!(first.result.is_err());
    assert_eq!(render(&first), render(&second));
    assert_eq!(svc.translation_cache_stats(), stats(1, 1));
}

/// A fixup script that fails once (user code: could be anything transient) is not cached,
/// so the next identical query runs the script again and succeeds.
#[tokio::test]
async fn script_failures_are_not_cached() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let marker = format!("_qf_flaky_{}", CALLS.fetch_add(1, Ordering::SeqCst));
    let script = format!(
        "import sys\n\
         def transform(sql, src, dst):\n    \
             n = getattr(sys, '{marker}', 0)\n    \
             setattr(sys, '{marker}', n + 1)\n    \
             if n == 0:\n        raise RuntimeError('transient')\n    \
             return sql\n"
    );
    let svc = service().with_policy(TranslationMode::Strict, false);
    let empty = SchemaContext::default();
    let fixups = [script];
    let first = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &fixups,
    )
    .await;
    assert!(first.result.is_err(), "first call fails");
    let second = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &fixups,
    )
    .await;
    assert!(second.result.is_ok(), "{:?}", second.result.err());
    let third = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &fixups,
    )
    .await;
    assert_eq!(render(&second), render(&third));
    assert_eq!(svc.translation_cache_stats(), stats(1, 2));
}

#[tokio::test]
async fn disabled_cache_translates_the_same() {
    let svc = service().with_cache_config(&SqlCacheConfig {
        translations: 0,
        table_extraction: 0,
        fingerprints: 0,
    });
    let empty = SchemaContext::default();
    let a = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    let b = run(
        &svc,
        SQL,
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(render(&a), render(&b));
    assert!(a.result.is_ok());
    assert_eq!(svc.translation_cache_stats(), CacheStatsSnapshot::default());
}

#[tokio::test]
async fn least_recently_used_entry_is_evicted() {
    let svc = service().with_cache_config(&SqlCacheConfig {
        translations: 1,
        ..SqlCacheConfig::default()
    });
    let empty = SchemaContext::default();
    run(
        &svc,
        "SELECT 1",
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    run(
        &svc,
        "SELECT 2",
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    run(
        &svc,
        "SELECT 1",
        SqlDialect::Trino,
        SqlDialect::Postgres,
        &empty,
        &[],
    )
    .await;
    assert_eq!(
        svc.translation_cache_stats(),
        CacheStatsSnapshot {
            hits: 0,
            misses: 3,
            evictions: 2
        }
    );
}

/// `t(x INT)`, answering after `delay`.
struct SlowCatalog {
    delay: Duration,
}

#[async_trait]
impl CatalogProvider for SlowCatalog {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
        Ok(vec!["t".to_string()])
    }
    async fn get_table_schema(
        &self,
        _catalog: &str,
        _database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        tokio::time::sleep(self.delay).await;
        Ok((table == "t").then(|| TableSchema {
            catalog: String::new(),
            database: String::new(),
            table: "t".to_string(),
            columns: vec![ColumnDef {
                name: "x".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
            }],
        }))
    }
}

/// A schema lookup that times out degrades that query to a dialect-only translation, but
/// does not pin it: once the catalog answers, the same SQL gets the schema-aware result.
#[tokio::test]
async fn schema_timeout_does_not_pin_the_dialect_only_result() {
    let svc = service().with_schema_resolution_timeout(Duration::from_millis(100));
    let sql = "SELECT x FROM t";
    let slow: Arc<dyn CatalogProvider> = Arc::new(SlowCatalog {
        delay: Duration::from_secs(2),
    });
    let fast: Arc<dyn CatalogProvider> = Arc::new(SlowCatalog {
        delay: Duration::ZERO,
    });

    let timed_out = svc
        .resolve_schema_context(sql, &SqlDialect::Trino, &slow, None, None)
        .await;
    assert!(timed_out.is_empty());
    let degraded = run(
        &svc,
        sql,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &timed_out,
        &[],
    )
    .await;
    assert_eq!(degraded.outcome.status, TranslationStatus::Fallback);

    let resolved = svc
        .resolve_schema_context(sql, &SqlDialect::Trino, &fast, None, None)
        .await;
    assert!(!resolved.is_empty());
    let full = run(
        &svc,
        sql,
        SqlDialect::Trino,
        SqlDialect::DuckDb,
        &resolved,
        &[],
    )
    .await;
    assert_eq!(full.outcome.status, TranslationStatus::Yes);
    assert!(full.result.unwrap().contains("\"t\".\"x\""));
    // The table references were extracted once and reused.
    assert_eq!(svc.table_refs_cache_stats(), stats(1, 1));
}

#[tokio::test]
async fn table_refs_cache_keys_on_sql_and_dialect() {
    let svc = service();
    let fast: Arc<dyn CatalogProvider> = Arc::new(SlowCatalog {
        delay: Duration::ZERO,
    });
    for (sql, dialect) in [
        ("SELECT x FROM t", SqlDialect::Trino),
        ("SELECT x FROM t", SqlDialect::Trino),
        ("SELECT x FROM u", SqlDialect::Trino),
        ("SELECT x FROM t", SqlDialect::Postgres),
    ] {
        let ctx = svc
            .resolve_schema_context(sql, &dialect, &fast, None, None)
            .await;
        assert_eq!(ctx.is_empty(), sql.ends_with('u'), "{sql}");
    }
    assert_eq!(svc.table_refs_cache_stats(), stats(1, 3));
}
