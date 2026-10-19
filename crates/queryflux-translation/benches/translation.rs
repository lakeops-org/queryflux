//! Dialect translation (sqlglot via PyO3) is usually the most expensive step QueryFlux adds
//! to a query that needs it. Benches the common cross-engine pairs over the shared corpus.
//!
//! Requires a PyO3 interpreter with `sqlglot` importable (same as the translation tests:
//! `make setup`, or set `PYO3_PYTHON` / `PYTHONPATH` to a venv with `requirements.txt`).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use queryflux_core::query::SqlDialect;
use queryflux_translation::{SchemaContext, SqlglotTranslator, TranslatorTrait};

#[path = "../../../benches/sql_corpus.rs"]
mod sql_corpus;

fn bench_translate(c: &mut Criterion) {
    SqlglotTranslator::check_available().expect(
        "sqlglot not importable — set PYO3_PYTHON to a venv with `pip install -r requirements.txt`",
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let schema = SchemaContext::default();

    let pairs = [
        (
            "trino_to_starrocks",
            SqlDialect::Trino,
            SqlDialect::StarRocks,
        ),
        ("trino_to_duckdb", SqlDialect::Trino, SqlDialect::DuckDb),
        ("mysql_to_trino", SqlDialect::MySql, SqlDialect::Trino),
    ];
    for (pair, src, tgt) in pairs {
        let translator = SqlglotTranslator::new(src, tgt, vec![]);
        let mut group = c.benchmark_group(format!("translation/{pair}"));
        for (name, sql) in sql_corpus::ALL {
            group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
                b.to_async(&rt)
                    .iter(|| async { translator.translate(black_box(sql), &schema).await.unwrap() })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_translate);
criterion_main!(benches);
