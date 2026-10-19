//! Fingerprinting runs on every query (cache keys, query history), so its cost is paid on
//! the hot path. `fast_hash` is the byte-level normalizer; `rich_fingerprint` parses with
//! polyglot-sql on the pooled big-stack thread.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use queryflux_fingerprint::{fast_hash, rich_fingerprint};

#[path = "../../../benches/sql_corpus.rs"]
mod sql_corpus;

fn bench_fast_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint/fast_hash");
    for (name, sql) in sql_corpus::ALL {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.iter(|| fast_hash(black_box(sql)))
        });
    }
    group.finish();
}

fn bench_rich_fingerprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint/rich");
    for (name, sql) in sql_corpus::ALL {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.iter(|| rich_fingerprint(black_box(sql), None, "trino", "trino"))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fast_hash, bench_rich_fingerprint);
criterion_main!(benches);
