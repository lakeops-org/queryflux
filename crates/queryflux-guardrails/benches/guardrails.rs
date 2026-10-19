//! Built-in guard chain cost per query. Like dispatch, each iteration builds a fresh
//! `SqlParseCache` shared by all guards — so this measures one polyglot parse plus every
//! guard's AST walk, i.e. what a query actually pays when these guards are configured.

use std::collections::{BTreeMap, HashMap};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use queryflux_core::query::{ClusterGroupName, EngineType};
use queryflux_core::sql_classify::SqlParseCache;
use queryflux_core::tags::QueryTags;
use queryflux_guardrails::built_in::{Guard, ReadOnlyGuard, RequirePredicateGuard, RowLimitGuard};
use queryflux_guardrails::{GuardChain, GuardContext, GuardLayer};

#[path = "../../../benches/sql_corpus.rs"]
mod sql_corpus;

fn bench_guard_chain(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let guards: Vec<Box<dyn Guard>> = vec![
        Box::new(ReadOnlyGuard),
        Box::new(RowLimitGuard {
            max_rows: Some(10_000),
        }),
        Box::new(RequirePredicateGuard { applies_to: vec![] }),
    ];
    let chain = GuardChain::new(guards);

    let engine_type = EngineType::Trino;
    let dialect = engine_type.dialect();
    let cluster_group = ClusterGroupName("default".into());
    let query_tags = QueryTags::new();
    let attributes = BTreeMap::new();
    let session_extra = HashMap::new();

    let mut group = c.benchmark_group("guardrails/built_in_chain");
    for (name, sql) in sql_corpus::ALL {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.to_async(&rt).iter(|| async {
                let parse = SqlParseCache::new(*sql, dialect.clone());
                let ctx = GuardContext {
                    sql: black_box(sql),
                    original_sql: None,
                    dialect: &dialect,
                    engine_type: &engine_type,
                    cluster_group: &cluster_group,
                    user: None,
                    groups: &[],
                    roles: &[],
                    attributes: &attributes,
                    agent_context: None,
                    query_tags: &query_tags,
                    session_extra: &session_extra,
                    schema: None,
                    sql_parse: Some(&parse),
                };
                chain.run(&ctx, GuardLayer::Plan).await
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_guard_chain);
criterion_main!(benches);
