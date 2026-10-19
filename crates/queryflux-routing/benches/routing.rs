//! Router chain evaluation runs once per query before dispatch. The chain below mirrors a
//! typical config: header override, regex deny + route rules, then tags, then fallback.
//! `fallthrough` is the worst case — every router is evaluated and none matches.

use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;

use queryflux_core::config::{QueryRegexRule, RegexRouteAction, TagRoutingRule};
use queryflux_core::query::{ClusterGroupName, FrontendProtocol};
use queryflux_core::session::SessionContext;
use queryflux_routing::chain::RouterChain;
use queryflux_routing::implementations::header::HeaderRouter;
use queryflux_routing::implementations::query_regex::QueryRegexRouter;
use queryflux_routing::implementations::tags::TagsRouter;
use queryflux_routing::RouterTrait;

#[path = "../../../benches/sql_corpus.rs"]
mod sql_corpus;

fn chain() -> RouterChain {
    let header = HeaderRouter::new(
        "X-Qf-Group".into(),
        HashMap::from([("adhoc".into(), ClusterGroupName("adhoc".into()))]),
    );
    let regex = QueryRegexRouter::from_rules(vec![
        QueryRegexRule {
            regex: r"(?i)^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE)\b".into(),
            target_group: None,
            action: RegexRouteAction::Deny,
            error: Some("writes are not permitted".into()),
        },
        QueryRegexRule {
            regex: r"(?i)\bfrom\s+prod\.".into(),
            target_group: Some("heavy".into()),
            action: RegexRouteAction::Route,
            error: None,
        },
        QueryRegexRule {
            regex: r"(?i)\bsystem\.runtime\b".into(),
            target_group: Some("admin".into()),
            action: RegexRouteAction::Route,
            error: None,
        },
    ]);
    let tags = TagsRouter::new(vec![TagRoutingRule {
        tags: HashMap::from([("team".into(), Some("finance".into()))]),
        target_group: "finance".into(),
    }]);
    let routers: Vec<Box<dyn RouterTrait>> =
        vec![Box::new(header), Box::new(regex), Box::new(tags)];
    RouterChain::new(routers, ClusterGroupName("default".into()))
}

fn bench_route(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let chain = chain();
    let session = SessionContext::default();
    let protocol = FrontendProtocol::TrinoHttp;

    let mut group = c.benchmark_group("routing/chain_fallthrough");
    for (name, sql) in sql_corpus::ALL {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.to_async(&rt).iter(|| async {
                chain
                    .route(black_box(sql), &session, &protocol, None)
                    .await
                    .unwrap()
            })
        });
    }
    group.finish();

    c.bench_function("routing/chain_trace_fallthrough/analytical", |b| {
        b.to_async(&rt).iter(|| async {
            chain
                .route_with_trace(black_box(sql_corpus::ANALYTICAL), &session, &protocol, None)
                .await
                .unwrap()
        })
    });
}

criterion_group!(benches, bench_route);
criterion_main!(benches);
