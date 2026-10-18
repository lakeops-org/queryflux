//! Hit, miss and eviction counters of the in-process SQL caches (translation, table
//! extraction, fingerprints), read from `queryflux_core::bounded_cache::totals` at scrape
//! time so the caches need no reference to the registry.
use std::sync::Mutex;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounterVec, Opts, Registry};

struct SqlCacheCollector {
    hits: IntCounterVec,
    misses: IntCounterVec,
    evictions: IntCounterVec,
    /// Serializes scrapes so two of them cannot both add the same delta.
    sync: Mutex<()>,
}

fn counter(name: &str, help: &str) -> Result<IntCounterVec, prometheus::Error> {
    IntCounterVec::new(Opts::new(name, help), &["cache"])
}

/// Advance `vec{cache}` to `total`. The source counters only grow, so this stays monotonic.
fn advance(vec: &IntCounterVec, cache: &str, total: u64) {
    let c = vec.with_label_values(&[cache]);
    let current = c.get();
    if total > current {
        c.inc_by(total - current);
    }
}

impl Collector for SqlCacheCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut d = self.hits.desc();
        d.extend(self.misses.desc());
        d.extend(self.evictions.desc());
        d
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let _guard = self.sync.lock().unwrap_or_else(|e| e.into_inner());
        for (cache, stats) in queryflux_core::bounded_cache::totals() {
            advance(&self.hits, cache, stats.hits);
            advance(&self.misses, cache, stats.misses);
            advance(&self.evictions, cache, stats.evictions);
        }
        let mut families = self.hits.collect();
        families.extend(self.misses.collect());
        families.extend(self.evictions.collect());
        families
    }
}

pub(crate) fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(SqlCacheCollector {
        hits: counter(
            "queryflux_sql_cache_hits_total",
            "Lookups served from an in-process SQL cache, by cache",
        )?,
        misses: counter(
            "queryflux_sql_cache_misses_total",
            "Lookups an in-process SQL cache could not serve, by cache",
        )?,
        evictions: counter(
            "queryflux_sql_cache_evictions_total",
            "Entries dropped from an in-process SQL cache to make room, by cache",
        )?,
        sync: Mutex::new(()),
    }))
}
