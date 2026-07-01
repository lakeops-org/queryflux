use async_trait::async_trait;
use prometheus::{CounterVec, Encoder, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use queryflux_core::error::Result;

use crate::{ClusterSnapshot, MetricsStore, QueryRecord};

/// Prometheus-backed metrics store.
///
/// Tracks real-time operational metrics exposed at `/metrics` for Prometheus scraping.
/// Use alongside `PostgresMetricsStore` (or `NoopMetricsStore`) for historical storage.
pub struct PrometheusMetrics {
    registry: Registry,
    /// queryflux_queries_total{engine_type, cluster_group, status, protocol}
    queries_total: CounterVec,
    /// queryflux_query_duration_seconds{engine_type, cluster_group}
    query_duration_seconds: HistogramVec,
    /// queryflux_translated_queries_total{src_dialect, tgt_dialect}
    translated_total: CounterVec,
    /// queryflux_running_queries{cluster_group, cluster_name}
    running_queries: prometheus::GaugeVec,
    /// queryflux_queued_queries{cluster_group}
    queued_queries: prometheus::GaugeVec,
    /// queryflux_query_tags_total{tag_key, tag_value, cluster_group}
    ///
    /// Incremented once per tag per completed query. Allows tag-based aggregation
    /// and filtering in Prometheus/Grafana. Tags in `tags_deny_list` are not emitted.
    query_tags_total: CounterVec,
    /// queryflux_coordination_failures_total{operation}
    ///
    /// Failures of distributed-coordination operations (capacity leases, queue
    /// claims) against the persistence backend. Each failure means the replica
    /// fell back to local-only behavior, so global limits were not enforced for
    /// that operation — alert on a sustained rate.
    coordination_failures_total: CounterVec,
    /// queryflux_capacity_degraded_total{cluster_group, cluster_name}
    ///
    /// Queries admitted in degraded mode — global capacity lease unavailable,
    /// replica fell back to local limits. Non-zero means global max_running_queries
    /// was not enforced for those admits. Pair with coordination_failures_total.
    capacity_degraded_total: CounterVec,
    /// queryflux_auth_failures_total{protocol}
    auth_failures_total: CounterVec,
    /// queryflux_queue_rejections_total{cluster_group}
    queue_rejections_total: CounterVec,
    /// queryflux_queue_duration_seconds{cluster_group}
    queue_duration_seconds: HistogramVec,
    /// queryflux_cache_hits_total{cluster_group}
    cache_hits_total: CounterVec,
    /// queryflux_cache_misses_total{cluster_group}
    cache_misses_total: CounterVec,
    /// queryflux_cache_writes_total{cluster_group}
    cache_writes_total: CounterVec,
    /// Tag keys that are excluded from `query_tags_total` to control cardinality.
    tags_deny_list: Vec<String>,
}

impl PrometheusMetrics {
    pub fn new() -> std::result::Result<Self, prometheus::Error> {
        Self::new_with_deny_list(vec![])
    }

    pub fn new_with_deny_list(
        tags_deny_list: Vec<String>,
    ) -> std::result::Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let queries_total = CounterVec::new(
            Opts::new("queryflux_queries_total", "Total completed queries"),
            &["engine_type", "cluster_group", "status", "protocol"],
        )?;

        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "queryflux_query_duration_seconds",
                "Query execution duration in seconds",
            )
            .buckets(vec![
                0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0,
            ]),
            &["engine_type", "cluster_group"],
        )?;

        let translated_total = CounterVec::new(
            Opts::new(
                "queryflux_translated_queries_total",
                "Total queries that required SQL dialect translation",
            ),
            &["src_dialect", "tgt_dialect"],
        )?;

        let running_queries = prometheus::GaugeVec::new(
            Opts::new(
                "queryflux_running_queries",
                "Current number of queries executing on each cluster",
            ),
            &["cluster_group", "cluster_name"],
        )?;

        let queued_queries = prometheus::GaugeVec::new(
            Opts::new(
                "queryflux_queued_queries",
                "Current number of queries queued waiting for cluster capacity",
            ),
            &["cluster_group"],
        )?;

        let query_tags_total = CounterVec::new(
            Opts::new(
                "queryflux_query_tags_total",
                "Total queries per tag key/value combination. Useful for cost attribution and workload analysis.",
            ),
            &["tag_key", "tag_value", "cluster_group"],
        )?;

        let coordination_failures_total = CounterVec::new(
            Opts::new(
                "queryflux_coordination_failures_total",
                "Distributed-coordination operations that failed and fell back to local-only behavior",
            ),
            &["operation"],
        )?;

        let capacity_degraded_total = CounterVec::new(
            Opts::new(
                "queryflux_capacity_degraded_total",
                "Queries admitted in degraded mode (global capacity lease unavailable, local limits used)",
            ),
            &["cluster_group", "cluster_name"],
        )?;

        let auth_failures_total = CounterVec::new(
            Opts::new(
                "queryflux_auth_failures_total",
                "Authentication failures (wrong password, expired token, etc.)",
            ),
            &["protocol"],
        )?;

        let queue_rejections_total = CounterVec::new(
            Opts::new(
                "queryflux_queue_rejections_total",
                "Queries rejected because maxQueuedQueries limit was reached",
            ),
            &["cluster_group"],
        )?;

        let queue_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "queryflux_queue_duration_seconds",
                "Time queries spent waiting in the proxy queue before dispatch",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0]),
            &["cluster_group"],
        )?;

        let cache_hits_total = CounterVec::new(
            Opts::new("queryflux_cache_hits_total", "Query result cache hits"),
            &["cluster_group"],
        )?;

        let cache_misses_total = CounterVec::new(
            Opts::new("queryflux_cache_misses_total", "Query result cache misses"),
            &["cluster_group"],
        )?;

        let cache_writes_total = CounterVec::new(
            Opts::new(
                "queryflux_cache_writes_total",
                "Query results written to cache",
            ),
            &["cluster_group"],
        )?;

        registry.register(Box::new(queries_total.clone()))?;
        registry.register(Box::new(query_duration_seconds.clone()))?;
        registry.register(Box::new(translated_total.clone()))?;
        registry.register(Box::new(running_queries.clone()))?;
        registry.register(Box::new(queued_queries.clone()))?;
        registry.register(Box::new(query_tags_total.clone()))?;
        registry.register(Box::new(coordination_failures_total.clone()))?;
        registry.register(Box::new(capacity_degraded_total.clone()))?;
        registry.register(Box::new(auth_failures_total.clone()))?;
        registry.register(Box::new(queue_rejections_total.clone()))?;
        registry.register(Box::new(queue_duration_seconds.clone()))?;
        registry.register(Box::new(cache_hits_total.clone()))?;
        registry.register(Box::new(cache_misses_total.clone()))?;
        registry.register(Box::new(cache_writes_total.clone()))?;

        Ok(Self {
            registry,
            queries_total,
            query_duration_seconds,
            translated_total,
            running_queries,
            queued_queries,
            query_tags_total,
            coordination_failures_total,
            capacity_degraded_total,
            auth_failures_total,
            queue_rejections_total,
            queue_duration_seconds,
            cache_hits_total,
            cache_misses_total,
            cache_writes_total,
            tags_deny_list,
        })
    }

    /// Render all metrics in Prometheus text exposition format.
    /// Returns the text to serve at the `/metrics` endpoint.
    pub fn gather_text(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .unwrap_or_default();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to create PrometheusMetrics")
    }
}

#[async_trait]
impl MetricsStore for PrometheusMetrics {
    fn on_query_started(&self, group: &str, cluster: &str) {
        self.running_queries
            .with_label_values(&[group, cluster])
            .inc();
    }

    fn on_query_finished(&self, group: &str, cluster: &str) {
        let g = self.running_queries.with_label_values(&[group, cluster]);
        // Guard against going negative if called without a matching start.
        if g.get() > 0.0 {
            g.dec();
        }
    }

    fn on_coordination_failure(&self, operation: &str) {
        self.coordination_failures_total
            .with_label_values(&[operation])
            .inc();
    }

    fn on_capacity_degraded(&self, cluster_group: &str, cluster_name: &str) {
        self.capacity_degraded_total
            .with_label_values(&[cluster_group, cluster_name])
            .inc();
    }

    fn on_auth_failure(&self, protocol: &str) {
        self.auth_failures_total
            .with_label_values(&[protocol])
            .inc();
    }

    fn on_queue_full(&self, cluster_group: &str) {
        self.queue_rejections_total
            .with_label_values(&[cluster_group])
            .inc();
    }

    fn on_cache_hit(&self, cluster_group: &str) {
        self.cache_hits_total
            .with_label_values(&[cluster_group])
            .inc();
    }

    fn on_cache_miss(&self, cluster_group: &str) {
        self.cache_misses_total
            .with_label_values(&[cluster_group])
            .inc();
    }

    fn on_cache_write(&self, cluster_group: &str) {
        self.cache_writes_total
            .with_label_values(&[cluster_group])
            .inc();
    }

    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        let engine = format!("{:?}", record.engine_type);
        let group = record.cluster_group.0.as_str().to_string();
        let status = format!("{:?}", record.status);
        let protocol = format!("{:?}", record.frontend_protocol);

        self.queries_total
            .with_label_values(&[&engine, &group, &status, &protocol])
            .inc();

        self.query_duration_seconds
            .with_label_values(&[&engine, &group])
            .observe(record.execution_duration_ms as f64 / 1000.0);

        if record.queue_duration_ms > 0 {
            self.queue_duration_seconds
                .with_label_values(&[&group])
                .observe(record.queue_duration_ms as f64 / 1000.0);
        }

        if record.was_translated {
            let src = format!("{:?}", record.source_dialect);
            let tgt = format!("{:?}", record.target_dialect);
            self.translated_total.with_label_values(&[&src, &tgt]).inc();
        }

        // Emit one counter increment per tag, filtered through the deny list.
        for (key, val) in &record.query_tags {
            if self.tags_deny_list.iter().any(|d| d == key) {
                continue;
            }
            let tag_value = val.as_deref().unwrap_or("");
            self.query_tags_total
                .with_label_values(&[key, tag_value, &group])
                .inc();
        }

        Ok(())
    }

    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()> {
        let group = snapshot.group_name.0.as_str().to_string();
        let cluster = snapshot.cluster_name.0.as_str().to_string();

        self.running_queries
            .with_label_values(&[&group, &cluster])
            .set(snapshot.running_queries as f64);

        self.queued_queries
            .with_label_values(&[&group])
            .set(snapshot.queued_queries as f64);

        Ok(())
    }
}
