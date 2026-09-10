use std::time::Duration;

/// Sink for access-control decision metrics. `queryflux-frontend` provides an impl backed
/// by the running `MetricsStore`; tests and the no-config path use [`NoopMetrics`].
pub trait AccessMetricsSink: Send + Sync {
    /// Called once per `AccessController::evaluate` that actually hit (or would have hit)
    /// the provider — i.e. not on a cache hit.
    fn record_decision(&self, outcome: DecisionMetric);
}

#[derive(Debug, Clone, Copy)]
pub struct DecisionMetric {
    pub latency: Duration,
    pub denied: bool,
    pub fail_open: bool,
    pub provider_error: bool,
    pub cache_hit: bool,
}

/// No-op sink.
pub struct NoopMetrics;

impl AccessMetricsSink for NoopMetrics {
    fn record_decision(&self, _outcome: DecisionMetric) {}
}
