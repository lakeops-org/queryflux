use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use queryflux_core::access_model::{AccessDecision, AccessRequest, Columns, Operation};

use crate::metrics::{AccessMetricsSink, DecisionMetric, NoopMetrics};
use crate::provider::PolicyDecisionProvider;

pub struct AccessControllerConfig {
    pub provider: Arc<dyn PolicyDecisionProvider>,
    pub metrics: Arc<dyn AccessMetricsSink>,
    /// Operations to evaluate.
    pub operations: Vec<Operation>,
    pub fail_open_default: bool,
    /// Per-cluster-group fail-open override.
    pub group_fail_open: HashMap<String, bool>,
    pub cache_ttl: Duration,
    pub cache_capacity: usize,
}

impl AccessControllerConfig {
    /// Convenience for tests / callers that don't wire metrics.
    pub fn new(provider: Arc<dyn PolicyDecisionProvider>) -> Self {
        Self {
            provider,
            metrics: Arc::new(NoopMetrics),
            operations: vec![Operation::table_select()],
            fail_open_default: false,
            group_fail_open: HashMap::new(),
            cache_ttl: Duration::from_secs(5),
            cache_capacity: 10_000,
        }
    }
}

struct CacheEntry {
    stored: Instant,
    decision: AccessDecision,
}

/// The single entry point dispatch calls. `evaluate` is **infallible** — a [`PolicyError`]
/// is folded into the returned [`AccessDecision`] per the effective fail-open setting.
pub struct AccessController {
    provider: Arc<dyn PolicyDecisionProvider>,
    metrics: Arc<dyn AccessMetricsSink>,
    operations: Vec<Operation>,
    fail_open_default: bool,
    group_fail_open: HashMap<String, bool>,
    cache_ttl: Duration,
    cache_capacity: usize,
    // Sharded (no single global lock) — this is on the per-query hot path, matching the
    // same DashMap-backed pattern already used elsewhere for per-request caches (e.g.
    // `snowflake::http::session_store`, `snowflake::in_flight`) rather than reinventing a
    // `Mutex<HashMap>` that would serialize every concurrent access-control check.
    cache: DashMap<u64, CacheEntry>,
}

impl AccessController {
    pub fn new(cfg: AccessControllerConfig) -> Self {
        Self {
            provider: cfg.provider,
            metrics: cfg.metrics,
            operations: cfg.operations,
            fail_open_default: cfg.fail_open_default,
            group_fail_open: cfg.group_fail_open,
            cache_ttl: cfg.cache_ttl,
            cache_capacity: cfg.cache_capacity,
            cache: DashMap::new(),
        }
    }

    pub fn provider_name(&self) -> &'static str {
        self.provider.name()
    }

    /// Whether `op` is configured for evaluation. When `false`, the stage is skipped.
    pub fn evaluates(&self, op: &Operation) -> bool {
        self.operations.iter().any(|o| o == op)
    }

    /// Effective fail-open for a cluster group (group override, else default).
    pub fn fail_open_for(&self, group: &str) -> bool {
        self.group_fail_open
            .get(group)
            .copied()
            .unwrap_or(self.fail_open_default)
    }

    /// Evaluate `req`. Infallible: a provider error becomes `deny_all` (or `allow_all` when
    /// fail-open is set for the request's cluster group). Consults + fills a short-TTL cache
    /// keyed on everything but `context.query_id`; caches allow decisions only.
    pub async fn evaluate(&self, req: &AccessRequest) -> AccessDecision {
        let key = cache_key(req);

        if !self.cache_ttl.is_zero() {
            if let Some(hit) = self.cache_get(key) {
                self.metrics.record_decision(DecisionMetric {
                    latency: Duration::ZERO,
                    denied: false,
                    fail_open: false,
                    provider_error: false,
                    cache_hit: true,
                });
                return hit;
            }
        }

        let started = Instant::now();
        let result = self.provider.evaluate(req).await;
        let latency = started.elapsed();

        match result {
            Ok(decision) => {
                let denied = !decision.is_allowed();
                if !denied && !self.cache_ttl.is_zero() {
                    self.cache_put(key, decision.clone());
                }
                self.metrics.record_decision(DecisionMetric {
                    latency,
                    denied,
                    fail_open: false,
                    provider_error: false,
                    cache_hit: false,
                });
                decision
            }
            Err(err) => {
                let fail_open = self.fail_open_for(&req.context.cluster_group);
                tracing::warn!(
                    provider = self.provider.name(),
                    group = %req.context.cluster_group,
                    fail_open,
                    "access-control provider error: {err}"
                );
                self.metrics.record_decision(DecisionMetric {
                    latency,
                    denied: !fail_open,
                    fail_open,
                    provider_error: true,
                    cache_hit: false,
                });
                if fail_open {
                    AccessDecision::allow_all()
                } else {
                    AccessDecision::deny_all(format!("policy engine unavailable: {err}"))
                }
            }
        }
    }

    fn cache_get(&self, key: u64) -> Option<AccessDecision> {
        let mut expired = false;
        let hit = self.cache.get(&key).and_then(|entry| {
            if entry.stored.elapsed() < self.cache_ttl {
                Some(entry.decision.clone())
            } else {
                expired = true;
                None
            }
        });
        if expired {
            self.cache.remove(&key);
        }
        hit
    }

    fn cache_put(&self, key: u64, decision: AccessDecision) {
        if self.cache.len() >= self.cache_capacity {
            // Crude bound: drop everything expired, then (if still full) clear.
            let ttl = self.cache_ttl;
            self.cache.retain(|_, e| e.stored.elapsed() < ttl);
            if self.cache.len() >= self.cache_capacity {
                self.cache.clear();
            }
        }
        self.cache.insert(
            key,
            CacheEntry {
                stored: Instant::now(),
                decision,
            },
        );
    }
}

/// Stable cache key over everything that changes the decision — **not** `context.query_id`.
fn cache_key(req: &AccessRequest) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    req.identity.user.hash(&mut h);
    hash_sorted(&mut h, req.identity.groups.iter().map(String::as_str));
    hash_sorted(&mut h, req.identity.roles.iter().map(String::as_str));
    for (k, v) in &req.identity.attributes {
        k.hash(&mut h);
        v.to_string().hash(&mut h);
    }
    req.operation.0.hash(&mut h);
    let mut resources: Vec<String> = req
        .resources
        .iter()
        .map(|r| {
            let cols = match &r.columns {
                Columns::All => "*".to_string(),
                Columns::Named(c) => {
                    let mut c = c.clone();
                    c.sort();
                    c.join(",")
                }
            };
            format!(
                "{}|{}|{}|{cols}",
                r.catalog.as_deref().unwrap_or(""),
                r.schema.as_deref().unwrap_or(""),
                r.table
            )
        })
        .collect();
    resources.sort();
    for r in resources {
        r.hash(&mut h);
    }
    req.context.cluster_group.hash(&mut h);
    req.context.engine.hash(&mut h);
    for (k, v) in &req.context.session_params {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    h.finish()
}

fn hash_sorted<'a>(h: &mut impl Hasher, items: impl Iterator<Item = &'a str>) {
    let mut v: Vec<&str> = items.collect();
    v.sort_unstable();
    for s in v {
        s.hash(h);
    }
}
