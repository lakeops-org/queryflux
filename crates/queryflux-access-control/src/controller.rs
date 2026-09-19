use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// Lossless, structured cache key. Fields are kept as typed values (not joined into a
/// delimited string) so quoted identifiers containing `,` / `|` can never make two distinct
/// requests compare equal, and the map compares full keys rather than a 64-bit digest.
#[derive(Hash, PartialEq, Eq)]
struct CacheKey {
    user: String,
    groups: Vec<String>,
    roles: Vec<String>,
    attributes: Vec<(String, String)>,
    operation: String,
    /// `(catalog, schema, table, columns)`; `columns == None` means [`Columns::All`].
    resources: Vec<ResourceKey>,
    cluster_group: String,
    engine: String,
    session_params: Vec<(String, String)>,
}

type ResourceKey = (Option<String>, Option<String>, String, Option<Vec<String>>);

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
    cache: Mutex<HashMap<CacheKey, CacheEntry>>,
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
            cache: Mutex::new(HashMap::new()),
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
            if let Some(hit) = self.cache_get(&key) {
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

    fn cache_get(&self, key: &CacheKey) -> Option<AccessDecision> {
        let mut guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.get(key) {
            if entry.stored.elapsed() < self.cache_ttl {
                return Some(entry.decision.clone());
            }
            guard.remove(key);
        }
        None
    }

    fn cache_put(&self, key: CacheKey, decision: AccessDecision) {
        let mut guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if guard.len() >= self.cache_capacity {
            // Crude bound: drop everything expired, then (if still full) clear.
            let ttl = self.cache_ttl;
            guard.retain(|_, e| e.stored.elapsed() < ttl);
            if guard.len() >= self.cache_capacity {
                guard.clear();
            }
        }
        guard.insert(
            key,
            CacheEntry {
                stored: Instant::now(),
                decision,
            },
        );
    }
}

/// Stable cache key over everything that changes the decision — **not** `context.query_id`.
fn cache_key(req: &AccessRequest) -> CacheKey {
    let mut resources: Vec<ResourceKey> = req
        .resources
        .iter()
        .map(|r| {
            let cols = match &r.columns {
                Columns::All => None,
                Columns::Named(c) => {
                    let mut c = c.clone();
                    c.sort();
                    Some(c)
                }
            };
            (r.catalog.clone(), r.schema.clone(), r.table.clone(), cols)
        })
        .collect();
    resources.sort();
    CacheKey {
        user: req.identity.user.clone(),
        groups: sorted(&req.identity.groups),
        roles: sorted(&req.identity.roles),
        attributes: req
            .identity
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect(),
        operation: req.operation.0.clone(),
        resources,
        cluster_group: req.context.cluster_group.clone(),
        engine: req.context.engine.clone(),
        session_params: req
            .context
            .session_params
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

fn sorted(items: &[String]) -> Vec<String> {
    let mut v = items.to_vec();
    v.sort_unstable();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::access_model::{AccessResource, Identity, RequestContext};

    fn req(table: &str, columns: Columns) -> AccessRequest {
        AccessRequest {
            identity: Identity {
                user: "alice".into(),
                ..Default::default()
            },
            operation: Operation::table_select(),
            resources: vec![AccessResource {
                catalog: None,
                schema: Some("s".into()),
                table: table.into(),
                columns,
            }],
            context: RequestContext::default(),
        }
    }

    #[test]
    fn cache_key_distinguishes_columns_containing_delimiters() {
        let joined = req("t", Columns::Named(vec!["a,b".into()]));
        let split = req("t", Columns::Named(vec!["a".into(), "b".into()]));
        assert!(cache_key(&joined) != cache_key(&split));
    }

    #[test]
    fn cache_key_distinguishes_table_containing_delimiters() {
        let a = req("x|y", Columns::All);
        let mut b = req("y", Columns::All);
        b.resources[0].schema = Some("s|x".into());
        assert!(cache_key(&a) != cache_key(&b));
    }

    #[test]
    fn cache_key_ignores_column_order_and_query_id() {
        let a = req("t", Columns::Named(vec!["b".into(), "a".into()]));
        let mut b = req("t", Columns::Named(vec!["a".into(), "b".into()]));
        b.context.query_id = "other".into();
        assert!(cache_key(&a) == cache_key(&b));
    }
}
