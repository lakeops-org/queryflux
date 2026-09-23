use std::collections::HashMap;
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
    // `Mutex<HashMap>` that would serialize every concurrent access-control check. Keyed on
    // the full typed `CacheKey`, not a bare digest — a `DashMap<u64, _>` has no fallback
    // `Eq` check the way a keyed-by-value map does, so a hash collision between two
    // different requests would serve one request's cached row filters/masks/allow decision
    // to the other.
    cache: DashMap<CacheKey, CacheEntry>,
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
        let mut expired = false;
        let hit = self.cache.get(key).and_then(|entry| {
            if entry.stored.elapsed() < self.cache_ttl {
                Some(entry.decision.clone())
            } else {
                expired = true;
                None
            }
        });
        if expired {
            self.cache.remove(key);
        }
        hit
    }

    fn cache_put(&self, key: CacheKey, decision: AccessDecision) {
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

/// A resource's structured cache-key form — a delimiter-joined string (the previous
/// approach) can collide: `schema = "s|t", table = "u"` would hash the same as
/// `schema = "s", table = "t|u"`, and a named column `"a,b"` the same as columns `"a"` and
/// `"b"` joined. Keeping fields separate and typed avoids that regardless of what
/// characters an identifier or column name contains.
#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheResourceKey {
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
    /// `None` = "all columns" (`Columns::All`); `Some(cols)` is sorted.
    columns: Option<Vec<String>>,
}

/// Stable, exact cache key over everything that changes the decision — **not**
/// `context.query_id`. A typed, `Eq`-checked key (not a bare digest): `DashMap` only
/// short-circuits false *misses* via the hash, it still compares keys for a hit, so two
/// different requests whose fields happened to hash equally are correctly kept apart
/// instead of one serving the other's cached decision.
#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    user: String,
    /// Sorted — membership, not order, is what a policy can act on.
    groups: Vec<String>,
    roles: Vec<String>,
    /// `BTreeMap` iteration is already key-sorted; `Value` isn't `Eq`/`Hash`, so each is
    /// captured by its JSON text (same fidelity `to_string()` gave the old digest).
    attributes: Vec<(String, String)>,
    operation: String,
    resources: Vec<CacheResourceKey>,
    cluster_group: String,
    engine: String,
    session_params: Vec<(String, String)>,
}

fn cache_key(req: &AccessRequest) -> CacheKey {
    let mut groups = req.identity.groups.clone();
    groups.sort();
    let mut roles = req.identity.roles.clone();
    roles.sort();
    let attributes = req
        .identity
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    let mut resources: Vec<CacheResourceKey> = req
        .resources
        .iter()
        .map(|r| CacheResourceKey {
            catalog: r.catalog.clone(),
            schema: r.schema.clone(),
            table: r.table.clone(),
            columns: match &r.columns {
                Columns::All => None,
                Columns::Named(c) => {
                    let mut c = c.clone();
                    c.sort();
                    Some(c)
                }
            },
        })
        .collect();
    resources.sort_by(|a, b| {
        (&a.catalog, &a.schema, &a.table, &a.columns)
            .cmp(&(&b.catalog, &b.schema, &b.table, &b.columns))
    });

    let session_params = req
        .context
        .session_params
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    CacheKey {
        user: req.identity.user.clone(),
        groups,
        roles,
        attributes,
        operation: req.operation.0.clone(),
        resources,
        cluster_group: req.context.cluster_group.clone(),
        engine: req.context.engine.clone(),
        session_params,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use queryflux_core::access_model::{AccessResource, Columns, Identity, RequestContext};

    use super::*;
    use crate::provider::PolicyError;

    /// Always allows (so the decision is cache-eligible) and counts how many times the
    /// provider was actually called — a cache hit must not increment it, and two requests
    /// that collide under a hash-only key incorrectly would look like just one call too.
    #[derive(Default)]
    struct CountingProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl PolicyDecisionProvider for CountingProvider {
        async fn evaluate(&self, _req: &AccessRequest) -> Result<AccessDecision, PolicyError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(AccessDecision::allow_all())
        }
        fn name(&self) -> &'static str {
            "counting"
        }
    }

    fn controller(provider: Arc<CountingProvider>) -> AccessController {
        AccessController::new(AccessControllerConfig {
            provider,
            metrics: Arc::new(NoopMetrics),
            operations: vec![Operation::table_select()],
            fail_open_default: false,
            group_fail_open: HashMap::new(),
            cache_ttl: Duration::from_secs(60),
            cache_capacity: 10_000,
        })
    }

    fn base_request() -> AccessRequest {
        AccessRequest {
            identity: Identity {
                user: "alice".to_string(),
                groups: vec![],
                roles: vec![],
                attributes: Default::default(),
            },
            operation: Operation::table_select(),
            resources: vec![AccessResource {
                catalog: None,
                schema: None,
                table: "orders".to_string(),
                columns: Columns::All,
            }],
            context: RequestContext {
                cluster_group: "default".to_string(),
                engine: "trino".to_string(),
                query_id: "q1".to_string(),
                session_params: Default::default(),
            },
        }
    }

    #[tokio::test]
    async fn identical_requests_share_one_cache_entry() {
        let provider = Arc::new(CountingProvider::default());
        let ctl = controller(provider.clone());
        let mut req = base_request();
        ctl.evaluate(&req).await;
        req.context.query_id = "q2".to_string(); // excluded from the key on purpose
        ctl.evaluate(&req).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    /// Regression: `hash_sorted`'s old digest fed each group/role string into the hasher
    /// with no boundary between the two lists, so groups=["a","b"] roles=[] produced the
    /// identical hash input as groups=["a"] roles=["b"]. A typed key must tell them apart.
    #[tokio::test]
    async fn group_role_boundary_does_not_collide() {
        let provider = Arc::new(CountingProvider::default());
        let ctl = controller(provider.clone());

        let mut a = base_request();
        a.identity.groups = vec!["a".to_string(), "b".to_string()];
        a.identity.roles = vec![];
        ctl.evaluate(&a).await;

        let mut b = base_request();
        b.identity.groups = vec!["a".to_string()];
        b.identity.roles = vec!["b".to_string()];
        ctl.evaluate(&b).await;

        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "distinct group/role split must not share a[b's] cache entry"
        );
    }

    /// Regression: joining `catalog|schema|table` with `|` let a `|`-containing schema
    /// name collide with a differently-split qualified name across the same delimiter.
    #[tokio::test]
    async fn resource_identifiers_containing_the_old_delimiter_do_not_collide() {
        let provider = Arc::new(CountingProvider::default());
        let ctl = controller(provider.clone());

        let mut a = base_request();
        a.resources = vec![AccessResource {
            catalog: None,
            schema: Some("s|t".to_string()),
            table: "u".to_string(),
            columns: Columns::All,
        }];
        ctl.evaluate(&a).await;

        let mut b = base_request();
        b.resources = vec![AccessResource {
            catalog: None,
            schema: Some("s".to_string()),
            table: "t|u".to_string(),
            columns: Columns::All,
        }];
        ctl.evaluate(&b).await;

        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "a `|` inside an identifier must not fake a different schema/table split"
        );
    }

    /// Regression: joining named columns with `,` let a single column called `"a,b"`
    /// collide with the two columns `"a"` and `"b"`.
    #[tokio::test]
    async fn column_name_containing_the_old_delimiter_does_not_collide_with_split_columns() {
        let provider = Arc::new(CountingProvider::default());
        let ctl = controller(provider.clone());

        let mut a = base_request();
        a.resources[0].columns = Columns::Named(vec!["a,b".to_string()]);
        ctl.evaluate(&a).await;

        let mut b = base_request();
        b.resources[0].columns = Columns::Named(vec!["a".to_string(), "b".to_string()]);
        ctl.evaluate(&b).await;

        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "a `,` inside a column name must not fake a second, split column"
        );
    }

    /// Column order within a request must not matter — `["b", "a"]` and `["a", "b"]` name
    /// the same set of masked/visible columns and must share a cache entry.
    #[tokio::test]
    async fn column_order_does_not_affect_the_key() {
        let provider = Arc::new(CountingProvider::default());
        let ctl = controller(provider.clone());

        let mut a = base_request();
        a.resources[0].columns = Columns::Named(vec!["b".to_string(), "a".to_string()]);
        ctl.evaluate(&a).await;

        let mut b = base_request();
        b.resources[0].columns = Columns::Named(vec!["a".to_string(), "b".to_string()]);
        ctl.evaluate(&b).await;

        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
}
