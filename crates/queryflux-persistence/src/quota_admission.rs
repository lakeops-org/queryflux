//! The holder side of capacity quotas (see [`crate::quota`]).
//!
//! [`QuotaAdmission`] wraps the process's [`CapacityStore`]. While quotas are on, a query
//! is admitted against this process's quota for the cluster with an atomic counter, with
//! no I/O. When the quota is used up or has expired, it falls back to the wrapped store's
//! per-query lease, which is the shared reserve. A background task reports demand
//! (running and waiting queries per cluster) and applies the grants that come back.
//!
//! While quotas are off (the default, or a control plane that has none), every call goes
//! straight to the wrapped store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use queryflux_core::error::Result;
use tokio::sync::Notify;

use crate::quota::{DemandReport, QuotaStore};
use crate::{CapacityStore, MetricsStore};

/// A waiting query that hasn't retried for this long has given up or been admitted
/// elsewhere, and no longer counts as demand.
const WAITING_TTL: Duration = Duration::from_secs(5);
/// A cluster with no quota, no work and no waiters for this long stops being reported,
/// and its row expires on the allocator's side.
const IDLE_TTL: Duration = Duration::from_secs(60);
/// Report at most this often when queries start waiting.
const FAST_REPORT: Duration = Duration::from_millis(250);
/// While quotas are off, how often a worker asks its control plane whether it has them.
const PROBE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ClusterQuota {
    quota: AtomicU64,
    seq: AtomicU64,
    /// Milliseconds since `QuotaAdmission::epoch` until which `quota` may be used.
    valid_until_ms: AtomicU64,
    /// Queries running on quota.
    running: AtomicU64,
    max: AtomicU64,
    /// Queries denied capacity, by id, with when they last asked.
    waiting: Mutex<HashMap<String, Instant>>,
    last_active: Mutex<Option<Instant>>,
    /// Woken when a quota slot frees or the grant grows, for queries in `wait_for_grant`.
    freed: Notify,
}

pub struct QuotaAdmission {
    inner: Arc<dyn CapacityStore>,
    source: Arc<dyn QuotaStore>,
    instance_id: String,
    metrics: Arc<dyn MetricsStore>,
    enabled: AtomicBool,
    /// A worker doesn't configure quotas: it asks its control plane every
    /// [`PROBE_INTERVAL`] and turns them on when the control plane has them.
    probe: AtomicBool,
    report_interval_ms: AtomicU64,
    clusters: DashMap<String, Arc<ClusterQuota>>,
    /// Queries admitted on quota, and their cluster.
    on_quota: DashMap<String, String>,
    wake: Notify,
    epoch: Instant,
}

impl QuotaAdmission {
    /// Wrap `inner`, start the reporter (inside a Tokio runtime), quotas off.
    pub fn new(
        inner: Arc<dyn CapacityStore>,
        source: Arc<dyn QuotaStore>,
        instance_id: String,
        metrics: Arc<dyn MetricsStore>,
    ) -> Arc<Self> {
        let me = Arc::new(Self {
            inner,
            source,
            instance_id,
            metrics,
            enabled: AtomicBool::new(false),
            probe: AtomicBool::new(false),
            report_interval_ms: AtomicU64::new(1000),
            clusters: DashMap::new(),
            on_quota: DashMap::new(),
            wake: Notify::new(),
            epoch: Instant::now(),
        });
        tokio::spawn(reporter(Arc::downgrade(&me)));
        me
    }

    /// Turn quotas on or off, and set how often demand is reported. A worker calls this
    /// when it activates a snapshot; a replica at startup.
    pub fn set_policy(&self, enabled: bool, report_interval: Duration) {
        self.report_interval_ms.store(
            report_interval.as_millis().clamp(100, 60_000) as u64,
            Ordering::Relaxed,
        );
        let was = self.enabled.swap(enabled, Ordering::Relaxed);
        if enabled && !was {
            tracing::info!("Capacity quotas on: admitting within this process's share");
            self.wake.notify_one();
        } else if !enabled && was {
            tracing::info!("Capacity quotas off: every admission takes a per-query lease");
        }
    }

    /// Follow the coordination store instead of configuration: turn quotas on when it
    /// returns grants and off when it says it has none. Used on workers, so an older
    /// control plane, or one with quotas off, keeps them on per-query leases.
    pub fn follow_source(&self) {
        self.probe.store(true, Ordering::Relaxed);
        self.wake.notify_one();
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Ask the source whether it has quotas (an empty report) and turn them on if so.
    async fn probe_source(&self) {
        match self.source.report_demand(&self.instance_id, &[]).await {
            Ok(Some(resp)) => {
                self.set_policy(true, Duration::from_millis(resp.report_interval_ms));
            }
            Ok(None) => {}
            Err(e) => tracing::debug!("Capacity quota probe failed: {e}"),
        }
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn cluster(&self, name: &str) -> Arc<ClusterQuota> {
        if let Some(c) = self.clusters.get(name) {
            return c.clone();
        }
        self.clusters.entry(name.to_string()).or_default().clone()
    }

    /// Take a slot on quota if one is free and the quota is still valid.
    fn admit_on_quota(&self, cq: &ClusterQuota) -> bool {
        if cq.valid_until_ms.load(Ordering::Relaxed) <= self.now_ms() {
            return false;
        }
        let quota = cq.quota.load(Ordering::Relaxed);
        cq.running
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |r| {
                (r < quota).then_some(r + 1)
            })
            .is_ok()
    }

    /// Current state for one cluster, for `/worker/status` and tests.
    pub fn snapshot(&self, cluster: &str) -> Option<(u64, u64, usize)> {
        self.clusters.get(cluster).map(|c| {
            (
                c.quota.load(Ordering::Relaxed),
                c.running.load(Ordering::Relaxed),
                c.waiting.lock().map(|w| w.len()).unwrap_or(0),
            )
        })
    }

    /// Send one demand report now and apply the grants. Returns false when the source
    /// has no quotas.
    pub async fn report_now(&self) -> bool {
        let now = Instant::now();
        let mut reports = Vec::new();
        let mut idle = Vec::new();
        for entry in self.clusters.iter() {
            let cq = entry.value();
            let waiting = {
                let mut w = cq.waiting.lock().unwrap_or_else(|e| e.into_inner());
                w.retain(|_, at| now.duration_since(*at) < WAITING_TTL);
                w.len() as u64
            };
            let running = cq.running.load(Ordering::Relaxed);
            let quota = cq.quota.load(Ordering::Relaxed);
            let mut last = cq.last_active.lock().unwrap_or_else(|e| e.into_inner());
            if running > 0 || waiting > 0 || quota > 0 {
                *last = Some(now);
            }
            if last.is_none_or(|t| now.duration_since(t) > IDLE_TTL) {
                idle.push(entry.key().clone());
                continue;
            }
            self.metrics
                .on_capacity_quota(entry.key(), quota, running, waiting);
            reports.push(DemandReport {
                cluster: entry.key().clone(),
                running,
                waiting,
                seen_seq: cq.seq.load(Ordering::Relaxed),
                max_running_queries: cq.max.load(Ordering::Relaxed),
            });
        }
        for name in idle {
            self.clusters
                .remove_if(&name, |_, c| c.running.load(Ordering::Relaxed) == 0);
        }
        if reports.is_empty() {
            return true;
        }
        let sent_ms = self.now_ms();
        match self.source.report_demand(&self.instance_id, &reports).await {
            Ok(Some(resp)) => {
                if self.probe.load(Ordering::Relaxed) {
                    self.report_interval_ms.store(
                        resp.report_interval_ms.clamp(100, 60_000),
                        Ordering::Relaxed,
                    );
                }
                let until = sent_ms + resp.lease_secs.saturating_mul(1000);
                let mut granted: HashMap<&str, (u64, u64)> = HashMap::new();
                for g in &resp.grants {
                    granted.insert(g.cluster.as_str(), (g.quota, g.seq));
                }
                for r in &reports {
                    let (quota, seq) = granted.get(r.cluster.as_str()).copied().unwrap_or((0, 0));
                    let cq = self.cluster(&r.cluster);
                    if quota > cq.quota.load(Ordering::Relaxed) {
                        cq.freed.notify_waiters();
                    }
                    cq.quota.store(quota, Ordering::Relaxed);
                    cq.seq.store(seq, Ordering::Relaxed);
                    cq.valid_until_ms.store(until, Ordering::Relaxed);
                }
                true
            }
            Ok(None) => {
                if self.enabled.swap(false, Ordering::Relaxed) {
                    tracing::warn!(
                        "Quotas are not available from the coordination store; \
                         using per-query leases"
                    );
                }
                // Nothing granted is valid any more.
                for entry in self.clusters.iter() {
                    entry.value().valid_until_ms.store(0, Ordering::Relaxed);
                }
                false
            }
            Err(e) => {
                self.metrics
                    .on_coordination_failure("capacity_quota_report");
                tracing::warn!("Capacity demand report failed: {e}");
                true
            }
        }
    }
}

async fn reporter(me: std::sync::Weak<QuotaAdmission>) {
    loop {
        let (interval, enabled) = {
            let Some(me) = me.upgrade() else { return };
            if !me.enabled() && me.probe.load(Ordering::Relaxed) {
                me.probe_source().await;
            }
            let waiting = me
                .clusters
                .iter()
                .any(|c| c.waiting.lock().map(|w| !w.is_empty()).unwrap_or(false));
            let base = Duration::from_millis(me.report_interval_ms.load(Ordering::Relaxed));
            let interval = match (me.enabled(), waiting) {
                (false, _) => PROBE_INTERVAL,
                (true, true) => base.min(FAST_REPORT),
                (true, false) => base,
            };
            (interval, me.enabled())
        };
        let wake = async {
            match me.upgrade() {
                Some(me) => me.wake.notified().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = wake => {
                // A burst of new waiters arrives together: one report for all of them.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let Some(me) = me.upgrade() else { return };
        if enabled && me.enabled() {
            me.report_now().await;
        }
    }
}

#[async_trait]
impl CapacityStore for QuotaAdmission {
    async fn try_acquire(
        &self,
        cluster_name: &str,
        max_running_queries: u64,
        instance_id: &str,
        query_id: &str,
    ) -> Result<bool> {
        let quotas = self.enabled() && max_running_queries != u64::MAX;
        let cq = quotas.then(|| self.cluster(cluster_name));
        if let Some(cq) = &cq {
            cq.max.store(max_running_queries, Ordering::Relaxed);
            *cq.last_active.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
            if self.admit_on_quota(cq) {
                self.on_quota
                    .insert(query_id.to_string(), cluster_name.to_string());
                let was_waiting = cq
                    .waiting
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(query_id)
                    .is_some();
                if was_waiting {
                    // It may still be in the reserve's line, or have been handed a lease
                    // there meanwhile: give both back.
                    if let Err(e) = self.inner.release(cluster_name, query_id).await {
                        tracing::debug!("Clearing a reserve wait for {query_id}: {e}");
                    }
                }
                return Ok(true);
            }
        }
        // No quota left: the reserve, one per-query lease.
        let granted = self
            .inner
            .try_acquire(cluster_name, max_running_queries, instance_id, query_id)
            .await;
        if let Some(cq) = &cq {
            let mut waiting = cq.waiting.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(granted, Ok(true)) {
                waiting.remove(query_id);
            } else if waiting
                .insert(query_id.to_string(), Instant::now())
                .is_none()
            {
                // A new waiter: tell the allocator now rather than at the next tick.
                self.wake.notify_one();
            }
        }
        granted
    }

    async fn release(&self, cluster_name: &str, query_id: &str) -> Result<()> {
        if let Some((_, cluster)) = self.on_quota.remove(query_id) {
            if let Some(cq) = self.clusters.get(&cluster) {
                let _ = cq
                    .running
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |r| {
                        Some(r.saturating_sub(1))
                    });
                cq.freed.notify_one();
            }
            return Ok(());
        }
        self.inner.release(cluster_name, query_id).await
    }

    async fn release_many(&self, leases: &[(String, String)]) -> Result<()> {
        let mut rest = Vec::with_capacity(leases.len());
        for (cluster, query_id) in leases {
            if self.on_quota.contains_key(query_id) {
                self.release(cluster, query_id).await?;
            } else {
                rest.push((cluster.clone(), query_id.clone()));
            }
        }
        self.inner.release_many(&rest).await
    }

    /// Wake on whichever comes first: the reserve handing this query a slot, or a quota
    /// slot freeing up on this process.
    async fn wait_for_grant(
        &self,
        cluster_name: &str,
        query_id: &str,
        timeout: Duration,
    ) -> Result<bool> {
        if !self.enabled() {
            return self
                .inner
                .wait_for_grant(cluster_name, query_id, timeout)
                .await;
        }
        let cq = self.cluster(cluster_name);
        let freed = cq.freed.notified();
        tokio::pin!(freed);
        freed.as_mut().enable();
        if cq.valid_until_ms.load(Ordering::Relaxed) > self.now_ms()
            && cq.running.load(Ordering::Relaxed) < cq.quota.load(Ordering::Relaxed)
        {
            return Ok(true);
        }
        tokio::select! {
            r = self.inner.wait_for_grant(cluster_name, query_id, timeout) => r,
            _ = &mut freed => Ok(true),
        }
    }

    async fn heartbeat(&self, instance_id: &str) -> Result<u64> {
        self.inner.heartbeat(instance_id).await
    }

    async fn expire_stale(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        self.inner.expire_stale(cutoff).await
    }

    async fn active_count(&self, cluster_name: &str) -> Result<u64> {
        self.inner.active_count(cluster_name).await
    }

    async fn publish_running_count(&self, cluster_name: &str, count: u64) -> Result<()> {
        self.inner.publish_running_count(cluster_name, count).await
    }

    async fn active_lease_count(&self, cluster_name: &str) -> Result<u64> {
        self.inner.active_lease_count(cluster_name).await
    }

    /// Shutdown: free this process's leases and its quota rows.
    async fn release_all_for_instance(&self, instance_id: &str) -> Result<u64> {
        self.enabled.store(false, Ordering::Relaxed);
        let leases = self.inner.release_all_for_instance(instance_id).await?;
        if let Err(e) = self.source.release_holder(instance_id).await {
            tracing::warn!("Failed to drop this process's capacity quotas: {e}");
        }
        Ok(leases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::{QuotaGrant, QuotaResponse};
    use std::sync::atomic::AtomicUsize;

    /// Grants whatever quota is set, and records reports.
    #[derive(Default)]
    struct FixedQuota {
        quota: AtomicU64,
        lease_secs: AtomicU64,
        reports: Mutex<Vec<Vec<DemandReport>>>,
        unsupported: AtomicBool,
        released: AtomicUsize,
    }

    #[async_trait]
    impl QuotaStore for FixedQuota {
        async fn report_demand(
            &self,
            _: &str,
            reports: &[DemandReport],
        ) -> Result<Option<QuotaResponse>> {
            if self.unsupported.load(Ordering::Relaxed) {
                return Ok(None);
            }
            self.reports.lock().unwrap().push(reports.to_vec());
            Ok(Some(QuotaResponse {
                grants: reports
                    .iter()
                    .map(|r| QuotaGrant {
                        cluster: r.cluster.clone(),
                        quota: self.quota.load(Ordering::Relaxed),
                        seq: 1,
                    })
                    .collect(),
                lease_secs: self.lease_secs.load(Ordering::Relaxed),
                report_interval_ms: 500,
            }))
        }
        async fn release_holder(&self, _: &str) -> Result<u64> {
            self.released.fetch_add(1, Ordering::Relaxed);
            Ok(1)
        }
    }

    /// Per-query leases with a limit, counting calls.
    #[derive(Default)]
    struct Reserve {
        limit: u64,
        held: Mutex<Vec<String>>,
        acquires: AtomicUsize,
    }

    #[async_trait]
    impl CapacityStore for Reserve {
        async fn try_acquire(&self, _: &str, _: u64, _: &str, q: &str) -> Result<bool> {
            self.acquires.fetch_add(1, Ordering::Relaxed);
            let mut held = self.held.lock().unwrap();
            if held.len() as u64 >= self.limit {
                return Ok(false);
            }
            held.push(q.to_string());
            Ok(true)
        }
        async fn release(&self, _: &str, q: &str) -> Result<()> {
            self.held.lock().unwrap().retain(|h| h != q);
            Ok(())
        }
        async fn heartbeat(&self, _: &str) -> Result<u64> {
            Ok(0)
        }
        async fn expire_stale(&self, _: DateTime<Utc>) -> Result<u64> {
            Ok(0)
        }
        async fn active_count(&self, _: &str) -> Result<u64> {
            Ok(0)
        }
        async fn publish_running_count(&self, _: &str, _: u64) -> Result<()> {
            Ok(())
        }
        async fn active_lease_count(&self, _: &str) -> Result<u64> {
            Ok(self.held.lock().unwrap().len() as u64)
        }
        async fn release_all_for_instance(&self, _: &str) -> Result<u64> {
            Ok(0)
        }
    }

    fn setup(quota: u64, reserve: u64) -> (Arc<QuotaAdmission>, Arc<FixedQuota>, Arc<Reserve>) {
        let src = Arc::new(FixedQuota {
            quota: AtomicU64::new(quota),
            lease_secs: AtomicU64::new(15),
            ..Default::default()
        });
        let res = Arc::new(Reserve {
            limit: reserve,
            ..Default::default()
        });
        let qa = QuotaAdmission::new(
            res.clone(),
            src.clone(),
            "inst".into(),
            Arc::new(crate::in_memory::InMemoryPersistence::new()),
        );
        // A long interval: tests drive reports with report_now.
        qa.set_policy(true, Duration::from_secs(60));
        (qa, src, res)
    }

    #[tokio::test]
    async fn admits_on_quota_without_touching_the_reserve() {
        let (qa, _src, res) = setup(3, 0);
        // First call: no quota yet, the reserve (limit 0) denies; the cluster is now known.
        assert!(!qa.try_acquire("c", 10, "inst", "q0").await.unwrap());
        assert!(qa.report_now().await);
        for i in 1..=3 {
            assert!(qa
                .try_acquire("c", 10, "inst", &format!("q{i}"))
                .await
                .unwrap());
        }
        assert_eq!(
            res.acquires.load(Ordering::Relaxed),
            1,
            "only the first call"
        );
        assert!(
            !qa.try_acquire("c", 10, "inst", "q4").await.unwrap(),
            "quota used up"
        );
        qa.release("c", "q2").await.unwrap();
        assert!(qa.try_acquire("c", 10, "inst", "q5").await.unwrap());
        assert_eq!(qa.snapshot("c").unwrap().1, 3);
    }

    #[tokio::test]
    async fn beyond_the_quota_it_uses_the_reserve() {
        let (qa, _src, res) = setup(1, 1);
        assert!(
            qa.try_acquire("c", 10, "inst", "warm").await.unwrap(),
            "reserve"
        );
        qa.release("c", "warm").await.unwrap();
        qa.report_now().await;
        assert!(
            qa.try_acquire("c", 10, "inst", "q1").await.unwrap(),
            "quota"
        );
        assert!(
            qa.try_acquire("c", 10, "inst", "q2").await.unwrap(),
            "reserve"
        );
        assert_eq!(res.held.lock().unwrap().as_slice(), ["q2"]);
        assert!(!qa.try_acquire("c", 10, "inst", "q3").await.unwrap());
        // Releases go back where they came from.
        qa.release("c", "q2").await.unwrap();
        assert!(res.held.lock().unwrap().is_empty());
        assert_eq!(qa.snapshot("c").unwrap().1, 1);
    }

    #[tokio::test]
    async fn reports_running_and_waiting_and_the_seen_sequence() {
        let (qa, src, _res) = setup(1, 0);
        qa.try_acquire("c", 7, "inst", "first").await.unwrap();
        qa.report_now().await;
        assert!(qa.try_acquire("c", 7, "inst", "q1").await.unwrap());
        assert!(!qa.try_acquire("c", 7, "inst", "q2").await.unwrap());
        qa.report_now().await;
        let reports = src.reports.lock().unwrap();
        let last = reports.last().unwrap();
        assert_eq!(
            last[0],
            DemandReport {
                cluster: "c".into(),
                running: 1,
                waiting: 2,
                seen_seq: 1,
                max_running_queries: 7,
            },
            "\"first\" and q2 are still waiting"
        );
    }

    #[tokio::test]
    async fn an_expired_quota_is_not_used() {
        let (qa, src, _res) = setup(5, 0);
        src.lease_secs.store(0, Ordering::Relaxed);
        qa.try_acquire("c", 10, "inst", "q0").await.unwrap();
        qa.report_now().await;
        assert!(!qa.try_acquire("c", 10, "inst", "q1").await.unwrap());
    }

    #[tokio::test]
    async fn a_source_without_quotas_turns_them_off() {
        let (qa, src, res) = setup(5, 10);
        src.unsupported.store(true, Ordering::Relaxed);
        qa.try_acquire("c", 10, "inst", "q0").await.unwrap();
        assert!(!qa.report_now().await);
        assert!(!qa.enabled());
        assert!(qa.try_acquire("c", 10, "inst", "q1").await.unwrap());
        assert_eq!(res.held.lock().unwrap().len(), 2, "both on leases");
    }

    #[tokio::test]
    async fn a_worker_turns_quotas_on_when_its_control_plane_has_them() {
        let (qa, src, _res) = setup(5, 0);
        qa.set_policy(false, Duration::from_secs(60));
        src.unsupported.store(true, Ordering::Relaxed);
        qa.probe_source().await;
        assert!(!qa.enabled(), "an older control plane: stays on leases");
        src.unsupported.store(false, Ordering::Relaxed);
        qa.follow_source();
        qa.probe_source().await;
        assert!(qa.enabled());
        assert_eq!(qa.report_interval_ms.load(Ordering::Relaxed), 500);
    }

    #[tokio::test]
    async fn quotas_off_passes_everything_through() {
        let (qa, src, res) = setup(5, 10);
        qa.set_policy(false, Duration::from_secs(1));
        assert!(qa.try_acquire("c", 10, "inst", "q1").await.unwrap());
        qa.report_now().await;
        assert!(src.reports.lock().unwrap().is_empty());
        assert_eq!(res.held.lock().unwrap().as_slice(), ["q1"]);
    }

    #[tokio::test]
    async fn a_quota_slot_freeing_up_wakes_a_waiter() {
        let (qa, _src, _res) = setup(1, 0);
        qa.try_acquire("c", 10, "inst", "q0").await.unwrap();
        qa.report_now().await;
        assert!(qa.try_acquire("c", 10, "inst", "q1").await.unwrap());
        assert!(!qa.try_acquire("c", 10, "inst", "q2").await.unwrap());
        let waiter = {
            let qa = qa.clone();
            tokio::spawn(async move {
                let start = Instant::now();
                let r = qa
                    .wait_for_grant("c", "q2", Duration::from_secs(10))
                    .await
                    .unwrap();
                (r, start.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        qa.release("c", "q1").await.unwrap();
        let (woken, waited) = waiter.await.unwrap();
        assert!(woken);
        assert!(waited < Duration::from_secs(2));
        assert!(qa.try_acquire("c", 10, "inst", "q2").await.unwrap());
    }

    #[tokio::test]
    async fn shutdown_drops_the_quota_rows() {
        let (qa, src, _res) = setup(5, 0);
        qa.release_all_for_instance("inst").await.unwrap();
        assert_eq!(src.released.load(Ordering::Relaxed), 1);
        assert!(!qa.enabled());
    }
}
