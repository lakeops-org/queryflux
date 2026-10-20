//! Capacity quotas (high-throughput plan, WS2).
//!
//! With per-query leases, every admission is a round trip to Postgres or the control
//! plane. With quotas, each process that admits queries (a *holder*: a worker, or a
//! replica serving queries itself) reports its demand about once a second, an allocator
//! gives it a share of each cluster's `maxRunningQueries`, and the holder admits within
//! its share with an atomic counter. The per-query lease stays as the path for whatever
//! no quota covers (the *reserve*), so the two share one limit.
//!
//! **The hard limit.** For each cluster the allocator keeps
//! `sum(commit) + leased <= maxRunningQueries`, where a holder's commit is
//! `max(ceiling, running)`. `ceiling` is the largest quota the holder may still be
//! enforcing: when a quota shrinks, the holder learns about it only on its next report, so
//! the old, larger value stays counted until the holder confirms the new one
//! (`seen_seq`). A holder can't stop queries it already runs, so `running` counts too.
//! Per-query acquires check `leased + quota_committed < max` under the same per-cluster
//! lock the allocator writes `quota_committed` under.
//!
//! **Expiry.** A holder trusts its quota for `lease_secs` from when it sent the report
//! that returned it. The allocator counts the ceiling until `lease_secs` plus a margin
//! after it processed that report, which is later, so the holder always stops first.
//! After that only the holder's last reported `running` counts, until its row is dropped
//! with the same 5-minute cutoff as stale leases.

use async_trait::async_trait;
use queryflux_core::error::Result;
use serde::{Deserialize, Serialize};

/// What a holder reports for one cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DemandReport {
    pub cluster: String,
    /// Queries running on quota (lease-admitted queries are counted as leases).
    pub running: u64,
    /// Queries waiting for capacity on this holder.
    pub waiting: u64,
    /// Sequence of the grant it applies now (0 before the first grant). Once it equals
    /// the allocator's latest sequence, the ceiling drops to the current grant.
    pub seen_seq: u64,
    /// The cluster's `maxRunningQueries` in the holder's config.
    pub max_running_queries: u64,
}

/// The allocator's current quota for one holder and cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaGrant {
    pub cluster: String,
    pub quota: u64,
    pub seq: u64,
}

/// Where holders send demand and get quotas: Postgres for a replica, the control plane
/// for a worker.
#[async_trait]
pub trait QuotaStore: Send + Sync {
    /// Record `instance_id`'s demand and return its current grants for those clusters.
    /// `None` when quotas are not available here (an older control plane, or quotas
    /// turned off): the holder keeps using per-query leases.
    async fn report_demand(
        &self,
        instance_id: &str,
        reports: &[DemandReport],
    ) -> Result<Option<QuotaResponse>>;

    /// Drop every quota row of a holder that is shutting down, so its capacity is free
    /// at once instead of after expiry.
    async fn release_holder(&self, instance_id: &str) -> Result<u64>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaResponse {
    pub grants: Vec<QuotaGrant>,
    /// How long, from sending the report, the holder may admit on these grants.
    pub lease_secs: u64,
    /// How often the holder should report (`capacity.reportIntervalMs`).
    #[serde(default = "default_report_interval_ms")]
    pub report_interval_ms: u64,
}

fn default_report_interval_ms() -> u64 {
    1000
}

/// Runs the allocator: recompute every cluster's quotas from the holders' reports.
#[async_trait]
pub trait QuotaAllocator: Send + Sync {
    async fn run_allocation(
        &self,
        policy: &AllocatorPolicy,
        metrics: &dyn crate::MetricsStore,
    ) -> Result<AllocationPass>;
}

/// One allocator pass, for metrics and logs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AllocationPass {
    pub clusters: usize,
    pub holders: usize,
    pub changed: usize,
}

/// Allocator settings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllocatorPolicy {
    /// Share of each cluster's limit never handed out as quota, left for per-query leases.
    pub reserve_fraction: f64,
    /// How long a holder may admit on a grant without reporting again.
    pub lease_secs: u64,
    /// Added to `lease_secs` before the allocator stops counting a silent holder's ceiling.
    pub margin_secs: u64,
}

impl Default for AllocatorPolicy {
    fn default() -> Self {
        Self {
            reserve_fraction: 0.15,
            lease_secs: 15,
            margin_secs: 5,
        }
    }
}

/// One holder's row, as the allocator reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderState {
    pub instance_id: String,
    pub running: u64,
    pub waiting: u64,
    pub granted: u64,
    pub ceiling: u64,
    /// Seconds since its last report.
    pub silent_secs: u64,
}

impl HolderState {
    fn active(&self, policy: &AllocatorPolicy) -> bool {
        self.silent_secs < policy.lease_secs + policy.margin_secs
    }

    /// Capacity this holder may be using: its ceiling while it can still admit on it,
    /// then only what it runs.
    #[cfg(test)]
    fn commit(&self, policy: &AllocatorPolicy) -> u64 {
        if self.active(policy) {
            self.ceiling.max(self.running)
        } else {
            self.running
        }
    }
}

/// A new grant and ceiling for one holder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub instance_id: String,
    pub granted: u64,
    pub ceiling: u64,
}

/// The allocator's decision for one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAllocation {
    /// Holders whose grant or ceiling changed.
    pub changed: Vec<Allocation>,
    /// Sum of every holder's commit after the change: what per-query acquires must leave.
    pub quota_committed: u64,
}

/// Compute new quotas for one cluster.
///
/// 1. **Targets** by max-min fair share (Doorman's `FAIR_SHARE`) of the assignable
///    capacity, `max - leased - reserve`, over each active holder's demand
///    (`running + waiting`), then a little headroom from what is left so a holder
///    absorbs small bursts without a round trip.
/// 2. **Decreases** take effect at once as grants; the ceiling stays until the holder
///    confirms (see the module docs).
/// 3. **Increases** are granted only from capacity no holder may be using
///    (`max - leased - sum(commit)`), so the limit holds while capacity is draining from
///    holders whose quota shrank.
///
/// Clusters too small to split (`max < 2 × active holders`) get no quotas: every
/// admission goes through per-query leases, which is exact for small limits.
pub fn allocate(
    max: u64,
    leased: u64,
    holders: &[HolderState],
    policy: &AllocatorPolicy,
) -> ClusterAllocation {
    let active: Vec<usize> = (0..holders.len())
        .filter(|&i| holders[i].active(policy))
        .collect();
    let capacity = max.saturating_sub(leased);

    // 1. Targets.
    let mut target = vec![0u64; holders.len()];
    if !active.is_empty() && max >= 2 * active.len() as u64 {
        let reserve = ((max as f64) * policy.reserve_fraction).floor() as u64;
        let assignable = capacity.saturating_sub(reserve);
        let demand: Vec<u64> = active
            .iter()
            .map(|&i| holders[i].running.saturating_add(holders[i].waiting))
            .collect();
        let fair = max_min_fair(assignable, &demand);
        let mut left = assignable - fair.iter().sum::<u64>();
        // Headroom: up to a quarter of demand (at least 1) each, round-robin from what is left.
        let want: Vec<u64> = demand.iter().map(|d| (d / 4).max(1)).collect();
        let mut extra = vec![0u64; active.len()];
        while left > 0 {
            let mut gave = false;
            for k in 0..active.len() {
                if left > 0 && extra[k] < want[k] {
                    extra[k] += 1;
                    left -= 1;
                    gave = true;
                }
            }
            if !gave {
                break;
            }
        }
        for (k, &i) in active.iter().enumerate() {
            target[i] = fair[k] + extra[k];
        }
    }

    // 2. Decreases now; ceilings unchanged.
    let mut granted: Vec<u64> = holders.iter().map(|h| h.granted).collect();
    let mut ceiling: Vec<u64> = holders.iter().map(|h| h.ceiling).collect();
    for i in 0..holders.len() {
        if target[i] < granted[i] {
            granted[i] = target[i];
        }
        // A holder past its lease can't admit on anything: clear its grant, so when it
        // reports again it gets a fresh grant instead of the one it had before going
        // silent, whose capacity may be in use elsewhere by now.
        if !holders[i].active(policy) {
            granted[i] = 0;
            ceiling[i] = 0;
        }
    }

    // 3. Increases from free capacity, smallest grant first so a starved holder goes first.
    let commit_of = |i: usize, ceiling: &[u64]| -> u64 {
        let h = &holders[i];
        if h.active(policy) {
            ceiling[i].max(h.running)
        } else {
            h.running
        }
    };
    let committed: u64 = (0..holders.len()).map(|i| commit_of(i, &ceiling)).sum();
    let mut free = capacity.saturating_sub(committed);
    let mut growing: Vec<usize> = (0..holders.len())
        .filter(|&i| target[i] > granted[i])
        .collect();
    growing.sort_by_key(|&i| (granted[i], holders[i].instance_id.clone()));
    for i in growing {
        // What the holder already has committed covers the first part of the increase;
        // only the rest comes out of free capacity.
        let current = commit_of(i, &ceiling);
        let take = target[i].saturating_sub(current).min(free);
        free -= take;
        granted[i] = (current + take).min(target[i]).max(granted[i]);
        ceiling[i] = ceiling[i].max(granted[i]);
    }

    let changed = holders
        .iter()
        .enumerate()
        .filter(|(i, h)| granted[*i] != h.granted || ceiling[*i] != h.ceiling)
        .map(|(i, h)| Allocation {
            instance_id: h.instance_id.clone(),
            granted: granted[i],
            ceiling: ceiling[i],
        })
        .collect();
    let quota_committed = (0..holders.len()).map(|i| commit_of(i, &ceiling)).sum();
    ClusterAllocation {
        changed,
        quota_committed,
    }
}

/// Max-min fair division of `capacity` over `demand`: everyone whose demand fits in an
/// equal share gets it, and what they leave is split again among the rest.
fn max_min_fair(capacity: u64, demand: &[u64]) -> Vec<u64> {
    let mut out = vec![0u64; demand.len()];
    let mut left = capacity;
    let mut open: Vec<usize> = (0..demand.len()).filter(|&i| demand[i] > 0).collect();
    while left > 0 && !open.is_empty() {
        let share = (left / open.len() as u64).max(1);
        let mut still = Vec::new();
        for &i in &open {
            if left == 0 {
                still.push(i);
                continue;
            }
            let give = (demand[i] - out[i]).min(share).min(left);
            out[i] += give;
            left -= give;
            if out[i] < demand[i] {
                still.push(i);
            }
        }
        if still.len() == open.len() && share == 1 && left == 0 {
            break;
        }
        open = still;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(id: &str, running: u64, waiting: u64, granted: u64, ceiling: u64) -> HolderState {
        HolderState {
            instance_id: id.into(),
            running,
            waiting,
            granted,
            ceiling,
            silent_secs: 0,
        }
    }

    fn policy() -> AllocatorPolicy {
        AllocatorPolicy {
            reserve_fraction: 0.0,
            ..Default::default()
        }
    }

    fn apply(holders: &mut [HolderState], a: &ClusterAllocation) {
        for c in &a.changed {
            let h = holders
                .iter_mut()
                .find(|h| h.instance_id == c.instance_id)
                .unwrap();
            h.granted = c.granted;
            h.ceiling = c.ceiling;
        }
    }

    #[test]
    fn fair_share_splits_by_demand() {
        assert_eq!(max_min_fair(10, &[2, 20, 20]), vec![2, 4, 4]);
        assert_eq!(max_min_fair(10, &[1, 1]), vec![1, 1]);
        assert_eq!(max_min_fair(3, &[5, 5, 5, 5]).iter().sum::<u64>(), 3);
        assert_eq!(max_min_fair(0, &[5]), vec![0]);
    }

    #[test]
    fn idle_cluster_gets_quotas_from_demand_plus_headroom() {
        let hs = vec![holder("a", 4, 0, 0, 0), holder("b", 0, 8, 0, 0)];
        let a = allocate(100, 0, &hs, &policy());
        let g: Vec<u64> = a.changed.iter().map(|c| c.granted).collect();
        assert_eq!(g, vec![5, 10], "demand + a quarter of it as headroom");
        assert_eq!(a.quota_committed, 15);
    }

    #[test]
    fn saturated_cluster_is_split_fairly_and_the_reserve_is_kept() {
        let hs = vec![holder("a", 0, 100, 0, 0), holder("b", 0, 100, 0, 0)];
        let p = AllocatorPolicy {
            reserve_fraction: 0.2,
            ..Default::default()
        };
        let a = allocate(100, 0, &hs, &p);
        let g: Vec<u64> = a.changed.iter().map(|c| c.granted).collect();
        assert_eq!(g, vec![40, 40]);
    }

    #[test]
    fn a_shrinking_quota_keeps_its_ceiling_until_confirmed() {
        // a holds 80 and runs 10; b now wants 80.
        let mut hs = vec![holder("a", 10, 0, 80, 80), holder("b", 0, 80, 0, 0)];
        let a1 = allocate(100, 0, &hs, &policy());
        apply(&mut hs, &a1);
        assert!(hs[0].granted < 80, "a's grant drops at once");
        assert_eq!(hs[0].ceiling, 80, "but a may still admit up to 80");
        assert_eq!(hs[1].granted, 20, "so b gets only what is free");
        assert!(a1.quota_committed <= 100);

        // a confirms its new grant: the ceiling follows, and b gets the rest.
        hs[0].ceiling = hs[0].granted;
        let a2 = allocate(100, 0, &hs, &policy());
        apply(&mut hs, &a2);
        assert!(hs[1].granted > 20);
        assert!(a2.quota_committed <= 100);
    }

    #[test]
    fn running_above_quota_still_counts() {
        // a's quota went to 5 and it confirmed, but it still runs 30.
        let hs = vec![holder("a", 30, 0, 5, 5), holder("b", 0, 100, 0, 0)];
        let a = allocate(100, 0, &hs, &policy());
        let b = a.changed.iter().find(|c| c.instance_id == "b").unwrap();
        assert_eq!(b.granted, 70);
    }

    #[test]
    fn per_query_leases_reduce_what_quotas_get() {
        let hs = vec![holder("a", 0, 100, 0, 0)];
        let a = allocate(100, 60, &hs, &policy());
        assert_eq!(a.changed[0].granted, 40);
        assert_eq!(a.quota_committed, 40);
    }

    #[test]
    fn silent_holders_stop_counting_their_ceiling_after_the_lease() {
        let mut gone = holder("gone", 3, 0, 50, 50);
        gone.silent_secs = 30;
        let hs = vec![gone, holder("b", 0, 100, 0, 0)];
        let a = allocate(100, 0, &hs, &policy());
        let b = a.changed.iter().find(|c| c.instance_id == "b").unwrap();
        assert_eq!(
            b.granted, 97,
            "only the silent holder's running queries count"
        );
        let g = a.changed.iter().find(|c| c.instance_id == "gone").unwrap();
        assert_eq!(g.granted, 0);
    }

    #[test]
    fn small_clusters_get_no_quotas() {
        let hs = vec![holder("a", 0, 5, 0, 0), holder("b", 0, 5, 0, 0)];
        let a = allocate(3, 0, &hs, &policy());
        assert!(a.changed.is_empty());
        assert_eq!(a.quota_committed, 0);
    }

    /// Random demand, reports that lag, confirmations that arrive late or never, holders
    /// going silent, and per-query leases: after every step the committed capacity plus
    /// leases stays within the limit, and the holders' real admissions never exceed it.
    #[test]
    fn the_limit_holds_under_random_churn() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n.max(1)
        };
        for round in 0..300 {
            let max = 5 + next(200);
            let n = 1 + next(6) as usize;
            let p = AllocatorPolicy {
                reserve_fraction: [0.0, 0.15, 0.3][round % 3],
                ..Default::default()
            };
            let mut hs: Vec<HolderState> = (0..n)
                .map(|i| holder(&format!("h{i}"), 0, 0, 0, 0))
                .collect();
            // What each holder actually enforces (may lag the allocator's grant).
            let mut enforcing = vec![0u64; n];
            let mut leased = 0u64;
            for _ in 0..200 {
                // Holders run work within what they enforce; some finish.
                for i in 0..n {
                    let h = &mut hs[i];
                    let room = enforcing[i].saturating_sub(h.running);
                    h.running += next(room + 1).min(room);
                    h.running -= next(h.running + 1).min(h.running);
                    h.waiting = next(max);
                    if next(10) == 0 {
                        h.silent_secs += 10;
                    } else if next(3) == 0 {
                        h.silent_secs = 0;
                    }
                    // A holder stops admitting on its grant `lease_secs` after its last
                    // successful report, which (time being continuous) always comes before
                    // the allocator stops counting it at `lease_secs + margin_secs`.
                    if h.silent_secs >= p.lease_secs {
                        enforcing[i] = 0;
                    }
                }
                // Per-query leases come and go within what quotas leave.
                let committed: u64 = hs.iter().map(|h| h.commit(&p)).sum();
                let room = max.saturating_sub(leased + committed);
                leased += next(room + 1).min(room);
                leased -= next(leased + 1).min(leased);

                let a = allocate(max, leased, &hs, &p);
                apply(&mut hs, &a);
                assert!(
                    a.quota_committed + leased <= max,
                    "committed {} + leased {leased} > max {max}",
                    a.quota_committed
                );
                // Some holders receive their grant and confirm it.
                for i in 0..n {
                    if hs[i].silent_secs < p.lease_secs && next(2) == 0 {
                        enforcing[i] = hs[i].granted;
                        if next(2) == 0 {
                            hs[i].ceiling = hs[i].granted;
                        }
                    }
                }
                // Ground truth: what holders could admit right now, plus leases.
                let real: u64 =
                    (0..n).map(|i| enforcing[i].max(hs[i].running)).sum::<u64>() + leased;
                assert!(real <= max, "real use {real} > max {max}");
            }
        }
    }
}
