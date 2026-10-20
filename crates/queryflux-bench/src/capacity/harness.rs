//! Drives one scenario against an [`AdmissionBackend`] and measures it.
//!
//! Every simulated query does what a worker's dispatch does: acquire a lease (on a denial,
//! release the query's lease id as the slot guard does, then retry with the same backoff as
//! `queued_backoff_delay`), hold it for the query's duration, then release it. The harness keeps its own count of admitted queries
//! per cluster (incremented on grant, decremented *before* release, so it never exceeds
//! the true number of held leases); any value above the cap is a violation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep_until;

use super::backend::{AdmissionBackend, WorkerAdmission};
use super::dist::{Dist, Rng};
use super::pgstats::{PgSnapshot, PgStatsProbe};
use super::report::{Latency, ScenarioReport};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum Load {
    /// `concurrency` queries in flight at all times (each starts when the previous ends).
    #[serde(rename_all = "camelCase")]
    Closed { concurrency: usize },
    /// Queries arrive at `rate_per_sec` regardless of how fast they are admitted.
    #[serde(rename_all = "camelCase")]
    Open {
        rate_per_sec: f64,
        /// Arrivals beyond this many unfinished queries are shed (counted, not run).
        max_outstanding: usize,
        /// Exponential inter-arrival times (Poisson); otherwise evenly spaced.
        poisson: bool,
    },
}

/// Retry delay after a denied acquire: `initial * 2^min(attempt, 4)`, capped at `max`.
/// Defaults match the dispatch path (100 ms .. 2 s).
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Retry {
    pub initial_ms: u64,
    pub max_ms: u64,
}

impl Retry {
    pub fn delay(&self, attempt: u64) -> Duration {
        let ms = self
            .initial_ms
            .saturating_mul(1 << attempt.min(4))
            .min(self.max_ms);
        Duration::from_millis(ms)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioSpec {
    pub name: String,
    pub workers: usize,
    pub load: Load,
    pub clusters: usize,
    /// `maxRunningQueries` for every cluster.
    pub cap: u64,
    pub query_duration: Dist,
    pub warmup_secs: f64,
    pub run_secs: f64,
    /// `coordinationPoolSize` of the store behind the transport.
    pub coordination_pool: u32,
    pub retry: Retry,
    pub sample_ms: u64,
    pub seed: u64,
}

struct ClusterState {
    name: String,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    violations: AtomicU64,
    /// Queries currently retrying after a denial.
    waiters: AtomicU64,
    /// When slots were freed while queries were waiting; the next grant consumes one.
    freed_while_waiting: Mutex<VecDeque<Instant>>,
    max_sampled: AtomicU64,
    sampled_violations: AtomicU64,
}

impl ClusterState {
    fn new(name: String) -> Self {
        Self {
            name,
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            violations: AtomicU64::new(0),
            waiters: AtomicU64::new(0),
            freed_while_waiting: Mutex::new(VecDeque::new()),
            max_sampled: AtomicU64::new(0),
            sampled_violations: AtomicU64::new(0),
        }
    }
}

#[derive(Default)]
struct Samples(Mutex<Vec<u32>>);

impl Samples {
    fn push(&self, d: Duration) {
        let us = d.as_micros().min(u32::MAX as u128) as u32;
        self.0.lock().unwrap().push(us);
    }
    fn take(&self) -> Vec<u32> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

#[derive(Default)]
struct Counters {
    admissions: AtomicU64,
    acquire_calls: AtomicU64,
    denials: AtomicU64,
    retries: AtomicU64,
    denial_releases: AtomicU64,
    acquire_errors: AtomicU64,
    release_errors: AtomicU64,
    offered: AtomicU64,
    shed: AtomicU64,
    lease_samples: AtomicU64,
    sample_errors: AtomicU64,
}

struct Shared {
    spec: ScenarioSpec,
    from: Instant,
    until: Instant,
    stop: AtomicBool,
    stop_rx: watch::Receiver<bool>,
    clusters: Vec<ClusterState>,
    next_cluster: AtomicU64,
    next_query: AtomicU64,
    run_id: String,
    c: Counters,
    acquire: Samples,
    admit: Samples,
    release: Samples,
    handoff: Samples,
    waited_admit: Samples,
    errors: Mutex<Vec<String>>,
}

impl Shared {
    fn in_window(&self, t: Instant) -> bool {
        t >= self.from && t < self.until
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn note_error(&self, e: &anyhow::Error) {
        let mut errs = self.errors.lock().unwrap();
        let msg = e.to_string();
        if errs.len() < 5 && !errs.contains(&msg) {
            errs.push(msg);
        }
    }

    /// Sleep `d`, waking early when the run stops.
    async fn pause(&self, d: Duration) {
        let mut rx = self.stop_rx.clone();
        tokio::select! {
            _ = tokio::time::sleep(d) => {}
            _ = rx.wait_for(|s| *s) => {}
        }
    }
}

/// One query: acquire (with retries), hold, release.
async fn run_query(sh: &Shared, worker: &dyn WorkerAdmission, rng: &mut Rng, arrival: Instant) {
    let ci = (sh.next_cluster.fetch_add(1, Ordering::Relaxed) % sh.clusters.len() as u64) as usize;
    let cl = &sh.clusters[ci];
    let qid = format!(
        "bench-{}-q{}",
        sh.run_id,
        sh.next_query.fetch_add(1, Ordering::Relaxed)
    );
    let cap = sh.spec.cap;

    let mut attempt = 0u64;
    let mut waiting = false;
    let granted_at = loop {
        if sh.stopped() {
            if waiting {
                cl.waiters.fetch_sub(1, Ordering::Relaxed);
            }
            return;
        }
        let t0 = Instant::now();
        let res = worker.try_acquire(&cl.name, cap, &qid).await;
        let t1 = Instant::now();
        let counted = sh.in_window(t1);
        if counted {
            sh.c.acquire_calls.fetch_add(1, Ordering::Relaxed);
            if attempt > 0 {
                sh.c.retries.fetch_add(1, Ordering::Relaxed);
            }
            sh.acquire.push(t1 - t0);
        }
        match res {
            Ok(true) => break t1,
            Ok(false) => {
                if counted {
                    sh.c.denials.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                if counted {
                    sh.c.acquire_errors.fetch_add(1, Ordering::Relaxed);
                }
                sh.note_error(&e);
            }
        }
        if !waiting {
            waiting = true;
            cl.waiters.fetch_add(1, Ordering::Relaxed);
        }
        // Dispatch releases its slot guard on every denial (an idempotent delete of the
        // query's lease), so a denied attempt costs two calls; do the same.
        if let Err(e) = worker.release(&cl.name, &qid).await {
            sh.note_error(&e);
        }
        if counted {
            sh.c.denial_releases.fetch_add(1, Ordering::Relaxed);
        }
        sh.pause(sh.spec.retry.delay(attempt)).await;
        attempt += 1;
    };
    if waiting {
        cl.waiters.fetch_sub(1, Ordering::Relaxed);
    }

    // Cap checker: count it as running before anything else can observe it.
    let now_in = cl.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    cl.max_in_flight.fetch_max(now_in, Ordering::Relaxed);
    if now_in > cap {
        cl.violations.fetch_add(1, Ordering::Relaxed);
    }
    let freed = cl.freed_while_waiting.lock().unwrap().pop_front();
    if sh.in_window(granted_at) {
        sh.c.admissions.fetch_add(1, Ordering::Relaxed);
        sh.admit.push(granted_at - arrival);
        if waiting {
            sh.waited_admit.push(granted_at - arrival);
        }
        if let Some(freed) = freed {
            sh.handoff.push(granted_at.saturating_duration_since(freed));
        }
    }

    let hold = sh.spec.query_duration.sample(rng);
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }

    // Decrement before releasing, so the harness count never runs ahead of real leases.
    cl.in_flight.fetch_sub(1, Ordering::SeqCst);
    let r0 = Instant::now();
    {
        let mut q = cl.freed_while_waiting.lock().unwrap();
        if cl.waiters.load(Ordering::Relaxed) > 0 {
            q.push_back(r0);
        } else {
            q.clear();
        }
    }
    let res = worker.release(&cl.name, &qid).await;
    let r1 = Instant::now();
    if sh.in_window(r0) {
        sh.release.push(r1 - r0);
    }
    if let Err(e) = res {
        if sh.in_window(r0) {
            sh.c.release_errors.fetch_add(1, Ordering::Relaxed);
        }
        sh.note_error(&e);
    }
}

async fn sample_leases(sh: Arc<Shared>, backend: Arc<dyn AdmissionBackend>) {
    let every = Duration::from_millis(sh.spec.sample_ms.max(10));
    while !sh.stopped() {
        for cl in &sh.clusters {
            match backend.active_leases(&cl.name).await {
                Ok(Some(n)) => {
                    sh.c.lease_samples.fetch_add(1, Ordering::Relaxed);
                    cl.max_sampled.fetch_max(n, Ordering::Relaxed);
                    if n > sh.spec.cap {
                        cl.sampled_violations.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    sh.c.sample_errors.fetch_add(1, Ordering::Relaxed);
                    sh.note_error(&e);
                }
            }
        }
        sh.pause(every).await;
    }
}

pub async fn run_scenario(
    spec: &ScenarioSpec,
    backend: Arc<dyn AdmissionBackend>,
    pg: Option<Arc<PgStatsProbe>>,
    run_id: &str,
) -> anyhow::Result<ScenarioReport> {
    anyhow::ensure!(spec.workers > 0, "workers must be > 0");
    anyhow::ensure!(spec.clusters > 0, "clusters must be > 0");

    let instance_ids: Vec<String> = (0..spec.workers)
        .map(|i| format!("bench-{run_id}-{}-w{i}", spec.name))
        .collect();
    let mut workers = Vec::with_capacity(spec.workers);
    for id in &instance_ids {
        workers.push(backend.worker(id).await?);
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let start = Instant::now();
    let from = start + Duration::from_secs_f64(spec.warmup_secs);
    let until = from + Duration::from_secs_f64(spec.run_secs);
    let sh = Arc::new(Shared {
        spec: spec.clone(),
        from,
        until,
        stop: AtomicBool::new(false),
        stop_rx,
        clusters: (0..spec.clusters)
            .map(|i| ClusterState::new(format!("bench-{run_id}-{}-c{i}", spec.name)))
            .collect(),
        next_cluster: AtomicU64::new(0),
        next_query: AtomicU64::new(0),
        run_id: run_id.to_string(),
        c: Counters::default(),
        acquire: Samples::default(),
        admit: Samples::default(),
        release: Samples::default(),
        handoff: Samples::default(),
        waited_admit: Samples::default(),
        errors: Mutex::new(Vec::new()),
    });

    let pg_task = pg.clone().map(|p| {
        tokio::spawn(async move {
            sleep_until(from.into()).await;
            let a = p.snapshot().await;
            sleep_until(until.into()).await;
            let b = p.snapshot().await;
            (a, b)
        })
    });
    let sampler = tokio::spawn(sample_leases(sh.clone(), backend.clone()));

    match spec.load {
        Load::Closed { concurrency } => {
            anyhow::ensure!(concurrency > 0, "concurrency must be > 0");
            let mut set = JoinSet::new();
            for i in 0..concurrency {
                let sh = sh.clone();
                let w = workers[i % workers.len()].clone();
                let mut rng = Rng::new(spec.seed.wrapping_add(i as u64));
                set.spawn(async move {
                    while !sh.stopped() {
                        run_query(&sh, w.as_ref(), &mut rng, Instant::now()).await;
                    }
                });
            }
            sleep_until(until.into()).await;
            sh.stop.store(true, Ordering::SeqCst);
            let _ = stop_tx.send(true);
            while set.join_next().await.is_some() {}
        }
        Load::Open {
            rate_per_sec,
            max_outstanding,
            poisson,
        } => {
            anyhow::ensure!(rate_per_sec > 0.0, "rate must be > 0");
            let outstanding = Arc::new(AtomicU64::new(0));
            let mut rng = Rng::new(spec.seed);
            let mean = 1.0 / rate_per_sec;
            let mut next = start;
            let mut n = 0u64;
            while next < until {
                sleep_until(next.into()).await;
                let arrival = next;
                let gap = if poisson { rng.exp(mean) } else { mean };
                next += Duration::from_secs_f64(gap);
                if sh.in_window(arrival) {
                    sh.c.offered.fetch_add(1, Ordering::Relaxed);
                }
                if outstanding.load(Ordering::Relaxed) >= max_outstanding as u64 {
                    if sh.in_window(arrival) {
                        sh.c.shed.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                outstanding.fetch_add(1, Ordering::Relaxed);
                let sh = sh.clone();
                let w = workers[(n % workers.len() as u64) as usize].clone();
                let mut qrng = Rng::new(spec.seed ^ n.wrapping_mul(0x2545_F491_4F6C_DD1D));
                let out = outstanding.clone();
                n += 1;
                tokio::spawn(async move {
                    run_query(&sh, w.as_ref(), &mut qrng, arrival).await;
                    out.fetch_sub(1, Ordering::Relaxed);
                });
            }
            sh.stop.store(true, Ordering::SeqCst);
            let _ = stop_tx.send(true);
            let deadline = Instant::now() + Duration::from_secs(60);
            while outstanding.load(Ordering::Relaxed) > 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    let _ = sampler.await;

    let mut notes = Vec::new();
    let (pg_xacts, pg_statements) = match pg_task {
        Some(t) => match t.await? {
            (Ok(a), Ok(b)) => rates(a, b, spec.run_secs),
            (Err(e), _) | (_, Err(e)) => {
                notes.push(format!("pg_stat sampling failed: {e}"));
                (None, None)
            }
        },
        None => (None, None),
    };
    if let Some(note) = pg.as_ref().and_then(|p| p.note.clone()) {
        notes.push(note);
    }

    // Every lease should be gone once all queries have released.
    let mut leaked: Option<u64> = None;
    for cl in &sh.clusters {
        if let Ok(Some(n)) = backend.active_leases(&cl.name).await {
            *leaked.get_or_insert(0) += n;
        }
    }
    backend.cleanup(&instance_ids).await?;

    let c = &sh.c;
    let admissions = c.admissions.load(Ordering::Relaxed);
    let sampled_any = c.lease_samples.load(Ordering::Relaxed) > 0;
    if c.sample_errors.load(Ordering::Relaxed) > 0 {
        notes.push(format!(
            "{} lease samples failed",
            c.sample_errors.load(Ordering::Relaxed)
        ));
    }
    let error_samples = sh.errors.lock().unwrap().clone();
    Ok(ScenarioReport {
        scenario: spec.clone(),
        transport: backend.name().to_string(),
        measured_secs: spec.run_secs,
        admissions,
        admissions_per_sec: admissions as f64 / spec.run_secs,
        offered: matches!(spec.load, Load::Open { .. }).then(|| c.offered.load(Ordering::Relaxed)),
        shed: c.shed.load(Ordering::Relaxed),
        acquire_calls: c.acquire_calls.load(Ordering::Relaxed),
        denials: c.denials.load(Ordering::Relaxed),
        retries: c.retries.load(Ordering::Relaxed),
        denial_releases: c.denial_releases.load(Ordering::Relaxed),
        acquire_errors: c.acquire_errors.load(Ordering::Relaxed),
        release_errors: c.release_errors.load(Ordering::Relaxed),
        acquire_latency: Latency::from_micros(sh.acquire.take()),
        admit_latency: Latency::from_micros(sh.admit.take()),
        release_latency: Latency::from_micros(sh.release.take()),
        handoff_latency: Latency::from_micros(sh.handoff.take()),
        waited_admit_latency: Latency::from_micros(sh.waited_admit.take()),
        max_in_flight: sh
            .clusters
            .iter()
            .map(|c| c.max_in_flight.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0),
        max_sampled_leases: sampled_any.then(|| {
            sh.clusters
                .iter()
                .map(|c| c.max_sampled.load(Ordering::Relaxed))
                .max()
                .unwrap_or(0)
        }),
        lease_samples: c.lease_samples.load(Ordering::Relaxed),
        cap_violations: sh
            .clusters
            .iter()
            .map(|c| c.violations.load(Ordering::Relaxed))
            .sum(),
        sampled_cap_violations: sh
            .clusters
            .iter()
            .map(|c| c.sampled_violations.load(Ordering::Relaxed))
            .sum(),
        leaked_leases: leaked,
        pg_xacts_per_sec: pg_xacts,
        pg_statements_per_sec: pg_statements,
        notes,
        error_samples,
    })
}

fn rates(a: PgSnapshot, b: PgSnapshot, secs: f64) -> (Option<f64>, Option<f64>) {
    let x = Some((b.xacts - a.xacts) as f64 / secs);
    let s = match (a.statements, b.statements) {
        (Some(a), Some(b)) => Some((b - a) as f64 / secs),
        _ => None,
    };
    (x, s)
}

#[cfg(test)]
mod tests {
    use super::super::backend::fake::FakeBackend;
    use super::*;

    fn spec(load: Load, cap: u64, duration: &str) -> ScenarioSpec {
        ScenarioSpec {
            name: "t".into(),
            workers: 3,
            load,
            clusters: 2,
            cap,
            query_duration: duration.parse().unwrap(),
            warmup_secs: 0.1,
            run_secs: 0.6,
            coordination_pool: 1,
            retry: Retry {
                initial_ms: 1,
                max_ms: 5,
            },
            sample_ms: 10,
            seed: 1,
        }
    }

    #[test]
    fn retry_matches_dispatch_backoff() {
        let r = Retry {
            initial_ms: 100,
            max_ms: 2000,
        };
        let ms: Vec<u128> = (0..7).map(|a| r.delay(a).as_millis()).collect();
        assert_eq!(ms, vec![100, 200, 400, 800, 1600, 1600, 1600]);
    }

    #[tokio::test]
    async fn honest_backend_has_no_violations_and_hands_off_slots() {
        let backend = Arc::new(FakeBackend::new(false));
        let r = run_scenario(
            &spec(Load::Closed { concurrency: 12 }, 2, "fixed:3"),
            backend,
            None,
            "x",
        )
        .await
        .unwrap();
        assert_eq!(r.cap_violations, 0);
        assert_eq!(r.sampled_cap_violations, 0);
        assert!(r.admissions > 0);
        assert!(r.denials > 0, "12 queries over 2x2 slots must be denied");
        assert!(r.handoff_latency.count > 0);
        assert!(r.waited_admit_latency.count > 0);
        assert_eq!(r.max_in_flight, 2);
        assert_eq!(r.leaked_leases, Some(0));
        assert!(!r.failed());
    }

    #[tokio::test]
    async fn over_admitting_backend_is_caught() {
        let backend = Arc::new(FakeBackend::new(true));
        let r = run_scenario(
            &spec(Load::Closed { concurrency: 12 }, 2, "fixed:3"),
            backend,
            None,
            "x",
        )
        .await
        .unwrap();
        assert!(r.cap_violations > 0);
        assert!(r.max_in_flight > 2);
        assert!(r.failed());
    }

    #[tokio::test]
    async fn open_loop_offers_the_requested_rate() {
        let backend = Arc::new(FakeBackend::new(false));
        let r = run_scenario(
            &spec(
                Load::Open {
                    rate_per_sec: 500.0,
                    max_outstanding: 1000,
                    poisson: false,
                },
                1000,
                "0",
            ),
            backend,
            None,
            "x",
        )
        .await
        .unwrap();
        let offered = r.offered.unwrap() as f64;
        assert!((offered - 300.0).abs() < 60.0, "offered {offered}");
        assert_eq!(r.denials, 0);
        assert_eq!(r.cap_violations, 0);
    }
}
