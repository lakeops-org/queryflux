//! Scenario results: latency summaries, the human-readable table and the JSON document.

use std::fmt::Write as _;

use serde::Serialize;

use super::harness::{Load, ScenarioSpec};

/// Latency summary in milliseconds (nearest-rank percentiles).
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Latency {
    pub count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

impl Latency {
    pub fn from_micros(mut v: Vec<u32>) -> Self {
        if v.is_empty() {
            return Self::default();
        }
        v.sort_unstable();
        let ms = |us: u32| us as f64 / 1000.0;
        let pct = |p: f64| {
            let rank = ((p * v.len() as f64).ceil() as usize).clamp(1, v.len());
            ms(v[rank - 1])
        };
        let sum: u64 = v.iter().map(|&x| x as u64).sum();
        Self {
            count: v.len() as u64,
            mean_ms: sum as f64 / v.len() as f64 / 1000.0,
            p50_ms: pct(0.50),
            p99_ms: pct(0.99),
            p999_ms: pct(0.999),
            max_ms: ms(*v.last().unwrap()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioReport {
    pub scenario: ScenarioSpec,
    pub transport: String,
    pub measured_secs: f64,
    /// Grants inside the measured window.
    pub admissions: u64,
    pub admissions_per_sec: f64,
    /// Open loop only: arrivals inside the window.
    pub offered: Option<u64>,
    /// Open loop only: arrivals dropped because too many queries were unfinished.
    pub shed: u64,
    pub acquire_calls: u64,
    /// Acquire calls answered "full".
    pub denials: u64,
    /// Acquire calls after a query's first attempt.
    pub retries: u64,
    /// Release calls made after denials (dispatch drops its slot guard on every denial).
    pub denial_releases: u64,
    pub acquire_errors: u64,
    pub release_errors: u64,
    /// One acquire call (request to answer).
    pub acquire_latency: Latency,
    /// Query arrival to grant, including denials and backoff.
    pub admit_latency: Latency,
    /// Arrival to grant for queries denied at least once (`count` = how many waited).
    pub waited_admit_latency: Latency,
    pub release_latency: Latency,
    /// A query ending while others waited on the same cluster, to the next grant there.
    pub handoff_latency: Latency,
    /// Highest number of admitted, unreleased queries on any cluster (harness count).
    pub max_in_flight: u64,
    /// Highest `active_lease_count` sampled on any cluster.
    pub max_sampled_leases: Option<u64>,
    pub lease_samples: u64,
    /// Grants that took a cluster above its cap (harness count). Must be 0.
    pub cap_violations: u64,
    /// Lease-count samples above the cap. Must be 0.
    pub sampled_cap_violations: u64,
    /// Leases still held after every query released. Should be 0.
    pub leaked_leases: Option<u64>,
    pub pg_xacts_per_sec: Option<f64>,
    pub pg_statements_per_sec: Option<f64>,
    pub notes: Vec<String>,
    pub error_samples: Vec<String>,
}

impl ScenarioReport {
    pub fn failed(&self) -> bool {
        self.cap_violations > 0 || self.sampled_cap_violations > 0
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Machine {
    pub os: &'static str,
    pub arch: &'static str,
    pub cpus: usize,
    pub postgres: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReport {
    pub run_id: String,
    pub machine: Machine,
    pub scenarios: Vec<ScenarioReport>,
    pub cap_violations: u64,
    pub passed: bool,
}

fn opt(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.0}")).unwrap_or_else(|| "-".into())
}

fn lat3(l: &Latency) -> String {
    if l.count == 0 {
        return "-".into();
    }
    format!("{:.2} / {:.2} / {:.2}", l.p50_ms, l.p99_ms, l.p999_ms)
}

fn p50_p99(l: &Latency, decimals: usize) -> String {
    if l.count == 0 {
        return "-".into();
    }
    format!("{:.*} / {:.*}", decimals, l.p50_ms, decimals, l.p99_ms)
}

fn load(s: &ScenarioSpec) -> String {
    match s.load {
        Load::Closed { concurrency } => format!("C={concurrency}"),
        Load::Open { rate_per_sec, .. } => format!("R={rate_per_sec}/s"),
    }
}

/// The table printed after a run.
pub fn table(reports: &[ScenarioReport]) -> String {
    let header = [
        "scenario",
        "transport",
        "load",
        "clusters",
        "cap",
        "pool",
        "adm/s",
        "acquire p50/p99/p999 ms",
        "admit p99 ms",
        "waited (n, p50/p99 ms)",
        "release p50/p99 ms",
        "denied",
        "errors",
        "hand-off p50/p99 ms",
        "max in-flight",
        "violations",
        "pg xact/s",
        "pg stmt/s",
    ];
    let rows: Vec<Vec<String>> = reports
        .iter()
        .map(|r| {
            let s = &r.scenario;
            vec![
                s.name.clone(),
                r.transport.clone(),
                load(s),
                s.clusters.to_string(),
                if s.cap == u64::MAX {
                    "unlimited".into()
                } else {
                    s.cap.to_string()
                },
                s.coordination_pool.to_string(),
                format!("{:.0}", r.admissions_per_sec),
                lat3(&r.acquire_latency),
                if r.admit_latency.count == 0 {
                    "-".into()
                } else {
                    format!("{:.2}", r.admit_latency.p99_ms)
                },
                if r.waited_admit_latency.count == 0 {
                    "0".into()
                } else {
                    format!(
                        "{}, {}",
                        r.waited_admit_latency.count,
                        p50_p99(&r.waited_admit_latency, 0)
                    )
                },
                p50_p99(&r.release_latency, 2),
                r.denials.to_string(),
                (r.acquire_errors + r.release_errors).to_string(),
                p50_p99(&r.handoff_latency, 1),
                format!(
                    "{}/{}",
                    r.max_in_flight,
                    if s.cap == u64::MAX {
                        "inf".into()
                    } else {
                        s.cap.to_string()
                    }
                ),
                (r.cap_violations + r.sampled_cap_violations).to_string(),
                opt(r.pg_xacts_per_sec),
                opt(r.pg_statements_per_sec),
            ]
        })
        .collect();

    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    let mut out = String::new();
    let line = |out: &mut String, cells: &[String]| {
        let parts: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        let _ = writeln!(out, "| {} |", parts.join(" | "));
    };
    line(
        &mut out,
        &header.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    );
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    let _ = writeln!(out, "|-{}-|", sep.join("-|-"));
    for row in &rows {
        line(&mut out, row);
    }

    let mut notes = Vec::new();
    for r in reports {
        let tag = format!("{}/{}", r.scenario.name, r.transport);
        if r.failed() {
            notes.push(format!(
                "FAIL {tag}: {} cap violations (harness), {} sampled lease counts above cap",
                r.cap_violations, r.sampled_cap_violations
            ));
        }
        if let Some(n) = r.leaked_leases.filter(|n| *n > 0) {
            notes.push(format!("{tag}: {n} leases still held after the run"));
        }
        if r.shed > 0 {
            notes.push(format!(
                "{tag}: {} arrivals shed (max outstanding reached)",
                r.shed
            ));
        }
        for e in &r.error_samples {
            notes.push(format!("{tag}: error: {e}"));
        }
        for n in &r.notes {
            // Notes such as "pg_stat_statements unavailable" repeat for every scenario.
            if !notes.contains(n) {
                notes.push(n.clone());
            }
        }
    }
    for n in notes {
        let _ = writeln!(out, "{n}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let l = Latency::from_micros((1..=1000).collect());
        assert_eq!(l.count, 1000);
        assert_eq!(l.p50_ms, 0.5);
        assert_eq!(l.p99_ms, 0.99);
        assert_eq!(l.p999_ms, 0.999);
        assert_eq!(l.max_ms, 1.0);
        assert!((l.mean_ms - 0.5005).abs() < 1e-9);
        assert_eq!(Latency::from_micros(vec![]).count, 0);
        assert_eq!(Latency::from_micros(vec![7000]).p999_ms, 7.0);
    }
}
