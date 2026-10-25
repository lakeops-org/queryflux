//! `queryflux-bench capacity`: capacity-admission benchmark with a cap-violation checker.
//!
//! Simulated workers acquire and release capacity leases against Postgres, either through
//! `CapacityStore` in process (`--transport store`) or through the control plane's
//! `/worker/capacity/*` API (`--transport http`, served in process by default). See
//! `website/docs/benchmarks.md` ("Capacity benchmark").

mod backend;
mod dist;
mod harness;
mod pgstats;
mod report;

use std::sync::Arc;

use anyhow::{bail, Context};
use clap::{Parser, ValueEnum};

use backend::{AdmissionBackend, HttpBackend, StoreBackend};
use dist::Dist;
use harness::{Load, Retry, ScenarioSpec};
use pgstats::PgStatsProbe;
use report::{Machine, RunReport};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Transport {
    Store,
    Http,
    Both,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Preset {
    /// One query at a time, one cluster.
    Serial,
    /// 32 concurrent, one cluster, every acquire granted.
    C32OneCluster,
    /// 32 concurrent over 8 clusters, every acquire granted.
    C32EightClusters,
    /// Same as c32-eight-clusters with the default coordinationPoolSize (2).
    C32EightClustersPool2,
    /// 32 concurrent on one cluster capped at 8, 20 ms queries: denials, retries, hand-off.
    Contended,
    /// 300 queries/s (Poisson) on one cluster capped at 8, 20 ms queries (75% of capacity):
    /// how long freed slots sit idle while retrying queries back off.
    ContendedOpen,
    /// All of the above.
    All,
}

/// Capacity admission benchmark: simulated workers acquiring and releasing capacity leases.
#[derive(Parser, Debug)]
#[command(name = "queryflux-bench capacity")]
struct Args {
    /// Postgres URL (defaults to $DATABASE_URL). Migrations run unless --no-migrate.
    #[arg(long)]
    database_url: Option<String>,

    /// Predefined scenarios (repeatable). Without it, one custom scenario from the flags below.
    #[arg(long = "scenario", value_enum)]
    scenarios: Vec<Preset>,

    /// How workers reach the lease store.
    #[arg(long, value_enum, default_value = "http")]
    transport: Transport,

    /// Simulated workers (each with its own client).
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Closed loop: queries in flight at all times.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    /// Open loop: arrivals per second (overrides --concurrency).
    #[arg(long)]
    rate: Option<f64>,

    /// Open loop: evenly spaced arrivals instead of Poisson.
    #[arg(long)]
    uniform_arrivals: bool,

    /// Open loop: shed arrivals beyond this many unfinished queries.
    #[arg(long, default_value_t = 10_000)]
    max_outstanding: usize,

    /// Clusters; queries are spread round-robin.
    #[arg(long, default_value_t = 1)]
    clusters: usize,

    /// maxRunningQueries per cluster (0 = unlimited).
    #[arg(long, default_value_t = 10_000)]
    cap: u64,

    /// How long a query holds its slot: 0, fixed:MS or exp:MEAN_MS.
    #[arg(long, default_value = "0")]
    query_duration: Dist,

    /// coordinationPoolSize of the store behind the transport.
    #[arg(long, default_value_t = 32)]
    coordination_pool_size: u32,

    /// Measured seconds per scenario (applies to presets too).
    #[arg(long, default_value_t = 10.0)]
    duration_secs: f64,

    /// Unmeasured seconds before each scenario (applies to presets too).
    #[arg(long, default_value_t = 2.0)]
    warmup_secs: f64,

    /// First retry delay after a denial (dispatch uses 100).
    #[arg(long, default_value_t = 100)]
    retry_initial_ms: u64,

    /// Longest retry delay (dispatch uses 2000).
    #[arg(long, default_value_t = 2000)]
    retry_max_ms: u64,

    /// How often to sample active_lease_count per cluster.
    #[arg(long, default_value_t = 250)]
    sample_ms: u64,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Write the JSON report to this file (`-` for stdout; the table then goes to stderr).
    #[arg(long)]
    json: Option<String>,

    /// http transport: use this running control plane instead of serving the API in process.
    /// --database-url must be its database.
    #[arg(long)]
    worker_api_url: Option<String>,

    /// http transport with --worker-api-url: controlPlane.sharedSecret.
    #[arg(long, default_value = "")]
    worker_secret: String,

    /// Do not run migrations.
    #[arg(long)]
    no_migrate: bool,
}

impl Args {
    fn base(&self, name: &str) -> ScenarioSpec {
        ScenarioSpec {
            name: name.to_string(),
            workers: self.workers,
            load: match self.rate {
                Some(rate_per_sec) => Load::Open {
                    rate_per_sec,
                    max_outstanding: self.max_outstanding,
                    poisson: !self.uniform_arrivals,
                },
                None => Load::Closed {
                    concurrency: self.concurrency,
                },
            },
            clusters: self.clusters,
            cap: if self.cap == 0 { u64::MAX } else { self.cap },
            query_duration: self.query_duration,
            warmup_secs: self.warmup_secs,
            run_secs: self.duration_secs,
            coordination_pool: self.coordination_pool_size,
            retry: Retry {
                initial_ms: self.retry_initial_ms,
                max_ms: self.retry_max_ms,
            },
            sample_ms: self.sample_ms,
            seed: self.seed,
        }
    }

    fn specs(&self) -> Vec<ScenarioSpec> {
        if self.scenarios.is_empty() {
            return vec![self.base("custom")];
        }
        let mut presets: Vec<Preset> = Vec::new();
        for p in &self.scenarios {
            if *p == Preset::All {
                presets.extend([
                    Preset::Serial,
                    Preset::C32OneCluster,
                    Preset::C32EightClusters,
                    Preset::C32EightClustersPool2,
                    Preset::Contended,
                    Preset::ContendedOpen,
                ]);
            } else {
                presets.push(*p);
            }
        }
        presets.dedup();
        presets.into_iter().map(|p| self.preset(p)).collect()
    }

    /// The plan's baseline table: caps high enough that every acquire is granted, zero-length
    /// queries (acquire then release), 4 workers sharing the load.
    fn preset(&self, p: Preset) -> ScenarioSpec {
        let closed = |c| Load::Closed { concurrency: c };
        let mut s = self.base("");
        s.query_duration = Dist::Fixed { ms: 0.0 };
        s.cap = 10_000;
        s.workers = 4;
        s.coordination_pool = 32;
        s.retry = Retry {
            initial_ms: 100,
            max_ms: 2000,
        };
        match p {
            Preset::Serial => {
                s.name = "serial".into();
                s.workers = 1;
                s.load = closed(1);
                s.clusters = 1;
            }
            Preset::C32OneCluster => {
                s.name = "c32-one-cluster".into();
                s.load = closed(32);
                s.clusters = 1;
            }
            Preset::C32EightClusters => {
                s.name = "c32-eight-clusters".into();
                s.load = closed(32);
                s.clusters = 8;
            }
            Preset::C32EightClustersPool2 => {
                s.name = "c32-eight-clusters-pool2".into();
                s.load = closed(32);
                s.clusters = 8;
                s.coordination_pool = 2;
            }
            Preset::Contended => {
                s.name = "contended".into();
                s.load = closed(32);
                s.clusters = 1;
                s.cap = 8;
                s.query_duration = Dist::Fixed { ms: 20.0 };
            }
            Preset::ContendedOpen => {
                s.name = "contended-open".into();
                s.load = Load::Open {
                    rate_per_sec: 300.0,
                    max_outstanding: 10_000,
                    poisson: true,
                };
                s.clusters = 1;
                s.cap = 8;
                s.query_duration = Dist::Fixed { ms: 20.0 };
            }
            Preset::All => unreachable!("expanded in specs()"),
        }
        s
    }
}

/// Entry point; `args[0]` is the subcommand name. Returns the process exit code.
pub async fn main(args: Vec<String>) -> anyhow::Result<i32> {
    let args = Args::parse_from(args);
    let database_url = match &args.database_url {
        Some(u) => u.clone(),
        None => {
            std::env::var("DATABASE_URL").context("--database-url or DATABASE_URL is required")?
        }
    };
    if args.worker_api_url.is_some() && args.transport != Transport::Http {
        bail!("--worker-api-url needs --transport http");
    }

    if !args.no_migrate {
        let store = backend::connect_store(&database_url, 1).await?;
        store.migrate().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    let pg = Arc::new(PgStatsProbe::connect(&database_url).await?);
    let run_id = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let transports = match args.transport {
        Transport::Both => vec![Transport::Store, Transport::Http],
        t => vec![t],
    };

    let mut reports = Vec::new();
    for spec in args.specs() {
        for t in &transports {
            let backend: Arc<dyn AdmissionBackend> = match (t, &args.worker_api_url) {
                (Transport::Store, _) => {
                    Arc::new(StoreBackend::connect(&database_url, spec.coordination_pool).await?)
                }
                (_, Some(url)) => {
                    Arc::new(HttpBackend::external(url, &args.worker_secret, &database_url).await?)
                }
                _ => Arc::new(HttpBackend::serve(&database_url, spec.coordination_pool).await?),
            };
            eprintln!(
                "[capacity] {} via {} ({}s warmup + {}s)...",
                spec.name,
                backend.name(),
                spec.warmup_secs,
                spec.run_secs
            );
            let r = harness::run_scenario(&spec, backend.clone(), Some(pg.clone()), &run_id).await;
            backend.shutdown().await;
            let r = r?;
            eprintln!(
                "[capacity]   {:.0} admissions/s, acquire p99 {:.2} ms, violations {}",
                r.admissions_per_sec,
                r.acquire_latency.p99_ms,
                r.cap_violations + r.sampled_cap_violations
            );
            reports.push(r);
        }
    }

    let violations: u64 = reports
        .iter()
        .map(|r| r.cap_violations + r.sampled_cap_violations)
        .sum();
    let table = report::table(&reports);
    let doc = RunReport {
        run_id,
        machine: Machine {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpus: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
            postgres: Some(pg.server_version.clone()).filter(|v| !v.is_empty()),
        },
        cap_violations: violations,
        passed: violations == 0,
        scenarios: reports,
    };
    match args.json.as_deref() {
        Some("-") => {
            eprint!("{table}");
            println!("{}", serde_json::to_string_pretty(&doc)?);
        }
        Some(path) => {
            print!("{table}");
            std::fs::write(path, serde_json::to_string_pretty(&doc)?)
                .with_context(|| format!("write {path}"))?;
            eprintln!("[capacity] JSON report written to {path}");
        }
        None => print!("{table}"),
    }
    if violations > 0 {
        eprintln!("[capacity] FAILED: {violations} cap violations");
        return Ok(1);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_reproduce_the_plan_table() {
        let args = Args::parse_from(["capacity", "--scenario", "all", "--duration-secs", "3"]);
        let specs = args.specs();
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "serial",
                "c32-one-cluster",
                "c32-eight-clusters",
                "c32-eight-clusters-pool2",
                "contended",
                "contended-open"
            ]
        );
        assert!(specs.iter().all(|s| s.run_secs == 3.0));
        assert_eq!(specs[3].coordination_pool, 2);
        assert_eq!(specs[2].clusters, 8);
        assert_eq!(specs[4].cap, 8);
    }

    #[test]
    fn rate_selects_open_loop_and_zero_cap_is_unlimited() {
        let args = Args::parse_from(["capacity", "--rate", "200", "--cap", "0"]);
        let s = &args.specs()[0];
        assert!(
            matches!(s.load, Load::Open { rate_per_sec, poisson: true, .. } if rate_per_sec == 200.0)
        );
        assert_eq!(s.cap, u64::MAX);
    }

    /// Runs one short scenario per transport against `DATABASE_URL`.
    /// `cargo test -p queryflux-bench -- --ignored`
    #[tokio::test]
    #[ignore = "needs DATABASE_URL"]
    async fn postgres_transports_hold_the_cap() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        backend::connect_store(&url, 1)
            .await
            .unwrap()
            .migrate()
            .await
            .unwrap();
        let args = Args::parse_from([
            "capacity",
            "--scenario",
            "contended",
            "--duration-secs",
            "2",
            "--warmup-secs",
            "0.2",
        ]);
        let mut spec = args.specs().remove(0);
        spec.retry = Retry {
            initial_ms: 2,
            max_ms: 20,
        };
        let pg = Arc::new(PgStatsProbe::connect(&url).await.unwrap());
        let backends: Vec<Arc<dyn AdmissionBackend>> = vec![
            Arc::new(StoreBackend::connect(&url, 8).await.unwrap()),
            Arc::new(HttpBackend::serve(&url, 8).await.unwrap()),
        ];
        for b in backends {
            let r = harness::run_scenario(&spec, b.clone(), Some(pg.clone()), "test")
                .await
                .unwrap();
            b.shutdown().await;
            assert_eq!(r.cap_violations, 0, "{}", r.transport);
            assert_eq!(r.sampled_cap_violations, 0, "{}", r.transport);
            assert_eq!(r.max_in_flight, 8, "{}", r.transport);
            assert!(r.denials > 0, "{}", r.transport);
            assert_eq!(
                r.acquire_errors + r.release_errors,
                0,
                "{:?}",
                r.error_samples
            );
            assert_eq!(r.leaked_leases, Some(0));
            assert!(r.pg_xacts_per_sec.unwrap() > 0.0);
        }
    }
}
