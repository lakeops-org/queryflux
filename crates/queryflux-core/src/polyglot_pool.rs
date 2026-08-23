//! Dedicated worker pool for `polyglot-sql` parse/generate work.
//!
//! `polyglot-sql` is a recursive-descent parser that can overflow the default
//! Tokio worker stack (~2 MiB). All call sites that invoke `polyglot_sql::parse`
//! (or similarly deep AST work) on a request path should go through [`run`].

use std::sync::{mpsc, Arc, LazyLock, Mutex};

/// Bounded queue depth for polyglot parse jobs.
const QUEUE_CAPACITY: usize = 128;

/// polyglot-sql needs a large stack; tokio workers use ~2 MiB by default.
const POLYGLOT_STACK_SIZE: usize = 16 * 1024 * 1024;

static POOL_TX: LazyLock<mpsc::SyncSender<Box<dyn FnOnce() + Send>>> = LazyLock::new(|| {
    let (tx, rx) = mpsc::sync_channel::<Box<dyn FnOnce() + Send>>(QUEUE_CAPACITY);
    let rx = Arc::new(Mutex::new(rx));
    let workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .clamp(2, 8);
    for i in 0..workers {
        let rx = Arc::clone(&rx);
        std::thread::Builder::new()
            .stack_size(POLYGLOT_STACK_SIZE)
            .name(format!("polyglot-{i}"))
            .spawn(move || loop {
                // Scope the lock to `recv()` only: `while let Ok(job) = rx.lock()...recv() {
                // job() }` would extend the guard's temporary scope across the loop body,
                // holding the lock for the parse itself and serializing every "parallel"
                // worker onto one mutex — the pool would do real work one job at a time
                // regardless of thread count.
                let job = {
                    let guard = rx.lock().unwrap_or_else(|e| e.into_inner());
                    match guard.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                };
                // Isolate a panicking parse (e.g. a polyglot-sql bug on adversarial input)
                // to this one job. Without `catch_unwind`, a panic here would unwind the
                // thread and permanently shrink the pool by one worker; this call site is
                // now on the primary query-dispatch path (guard checks, ADBC routing), not
                // just the original fingerprinting use, so losing workers over time would
                // eventually stall every query that needs SQL classification.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
            })
            .expect("failed to spawn polyglot worker thread");
    }
    tx
});

/// Run `job` on a pooled thread with a 16 MiB stack (required for polyglot-sql).
pub fn run<R: Send + 'static>(job: impl FnOnce() -> R + Send + 'static) -> Option<R> {
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let task = Box::new(move || {
        let _ = done_tx.send(job());
    });

    match POOL_TX.send(task) {
        Ok(()) => done_rx.recv().ok(),
        Err(err) => {
            let task = err.0;
            std::thread::Builder::new()
                .stack_size(POLYGLOT_STACK_SIZE)
                .spawn(task)
                .ok()?
                .join()
                .ok()?;
            done_rx.recv().ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run;
    use std::time::{Duration, Instant};

    /// A panic inside one job must not poison the pool for subsequent jobs — the
    /// worker that ran it must keep serving the queue.
    #[test]
    fn panic_in_one_job_does_not_poison_later_jobs() {
        assert_eq!(run(|| -> i32 { panic!("boom") }), None);
        assert_eq!(run(|| 42), Some(42));
    }

    /// The lock guarding the shared `Receiver` must be released before running the
    /// job, not held for its duration — otherwise every "parallel" worker thread
    /// serializes onto that one mutex and the pool never runs jobs concurrently.
    /// With at least 2 workers, N jobs that each sleep `D` must finish in well
    /// under `N * D` wall-clock time.
    #[test]
    fn jobs_run_concurrently_not_serialized_on_the_queue_lock() {
        const JOBS: usize = 4;
        const SLEEP: Duration = Duration::from_millis(150);

        let start = Instant::now();
        let handles: Vec<_> = (0..JOBS)
            .map(|_| {
                std::thread::spawn(|| {
                    run(|| {
                        std::thread::sleep(SLEEP);
                    })
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = start.elapsed();

        // Fully serialized would take JOBS * SLEEP (600ms); with >=2 real workers
        // running concurrently it should land well under that.
        assert!(
            elapsed < SLEEP * (JOBS as u32) - SLEEP / 2,
            "jobs took {elapsed:?}, expected well under {:?} if actually concurrent",
            SLEEP * (JOBS as u32)
        );
    }
}
