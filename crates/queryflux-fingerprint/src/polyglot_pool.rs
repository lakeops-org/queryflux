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
            .spawn(move || {
                while let Ok(job) = rx.lock().unwrap().recv() {
                    job();
                }
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
