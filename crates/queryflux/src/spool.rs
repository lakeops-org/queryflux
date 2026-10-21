//! Delivery off the query path: a bounded queue that a background task drains to an HTTP
//! endpoint. Worker history (`history_uplink`) and OpenLineage events (`openlineage`) use it.
//!
//! Recording never waits on the network. When the endpoint is down the queue fills, the
//! oldest items are dropped and counted, and delivery resumes with backoff. A failed batch
//! goes back at the front of the queue, in order.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

/// How long an item may wait for a batch to fill.
pub(crate) const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// A failed upload. The batch is back in the spool.
#[derive(Debug)]
pub(crate) struct UploadError {
    pub(crate) message: String,
    /// The endpoint's `Retry-After`, when it sent one.
    pub(crate) retry_after: Option<Duration>,
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// `Retry-After` in seconds (the HTTP-date form is not used by QueryFlux), capped.
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs).min(MAX_BACKOFF))
}

/// The delay before the next attempt: the endpoint's `Retry-After` plus up to half again
/// (so a fleet told to wait does not come back in one wave), else exponential backoff.
pub(crate) fn next_backoff(previous: Duration, retry_after: Option<Duration>) -> Duration {
    match retry_after {
        Some(after) => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let jitter = after.mul_f64(f64::from(nanos % 1000) / 2000.0);
            (after + jitter).clamp(Duration::from_millis(100), MAX_BACKOFF)
        }
        None => (previous * 2).clamp(Duration::from_secs(1), MAX_BACKOFF),
    }
}

/// The bounded queue.
pub(crate) struct Spool<T> {
    items: Mutex<VecDeque<T>>,
    max: usize,
    wake: Notify,
}

impl<T> Spool<T> {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            max: max.max(1),
            wake: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<T>> {
        self.items.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queue items, dropping the oldest beyond the limit. `front` puts a failed batch back
    /// ahead of newer items, in order. Returns `(queued, dropped)`.
    pub(crate) fn push(&self, items: impl IntoIterator<Item = T>, front: bool) -> (usize, u64) {
        let mut spool = self.lock();
        if front {
            let items: Vec<T> = items.into_iter().collect();
            for item in items.into_iter().rev() {
                spool.push_front(item);
            }
        } else {
            spool.extend(items);
        }
        let mut dropped = 0u64;
        while spool.len() > self.max {
            spool.pop_front();
            dropped += 1;
        }
        (spool.len(), dropped)
    }

    /// Up to `n` items from the front.
    pub(crate) fn take(&self, n: usize) -> Vec<T> {
        let mut spool = self.lock();
        let n = spool.len().min(n);
        spool.drain(..n).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Flush now instead of at the next interval (a batch is full).
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn items(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.lock().iter().cloned().collect()
    }
}

/// A store that delivers its [`Spool`] with [`spawn_flusher`].
pub(crate) trait Uploader: Send + Sync + 'static {
    type Item: Send;
    /// What is delivered, for log lines ("Query history").
    const WHAT: &'static str;
    fn spool(&self) -> &Spool<Self::Item>;
    /// Send everything queued. Stops at the first failure, re-queuing that batch.
    fn flush(&self) -> impl Future<Output = Result<(), UploadError>> + Send;
}

/// Deliver until shutdown, then return. Call [`final_flush`] after the frontends drain,
/// so queries that finish while draining are sent too.
pub(crate) fn spawn_flusher<U: Uploader>(
    uploader: Arc<U>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut backoff = Duration::ZERO;
        loop {
            tokio::select! {
                _ = uploader.spool().wake.notified() => {}
                _ = tokio::time::sleep(FLUSH_INTERVAL) => {}
                _ = shutdown_rx.changed() => return,
            }
            if !backoff.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown_rx.changed() => return,
                }
            }
            match uploader.flush().await {
                Ok(()) => backoff = Duration::ZERO,
                Err(e) => {
                    backoff = next_backoff(backoff, e.retry_after);
                    tracing::warn!(retry_in = ?backoff, "{} upload failed: {e}", U::WHAT);
                }
            }
        }
    });
}

/// One last delivery attempt at shutdown, bounded by `timeout`.
pub(crate) async fn final_flush<U: Uploader>(uploader: &U, timeout: Duration) {
    match tokio::time::timeout(timeout, uploader.flush()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!("{} not delivered at shutdown: {e}", U::WHAT),
        Err(_) => tracing::warn!("{} delivery timed out at shutdown", U::WHAT),
    }
    let left = uploader.spool().len();
    if left > 0 {
        tracing::warn!(items = left, "{} lost at shutdown", U::WHAT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_without_retry_after_doubles_up_to_the_cap() {
        assert_eq!(next_backoff(Duration::ZERO, None), Duration::from_secs(1));
        assert_eq!(
            next_backoff(Duration::from_secs(4), None),
            Duration::from_secs(8)
        );
        assert_eq!(next_backoff(Duration::from_secs(50), None), MAX_BACKOFF);
    }

    #[test]
    fn a_full_spool_drops_the_oldest_and_a_failed_batch_keeps_its_place() {
        let spool = Spool::new(3);
        assert_eq!(spool.push([1, 2], false), (2, 0));
        assert_eq!(spool.push([3, 4, 5], false), (3, 2));
        let batch = spool.take(2);
        assert_eq!(batch, [3, 4]);
        spool.push([6], false);
        spool.push(batch, true);
        assert_eq!(
            spool.items(),
            [4, 5, 6],
            "requeued at the front, oldest dropped"
        );
    }
}
