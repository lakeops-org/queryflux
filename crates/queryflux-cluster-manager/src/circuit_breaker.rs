use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use queryflux_core::config::CircuitBreakerConfig;
use serde::{Deserialize, Serialize};

/// A backend request's effect on cluster availability. SQL errors and client
/// cancellations are not failures of the backend's availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendOutcome {
    Success,
    Failure,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    outcome: BackendOutcome,
}

#[derive(Debug)]
struct Inner {
    state: CircuitState,
    samples: VecDeque<Sample>,
    failures: usize,
    timeouts: usize,
    retry_at: Option<Instant>,
    backoff: Duration,
}

/// Local breaker state for one runtime cluster. Its mutex is held only for
/// counters and transitions, never during an adapter call.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let backoff = Duration::from_secs(config.initial_backoff_secs);
        Self {
            config,
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                samples: VecDeque::new(),
                failures: 0,
                timeouts: 0,
                retry_at: None,
                backoff,
            }),
        }
    }

    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn state(&self) -> CircuitState {
        self.lock().state
    }

    pub fn allows_queries(&self) -> bool {
        self.state() == CircuitState::Closed
    }

    pub fn record(&self, outcome: BackendOutcome) -> Option<CircuitState> {
        self.record_at(outcome, Instant::now())
    }

    fn record_at(&self, outcome: BackendOutcome, now: Instant) -> Option<CircuitState> {
        let mut inner = self.lock();
        if inner.state != CircuitState::Closed {
            return None;
        }
        inner.samples.push_back(Sample { at: now, outcome });
        inner.failures += usize::from(outcome != BackendOutcome::Success);
        inner.timeouts += usize::from(outcome == BackendOutcome::Timeout);
        let window = Duration::from_secs(self.config.window_secs);
        while inner
            .samples
            .front()
            .is_some_and(|sample| now.saturating_duration_since(sample.at) > window)
        {
            inner.evict_oldest();
        }
        // Keeping at most min_requests / threshold samples is insufficient for
        // a time window. Bound memory by a large fixed cap while retaining the
        // most recent evidence under very high request rates.
        const MAX_SAMPLES: usize = 10_000;
        while inner.samples.len() > MAX_SAMPLES {
            inner.evict_oldest();
        }
        let total = inner.samples.len();
        if total < self.config.min_requests {
            return None;
        }
        if inner.failures * 100 >= total * usize::from(self.config.failure_rate_percent)
            || inner.timeouts * 100 >= total * usize::from(self.config.timeout_rate_percent)
        {
            inner.state = CircuitState::Open;
            inner.retry_at = Some(now + inner.backoff);
            inner.samples.clear();
            inner.failures = 0;
            inner.timeouts = 0;
            return Some(CircuitState::Open);
        }
        None
    }

    /// Reserve the sole half-open health probe when the backoff has expired.
    /// Queries remain excluded until the probe completes successfully.
    pub fn begin_probe(&self) -> bool {
        self.begin_probe_at(Instant::now())
    }

    fn begin_probe_at(&self, now: Instant) -> bool {
        let mut inner = self.lock();
        if inner.state != CircuitState::Open || inner.retry_at.is_none_or(|at| now < at) {
            return false;
        }
        inner.state = CircuitState::HalfOpen;
        true
    }

    pub fn finish_probe(&self, healthy: bool) -> CircuitState {
        self.finish_probe_at(healthy, Instant::now())
    }

    fn finish_probe_at(&self, healthy: bool, now: Instant) -> CircuitState {
        let mut inner = self.lock();
        if inner.state != CircuitState::HalfOpen {
            return inner.state;
        }
        if healthy {
            inner.state = CircuitState::Closed;
            inner.samples.clear();
            inner.failures = 0;
            inner.timeouts = 0;
            inner.retry_at = None;
            inner.backoff = Duration::from_secs(self.config.initial_backoff_secs);
        } else {
            inner.state = CircuitState::Open;
            inner.backoff = inner
                .backoff
                .saturating_mul(2)
                .min(Duration::from_secs(self.config.max_backoff_secs));
            inner.retry_at = Some(now + inner.backoff);
        }
        inner.state
    }
}

impl Inner {
    fn evict_oldest(&mut self) {
        if let Some(sample) = self.samples.pop_front() {
            self.failures -= usize::from(sample.outcome != BackendOutcome::Success);
            self.timeouts -= usize::from(sample.outcome == BackendOutcome::Timeout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            window_secs: 10,
            min_requests: 4,
            failure_rate_percent: 50,
            timeout_rate_percent: 25,
            initial_backoff_secs: 2,
            max_backoff_secs: 8,
        }
    }

    #[test]
    fn opens_on_rolling_failures_and_recovers_after_one_probe() {
        let breaker = CircuitBreaker::new(config());
        let start = Instant::now();
        assert_eq!(breaker.record_at(BackendOutcome::Success, start), None);
        assert_eq!(breaker.record_at(BackendOutcome::Success, start), None);
        assert_eq!(breaker.record_at(BackendOutcome::Failure, start), None);
        assert_eq!(
            breaker.record_at(BackendOutcome::Failure, start),
            Some(CircuitState::Open)
        );
        assert!(!breaker.allows_queries());
        assert!(!breaker.begin_probe_at(start + Duration::from_secs(1)));
        assert!(breaker.begin_probe_at(start + Duration::from_secs(2)));
        assert!(!breaker.begin_probe_at(start + Duration::from_secs(2)));
        assert!(!breaker.allows_queries());
        assert_eq!(
            breaker.finish_probe_at(true, start + Duration::from_secs(2)),
            CircuitState::Closed
        );
        assert!(breaker.allows_queries());
    }

    #[test]
    fn failed_probes_back_off_exponentially() {
        let breaker = CircuitBreaker::new(config());
        let start = Instant::now();
        for _ in 0..4 {
            breaker.record_at(BackendOutcome::Timeout, start);
        }
        assert!(breaker.begin_probe_at(start + Duration::from_secs(2)));
        breaker.finish_probe_at(false, start + Duration::from_secs(2));
        assert!(!breaker.begin_probe_at(start + Duration::from_secs(5)));
        assert!(breaker.begin_probe_at(start + Duration::from_secs(6)));
        breaker.finish_probe_at(false, start + Duration::from_secs(6));
        assert!(!breaker.begin_probe_at(start + Duration::from_secs(13)));
        assert!(breaker.begin_probe_at(start + Duration::from_secs(14)));
    }

    #[test]
    fn old_failures_age_out_of_window() {
        let breaker = CircuitBreaker::new(config());
        let start = Instant::now();
        breaker.record_at(BackendOutcome::Failure, start);
        breaker.record_at(BackendOutcome::Failure, start);
        let later = start + Duration::from_secs(11);
        for _ in 0..4 {
            breaker.record_at(BackendOutcome::Success, later);
        }
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn bounded_window_evicts_old_failures_from_counters() {
        let mut config = config();
        config.min_requests = 10_000;
        config.failure_rate_percent = 100;
        config.timeout_rate_percent = 100;
        let breaker = CircuitBreaker::new(config);
        let now = Instant::now();
        for _ in 0..9_999 {
            breaker.record_at(BackendOutcome::Failure, now);
        }
        for _ in 0..10_000 {
            breaker.record_at(BackendOutcome::Success, now);
        }
        assert_eq!(breaker.state(), CircuitState::Closed);
    }
}
