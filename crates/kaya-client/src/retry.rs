//! Retry policy and client observability hooks.
//!
//! [`RetryPolicy`] separates the *retry* budget from the leader-redirect budget
//! and adds exponential backoff with optional full jitter plus a per-attempt
//! request timeout — none of which the original fixed 60 ms sleep provided.
//!
//! [`ClientObserver`] lets callers plug their own metrics/tracing stack in
//! without the client taking a dependency on any specific framework.

use std::sync::Arc;
use std::time::Duration;

/// Controls how a [`crate::KayaClient`] retries transient failures.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of *attempts* per operation (including the first), for
    /// transport errors and timeouts. Leader redirects have their own budget
    /// (`max_redirects`) so a redirect does not consume a retry.
    pub max_attempts: usize,
    /// Backoff before the first retry; doubles each subsequent retry.
    pub base_backoff: Duration,
    /// Upper bound on a single backoff interval.
    pub max_backoff: Duration,
    /// Apply full jitter (uniform in `[0, computed]`) to each backoff. Keeps
    /// many clients from retrying in lockstep after a shared failure.
    pub jitter: bool,
    /// Per-attempt timeout. `None` disables the timeout (an attempt may block
    /// indefinitely on a slow server).
    pub request_timeout: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
            jitter: true,
            request_timeout: Some(Duration::from_secs(5)),
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries and never times out (matches the historical
    /// single-shot behavior before configurable retries existed).
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            jitter: false,
            request_timeout: None,
        }
    }

    /// Backoff duration before the retry that follows `retry_index` (0-based:
    /// `0` is the wait before the first retry). `rng_state` is advanced to make
    /// jitter reproducible and testable without a global RNG.
    pub fn backoff(&self, retry_index: u32, rng_state: &mut u64) -> Duration {
        // Exponential: base * 2^retry_index, saturating and capped at max.
        let base_us = self.base_backoff.as_micros() as u64;
        let scaled = base_us.saturating_mul(1u64.checked_shl(retry_index).unwrap_or(u64::MAX));
        let capped = scaled.min(self.max_backoff.as_micros() as u64);
        if !self.jitter || capped == 0 {
            return Duration::from_micros(capped);
        }
        // Full jitter: uniform in [0, capped]. xorshift64* keeps it deterministic.
        let mut x = *rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *rng_state = x;
        let r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        Duration::from_micros(r % (capped + 1))
    }
}

/// Outcome of a client operation, reported to a [`ClientObserver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpOutcome {
    /// Server returned `STATUS_OK`.
    Ok,
    /// GET returned `STATUS_NOT_FOUND`.
    NotFound,
    /// Server returned `STATUS_INVALID_ARGUMENT`.
    InvalidArgument,
    /// Server returned an error status.
    ServerError,
    /// The attempt timed out.
    Timeout,
    /// A transport/connection error occurred and the budget was exhausted.
    ConnectionError,
}

/// A single completed client operation, including how many attempts it took.
#[derive(Debug, Clone)]
pub struct OpObservation {
    /// Protocol opcode (1 = PUT, 2 = GET, …).
    pub opcode: u8,
    /// Total attempts made (1 when it succeeded first try).
    pub attempts: usize,
    /// Number of leader redirects followed.
    pub redirects: usize,
    /// Final outcome.
    pub outcome: OpOutcome,
    /// End-to-end latency across all attempts.
    pub latency: Duration,
}

/// Hook for per-operation metrics/tracing. Implementations must be cheap and
/// non-blocking; the client calls [`on_operation`](ClientObserver::on_operation)
/// on the hot path once per completed operation.
pub trait ClientObserver: Send + Sync {
    fn on_operation(&self, obs: &OpObservation);
}

/// Convenience: any `Fn(&OpObservation)` can be used as an observer.
impl<F> ClientObserver for F
where
    F: Fn(&OpObservation) + Send + Sync,
{
    fn on_operation(&self, obs: &OpObservation) {
        self(obs)
    }
}

/// Shared, cloneable observer handle stored by the client.
pub type SharedObserver = Arc<dyn ClientObserver>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(400),
            jitter: false,
            request_timeout: None,
        };
        let mut s = 1;
        assert_eq!(policy.backoff(0, &mut s), Duration::from_millis(100));
        assert_eq!(policy.backoff(1, &mut s), Duration::from_millis(200));
        assert_eq!(policy.backoff(2, &mut s), Duration::from_millis(400));
        // Capped.
        assert_eq!(policy.backoff(3, &mut s), Duration::from_millis(400));
        assert_eq!(policy.backoff(30, &mut s), Duration::from_millis(400));
    }

    #[test]
    fn jitter_stays_within_bounds_and_is_deterministic() {
        let policy = RetryPolicy {
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            jitter: true,
            ..RetryPolicy::default()
        };
        let mut s1 = 42;
        let mut s2 = 42;
        for retry in 0..6 {
            let a = policy.backoff(retry, &mut s1);
            let b = policy.backoff(retry, &mut s2);
            assert_eq!(a, b, "same seed must yield same jitter");
            let cap = (100u128 << retry).min(1000) as u64;
            assert!(a.as_millis() as u64 <= cap, "jitter exceeded cap");
        }
    }

    #[test]
    fn none_policy_is_single_shot() {
        let p = RetryPolicy::none();
        assert_eq!(p.max_attempts, 1);
        assert!(p.request_timeout.is_none());
    }

    #[test]
    fn closure_is_an_observer() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let obs: SharedObserver = Arc::new(move |_o: &OpObservation| {
            c.fetch_add(1, Ordering::SeqCst);
        });
        obs.on_operation(&OpObservation {
            opcode: 1,
            attempts: 1,
            redirects: 0,
            outcome: OpOutcome::Ok,
            latency: Duration::ZERO,
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
