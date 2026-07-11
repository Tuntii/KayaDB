//! A compact, dependency-free latency histogram with fixed microsecond buckets.
//!
//! Bucket bounds are Prometheus-friendly (`le` upper bounds in microseconds) so
//! the same structure serves both a human `p50`/`p99` summary and Prometheus
//! `_bucket{le=…}` exposition. Percentiles are bucket-boundary estimates, not
//! exact samples — the histogram keeps `O(1)` memory regardless of load.

/// Inclusive upper bounds (microseconds) for each finite bucket. A trailing
/// implicit `+Inf` bucket catches everything larger.
pub const LATENCY_BUCKET_BOUNDS_US: [u64; 13] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
];

/// Fixed-bucket latency histogram over microsecond samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyHistogram {
    /// One counter per finite bound plus a trailing `+Inf` overflow bucket.
    buckets: [u64; LATENCY_BUCKET_BOUNDS_US.len() + 1],
    count: u64,
    sum_us: u64,
    max_us: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        Self {
            buckets: [0; LATENCY_BUCKET_BOUNDS_US.len() + 1],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }

    /// Record one latency sample in microseconds.
    pub fn observe(&mut self, us: u64) {
        let idx = LATENCY_BUCKET_BOUNDS_US
            .iter()
            .position(|&bound| us <= bound)
            .unwrap_or(LATENCY_BUCKET_BOUNDS_US.len());
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum_us += us;
        if us > self.max_us {
            self.max_us = us;
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sum_us(&self) -> u64 {
        self.sum_us
    }

    pub fn max_us(&self) -> u64 {
        self.max_us
    }

    /// Mean latency in microseconds, or `0` when no samples were recorded.
    pub fn mean_us(&self) -> u64 {
        self.sum_us.checked_div(self.count).unwrap_or(0)
    }

    /// Estimate the `p` (0.0–1.0) percentile as the upper bound of the bucket
    /// that contains it. Returns `0` when empty; the `+Inf` bucket reports
    /// `max_us` so the estimate stays finite.
    pub fn percentile_us(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let p = p.clamp(0.0, 1.0);
        // Rank of the target sample (1-based).
        let target = ((self.count as f64) * p).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (i, &bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket;
            if cumulative >= target {
                return match LATENCY_BUCKET_BOUNDS_US.get(i) {
                    Some(&bound) => bound,
                    None => self.max_us, // +Inf bucket
                };
            }
        }
        self.max_us
    }

    /// Merge another histogram into this one (e.g. aggregating shards).
    pub fn merge(&mut self, other: &LatencyHistogram) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += *b;
        }
        self.count += other.count;
        self.sum_us += other.sum_us;
        if other.max_us > self.max_us {
            self.max_us = other.max_us;
        }
    }

    /// Cumulative bucket counts paired with their `le` upper bound in
    /// microseconds, for Prometheus `_bucket{le=…}` lines. The final entry uses
    /// `u64::MAX` to represent `+Inf`.
    pub fn cumulative_buckets(&self) -> Vec<(u64, u64)> {
        let mut out = Vec::with_capacity(self.buckets.len());
        let mut cumulative = 0u64;
        for (i, &bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket;
            let le = LATENCY_BUCKET_BOUNDS_US.get(i).copied().unwrap_or(u64::MAX);
            out.push((le, cumulative));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_reports_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile_us(0.5), 0);
        assert_eq!(h.mean_us(), 0);
    }

    #[test]
    fn observe_tracks_count_sum_max() {
        let mut h = LatencyHistogram::new();
        h.observe(5);
        h.observe(30);
        h.observe(200);
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum_us(), 235);
        assert_eq!(h.max_us(), 200);
        assert_eq!(h.mean_us(), 78);
    }

    #[test]
    fn percentiles_fall_in_expected_buckets() {
        let mut h = LatencyHistogram::new();
        // 90 samples at ~5us, 10 samples at ~9000us.
        for _ in 0..90 {
            h.observe(5);
        }
        for _ in 0..10 {
            h.observe(9_000);
        }
        // p50 lands in the smallest bucket (<=10us).
        assert_eq!(h.percentile_us(0.5), 10);
        // p99 lands in the tail bucket (<=10000us).
        assert_eq!(h.percentile_us(0.99), 10_000);
    }

    #[test]
    fn overflow_bucket_reports_max() {
        let mut h = LatencyHistogram::new();
        h.observe(1_000_000); // beyond the largest finite bound
        assert_eq!(h.percentile_us(0.99), 1_000_000);
        let cum = h.cumulative_buckets();
        assert_eq!(cum.last().unwrap().0, u64::MAX);
        assert_eq!(cum.last().unwrap().1, 1);
    }

    #[test]
    fn merge_combines_two_histograms() {
        let mut a = LatencyHistogram::new();
        a.observe(5);
        let mut b = LatencyHistogram::new();
        b.observe(9_000);
        a.merge(&b);
        assert_eq!(a.count(), 2);
        assert_eq!(a.sum_us(), 9_005);
        assert_eq!(a.max_us(), 9_000);
    }
}
