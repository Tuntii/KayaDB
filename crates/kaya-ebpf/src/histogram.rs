use crate::event::{ProbeEvent, SyscallKind};

/// Prometheus histogram bucket upper bounds (microseconds).
pub const FSYNC_LATENCY_BUCKETS_US: &[u64] = &[50, 100, 250, 500, 1_000, 5_000, 10_000, 50_000];

/// Aggregated fsync/fdatasync latency histogram for Prometheus exposition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsyncHistogram {
    pub fsync_buckets: Vec<u64>,
    pub fdatasync_buckets: Vec<u64>,
    pub fsync_count: u64,
    pub fdatasync_count: u64,
    pub fsync_sum_us: u64,
    pub fdatasync_sum_us: u64,
}

impl FsyncHistogram {
    pub fn new() -> Self {
        let bucket_len = FSYNC_LATENCY_BUCKETS_US.len() + 1;
        Self {
            fsync_buckets: vec![0; bucket_len],
            fdatasync_buckets: vec![0; bucket_len],
            fsync_count: 0,
            fdatasync_count: 0,
            fsync_sum_us: 0,
            fdatasync_sum_us: 0,
        }
    }

    pub fn observe(&mut self, syscall: SyscallKind, latency_us: u64) {
        let bucket_idx = bucket_index(latency_us);
        match syscall {
            SyscallKind::Fsync => {
                self.fsync_buckets[bucket_idx] += 1;
                self.fsync_count += 1;
                self.fsync_sum_us += latency_us;
            }
            SyscallKind::Fdatasync => {
                self.fdatasync_buckets[bucket_idx] += 1;
                self.fdatasync_count += 1;
                self.fdatasync_sum_us += latency_us;
            }
        }
    }

    pub fn ingest(&mut self, event: &ProbeEvent) {
        if let ProbeEvent::FsyncLatency {
            syscall,
            latency_us,
            ..
        } = event
        {
            self.observe(*syscall, *latency_us);
        }
    }

    pub fn total_count(&self) -> u64 {
        self.fsync_count + self.fdatasync_count
    }

    pub fn has_nonzero_observations(&self) -> bool {
        self.total_count() > 0 && (self.fsync_sum_us > 0 || self.fdatasync_sum_us > 0)
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        render_syscall_histogram(
            &mut out,
            "kaya_ebpf_fsync_latency_us",
            "kernel-slot fsync latency (microseconds)",
            SyscallKind::Fsync,
            &self.fsync_buckets,
            self.fsync_count,
            self.fsync_sum_us,
        );
        render_syscall_histogram(
            &mut out,
            "kaya_ebpf_fdatasync_latency_us",
            "kernel-slot fdatasync latency (microseconds)",
            SyscallKind::Fdatasync,
            &self.fdatasync_buckets,
            self.fdatasync_count,
            self.fdatasync_sum_us,
        );
        out
    }
}

fn bucket_index(latency_us: u64) -> usize {
    FSYNC_LATENCY_BUCKETS_US
        .iter()
        .position(|&upper| latency_us <= upper)
        .unwrap_or(FSYNC_LATENCY_BUCKETS_US.len())
}

fn render_syscall_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    syscall: SyscallKind,
    buckets: &[u64],
    count: u64,
    sum_us: u64,
) {
    let label = syscall.as_str();
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} histogram\n"));
    let mut cumulative = 0u64;
    for (idx, &upper) in FSYNC_LATENCY_BUCKETS_US.iter().enumerate() {
        cumulative += buckets[idx];
        out.push_str(&format!(
            "{name}_bucket{{syscall=\"{label}\",le=\"{upper}\"}} {cumulative}\n"
        ));
    }
    cumulative += buckets[FSYNC_LATENCY_BUCKETS_US.len()];
    out.push_str(&format!(
        "{name}_bucket{{syscall=\"{label}\",le=\"+Inf\"}} {cumulative}\n"
    ));
    out.push_str(&format!("{name}_count{{syscall=\"{label}\"}} {count}\n"));
    out.push_str(&format!("{name}_sum{{syscall=\"{label}\"}} {sum_us}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_observes_and_renders_non_zero_buckets() {
        let mut hist = FsyncHistogram::new();
        hist.observe(SyscallKind::Fsync, 75);
        hist.observe(SyscallKind::Fsync, 300);
        let body = hist.render_prometheus();
        assert!(body.contains("kaya_ebpf_fsync_latency_us_bucket"));
        assert!(body.contains("kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"} 2"));
        assert!(body.contains("kaya_ebpf_fsync_latency_us_sum{syscall=\"fsync\"} 375"));
    }
}
