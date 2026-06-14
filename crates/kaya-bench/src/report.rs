//! Benchmark report metadata per `spec/docs/benchmarking-spec.md` §4.

use std::env;

/// Context captured alongside a benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub git_commit: String,
    pub build_profile: String,
    pub os: String,
    pub arch: String,
    pub rustc_version: String,
    pub bench_name: String,
    pub durability_mode: String,
    pub dataset_ops: u64,
    pub throughput_ops_per_sec: Option<f64>,
    pub latency_avg_ns: Option<u64>,
}

impl BenchmarkReport {
    pub fn capture_context(bench_name: &str, durability_mode: &str, dataset_ops: u64) -> Self {
        Self {
            git_commit: env::var("KAYADB_GIT_COMMIT").unwrap_or_else(|_| "unknown".to_owned()),
            build_profile: if cfg!(debug_assertions) {
                "debug".to_owned()
            } else {
                "release".to_owned()
            },
            os: env::consts::OS.to_owned(),
            arch: env::consts::ARCH.to_owned(),
            rustc_version: env::var("KAYADB_RUSTC").unwrap_or_else(|_| "unknown".to_owned()),
            bench_name: bench_name.to_owned(),
            durability_mode: durability_mode.to_owned(),
            dataset_ops,
            throughput_ops_per_sec: None,
            latency_avg_ns: None,
        }
    }

    pub fn with_timing(mut self, avg_ns: u64, ops: u64) -> Self {
        self.latency_avg_ns = Some(avg_ns);
        if avg_ns > 0 {
            self.throughput_ops_per_sec = Some(ops as f64 * 1_000_000_000.0 / avg_ns as f64);
        }
        self
    }

    pub fn to_markdown(&self) -> String {
        format!(
            "## Benchmark: {}\n\n\
             | Field | Value |\n|---|---|\n\
             | KayaDB commit | {} |\n\
             | Build profile | {} |\n\
             | OS | {} |\n\
             | Arch | {} |\n\
             | Rustc | {} |\n\
             | Durability mode | {} |\n\
             | Dataset ops | {} |\n\
             | Throughput | {} |\n\
             | Avg latency | {} |\n",
            self.bench_name,
            self.git_commit,
            self.build_profile,
            self.os,
            self.arch,
            self.rustc_version,
            self.durability_mode,
            self.dataset_ops,
            self.throughput_ops_per_sec
                .map(|t| format!("{t:.0} ops/sec"))
                .unwrap_or_else(|| "n/a".to_owned()),
            self.latency_avg_ns
                .map(|n| format!("{n} ns"))
                .unwrap_or_else(|| "n/a".to_owned()),
        )
    }

    pub fn to_jsonl(&self) -> String {
        format!(
            r#"{{"bench":"{}","commit":"{}","profile":"{}","os":"{}","arch":"{}","rustc":"{}","durability":"{}","ops":{},"throughput":{},"latency_ns":{}}}"#,
            self.bench_name,
            self.git_commit,
            self.build_profile,
            self.os,
            self.arch,
            self.rustc_version,
            self.durability_mode,
            self.dataset_ops,
            self.throughput_ops_per_sec
                .map(|t| t.to_string())
                .unwrap_or_else(|| "null".to_owned()),
            self.latency_avg_ns
                .map(|n| n.to_string())
                .unwrap_or_else(|| "null".to_owned()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_contains_required_fields() {
        let r = BenchmarkReport::capture_context("smoke_put_get", "relaxed", 10);
        let md = r.to_markdown();
        assert!(md.contains("Durability mode"));
        assert!(md.contains("relaxed"));
    }
}