mod cluster;
pub mod linear;
mod model;
mod rng;
mod runner;
mod trace;

pub use cluster::{ClusterSim, ClusterSimReport, SimNetwork, SimNetworkConfig};
pub use linear::{HistoryEntry, LinearizabilityChecker, Op, OpResult};

pub use kaya_io::{FaultKind, FaultRule, FaultSchedule, SimDisk, SimSeed};

/// Configuration for a single deterministic simulation run.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    pub seed: SimSeed,
    /// Total number of operations to execute.
    pub max_operations: u64,
    /// Number of distinct keys the generator may pick from.
    /// Keys are formatted as `key:{i:04x}`.
    pub keyspace_size: u64,
    /// Maximum byte length of a generated value.
    pub max_value_bytes: usize,
    // ── Operation weights (relative integers; sum must be > 0) ───────────────
    pub put_weight: u32,
    pub get_weight: u32,
    pub delete_weight: u32,
    pub scan_weight: u32,
    pub flush_weight: u32,
    pub compact_weight: u32,
    pub crash_weight: u32,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: SimSeed(0xdead_beef),
            max_operations: 1_000,
            keyspace_size: 100,
            max_value_bytes: 64,
            put_weight: 45,
            get_weight: 30,
            delete_weight: 10,
            scan_weight: 8,
            flush_weight: 3,
            compact_weight: 2,
            crash_weight: 2,
        }
    }
}

/// Report produced after a [`SimRunner`] run completes.
#[derive(Debug, Clone)]
pub struct SimulationReport {
    pub seed: SimSeed,
    pub operations_executed: u64,
    /// Non-empty when at least one invariant was violated.
    pub invariant_failures: Vec<String>,
    /// Full JSONL trace of all events and invariant checks.
    pub trace: String,
}

/// Drives a deterministic simulation against a real [`kaya_engine::Engine`]
/// on a [`SimDisk`].
pub struct SimRunner {
    config: SimulationConfig,
}

impl SimRunner {
    pub fn new(config: SimulationConfig) -> Self {
        Self { config }
    }

    /// Execute the simulation synchronously and return the final report.
    pub fn run(self) -> SimulationReport {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio current-thread runtime")
            .block_on(runner::run_async(self.config))
    }
}

/// Replay a JSONL trace produced by [`SimRunner::run`] against a fresh engine
/// and verify that GET and SCAN results match.
///
/// Returns `Ok(())` when no divergence is found, or `Err(detail)` listing
/// the mismatches.
pub fn replay_trace(trace_jsonl: &str) -> Result<(), String> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|e| e.to_string())?
        .block_on(runner::replay_async(trace_jsonl))
}

#[cfg(test)]
mod tests {
    use super::*;

    // KD-0603: 10 deterministic seeds × 1 000 ops — must produce zero
    // invariant violations.  This is the CI small-seed suite.
    #[test]
    fn sim_small_seed_suite() {
        const SEEDS: &[u64] = &[
            0xdead_beef,
            0x1234_5678,
            0xabcd_ef01,
            0x0bad_f00d,
            0xcafe_babe,
            0x1111_1111,
            0x2222_2222,
            0x3333_3333,
            0x4444_4444,
            0x5555_5555,
        ];
        for &seed in SEEDS {
            let config = SimulationConfig {
                seed: SimSeed(seed),
                max_operations: 1_000,
                ..SimulationConfig::default()
            };
            let report = SimRunner::new(config).run();
            assert!(
                report.invariant_failures.is_empty(),
                "seed 0x{seed:x}: invariant violations:\n{}",
                report.invariant_failures.join("\n")
            );
        }
    }

    // KD-0602: replay a short trace and verify no divergence.
    #[test]
    fn sim_replay_no_divergence() {
        let config = SimulationConfig {
            seed: SimSeed(0xdead_beef),
            max_operations: 200,
            ..SimulationConfig::default()
        };
        let report = SimRunner::new(config).run();
        assert!(
            report.invariant_failures.is_empty(),
            "base run had violations"
        );
        let result = replay_trace(&report.trace);
        assert!(result.is_ok(), "replay diverged: {}", result.unwrap_err());
    }
}
