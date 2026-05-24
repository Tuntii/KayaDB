mod cluster;
pub mod control;
pub mod linear;
mod model;
mod rng;
mod runner;
mod trace;

pub use cluster::{ClusterSim, ClusterSimReport, SimNetwork, SimNetworkConfig};
pub use control::NodeController;
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


    fn find_server_binary() -> std::path::PathBuf {
        let mut exe = std::env::current_exe().expect("failed to get current exe path");
        exe.pop(); // remove exe filename
        if exe.file_name().and_then(|s| s.to_str()) == Some("deps") {
            exe.pop(); // remove deps
        }
        #[cfg(target_os = "windows")]
        let bin_name = "kayadb-server.exe";
        #[cfg(not(target_os = "windows"))]
        let bin_name = "kayadb-server";
        
        exe.join(bin_name)
    }

    fn get_free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn test_jepsen_node_controller_lifecycle() {
        let binary_path = find_server_binary();
        if !binary_path.exists() {
            // Run cargo build -p kaya-server --bin kayadb-server to ensure it is built
            let mut build_cmd = std::process::Command::new("cargo");
            build_cmd.arg("build").arg("-p").arg("kaya-server").arg("--bin").arg("kayadb-server");
            let status = build_cmd.status().expect("failed to execute cargo build");
            assert!(status.success(), "failed to build kayadb-server binary");

        }

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_node_ctrl_test_{}", test_id));

        let client_port = get_free_port();
        let raft_port = get_free_port();

        // Spawn node controller
        let mut node = NodeController::spawn(
            1,
            &binary_path,
            &data_dir,
            client_port,
            raft_port,
            &[],
        ).expect("failed to spawn node");

        let client_addr = format!("127.0.0.1:{}", client_port);

        // Wait for it to start up
        let mut connected = false;
        for _ in 0..50 {
            if std::net::TcpStream::connect(&client_addr).is_ok() {
                connected = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(connected, "Failed to connect to spawned node");

        // Now pause the process
        node.pause().expect("failed to pause process");

        // Give OS a split second to apply pause
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Resume the process
        node.resume().expect("failed to resume process");

        // Verify we can still connect/communicate
        let mut reconnected = false;
        for _ in 0..20 {
            if std::net::TcpStream::connect(&client_addr).is_ok() {
                reconnected = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(reconnected, "Failed to reconnect to node after resuming");

        // Now stop the process
        node.stop().expect("failed to stop process");

        // Verify port is freed and we cannot connect anymore
        let mut stopped = false;
        for _ in 0..50 {
            if std::net::TcpStream::connect(&client_addr).is_err() {
                stopped = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(stopped, "Port was not freed/closed after stopping");

        // Clean up data directory
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // KD-0604: Large-Seed Simulation Burn-In (100 Seeds x 10,000 Ops).
    // Marked as #[ignore] so it is not run during standard rapid CI, but can
    // be run manually using `cargo test -p kaya-sim --lib -- --ignored`.
    #[test]
    #[ignore]
    fn sim_large_seed_burn_in() {
        println!("[burn-in] starting 100-seed burn-in stress test (10,000 operations per seed)...");
        let start = std::time::Instant::now();
        
        // Run 100 seeds. We can generate deterministic seeds using a simple linear congruential generator or simple sequence.
        for i in 1..=100 {
            let seed = 0xf00d_0000_u64 + (i * 1337);
            let config = SimulationConfig {
                seed: SimSeed(seed),
                max_operations: 10_000,
                ..SimulationConfig::default()
            };
            
            let seed_start = std::time::Instant::now();
            let report = SimRunner::new(config).run();
            
            assert!(
                report.invariant_failures.is_empty(),
                "seed 0x{seed:x} (run #{i}) failed with invariant violations:\n{}",
                report.invariant_failures.join("\n")
            );
            
            println!(
                "[burn-in] seed 0x{seed:x} (run #{i}/100) completed successfully in {:?}",
                seed_start.elapsed()
            );
        }
        
        println!(
            "[burn-in] success! All 100 seeds completed with zero invariant violations in {:?}",
            start.elapsed()
        );
    }
}

