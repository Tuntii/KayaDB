//! Runnable Jepsen-style demo for KayaDB.
//!
//! This example drives a live 3-node cluster using the real kaya-client,
//! injects failures via the nemesis (including network partition), records
//! history, and verifies sequential linearizability.
//!
//! ## Prerequisites (run from repo root)
//!
//! 1. Build the server:
//!    cargo build -p kaya-server
//!
//! 2. (Recommended) Start the cluster using the cross-platform scripts.
//!    On Windows (from an **Administrator** PowerShell for firewall/partition tests):
//!      $env:ClusterDir = "$env:TEMP\kayadb-jepsen-demo"
//!      powershell -ExecutionPolicy Bypass -File scripts/start-cluster.ps1 -ClusterDir $env:ClusterDir -KayaServer "target\debug\kayadb-server.exe"
//!
//!    On Linux/macOS:
//!      CLUSTER_DIR=/tmp/kayadb-jepsen-demo ./scripts/start-cluster.sh
//!
//!    Wait ~5-8 seconds for Raft leader election.
//!
//! 3. Run this demo (it will connect to the default ports):
//!    cargo run -p kaya-jepsen-test --example jepsen_demo
//!
//! 4. Stop the cluster when done:
//!    powershell -File scripts/stop-cluster.ps1 -ClusterDir $env:ClusterDir
//!    (or the .sh equivalent)
//!
//! ## What it demonstrates (task 2 + 3)
//! - Full end-to-end: real servers + real clients + history recording
//! - Nemesis: Partition (newly implemented cross-platform via firewall/iptables scripts)
//! - Linearizability check using kaya-sim::LinearizabilityChecker
//! - Trace export on failure for debugging
//!
//! NOTE: Partition rules typically require elevated privileges. If the script
//! cannot install firewall rules, the test will still run (clients will see
//! more errors/timeouts, which are recorded and tolerated by the checker).

use kaya_jepsen_test::{NemesisConfig, NemesisType, TestConfig, TestRunner, WorkloadConfig};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== KayaDB Jepsen Demo (Partition nemesis) ===");
    println!("This will run a short Register workload against a live cluster");
    println!("while periodically partitioning one of the nodes.");
    println!();

    // Choose a ClusterDir that matches what you passed to start-cluster.
    // On Windows the start script defaults to $env:TEMP\kayadb-cluster
    // We use a dedicated demo dir for safety.
    let cluster_dir = if cfg!(windows) {
        std::env::var("TEMP")
            .map(|t| format!("{}\\kayadb-jepsen-demo", t))
            .unwrap_or_else(|_| "C:\\tmp\\kayadb-jepsen-demo".to_string())
    } else {
        "/tmp/kayadb-jepsen-demo".to_string()
    };

    let config = TestConfig {
        // Default client ports from start-cluster scripts
        nodes: vec![
            "127.0.0.1:7379".parse().unwrap(),
            "127.0.0.1:7380".parse().unwrap(),
            "127.0.0.1:7381".parse().unwrap(),
        ],
        workload: WorkloadConfig {
            clients: 4,
            duration: Duration::from_secs(20),
            ..Default::default() // Register workload
        },
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Partition, // <-- This is the newly completed feature
            interval: Duration::from_secs(7),
            duration: Duration::from_secs(9),
            probability: 0.9,
        }),
        duration_secs: 25,
        cluster_dir,
    };

    println!("Cluster dir (must match your start-cluster invocation): {}", config.cluster_dir);
    println!("Nemesis: {:?}", config.nemesis.as_ref().unwrap().nemesis_type);
    println!("Duration: {}s with {} concurrent clients", config.duration_secs, config.workload.clients);
    println!();
    println!(">>> Starting test run. Make sure the 3-node cluster is up! <<<");
    println!();

    match TestRunner::new(config).run().await {
        Ok(result) => {
            println!("\n=== Test Finished ===");
            println!("Passed: {}", result.passed);
            println!("Stats: {}", result.stats);
            if !result.passed {
                println!("Violations (first few):");
                for v in result.violations.iter().take(3) {
                    println!("  - {}", v);
                }
                if let Some(trace) = &result.trace {
                    println!("Trace exported ({} bytes) - can be replayed with kaya-sim", trace.len());
                }
            } else {
                println!("No linearizability violations detected. Great!");
            }
        }
        Err(e) => {
            eprintln!("Runner error: {}", e);
            std::process::exit(1);
        }
    }

    println!("\nDemo complete. Remember to stop the cluster:");
    println!("  powershell -File scripts/stop-cluster.ps1 -ClusterDir <your-dir>");
    println!("  (or ./scripts/stop-cluster.sh)");
}