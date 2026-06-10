//! Test orchestration and verification.

use crate::history::History;
use crate::nemesis::{Nemesis, NemesisConfig};
use crate::workload::{Workload, WorkloadConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Test configuration.
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Cluster node addresses
    pub nodes: Vec<SocketAddr>,
    /// Workload configuration
    pub workload: WorkloadConfig,
    /// Nemesis configuration (None = no failures)
    pub nemesis: Option<NemesisConfig>,
    /// Test duration
    pub duration_secs: u64,
    /// Cluster directory (for process-control scripts)
    pub cluster_dir: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            nodes: vec![
                "127.0.0.1:7379".parse().unwrap(),
                "127.0.0.1:7380".parse().unwrap(),
                "127.0.0.1:7381".parse().unwrap(),
            ],
            workload: WorkloadConfig::default(),
            nemesis: None,
            duration_secs: 60,
            cluster_dir: "/tmp/kayadb-cluster".to_string(),
        }
    }
}

/// Test result.
#[derive(Debug)]
pub struct TestResult {
    /// Whether the test passed (no linearizability violations)
    pub passed: bool,
    /// Linearizability violations (if any)
    pub violations: Vec<String>,
    /// Operation history statistics
    pub stats: crate::history::HistoryStats,
    /// JSONL trace (if violations detected)
    pub trace: Option<String>,
}

/// Test runner that orchestrates workloads and nemeses.
pub struct TestRunner {
    config: TestConfig,
}

impl TestRunner {
    /// Create a new test runner.
    pub fn new(config: TestConfig) -> Self {
        Self { config }
    }

    /// Run the test.
    pub async fn run(&self) -> Result<TestResult, String> {
        eprintln!("Starting Jepsen-style test...");
        eprintln!("  Nodes: {:?}", self.config.nodes);
        eprintln!("  Duration: {}s", self.config.duration_secs);
        eprintln!("  Clients: {}", self.config.workload.clients);
        eprintln!(
            "  Nemesis: {:?}",
            self.config.nemesis.as_ref().map(|n| &n.nemesis_type)
        );

        let history = Arc::new(History::new());

        // Create stop signal
        let (stop_tx, stop_rx) = watch::channel(false);

        // Start nemesis (if configured)
        let nemesis_handle = if let Some(nemesis_config) = &self.config.nemesis {
            let nemesis = Nemesis::new(nemesis_config.clone(), self.config.cluster_dir.clone());
            let stop_rx = stop_rx.clone();
            Some(tokio::spawn(async move {
                nemesis.run(stop_rx).await;
            }))
        } else {
            None
        };

        // Run workload
        let workload = Workload::new(
            self.config.workload.clone(),
            self.config.nodes.clone(),
            history.clone(),
        );

        let workload_handle = tokio::spawn(async move {
            workload.run().await;
        });

        // Wait for duration
        tokio::time::sleep(Duration::from_secs(self.config.duration_secs)).await;

        // Signal stop
        let _ = stop_tx.send(true);

        // Wait for workload to finish
        let _ = workload_handle.await;

        // Wait for nemesis to finish
        if let Some(handle) = nemesis_handle {
            let _ = handle.await;
        }

        // Verify linearizability
        eprintln!("Verifying linearizability...");
        let stats = history.stats();
        eprintln!("{}", stats);

        match history.check_linearizability() {
            Ok(()) => {
                eprintln!("✓ Test PASSED: No linearizability violations");
                Ok(TestResult {
                    passed: true,
                    violations: vec![],
                    stats,
                    trace: None,
                })
            }
            Err(violations) => {
                eprintln!(
                    "✗ Test FAILED: {} linearizability violations",
                    violations.len()
                );
                for (i, v) in violations.iter().take(5).enumerate() {
                    eprintln!("  Violation {}: {}", i + 1, v);
                }
                if violations.len() > 5 {
                    eprintln!("  ... and {} more", violations.len() - 5);
                }

                let trace = history.to_trace(0xdead_beef);
                eprintln!("Trace exported ({} bytes)", trace.len());

                Ok(TestResult {
                    passed: false,
                    violations,
                    stats,
                    trace: Some(trace),
                })
            }
        }
    }
}
