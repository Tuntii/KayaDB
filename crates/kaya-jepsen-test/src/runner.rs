//! Test orchestration and verification.

use crate::cluster_controller::ClusterController;
use crate::history::History;
use crate::nemesis::{Nemesis, NemesisAction, NemesisConfig};
use crate::scenario::{Scenario, VerifyMode};
use crate::workload::{Workload, WorkloadConfig};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

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

impl TestConfig {
    /// Build a [`TestConfig`] from a [`Scenario`] and cluster base directory.
    pub fn from_scenario(scenario: &Scenario, cluster_dir: &Path) -> Self {
        Self {
            nodes: vec![],
            workload: scenario.workload.clone(),
            nemesis: scenario.nemesis.clone(),
            duration_secs: scenario.duration_secs,
            cluster_dir: cluster_dir.to_string_lossy().into_owned(),
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

    /// Run the test using script-based cluster control.
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
        let (stop_tx, stop_rx) = watch::channel(false);

        let nemesis_handle = if let Some(nemesis_config) = &self.config.nemesis {
            let nemesis = Nemesis::new(nemesis_config.clone(), self.config.cluster_dir.clone());
            let stop_rx = stop_rx.clone();
            Some(tokio::spawn(async move {
                nemesis.run(stop_rx).await;
            }))
        } else {
            None
        };

        let workload = Workload::new(
            self.config.workload.clone(),
            self.config.nodes.clone(),
            history.clone(),
        );

        let workload_handle = tokio::spawn(async move {
            workload.run().await;
        });

        tokio::time::sleep(Duration::from_secs(self.config.duration_secs)).await;
        let _ = stop_tx.send(true);
        let _ = workload_handle.await;

        if let Some(handle) = nemesis_handle {
            let _ = handle.await;
        }

        Self::verify_history(&history, VerifyMode::Sequential)
    }

    /// Run a declarative scenario against an in-process [`ClusterController`].
    pub async fn run_scenario(
        &self,
        scenario: &Scenario,
        cluster: &mut ClusterController,
    ) -> Result<TestResult, String> {
        eprintln!("Starting scenario '{}'...", scenario.id);
        eprintln!("  Topology: {:?}", scenario.topology);
        eprintln!("  Duration: {}s", scenario.duration_secs);
        eprintln!("  Clients: {}", scenario.workload.clients);
        eprintln!("  Verify: {:?}", scenario.verify);
        eprintln!(
            "  Nemesis: {:?}",
            scenario.nemesis.as_ref().map(|n| &n.nemesis_type)
        );

        cluster
            .wait_for_leader(Duration::from_secs(15))
            .await?;

        let endpoints = cluster.client_endpoints();
        eprintln!("  Endpoints: {:?}", endpoints);

        let history = Arc::new(History::new());
        let (stop_tx, stop_rx) = watch::channel(false);

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let nemesis_handle = if let Some(nemesis_config) = &scenario.nemesis {
            let nemesis = Nemesis::new(
                nemesis_config.clone(),
                self.config.cluster_dir.clone(),
            );
            let stop_rx = stop_rx.clone();
            let endpoints = endpoints.clone();
            Some(tokio::spawn(async move {
                nemesis
                    .run_controller_commands(cmd_tx, endpoints, stop_rx)
                    .await;
            }))
        } else {
            None
        };

        let mut workload_config = scenario.workload.clone();
        workload_config.duration = Duration::from_secs(scenario.duration_secs);

        let workload = Workload::new(workload_config, endpoints.clone(), history.clone());
        let workload_handle = tokio::spawn(async move {
            workload.run().await;
        });

        let deadline = Instant::now() + Duration::from_secs(scenario.duration_secs);
        while Instant::now() < deadline {
            tokio::select! {
                action = cmd_rx.recv() => {
                    if let Some(action) = action {
                        apply_nemesis_action(cluster, action).await?;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }

        let _ = stop_tx.send(true);
        let _ = workload_handle.await;

        if let Some(handle) = nemesis_handle {
            let _ = handle.await;
        }

        Self::verify_history(&history, scenario.verify)
    }

    fn verify_history(history: &History, verify: VerifyMode) -> Result<TestResult, String> {
        eprintln!("Verifying {:?} linearizability...", verify);
        let stats = history.stats();
        eprintln!("{}", stats);

        let verify_result = match verify {
            VerifyMode::Sequential => history.check_linearizability(),
            VerifyMode::Concurrent => history.check_concurrent(),
        };

        match verify_result {
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

async fn apply_nemesis_action(
    cluster: &mut ClusterController,
    action: NemesisAction,
) -> Result<(), String> {
    match action {
        NemesisAction::KillNode(id) => {
            eprintln!("[Runner] Killing node {id}");
            cluster.kill_node(id)?;
        }
        NemesisAction::RestartNode(id) => {
            eprintln!("[Runner] Restarting node {id}");
            cluster.restart_node(id)?;
        }
        NemesisAction::PartitionNode(id) => {
            eprintln!("[Runner] Partitioning node {id}");
            if let Err(e) = cluster.partition_node(id).await {
                eprintln!("[Runner] Partition failed (non-fatal): {e}");
            }
        }
        NemesisAction::HealPartition(id) => {
            eprintln!("[Runner] Healing partition for node {id}");
            let _ = cluster.heal_partition(id).await;
        }
        NemesisAction::AddMember(spec) => {
            eprintln!("[Runner] ADD_MEMBER node {}", spec.node_id);
            // Membership roundtrip is performed inside the nemesis task.
        }
        NemesisAction::RemoveMember(node_id) => {
            eprintln!("[Runner] REMOVE_MEMBER node {node_id}");
            // Membership roundtrip is performed inside the nemesis task.
        }
        NemesisAction::Sleep(duration) => {
            tokio::time::sleep(duration).await;
        }
    }
    Ok(())
}