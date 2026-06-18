//! Test orchestration and verification.

use crate::cluster_controller::ClusterController;
use crate::history::History;
use crate::nemesis::{MemberSpec, Nemesis, NemesisAction, NemesisConfig};
use crate::scenario::{Scenario, Topology, VerifyMode, WorkloadHook};
use crate::workload::{Workload, WorkloadConfig};
use kaya_client::KayaClient;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

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

        if scenario.topology == Topology::FourNodeJoin {
            let seeds = cluster.seed_peers();
            cluster.spawn_join_node(4, seeds).await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            eprintln!("  Join node 4 spawned (awaiting ADD_MEMBER nemesis)");
        }

        let t7_leader = if scenario.id == "t7" {
            let follower_id = cluster.find_follower_id().await?;
            eprintln!("[Runner] T7: stopping follower {follower_id} before burst writes");
            cluster.kill_node(follower_id)?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let leader = cluster.wait_for_leader(Duration::from_secs(15)).await?;
            Some(leader.client_addr)
        } else {
            None
        };

        for hook in &scenario.hooks {
            run_workload_hook(cluster, hook, t7_leader).await?;
        }

        let endpoints = match scenario.topology {
            Topology::FourNodeJoin => {
                cluster.client_endpoints_for_ids(&[1, 2, 3])
            }
            _ => cluster.client_endpoints(),
        };
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

        if scenario.id == "t7" {
            let _ = cluster.restart_last_killed();
            tokio::time::sleep(Duration::from_secs(2)).await;
            t7_durability_check(cluster).await?;
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
            let resolved = resolve_member_spec(cluster, &spec)?;
            let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;
            cluster
                .add_member(leader.client_addr, &resolved)
                .await?;
        }
        NemesisAction::RemoveMember(node_id) => {
            let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;
            cluster.remove_member(leader.client_addr, node_id).await?;
        }
        NemesisAction::KillFollower => {
            let follower_id = cluster.find_follower_id().await?;
            eprintln!("[Runner] Killing follower node {follower_id}");
            cluster.kill_node(follower_id)?;
        }
        NemesisAction::RestartFollower => {
            eprintln!("[Runner] Restarting last killed follower");
            cluster.restart_last_killed()?;
        }
        NemesisAction::Sleep(duration) => {
            tokio::time::sleep(duration).await;
        }
    }
    Ok(())
}

fn resolve_member_spec(cluster: &ClusterController, spec: &MemberSpec) -> Result<MemberSpec, String> {
    if spec.raft_addr.ends_with(":0") || spec.client_addr.ends_with(":0") {
        cluster.member_spec_for_node(spec.node_id)
    } else {
        Ok(spec.clone())
    }
}

async fn run_workload_hook(
    cluster: &ClusterController,
    hook: &WorkloadHook,
    leader_hint: Option<SocketAddr>,
) -> Result<(), String> {
    match hook {
        WorkloadHook::BurstWrites { count, key_prefix } => {
            eprintln!(
                "[Runner] BurstWrites: {count} keys with prefix '{key_prefix}'"
            );
            let leader = if let Some(addr) = leader_hint {
                addr
            } else {
                cluster
                    .wait_for_leader(Duration::from_secs(15))
                    .await?
                    .client_addr
            };
            let mut client = KayaClient::connect(leader)
                .await
                .map_err(|e| e.to_string())?;
            for i in 0..*count {
                let key = format!("{key_prefix}-{i}");
                let val = format!("v{i}");
                client
                    .put(key.as_bytes(), val.as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
            }

            let mut compacted = false;
            for _ in 0..80 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Ok(mut leader_client) = KayaClient::connect(leader).await {
                    if let Ok(stats) = leader_client.stats().await {
                        if applied_index_from_stats(&stats).unwrap_or(0) >= 64 {
                            compacted = true;
                            break;
                        }
                    }
                }
            }
            if !compacted {
                return Err("leader did not reach compaction threshold after burst writes".into());
            }
            Ok(())
        }
    }
}

fn applied_index_from_stats(stats: &str) -> Option<u64> {
    let needle = "\"applied_index\":";
    let start = stats.find(needle)? + needle.len();
    let rest = &stats[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

async fn t7_durability_check(cluster: &ClusterController) -> Result<(), String> {
    eprintln!("[Runner] T7 durability check: GET snap-127 on all endpoints");
    let key = b"snap-127";
    let expected = b"v127";

    for endpoint in cluster.client_endpoints() {
        let mut found = false;
        for attempt in 0..60 {
            if let Ok(Ok(mut client)) =
                timeout(Duration::from_millis(500), KayaClient::connect(endpoint)).await
            {
                if let Ok(Ok(stats)) =
                    timeout(Duration::from_millis(500), client.stats()).await
                {
                    let applied = applied_index_from_stats(&stats).unwrap_or(0);
                    if applied < 64 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                if let Ok(Ok(Some(val))) =
                    timeout(Duration::from_millis(500), client.get(key)).await
                {
                    if val == expected {
                        found = true;
                        break;
                    }
                }
            }
            if attempt % 10 == 9 {
                eprintln!("[Runner] T7 durability: still waiting on {endpoint}...");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !found {
            return Err(format!(
                "T7 durability check failed: snap-127 not found on {endpoint}"
            ));
        }
    }
    eprintln!("[Runner] T7 durability check passed");
    Ok(())
}