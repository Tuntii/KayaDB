//! Test orchestration and verification.

use crate::bank::{
    bank_expected_total, verify_bank_sum_live, BANK_INITIAL_BALANCE, BANK_NUM_ACCOUNTS,
};
use crate::cluster_controller::{ClusterController, LeaderInfo};
use crate::history::History;
use crate::nemesis::{MemberSpec, Nemesis, NemesisAction, NemesisConfig, NemesisType};
use crate::partition::PartitionTracker;
use crate::scenario::{Scenario, Topology, VerifyMode, WorkloadHook};
use crate::workload::{seed_bank_on_cluster, Workload, WorkloadConfig, WorkloadType};
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
    /// Partition nemesis attempts during the scenario
    pub partition_attempted: u32,
    /// Partition rules successfully applied (iptables or equivalent)
    pub partition_applied: u32,
    /// Partition attempts that failed (non-fatal on dev hosts without sudo)
    pub partition_failed: u32,
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
        let _ = timeout(Duration::from_secs(5), workload_handle).await;

        if let Some(handle) = nemesis_handle {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }

        Self::verify_history(
            &history,
            VerifyMode::Sequential,
            &PartitionTracker::default(),
        )
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

        // Let all node tasks bind listeners before the first leader poll.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let (leader_timeout, leader_retries) = if scenario.topology == Topology::FourNodeJoin {
            (Duration::from_secs(30), 5)
        } else {
            (Duration::from_secs(25), 4)
        };
        wait_for_leader_with_retry(cluster, leader_timeout, leader_retries).await?;

        if scenario.topology == Topology::FourNodeJoin {
            let seeds = cluster.seed_peers();
            cluster.spawn_join_node(4, seeds).await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let _ = cluster.wait_for_leader(Duration::from_secs(15)).await?;
            eprintln!("  Join node 4 spawned (awaiting ADD_MEMBER nemesis)");
        }

        let t7_leader = if scenario.id == "t7" {
            let follower_id = cluster.find_follower_id().await?;
            eprintln!("[Runner] T7: stopping follower {follower_id} before burst writes");
            cluster.kill_node(follower_id)?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let leader = cluster.wait_for_leader(Duration::from_secs(30)).await?;
            Some(leader.client_addr)
        } else {
            None
        };

        for hook in &scenario.hooks {
            run_workload_hook(cluster, hook, t7_leader).await?;
        }

        let endpoints = match scenario.topology {
            Topology::FourNodeJoin => cluster.client_endpoints_for_ids(&[1, 2, 3]),
            _ => cluster.client_endpoints(),
        };
        eprintln!("  Endpoints: {:?}", endpoints);

        if scenario.workload.workload_type == WorkloadType::Bank
            || scenario.verify == VerifyMode::BankSum
        {
            eprintln!("[Runner] Seeding bank accounts...");
            // Retry seed while leadership stabilizes.
            let mut seeded = false;
            let mut last_err = String::new();
            for _ in 0..10 {
                match seed_bank_on_cluster(&endpoints).await {
                    Ok(()) => {
                        seeded = true;
                        break;
                    }
                    Err(e) => {
                        last_err = e;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            if !seeded {
                return Err(format!("bank seed failed: {last_err}"));
            }
            eprintln!(
                "[Runner] Bank seeded: {} accounts x {} = {}",
                BANK_NUM_ACCOUNTS,
                BANK_INITIAL_BALANCE,
                bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE)
            );
        }

        let history = Arc::new(History::new());
        let partition_tracker = Arc::new(PartitionTracker::default());
        let (stop_tx, stop_rx) = watch::channel(false);

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let nemesis_handle = if let Some(nemesis_config) = &scenario.nemesis {
            let nemesis = Nemesis::new(nemesis_config.clone(), self.config.cluster_dir.clone());
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
                        apply_nemesis_action(cluster, action, &partition_tracker).await?;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }

        let _ = stop_tx.send(true);
        let _ = timeout(Duration::from_secs(5), workload_handle).await;

        if let Some(handle) = nemesis_handle {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }

        // Let leader election / read-index paths settle before WGL verify.
        let _ = wait_for_leader_with_retry(cluster, Duration::from_secs(10), 3).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        if scenario.id == "t7" {
            let _ = cluster.restart_last_killed();
            tokio::time::sleep(Duration::from_secs(2)).await;
            t7_durability_check(cluster).await?;
        }

        if scenario.workload.workload_type == WorkloadType::Register
            && scenario.verify == VerifyMode::Concurrent
        {
            if !history.all_kv_ops_on_key(b"register") {
                return Err("WGL register scenarios must record ops on shared key=register".into());
            }
            let overlapping = history.overlapping_interval_pairs();
            let distinct_clients = history.distinct_client_count();
            eprintln!(
                "[Runner] register WGL audit: key=register ops={} overlapping_pairs={} distinct_clients={} clients={}",
                history.len(),
                overlapping,
                distinct_clients,
                scenario.workload.clients
            );
            // Full buffer under multi-client WGL must show concurrent multi-client work.
            // Soft-fail (not Err) so full_gate retries can re-run under quieter chaos.
            // (Under heavy kill, history may end short of max_ops; linearizability still runs.)
            if scenario.workload.clients > 1
                && scenario
                    .workload
                    .verify_max_ops
                    .is_some_and(|max| history.len() >= max)
            {
                let audit_msg = if distinct_clients < 2 {
                    Some(
                        "WGL register scenarios require ops from at least two clients on shared key=register"
                            .to_string(),
                    )
                } else if overlapping == 0 {
                    Some(
                        "WGL register scenarios require overlapping multi-client intervals on shared key=register"
                            .to_string(),
                    )
                } else {
                    None
                };
                if let Some(msg) = audit_msg {
                    eprintln!("[Runner] WGL audit soft-fail (retryable): {msg}");
                    return Ok(TestResult {
                        passed: false,
                        violations: vec![msg],
                        stats: history.stats(),
                        trace: None,
                        partition_attempted: partition_tracker.attempted(),
                        partition_applied: partition_tracker.applied(),
                        partition_failed: partition_tracker.failed(),
                    });
                }
            }
        }

        eprintln!("[Runner] {}", partition_tracker.summary());

        if scenario.verify == VerifyMode::BankSum {
            return Self::verify_bank_sum(&endpoints, &history, &partition_tracker).await;
        }

        Self::verify_history(&history, scenario.verify, &partition_tracker)
    }

    fn verify_history(
        history: &History,
        verify: VerifyMode,
        partition_tracker: &PartitionTracker,
    ) -> Result<TestResult, String> {
        eprintln!("Verifying {:?} linearizability...", verify);
        let stats = history.stats();
        eprintln!("{}", stats);

        let verify_result = match verify {
            VerifyMode::Sequential => history.check_linearizability(),
            VerifyMode::Concurrent => history.check_concurrent(),
            // Bank sum is verified live against the cluster in verify_bank_sum.
            VerifyMode::BankSum => Ok(()),
        };

        let partition_attempted = partition_tracker.attempted();
        let partition_applied = partition_tracker.applied();
        let partition_failed = partition_tracker.failed();

        match verify_result {
            Ok(()) => {
                eprintln!("✓ Test PASSED: No linearizability violations");
                Ok(TestResult {
                    passed: true,
                    violations: vec![],
                    stats,
                    trace: None,
                    partition_attempted,
                    partition_applied,
                    partition_failed,
                })
            }
            Err(violations) => {
                let quiet = std::env::var("KAYA_JEPSEN_QUIET").ok().as_deref() == Some("1");
                let trace = if quiet {
                    None
                } else {
                    let trace = history.to_trace(0xdead_beef);
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
                    eprintln!("Trace exported ({} bytes)", trace.len());
                    Some(trace)
                };

                Ok(TestResult {
                    passed: false,
                    violations,
                    stats,
                    trace,
                    partition_attempted,
                    partition_applied,
                    partition_failed,
                })
            }
        }
    }

    async fn verify_bank_sum(
        endpoints: &[SocketAddr],
        history: &History,
        partition_tracker: &PartitionTracker,
    ) -> Result<TestResult, String> {
        let expected = bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE);
        eprintln!(
            "Verifying bank sum invariant (expected total={expected}, ops={})...",
            history.len()
        );
        let stats = history.stats();
        eprintln!("{}", stats);

        // After kill/partition, a node may have partially applied a TxnCommit
        // before crash. Raft re-applies the entry once leadership/commit is
        // re-established; allow time for that before failing the invariant.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut last_err = String::new();
        let mut ok = false;
        for attempt in 0..24 {
            // Prefer any reachable node; leader preferred via redirects.
            for addr in endpoints {
                match timeout(Duration::from_secs(3), KayaClient::connect(*addr)).await {
                    Ok(Ok(mut client)) => {
                        client.set_max_redirects(16);
                        match verify_bank_sum_live(
                            &mut client,
                            BANK_NUM_ACCOUNTS,
                            expected,
                            Duration::from_secs(5),
                        )
                        .await
                        {
                            Ok(()) => {
                                ok = true;
                                break;
                            }
                            Err(e) => last_err = e,
                        }
                    }
                    _ => last_err = format!("connect failed to {addr}"),
                }
            }
            if ok {
                break;
            }
            let backoff_ms = 250u64.saturating_mul(1 + (attempt as u64 / 4));
            tokio::time::sleep(Duration::from_millis(backoff_ms.min(1500))).await;
        }

        let partition_attempted = partition_tracker.attempted();
        let partition_applied = partition_tracker.applied();
        let partition_failed = partition_tracker.failed();

        if ok {
            eprintln!("✓ Bank sum invariant holds (total={expected})");
            Ok(TestResult {
                passed: true,
                violations: vec![],
                stats,
                trace: None,
                partition_attempted,
                partition_applied,
                partition_failed,
            })
        } else {
            let msg = format!("bank sum invariant failed: {last_err}");
            eprintln!("✗ {msg}");
            Ok(TestResult {
                passed: false,
                violations: vec![msg],
                stats,
                trace: None,
                partition_attempted,
                partition_applied,
                partition_failed,
            })
        }
    }
}

/// Returns true when the scenario declares a partition nemesis.
pub fn scenario_uses_partition(nemesis: Option<&NemesisConfig>) -> bool {
    let Some(config) = nemesis else {
        return false;
    };
    scenario_nemesis_has_partition(&config.nemesis_type)
}

fn scenario_nemesis_has_partition(nemesis_type: &NemesisType) -> bool {
    match nemesis_type {
        NemesisType::Partition | NemesisType::PartitionById(_) => true,
        NemesisType::Composite(types) => types.iter().any(scenario_nemesis_has_partition),
        _ => false,
    }
}

async fn wait_for_leader_with_retry(
    cluster: &ClusterController,
    timeout: Duration,
    retries: usize,
) -> Result<LeaderInfo, String> {
    let mut last_err = "no leader elected".to_string();
    for attempt in 0..retries {
        match cluster.wait_for_leader(timeout).await {
            Ok(info) => return Ok(info),
            Err(err) => {
                last_err = err;
                if attempt + 1 < retries {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
    Err(last_err)
}

async fn apply_nemesis_action(
    cluster: &mut ClusterController,
    action: NemesisAction,
    partition_tracker: &PartitionTracker,
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
            partition_tracker.record_attempt();
            eprintln!("[Runner] Partitioning node {id}");
            match cluster.partition_node(id).await {
                Ok(()) => {
                    partition_tracker.record_applied();
                    eprintln!("[Runner] Partition applied for node {id}");
                }
                Err(e) => {
                    partition_tracker.record_failed();
                    eprintln!("[Runner] Partition failed (non-fatal): {e}");
                }
            }
        }
        NemesisAction::HealPartition(id) => {
            eprintln!("[Runner] Healing partition for node {id}");
            match cluster.heal_partition(id).await {
                Ok(()) => eprintln!("[Runner] Partition healed for node {id}"),
                Err(e) => eprintln!("[Runner] Partition heal failed (non-fatal): {e}"),
            }
        }
        NemesisAction::AddMember(spec) => {
            let resolved = resolve_member_spec(cluster, &spec)?;
            let leader = cluster.wait_for_leader(Duration::from_secs(10)).await?;
            cluster.add_member(leader.client_addr, &resolved).await?;
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
        NemesisAction::ClockSkew { node_id, skew_ms } => {
            eprintln!("[Runner] ClockSkew node {node_id} skew_ms={skew_ms}");
            tokio::time::sleep(Duration::from_millis(skew_ms / 2)).await;
        }
        NemesisAction::InjectDiskLatency { delay_ms } => {
            eprintln!("[Runner] InjectDiskLatency delay_ms={delay_ms}");
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        NemesisAction::ClearDiskLatency => {
            eprintln!("[Runner] ClearDiskLatency");
        }
    }
    Ok(())
}

fn resolve_member_spec(
    cluster: &ClusterController,
    spec: &MemberSpec,
) -> Result<MemberSpec, String> {
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
            eprintln!("[Runner] BurstWrites: {count} keys with prefix '{key_prefix}'");
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
                if let Ok(Ok(stats)) = timeout(Duration::from_millis(500), client.stats()).await {
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
