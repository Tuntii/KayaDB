//! Failure injectors (nemeses) for Jepsen-style testing.

use kaya_net::{encode_member_payload, encode_remove_member_payload, roundtrip, STATUS_OK};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Specification for a cluster member to add via ADD_MEMBER.
#[derive(Debug, Clone)]
pub struct MemberSpec {
    pub node_id: u64,
    pub raft_addr: String,
    pub client_addr: String,
}

/// Nemesis configuration.
#[derive(Debug, Clone)]
pub struct NemesisConfig {
    /// Type of nemesis
    pub nemesis_type: NemesisType,
    /// How often to inject failures (seconds)
    pub interval: Duration,
    /// How long failures last (seconds)
    pub duration: Duration,
    /// Probability of injection (0.0 to 1.0)
    pub probability: f64,
}

impl Default for NemesisConfig {
    fn default() -> Self {
        Self {
            nemesis_type: NemesisType::KillNode,
            interval: Duration::from_secs(30),
            duration: Duration::from_secs(20),
            probability: 1.0,
        }
    }
}

/// Type of failure to inject.
#[derive(Debug, Clone)]
pub enum NemesisType {
    /// Kill a random node
    KillNode,
    /// Kill a specific node
    KillNodeById(usize),
    /// Kill a non-leader node (resolved at injection time)
    KillFollower,
    /// Partition a random node
    Partition,
    /// Partition a specific node
    PartitionById(usize),
    /// Add a cluster member via leader roundtrip
    AddMember(MemberSpec),
    /// Remove a cluster member via leader roundtrip
    RemoveMember(u64),
    /// Run multiple nemesis types sequentially each cycle
    Composite(Vec<NemesisType>),
    /// Inject logical clock skew (harness sleep simulating fast/slow node)
    ClockSkew { node_id: u64, skew_ms: u64 },
    /// Inject disk latency on I/O path (harness sleep before heal)
    DiskLatency { delay_ms: u64 },
    /// No-op (for testing)
    None,
}

/// Commands sent from the nemesis task to the runner for in-process cluster control.
#[derive(Debug, Clone)]
pub enum NemesisAction {
    KillNode(u64),
    RestartNode(u64),
    PartitionNode(u64),
    HealPartition(u64),
    AddMember(MemberSpec),
    RemoveMember(u64),
    KillFollower,
    RestartFollower,
    Sleep(Duration),
    ClockSkew { node_id: u64, skew_ms: u64 },
    InjectDiskLatency { delay_ms: u64 },
    ClearDiskLatency,
}

/// A nemesis that injects failures.
pub struct Nemesis {
    config: NemesisConfig,
    cluster_dir: String,
}

impl Nemesis {
    /// Create a new nemesis.
    pub fn new(config: NemesisConfig, cluster_dir: String) -> Self {
        Self {
            config,
            cluster_dir,
        }
    }

    /// Run the nemesis, injecting failures periodically via shell scripts.
    pub async fn run(&self, stop_signal: tokio::sync::watch::Receiver<bool>) {
        let mut rng = StdRng::from_entropy();

        loop {
            if self.wait_interval_or_stop(&stop_signal).await {
                break;
            }

            if rng.gen::<f64>() > self.config.probability {
                continue;
            }

            self.inject_script(&mut rng).await;
        }
    }

    /// Emit cluster-control commands for an in-process [`ClusterController`].
    pub async fn run_controller_commands(
        &self,
        cmd_tx: mpsc::UnboundedSender<NemesisAction>,
        client_endpoints: Vec<SocketAddr>,
        stop_signal: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut rng = StdRng::from_entropy();

        loop {
            if self.wait_interval_or_stop(&stop_signal).await {
                break;
            }

            if rng.gen::<f64>() > self.config.probability {
                continue;
            }

            self.emit_controller_actions(&mut rng, &cmd_tx, &client_endpoints)
                .await;
        }
    }

    async fn wait_interval_or_stop(
        &self,
        stop_signal: &tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        tokio::select! {
            _ = sleep(self.config.interval) => false,
            _ = async {
                let mut rx = stop_signal.clone();
                while !*rx.borrow_and_update() {
                    rx.changed().await.unwrap();
                }
            } => true,
        }
    }

    async fn inject_script(&self, rng: &mut StdRng) {
        match &self.config.nemesis_type {
            NemesisType::Composite(types) => {
                for nemesis_type in types {
                    Self::inject_script_one(
                        nemesis_type,
                        rng,
                        &self.cluster_dir,
                        self.config.duration,
                    )
                    .await;
                }
            }
            other => {
                Self::inject_script_one(other, rng, &self.cluster_dir, self.config.duration).await;
            }
        }
    }

    async fn inject_script_one(
        nemesis_type: &NemesisType,
        rng: &mut StdRng,
        cluster_dir: &str,
        failure_duration: Duration,
    ) {
        match nemesis_type {
            NemesisType::KillNode => {
                let node_id = rng.gen_range(1..=3);
                Self::kill_node_script(node_id, cluster_dir).await;
                sleep(failure_duration).await;
                Self::restart_node_script(node_id, cluster_dir).await;
            }
            NemesisType::KillNodeById(id) => {
                Self::kill_node_script(*id, cluster_dir).await;
                sleep(failure_duration).await;
                Self::restart_node_script(*id, cluster_dir).await;
            }
            NemesisType::KillFollower => {
                eprintln!(
                    "[Nemesis] KillFollower requires ClusterController (script path unsupported)"
                );
            }
            NemesisType::Partition => {
                let node_id = rng.gen_range(1..=3);
                Self::partition_node_script(node_id, cluster_dir).await;
                sleep(failure_duration).await;
                Self::heal_partition_script(node_id, cluster_dir).await;
            }
            NemesisType::PartitionById(id) => {
                Self::partition_node_script(*id, cluster_dir).await;
                sleep(failure_duration).await;
                Self::heal_partition_script(*id, cluster_dir).await;
            }
            NemesisType::AddMember(spec) => {
                Self::add_member_roundtrip(&[], spec).await;
            }
            NemesisType::RemoveMember(node_id) => {
                Self::remove_member_roundtrip(&[], *node_id).await;
            }
            NemesisType::ClockSkew { node_id, skew_ms } => {
                eprintln!("[Nemesis] ClockSkew node {node_id} skew_ms={skew_ms}");
                sleep(Duration::from_millis(*skew_ms / 2)).await;
            }
            NemesisType::DiskLatency { delay_ms } => {
                eprintln!("[Nemesis] DiskLatency delay_ms={delay_ms}");
                sleep(Duration::from_millis(*delay_ms)).await;
            }
            NemesisType::Composite(_) => {}
            NemesisType::None => {}
        }
    }

    async fn emit_controller_actions(
        &self,
        rng: &mut StdRng,
        cmd_tx: &mpsc::UnboundedSender<NemesisAction>,
        client_endpoints: &[SocketAddr],
    ) {
        match &self.config.nemesis_type {
            NemesisType::Composite(types) => {
                for nemesis_type in types {
                    Self::emit_controller_action_one(
                        nemesis_type,
                        rng,
                        cmd_tx,
                        client_endpoints,
                        self.config.duration,
                    )
                    .await;
                }
            }
            other => {
                Self::emit_controller_action_one(
                    other,
                    rng,
                    cmd_tx,
                    client_endpoints,
                    self.config.duration,
                )
                .await;
            }
        }
    }

    async fn emit_controller_action_one(
        nemesis_type: &NemesisType,
        rng: &mut StdRng,
        cmd_tx: &mpsc::UnboundedSender<NemesisAction>,
        _client_endpoints: &[SocketAddr],
        failure_duration: Duration,
    ) {
        match nemesis_type {
            NemesisType::KillNode => {
                let node_id = rng.gen_range(1..=3) as u64;
                let _ = cmd_tx.send(NemesisAction::KillNode(node_id));
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::RestartNode(node_id));
            }
            NemesisType::KillNodeById(id) => {
                let node_id = *id as u64;
                let _ = cmd_tx.send(NemesisAction::KillNode(node_id));
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::RestartNode(node_id));
            }
            NemesisType::KillFollower => {
                let _ = cmd_tx.send(NemesisAction::KillFollower);
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::RestartFollower);
            }
            NemesisType::Partition => {
                let node_id = rng.gen_range(1..=3) as u64;
                let _ = cmd_tx.send(NemesisAction::PartitionNode(node_id));
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::HealPartition(node_id));
            }
            NemesisType::PartitionById(id) => {
                let node_id = *id as u64;
                let _ = cmd_tx.send(NemesisAction::PartitionNode(node_id));
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::HealPartition(node_id));
            }
            NemesisType::AddMember(spec) => {
                let _ = cmd_tx.send(NemesisAction::AddMember(spec.clone()));
            }
            NemesisType::RemoveMember(node_id) => {
                let _ = cmd_tx.send(NemesisAction::RemoveMember(*node_id));
            }
            NemesisType::ClockSkew { node_id, skew_ms } => {
                let _ = cmd_tx.send(NemesisAction::ClockSkew {
                    node_id: *node_id,
                    skew_ms: *skew_ms,
                });
            }
            NemesisType::DiskLatency { delay_ms } => {
                let _ = cmd_tx.send(NemesisAction::InjectDiskLatency {
                    delay_ms: *delay_ms,
                });
                let _ = cmd_tx.send(NemesisAction::Sleep(failure_duration));
                let _ = cmd_tx.send(NemesisAction::ClearDiskLatency);
            }
            NemesisType::Composite(_) => {}
            NemesisType::None => {}
        }
    }

    async fn add_member_roundtrip(endpoints: &[SocketAddr], spec: &MemberSpec) {
        let leader = match find_leader(endpoints).await {
            Some(addr) => addr,
            None => {
                eprintln!(
                    "[Nemesis] ADD_MEMBER skipped: no leader (node {})",
                    spec.node_id
                );
                return;
            }
        };

        eprintln!("[Nemesis] ADD_MEMBER node {} via {}", spec.node_id, leader);
        let payload = encode_member_payload(spec.node_id, &spec.raft_addr, &spec.client_addr);
        match roundtrip(leader, 7, &payload).await {
            Ok((status, _body)) if status == STATUS_OK => {}
            Ok((status, body)) => {
                eprintln!(
                    "[Nemesis] ADD_MEMBER failed status={status}: {:?}",
                    String::from_utf8(body)
                );
            }
            Err(e) => eprintln!("[Nemesis] ADD_MEMBER roundtrip error: {e}"),
        }
    }

    async fn remove_member_roundtrip(endpoints: &[SocketAddr], node_id: u64) {
        let leader = match find_leader(endpoints).await {
            Some(addr) => addr,
            None => {
                eprintln!("[Nemesis] REMOVE_MEMBER skipped: no leader (node {node_id})");
                return;
            }
        };

        eprintln!("[Nemesis] REMOVE_MEMBER node {node_id} via {leader}");
        let payload = encode_remove_member_payload(node_id);
        match roundtrip(leader, 8, &payload).await {
            Ok((status, _body)) if status == STATUS_OK => {}
            Ok((status, body)) => {
                eprintln!(
                    "[Nemesis] REMOVE_MEMBER failed status={status}: {:?}",
                    String::from_utf8(body)
                );
            }
            Err(e) => eprintln!("[Nemesis] REMOVE_MEMBER roundtrip error: {e}"),
        }
    }

    async fn kill_node_script(node_id: usize, cluster_dir: &str) {
        eprintln!("[Nemesis] Killing node {}", node_id);

        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/kill-node.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                cluster_dir,
            ])
            .output();

        if result.is_err() {
            let _ = Command::new("bash")
                .args(["scripts/kill-node.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", cluster_dir)
                .output();
        }
    }

    async fn restart_node_script(node_id: usize, cluster_dir: &str) {
        eprintln!("[Nemesis] Restarting node {}", node_id);

        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/restart-node.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                cluster_dir,
            ])
            .output();

        if result.is_err() {
            let _ = Command::new("bash")
                .args(["scripts/restart-node.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", cluster_dir)
                .output();
        }
    }

    async fn partition_node_script(node_id: usize, cluster_dir: &str) {
        eprintln!("[Nemesis] Partitioning node {}", node_id);

        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/partition-node.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                cluster_dir,
            ])
            .output();

        if result.is_err() {
            let _ = Command::new("bash")
                .args(["scripts/partition-node.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", cluster_dir)
                .output();
        }
    }

    async fn heal_partition_script(node_id: usize, cluster_dir: &str) {
        eprintln!("[Nemesis] Healing partition for node {}", node_id);

        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/heal-partition.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                cluster_dir,
            ])
            .output();

        if result.is_err() {
            let _ = Command::new("bash")
                .args(["scripts/heal-partition.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", cluster_dir)
                .output();
        }
    }
}

async fn find_leader(endpoints: &[SocketAddr]) -> Option<SocketAddr> {
    for &addr in endpoints {
        if let Ok((status, body)) = roundtrip(addr, 5, &[]).await {
            if status == STATUS_OK && String::from_utf8(body).ok().as_deref() == Some("leader") {
                return Some(addr);
            }
        }
    }
    None
}

#[cfg(test)]
mod rich_nemesis_tests {
    use super::*;
    use rand::SeedableRng;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn clock_skew_and_disk_latency_emit_actions() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rng = StdRng::from_entropy();

        Nemesis::emit_controller_action_one(
            &NemesisType::ClockSkew {
                node_id: 2,
                skew_ms: 40,
            },
            &mut rng,
            &tx,
            &[],
            Duration::from_millis(5),
        )
        .await;
        match rx.try_recv().unwrap() {
            NemesisAction::ClockSkew { node_id, skew_ms } => {
                assert_eq!(node_id, 2);
                assert_eq!(skew_ms, 40);
            }
            other => panic!("expected ClockSkew, got {other:?}"),
        }

        Nemesis::emit_controller_action_one(
            &NemesisType::DiskLatency { delay_ms: 25 },
            &mut rng,
            &tx,
            &[],
            Duration::from_millis(5),
        )
        .await;
        match rx.try_recv().unwrap() {
            NemesisAction::InjectDiskLatency { delay_ms } => assert_eq!(delay_ms, 25),
            other => panic!("expected InjectDiskLatency, got {other:?}"),
        }
        assert!(matches!(rx.try_recv().unwrap(), NemesisAction::Sleep(_)));
        assert!(matches!(
            rx.try_recv().unwrap(),
            NemesisAction::ClearDiskLatency
        ));
    }
}
