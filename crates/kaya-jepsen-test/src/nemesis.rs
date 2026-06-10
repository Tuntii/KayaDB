//! Failure injectors (nemeses) for Jepsen-style testing.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

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
    /// Partition a random node
    Partition,
    /// Partition a specific node
    PartitionById(usize),
    /// No-op (for testing)
    None,
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

    /// Run the nemesis, injecting failures periodically.
    pub async fn run(&self, stop_signal: tokio::sync::watch::Receiver<bool>) {
        let mut rng = StdRng::from_entropy();

        loop {
            // Wait for interval
            tokio::select! {
                _ = sleep(self.config.interval) => {}
                _ = async {
                    let mut rx = stop_signal.clone();
                    while !*rx.borrow_and_update() {
                        rx.changed().await.unwrap();
                    }
                } => {
                    break;
                }
            }

            // Check probability
            if rng.gen::<f64>() > self.config.probability {
                continue;
            }

            // Inject failure
            match &self.config.nemesis_type {
                NemesisType::KillNode => {
                    let node_id = rng.gen_range(1..=3);
                    self.kill_node(node_id).await;
                    sleep(self.config.duration).await;
                    self.restart_node(node_id).await;
                }
                NemesisType::KillNodeById(id) => {
                    self.kill_node(*id).await;
                    sleep(self.config.duration).await;
                    self.restart_node(*id).await;
                }
                NemesisType::Partition => {
                    let node_id = rng.gen_range(1..=3);
                    self.partition_node(node_id).await;
                    sleep(self.config.duration).await;
                    self.heal_partition(node_id).await;
                }
                NemesisType::PartitionById(id) => {
                    self.partition_node(*id).await;
                    sleep(self.config.duration).await;
                    self.heal_partition(*id).await;
                }
                NemesisType::None => {}
            }
        }
    }

    async fn kill_node(&self, node_id: usize) {
        eprintln!("[Nemesis] Killing node {}", node_id);

        // Try PowerShell script first (Windows)
        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/kill-node.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                &self.cluster_dir,
            ])
            .output();

        if result.is_err() {
            // Fallback to bash script (Unix)
            let _ = Command::new("bash")
                .args(["scripts/kill-node.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", &self.cluster_dir)
                .output();
        }
    }

    async fn restart_node(&self, node_id: usize) {
        eprintln!("[Nemesis] Restarting node {}", node_id);

        // Try PowerShell script first (Windows)
        let result = Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "scripts/restart-node.ps1",
                "-NodeId",
                &node_id.to_string(),
                "-ClusterDir",
                &self.cluster_dir,
            ])
            .output();

        if result.is_err() {
            // Fallback to bash script (Unix)
            let _ = Command::new("bash")
                .args(["scripts/restart-node.sh", &node_id.to_string()])
                .env("CLUSTER_DIR", &self.cluster_dir)
                .output();
        }
    }

    async fn partition_node(&self, node_id: usize) {
        eprintln!(
            "[Nemesis] Partitioning node {} (not implemented yet)",
            node_id
        );
        // TODO: Implement network partition using iptables/tc (Linux) or firewall rules (Windows)
    }

    async fn heal_partition(&self, node_id: usize) {
        eprintln!(
            "[Nemesis] Healing partition for node {} (not implemented yet)",
            node_id
        );
        // TODO: Implement partition healing
    }
}
