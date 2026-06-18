//! Programmatic cluster lifecycle for Jepsen-style chaos tests.
//!
//! Spawns in-process [`ClusterNode`] instances with dynamic ports, supports
//! kill/restart, and exposes client endpoints for workloads and nemeses.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kaya_net::roundtrip;
use kaya_server::{ClusterConfig, ClusterNode};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// A cluster member managed by [`ClusterController`].
pub struct ManagedNode {
    pub id: u64,
    pub data_dir: PathBuf,
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
    handle: Option<JoinHandle<()>>,
}

/// Elected leader identity and client-facing address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderInfo {
    pub id: u64,
    pub client_addr: SocketAddr,
}

/// In-process controller for a multi-node KayaDB cluster.
pub struct ClusterController {
    base_dir: PathBuf,
    nodes: Vec<ManagedNode>,
}

impl ClusterController {
    /// Spawn a fresh three-node cluster under `base_dir` with dynamic ports.
    pub async fn spawn_three_node(base_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&base_dir).map_err(|e| e.to_string())?;

        let mut raft_addrs = Vec::with_capacity(3);
        let mut client_addrs = Vec::with_capacity(3);
        for _ in 0..3 {
            raft_addrs.push(alloc_local_addr().await?);
            client_addrs.push(alloc_local_addr().await?);
        }

        let mut nodes = Vec::with_capacity(3);
        for id in 1..=3 {
            let idx = (id - 1) as usize;
            let data_dir = base_dir.join(format!("node{id}"));
            std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

            let raft_addr = raft_addrs[idx];
            let client_addr = client_addrs[idx];
            let peers: Vec<(u64, SocketAddr, SocketAddr)> = (1..=3u64)
                .filter(|&peer_id| peer_id != id)
                .map(|peer_id| {
                    let peer_idx = (peer_id - 1) as usize;
                    (peer_id, raft_addrs[peer_idx], client_addrs[peer_idx])
                })
                .collect();

            let config = ClusterConfig::new(id, &data_dir, raft_addr, client_addr, peers);
            let handle = spawn_node(config);

            nodes.push(ManagedNode {
                id,
                data_dir,
                raft_addr,
                client_addr,
                handle: Some(handle),
            });
        }

        Ok(Self { base_dir, nodes })
    }

    /// Poll client endpoints until one reports `"leader"` or `timeout` elapses.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<LeaderInfo, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for node in &self.nodes {
                if let Some(role) = check_health(node.client_addr).await {
                    if role == "leader" {
                        return Ok(LeaderInfo {
                            id: node.id,
                            client_addr: node.client_addr,
                        });
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "no leader elected within {:?}",
                    timeout
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Abort the tokio task for `id`, simulating a node crash.
    pub fn kill_node(&mut self, id: u64) -> Result<(), String> {
        let node = self.node_mut(id)?;
        if let Some(handle) = node.handle.take() {
            handle.abort();
        }
        Ok(())
    }

    /// Respawn a previously killed node with the same data directory and addresses.
    pub fn restart_node(&mut self, id: u64) -> Result<(), String> {
        let peers = self
            .nodes
            .iter()
            .filter(|n| n.id != id)
            .map(|n| (n.id, n.raft_addr, n.client_addr))
            .collect();

        let node = self.node_mut(id)?;
        if let Some(handle) = node.handle.take() {
            handle.abort();
        }

        let config = ClusterConfig::new(
            id,
            &node.data_dir,
            node.raft_addr,
            node.client_addr,
            peers,
        );
        node.handle = Some(spawn_node(config));
        Ok(())
    }

    /// Block loopback TCP traffic to a node's client and raft ports (Linux iptables).
    pub async fn partition_node(&self, id: u64) -> Result<(), String> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            return Err("partition requires linux".into());
        }
        #[cfg(target_os = "linux")]
        {
            let node = self.node(id)?;
            let comment = iptables_comment(id);
            for port in [node.client_addr.port(), node.raft_addr.port()] {
                iptables_insert_drop(port, &comment)?;
            }
            Ok(())
        }
    }

    /// Remove iptables DROP rules previously added by [`Self::partition_node`].
    pub async fn heal_partition(&self, id: u64) -> Result<(), String> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            return Err("partition requires linux".into());
        }
        #[cfg(target_os = "linux")]
        {
            let node = self.node(id)?;
            let comment = iptables_comment(id);
            for port in [node.client_addr.port(), node.raft_addr.port()] {
                iptables_delete_drop(port, &comment)?;
            }
            iptables_delete_by_comment(&comment)?;
            Ok(())
        }
    }

    /// Return all live client listener addresses.
    pub fn client_endpoints(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|n| n.client_addr).collect()
    }

    /// Abort every node task and remove the cluster base directory.
    pub async fn shutdown_all(&mut self) {
        for node in &mut self.nodes {
            if let Some(handle) = node.handle.take() {
                handle.abort();
            }
        }

        // Give aborted tasks a moment to release file handles (Windows).
        sleep(Duration::from_millis(100)).await;
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn node(&self, id: u64) -> Result<&ManagedNode, String> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| format!("node {id} not found"))
    }

    fn node_mut(&mut self, id: u64) -> Result<&mut ManagedNode, String> {
        self.nodes
            .iter_mut()
            .find(|n| n.id == id)
            .ok_or_else(|| format!("node {id} not found"))
    }
}

#[cfg(target_os = "linux")]
fn iptables_comment(id: u64) -> String {
    format!("kaya-jepsen-n{id}")
}

#[cfg(target_os = "linux")]
fn iptables_insert_drop(port: u16, comment: &str) -> Result<(), String> {
    let status = std::process::Command::new("sudo")
        .args([
            "iptables",
            "-I",
            "OUTPUT",
            "1",
            "-p",
            "tcp",
            "-d",
            "127.0.0.1",
            "--dport",
            &port.to_string(),
            "-m",
            "comment",
            "--comment",
            comment,
            "-j",
            "DROP",
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("iptables failed for port {port}"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn iptables_delete_drop(port: u16, comment: &str) -> Result<(), String> {
    let status = std::process::Command::new("sudo")
        .args([
            "iptables",
            "-D",
            "OUTPUT",
            "-p",
            "tcp",
            "-d",
            "127.0.0.1",
            "--dport",
            &port.to_string(),
            "-m",
            "comment",
            "--comment",
            comment,
            "-j",
            "DROP",
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if !status.success() {
        // Rule may already be gone; fall through to comment-based cleanup.
        let _ = status;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn iptables_delete_by_comment(comment: &str) -> Result<(), String> {
    loop {
        let output = std::process::Command::new("sudo")
            .args(["iptables", "-L", "OUTPUT", "-n", "--line-numbers"])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err("iptables list failed".into());
        }
        let listing = String::from_utf8_lossy(&output.stdout);
        let line_num = listing
            .lines()
            .find(|line| line.contains(comment))
            .and_then(|line| line.split_whitespace().next())
            .and_then(|s| s.parse::<u32>().ok());
        match line_num {
            Some(n) => {
                let status = std::process::Command::new("sudo")
                    .args(["iptables", "-D", "OUTPUT", &n.to_string()])
                    .status()
                    .map_err(|e| e.to_string())?;
                if !status.success() {
                    return Err(format!("iptables -D failed for comment {comment}"));
                }
            }
            None => break,
        }
    }
    Ok(())
}

fn spawn_node(config: ClusterConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _ = ClusterNode::new(config).run().await;
    })
}

async fn alloc_local_addr() -> Result<SocketAddr, String> {
    let port = get_free_port().await?;
    format!("127.0.0.1:{port}")
        .parse()
        .map_err(|e| format!("invalid socket addr: {e}"))
}

async fn get_free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    Ok(listener.local_addr().map_err(|e| e.to_string())?.port())
}

async fn check_health(addr: SocketAddr) -> Option<String> {
    if let Ok((status, body)) = roundtrip(addr, 5, &[]).await {
        if status == 0 {
            return String::from_utf8(body).ok();
        }
    }
    None
}