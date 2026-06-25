//! Programmatic cluster lifecycle for Jepsen-style chaos tests.
//!
//! Spawns in-process [`ClusterNode`] instances with dynamic ports, supports
//! kill/restart, and exposes client endpoints for workloads and nemeses.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::nemesis::MemberSpec;
use kaya_net::{
    encode_member_payload, encode_remove_member_payload, roundtrip, STATUS_NOT_LEADER, STATUS_OK,
};
use kaya_server::{ClusterConfig, ClusterNode};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// A cluster member managed by [`ClusterController`].
pub struct ManagedNode {
    pub id: u64,
    pub data_dir: PathBuf,
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
    network_partitioned: Arc<AtomicBool>,
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
    /// Last node killed via [`Self::kill_node`] (used by T7 follower restart).
    last_killed: Option<u64>,
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

            let network_partitioned = Arc::new(AtomicBool::new(false));
            let config = ClusterConfig::new(id, &data_dir, raft_addr, client_addr, peers)
                .with_network_partitioned(network_partitioned.clone());
            let handle = spawn_node(config);

            nodes.push(ManagedNode {
                id,
                data_dir,
                raft_addr,
                client_addr,
                network_partitioned,
                handle: Some(handle),
            });
        }

        Ok(Self {
            base_dir,
            nodes,
            last_killed: None,
        })
    }

    /// Spawn a join-cluster node that discovers the roster via seed peers.
    pub async fn spawn_join_node(
        &mut self,
        id: u64,
        seeds: Vec<(u64, SocketAddr, SocketAddr)>,
    ) -> Result<(), String> {
        if self.node(id).is_ok() {
            return Err(format!("node {id} already exists"));
        }

        let raft_addr = alloc_local_addr().await?;
        let client_addr = alloc_local_addr().await?;
        let data_dir = self.base_dir.join(format!("node{id}"));
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let network_partitioned = Arc::new(AtomicBool::new(false));
        let config = ClusterConfig::new(id, &data_dir, raft_addr, client_addr, seeds)
            .with_join_cluster()
            .with_network_partitioned(network_partitioned.clone());
        let handle = spawn_node(config);

        self.nodes.push(ManagedNode {
            id,
            data_dir,
            raft_addr,
            client_addr,
            network_partitioned,
            handle: Some(handle),
        });
        Ok(())
    }

    /// Seed peer list for join-cluster nodes (all currently managed nodes).
    pub fn seed_peers(&self) -> Vec<(u64, SocketAddr, SocketAddr)> {
        self.nodes
            .iter()
            .map(|n| (n.id, n.raft_addr, n.client_addr))
            .collect()
    }

    /// Build a [`MemberSpec`] from a managed node's live addresses.
    pub fn member_spec_for_node(&self, node_id: u64) -> Result<MemberSpec, String> {
        let node = self.node(node_id)?;
        Ok(MemberSpec {
            node_id,
            raft_addr: node.raft_addr.to_string(),
            client_addr: node.client_addr.to_string(),
        })
    }

    /// Propose adding a member via ADD_MEMBER (opcode 7) on the leader.
    pub async fn add_member(&self, leader: SocketAddr, spec: &MemberSpec) -> Result<(), String> {
        eprintln!(
            "[ClusterController] ADD_MEMBER node {} via {}",
            spec.node_id, leader
        );
        let payload = encode_member_payload(spec.node_id, &spec.raft_addr, &spec.client_addr);
        admin_roundtrip(leader, 7, &payload, "ADD_MEMBER").await
    }

    /// Propose removing a member via REMOVE_MEMBER (opcode 8) on the leader.
    pub async fn remove_member(&self, leader: SocketAddr, node_id: u64) -> Result<(), String> {
        eprintln!("[ClusterController] REMOVE_MEMBER node {node_id} via {leader}");
        let payload = encode_remove_member_payload(node_id);
        admin_roundtrip(leader, 8, &payload, "REMOVE_MEMBER").await
    }

    /// Return the id of a node that is not the current Raft leader.
    pub async fn find_follower_id(&self) -> Result<u64, String> {
        let leader = self.wait_for_leader(Duration::from_secs(10)).await?;
        self.nodes
            .iter()
            .find(|n| n.id != leader.id)
            .map(|n| n.id)
            .ok_or_else(|| "no follower found".into())
    }

    /// Restart the node most recently killed via [`Self::kill_node`].
    pub fn restart_last_killed(&mut self) -> Result<(), String> {
        let id = self
            .last_killed
            .ok_or_else(|| "no node to restart".to_string())?;
        self.restart_node(id)
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
                return Err(format!("no leader elected within {:?}", timeout));
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
        self.last_killed = Some(id);
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

        node.network_partitioned.store(false, Ordering::SeqCst);
        let config =
            ClusterConfig::new(id, &node.data_dir, node.raft_addr, node.client_addr, peers)
                .with_network_partitioned(node.network_partitioned.clone());
        node.handle = Some(spawn_node(config));
        Ok(())
    }

    /// Partition a node: in-process inbound drop (always) plus best-effort OS rules.
    pub async fn partition_node(&self, id: u64) -> Result<(), String> {
        let node = self.node(id)?;
        node.network_partitioned.store(true, Ordering::SeqCst);
        let client_port = node.client_addr.port();
        let raft_port = node.raft_addr.port();
        #[cfg(target_os = "linux")]
        {
            let comment = iptables_comment(id);
            for port in [client_port, raft_port] {
                let _ = iptables_insert_drop(port, &comment);
            }
        }
        #[cfg(target_os = "windows")]
        {
            let _ = windows_firewall_block(id, client_port, raft_port);
        }
        Ok(())
    }

    /// Heal partition: clear in-process flag and best-effort OS cleanup.
    pub async fn heal_partition(&self, id: u64) -> Result<(), String> {
        let node = self.node(id)?;
        node.network_partitioned.store(false, Ordering::SeqCst);
        #[cfg(target_os = "linux")]
        {
            let (client_port, raft_port) = (node.client_addr.port(), node.raft_addr.port());
            let comment = iptables_comment(id);
            for port in [client_port, raft_port] {
                let _ = iptables_delete_drop(port, &comment);
            }
            let _ = iptables_delete_by_comment(&comment);
        }
        #[cfg(target_os = "windows")]
        {
            let _ = windows_firewall_unblock(id);
        }
        Ok(())
    }

    /// Return all live client listener addresses.
    pub fn client_endpoints(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|n| n.client_addr).collect()
    }

    /// Return client endpoints for a subset of node ids (stable iteration order by id).
    pub fn client_endpoints_for_ids(&self, ids: &[u64]) -> Vec<SocketAddr> {
        let mut endpoints: Vec<SocketAddr> = self
            .nodes
            .iter()
            .filter(|n| ids.contains(&n.id))
            .map(|n| n.client_addr)
            .collect();
        endpoints.sort_by_key(|a| a.port());
        endpoints
    }

    /// Abort every node task and remove the cluster base directory.
    pub async fn shutdown_all(&mut self) {
        for node in &mut self.nodes {
            if let Some(handle) = node.handle.take() {
                handle.abort();
            }
        }

        // Give aborted tasks time to release sockets (Windows port reuse is slow).
        sleep(Duration::from_millis(1500)).await;
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }

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

#[cfg(target_os = "windows")]
fn windows_rule_name(id: u64) -> String {
    format!("KayaDB-Partition-Node{id}")
}

#[cfg(target_os = "windows")]
fn windows_firewall_block(id: u64, client_port: u16, raft_port: u16) -> Result<(), String> {
    let rule = windows_rule_name(id);
    let ports = format!("{client_port},{raft_port}");
    let script = format!(
        "$ErrorActionPreference='Stop'; \
         New-NetFirewallRule -DisplayName '{rule}' -Direction Outbound -Protocol TCP \
         -RemoteAddress 127.0.0.1 -RemotePort {ports} -Action Block | Out-Null; \
         New-NetFirewallRule -DisplayName '{rule}-In' -Direction Inbound -Protocol TCP \
         -LocalAddress 127.0.0.1 -LocalPort {ports} -Action Block -ErrorAction SilentlyContinue | Out-Null"
    );
    let status = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "windows firewall partition failed for node {id} (ports {ports}); run as Administrator?"
        ))
    }
}

#[cfg(target_os = "windows")]
fn windows_firewall_unblock(id: u64) -> Result<(), String> {
    let rule = windows_rule_name(id);
    let script = format!(
        "Remove-NetFirewallRule -DisplayName '{rule}' -ErrorAction SilentlyContinue; \
         Remove-NetFirewallRule -DisplayName '{rule}-In' -ErrorAction SilentlyContinue"
    );
    let _ = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .status();
    Ok(())
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

const ADMIN_MAX_REDIRECTS: usize = 8;

async fn admin_roundtrip(
    mut addr: SocketAddr,
    opcode: u8,
    payload: &[u8],
    label: &str,
) -> Result<(), String> {
    let mut last_err = String::new();
    for attempt in 0..=ADMIN_MAX_REDIRECTS {
        match timeout(Duration::from_secs(10), roundtrip(addr, opcode, payload)).await {
            Ok(Ok((status, _body))) if status == STATUS_OK => return Ok(()),
            Ok(Ok((status, body))) => {
                let msg = String::from_utf8_lossy(&body);
                if status == kaya_net::STATUS_INVALID_ARGUMENT && msg.contains("already a voter") {
                    return Ok(());
                }
                if status == STATUS_NOT_LEADER {
                    if let Some(hint) = parse_leader_hint(&body) {
                        eprintln!(
                            "[ClusterController] {label} redirect to {hint} (attempt {}/{})",
                            attempt + 1,
                            ADMIN_MAX_REDIRECTS
                        );
                        addr = hint;
                        sleep(Duration::from_millis(60)).await;
                        continue;
                    }
                    if attempt < ADMIN_MAX_REDIRECTS {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                last_err = format!("{label} failed status={status}: {msg}");
                if attempt == ADMIN_MAX_REDIRECTS {
                    return Err(last_err);
                }
            }
            Ok(Err(e)) => {
                last_err = format!("{label} roundtrip error: {e}");
                if attempt == ADMIN_MAX_REDIRECTS {
                    return Err(last_err);
                }
            }
            Err(_) => {
                last_err = format!("{label} roundtrip timed out");
                if attempt == ADMIN_MAX_REDIRECTS {
                    return Err(last_err);
                }
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(last_err)
}

fn parse_leader_hint(body: &[u8]) -> Option<SocketAddr> {
    let hint = std::str::from_utf8(body).ok()?.trim();
    if hint.is_empty() {
        return None;
    }
    hint.parse().ok()
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
