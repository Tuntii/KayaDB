use std::collections::{HashMap, HashSet, VecDeque};

use kaya_raft::{Envelope, LogIndex, NodeId, RaftConfig, RaftNode, RaftStatus, Term};

use crate::rng::SimRng;

// ── Simulated network ─────────────────────────────────────────────────────────

/// Configuration for the simulated network fault injector.
#[derive(Debug, Clone)]
pub struct SimNetworkConfig {
    /// Percentage of messages to silently drop (0–100).
    pub drop_percent: u32,
    /// Percentage of messages to duplicate (0–100).
    pub dup_percent: u32,
}

impl Default for SimNetworkConfig {
    fn default() -> Self {
        Self {
            drop_percent: 10,
            dup_percent: 5,
        }
    }
}

/// Deterministic simulated network for Raft message passing.
///
/// Messages are queued per destination node. Fault injection (drops, duplicates,
/// partitions) is applied at injection time and is fully reproducible by seed.
pub struct SimNetwork {
    config: SimNetworkConfig,
    rng: SimRng,
    /// In-flight messages, keyed by destination node.
    queues: HashMap<NodeId, VecDeque<Envelope>>,
    /// Links on which all messages are silently dropped (unidirectional).
    partitions: HashSet<(NodeId, NodeId)>,
}

impl SimNetwork {
    pub fn new(seed: u64, config: SimNetworkConfig) -> Self {
        Self {
            config,
            // Offset the seed so the network RNG is independent from the operation RNG.
            rng: SimRng::new(seed.wrapping_add(0x9e37_79b9_7f4a_7c15)),
            queues: HashMap::new(),
            partitions: HashSet::new(),
        }
    }

    /// Inject a batch of outgoing envelopes into the network.
    ///
    /// Partitioned links and random drops are applied here; surviving messages
    /// are queued for the destination and can be retrieved via [`drain`].
    pub fn inject(&mut self, envelopes: Vec<Envelope>) {
        for env in envelopes {
            // Partition check.
            if self.partitions.contains(&(env.from, env.to)) {
                continue;
            }
            // Random drop.
            if self.rng.usize_below(100) < self.config.drop_percent as usize {
                continue;
            }
            // Random duplicate.
            let dup = self.rng.usize_below(100) < self.config.dup_percent as usize;
            let queue = self.queues.entry(env.to).or_default();
            queue.push_back(env.clone());
            if dup {
                queue.push_back(env);
            }
        }
    }

    /// Drain all queued messages and return them.
    pub fn drain(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        for queue in self.queues.values_mut() {
            while let Some(env) = queue.pop_front() {
                out.push(env);
            }
        }
        out
    }

    /// Partition the link from `a` to `b` (one-directional).
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert((a, b));
    }

    /// Remove the partition from `a` to `b`.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&(a, b));
    }

    /// Symmetrically isolate `node` from all `peers`.
    pub fn isolate(&mut self, node: NodeId, peers: &[NodeId]) {
        for &peer in peers {
            self.partition(node, peer);
            self.partition(peer, node);
        }
    }

    /// Symmetrically reconnect `node` to all `peers`.
    pub fn reconnect(&mut self, node: NodeId, peers: &[NodeId]) {
        for &peer in peers {
            self.heal(node, peer);
            self.heal(peer, node);
        }
    }
}

// ── Cluster simulator ─────────────────────────────────────────────────────────

/// Report produced at the end of a [`ClusterSim`] run.
#[derive(Debug, Clone)]
pub struct ClusterSimReport {
    pub seed: u64,
    pub ticks: u64,
    pub invariant_failures: Vec<String>,
}

/// Drives a multi-node Raft cluster in deterministic simulation.
///
/// Each logical tick:
/// 1. All nodes are ticked (timers advance, heartbeats fire, elections start).
/// 2. Outgoing messages pass through [`SimNetwork`] (drops / duplicates / partitions).
/// 3. Surviving messages are delivered in two rounds so request → response
///    pairs complete within the same tick.
/// 4. Invariants are checked (election safety: ≤1 leader per term).
pub struct ClusterSim {
    nodes: HashMap<NodeId, RaftNode>,
    all_ids: Vec<NodeId>,
    network: SimNetwork,
    ticks: u64,
    violations: Vec<String>,
}

impl ClusterSim {
    /// Create a cluster with `num_nodes` nodes.
    ///
    /// Election timeouts are staggered: node `i` (1-based) gets a timeout of
    /// `10 + (i − 1) × 3` ticks so that node 1 always wins the first election
    /// deterministically when there are no faults.
    pub fn new(num_nodes: u64, seed: u64, net_config: SimNetworkConfig) -> Self {
        let all_ids: Vec<NodeId> = (1..=num_nodes).map(NodeId).collect();
        let mut nodes = HashMap::new();
        for &id in &all_ids {
            let peers: Vec<NodeId> = all_ids.iter().copied().filter(|&p| p != id).collect();
            let timeout = 10 + (id.0 - 1) * 3;
            let config = RaftConfig {
                id,
                peers,
                election_timeout_ticks: timeout,
                heartbeat_interval_ticks: 3,
            };
            nodes.insert(id, RaftNode::new(config));
        }
        Self {
            nodes,
            all_ids,
            network: SimNetwork::new(seed, net_config),
            ticks: 0,
            violations: Vec::new(),
        }
    }

    /// Execute one logical tick across all nodes.
    pub fn step(&mut self) {
        self.ticks += 1;

        // Phase 1: tick all nodes, collect outgoing messages.
        let ids: Vec<NodeId> = self.all_ids.clone();
        for &id in &ids {
            let out = self.nodes.get_mut(&id).unwrap().tick();
            self.network.inject(out);
        }

        // Phase 2: deliver messages (round 1) and collect responses.
        let round1 = self.network.drain();
        for env in round1 {
            if let Some(node) = self.nodes.get_mut(&env.to) {
                let out = node.handle(env);
                self.network.inject(out);
            }
        }

        // Phase 3: deliver responses (round 2) so request→response completes
        // in the same tick. Further messages are queued for the next tick.
        let round2 = self.network.drain();
        for env in round2 {
            if let Some(node) = self.nodes.get_mut(&env.to) {
                let out = node.handle(env);
                self.network.inject(out);
            }
        }

        self.check_election_safety();
    }

    /// Run the cluster for `ticks` logical ticks.
    pub fn run_ticks(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Propose a command to the current leader.
    ///
    /// Returns `(leader_id, log_index)` or `None` if there is no unique leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<(NodeId, LogIndex)> {
        let leader = self.current_leader()?;
        let node = self.nodes.get_mut(&leader)?;
        let idx = node.propose(command)?;
        Some((leader, idx))
    }

    /// Return the ID of the unique current leader, or `None`.
    pub fn current_leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect();
        match leaders.as_slice() {
            [single] => Some(*single),
            _ => None,
        }
    }

    /// Return the current status of all nodes.
    pub fn statuses(&self) -> HashMap<NodeId, RaftStatus> {
        self.nodes.iter().map(|(&id, n)| (id, n.status())).collect()
    }

    /// Return the entries that `node_id` has applied to its state machine.
    pub fn applied_entries(&self, node_id: NodeId) -> &[(LogIndex, Term, Vec<u8>)] {
        self.nodes
            .get(&node_id)
            .map(|n| n.applied_entries.as_slice())
            .unwrap_or(&[])
    }

    /// Mutable access to the network for fault injection.
    pub fn network_mut(&mut self) -> &mut SimNetwork {
        &mut self.network
    }

    /// All invariant violations recorded so far.
    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    /// Consume the simulator and return a final report.
    pub fn report(self) -> ClusterSimReport {
        ClusterSimReport {
            seed: 0,
            ticks: self.ticks,
            invariant_failures: self.violations,
        }
    }

    // ── Invariant checks ──────────────────────────────────────────────────────

    /// RAFT-INV-001: at most one leader per term.
    fn check_election_safety(&mut self) {
        let mut leaders_per_term: HashMap<Term, Vec<NodeId>> = HashMap::new();
        for node in self.nodes.values() {
            if node.is_leader() {
                leaders_per_term
                    .entry(node.status().current_term)
                    .or_default()
                    .push(node.id());
            }
        }
        for (term, leaders) in &leaders_per_term {
            if leaders.len() > 1 {
                let msg = format!(
                    "RAFT-INV-001: multiple leaders in term {}: {:?}",
                    term.0, leaders
                );
                if !self.violations.contains(&msg) {
                    self.violations.push(msg);
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn no_fault_config() -> SimNetworkConfig {
        SimNetworkConfig {
            drop_percent: 0,
            dup_percent: 0,
        }
    }

    /// A 3-node cluster elects exactly one leader.
    #[test]
    fn leader_elected_3_nodes() {
        let mut sim = ClusterSim::new(3, 42, no_fault_config());
        sim.run_ticks(60);
        assert!(
            sim.current_leader().is_some(),
            "no leader after 60 ticks; statuses: {:?}",
            sim.statuses()
        );
    }

    /// A 5-node cluster elects exactly one leader.
    #[test]
    fn leader_elected_5_nodes() {
        let mut sim = ClusterSim::new(5, 99, no_fault_config());
        sim.run_ticks(80);
        assert!(sim.current_leader().is_some(), "no leader after 80 ticks");
    }

    /// RAFT-INV-001: no two leaders in the same term (fault-free).
    #[test]
    fn election_safety_no_faults() {
        let mut sim = ClusterSim::new(3, 1, no_fault_config());
        sim.run_ticks(100);
        assert!(
            sim.violations().is_empty(),
            "invariant violations: {:?}",
            sim.violations()
        );
    }

    /// RAFT-INV-001 holds under message drops and duplicates.
    #[test]
    fn election_safety_with_faults() {
        let mut sim = ClusterSim::new(
            3,
            7777,
            SimNetworkConfig {
                drop_percent: 20,
                dup_percent: 10,
            },
        );
        sim.run_ticks(300);
        assert!(
            sim.violations().is_empty(),
            "invariant violations: {:?}",
            sim.violations()
        );
    }

    /// Commands proposed to the leader are applied on all nodes.
    #[test]
    fn log_replication() {
        let mut sim = ClusterSim::new(3, 42, no_fault_config());

        // Wait until a leader is elected.
        for _ in 0..60 {
            sim.step();
            if sim.current_leader().is_some() {
                break;
            }
        }
        assert!(sim.current_leader().is_some(), "no leader elected");

        let cmd = b"hello-world".to_vec();
        assert!(
            sim.propose(cmd.clone()).is_some(),
            "propose returned None — no leader"
        );

        // Run to convergence.
        sim.run_ticks(30);

        // Every node should have applied the command.
        let replicated = (1u64..=3)
            .map(NodeId)
            .filter(|&id| sim.applied_entries(id).iter().any(|(_, _, c)| c == &cmd))
            .count();
        assert_eq!(replicated, 3, "command not replicated to all nodes");
    }

    /// An isolated minority node does not become an additional leader.
    #[test]
    fn isolated_minority_cannot_lead() {
        let mut sim = ClusterSim::new(3, 55, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("no initial leader");

        // Isolate a follower.
        let follower = (1u64..=3).map(NodeId).find(|&id| id != leader).unwrap();
        let others: Vec<NodeId> = (1u64..=3)
            .map(NodeId)
            .filter(|&id| id != follower)
            .collect();
        sim.network_mut().isolate(follower, &others);

        sim.run_ticks(60);
        assert!(
            sim.violations().is_empty(),
            "safety violated under partition: {:?}",
            sim.violations()
        );
    }

    /// After a partition heals the isolated node catches up.
    #[test]
    fn partition_rejoin_catches_up() {
        let mut sim = ClusterSim::new(3, 77, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("no initial leader");

        let follower = (1u64..=3).map(NodeId).find(|&id| id != leader).unwrap();
        let others: Vec<NodeId> = (1u64..=3)
            .map(NodeId)
            .filter(|&id| id != follower)
            .collect();

        // Isolate follower, propose commands.
        sim.network_mut().isolate(follower, &others);
        for i in 1u8..=5 {
            sim.propose(vec![i]);
            sim.run_ticks(5);
        }

        // Heal and converge.
        sim.network_mut().reconnect(follower, &others);
        sim.run_ticks(60);

        let leader_applied = sim.applied_entries(leader).to_vec();
        let follower_applied = sim.applied_entries(follower).to_vec();
        assert_eq!(
            leader_applied.len(),
            follower_applied.len(),
            "follower {:?} has {} entries, leader {:?} has {}",
            follower,
            follower_applied.len(),
            leader,
            leader_applied.len()
        );
        assert_eq!(
            leader_applied, follower_applied,
            "applied entries differ after rejoin"
        );
    }

    /// Election safety holds across several independent seeds (CI seed suite).
    #[test]
    fn election_safety_multi_seed() {
        for seed in [0u64, 1, 2, 3, 42, 9999, 12345, 0xdead_beef, 0xcafe_babe] {
            let mut sim = ClusterSim::new(3, seed, no_fault_config());
            sim.run_ticks(100);
            assert!(
                sim.violations().is_empty(),
                "seed {seed}: violations {:?}",
                sim.violations()
            );
        }
    }

    /// The `Role` type is accessible on node status.
    #[test]
    fn status_role_accessible() {
        use kaya_raft::Role;
        let mut sim = ClusterSim::new(3, 0, no_fault_config());
        sim.run_ticks(60);
        let statuses = sim.statuses();
        let has_leader = statuses.values().any(|s| s.role == Role::Leader);
        assert!(has_leader, "no node in Leader role after 60 ticks");
    }
}
