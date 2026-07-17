use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use kaya_core::{EngineConfig, Lsn};
use kaya_engine::{Engine, ScanOptions, WriteOptions};
use kaya_io::SimDisk;
use kaya_raft::{
    ClusterMember, Envelope, LogIndex, NodeId, RaftApplyCommand, RaftCommand, RaftConfig, RaftNode,
    RaftStatus, Term,
};

use crate::model::RefModel;

use crate::rng::SimRng;

// ── Simulated network ─────────────────────────────────────────────────────────

/// Configuration for the simulated network fault injector.
#[derive(Debug, Clone)]
pub struct SimNetworkConfig {
    /// Percentage of messages to silently drop (0–100).
    pub drop_percent: u32,
    /// Percentage of messages to duplicate (0–100).
    pub dup_percent: u32,
    /// Delivery delay in logical ticks applied to every message. `0` keeps the
    /// same-tick request→response delivery the cluster relied on historically.
    pub latency_ticks: u32,
    /// Percentage of drained batches to deterministically reorder (0–100). `0`
    /// preserves per-destination FIFO order.
    pub reorder_percent: u32,
}

impl Default for SimNetworkConfig {
    fn default() -> Self {
        Self {
            drop_percent: 10,
            dup_percent: 5,
            latency_ticks: 0,
            reorder_percent: 0,
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
    /// In-flight messages, keyed by destination node. Each entry carries the
    /// logical tick at which it becomes deliverable (`deliver_at`).
    queues: HashMap<NodeId, VecDeque<(u64, Envelope)>>,
    /// Links on which all messages are silently dropped (unidirectional).
    partitions: HashSet<(NodeId, NodeId)>,
    /// Current logical tick, used to enforce `latency_ticks` delivery delay.
    current_tick: u64,
}

impl SimNetwork {
    pub fn new(seed: u64, config: SimNetworkConfig) -> Self {
        Self {
            config,
            // Offset the seed so the network RNG is independent from the operation RNG.
            rng: SimRng::new(seed.wrapping_add(0x9e37_79b9_7f4a_7c15)),
            queues: HashMap::new(),
            partitions: HashSet::new(),
            current_tick: 0,
        }
    }

    /// Advance the network's logical clock by one tick. Held (latency-delayed)
    /// messages become deliverable once `current_tick` reaches their
    /// `deliver_at`. Called once per [`ClusterSim`] step.
    pub fn advance_tick(&mut self) {
        self.current_tick += 1;
    }

    /// Inject a batch of outgoing envelopes into the network.
    ///
    /// Partitioned links and random drops are applied here; surviving messages
    /// are queued for the destination and can be retrieved via [`drain`].
    pub fn inject(&mut self, envelopes: Vec<Envelope>) {
        let deliver_at = self.current_tick + self.config.latency_ticks as u64;
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
            queue.push_back((deliver_at, env.clone()));
            if dup {
                queue.push_back((deliver_at, env));
            }
        }
    }

    /// Drain all messages whose delivery tick has arrived and return them.
    ///
    /// Messages still within their `latency_ticks` delay are retained. When
    /// `reorder_percent > 0` a deterministic fraction of the returned batch is
    /// shuffled to model out-of-order delivery.
    pub fn drain(&mut self) -> Vec<Envelope> {
        let now = self.current_tick;
        let mut out = Vec::new();
        for queue in self.queues.values_mut() {
            let mut retained = VecDeque::with_capacity(queue.len());
            while let Some((deliver_at, env)) = queue.pop_front() {
                if deliver_at <= now {
                    out.push(env);
                } else {
                    retained.push_back((deliver_at, env));
                }
            }
            *queue = retained;
        }
        if self.config.reorder_percent > 0 {
            self.reorder(&mut out);
        }
        out
    }

    /// Deterministically reorder a fraction of the batch by swapping selected
    /// elements. Fully reproducible via the network RNG.
    fn reorder(&mut self, batch: &mut [Envelope]) {
        if batch.len() < 2 {
            return;
        }
        for i in 0..batch.len() {
            if self.rng.usize_below(100) < self.config.reorder_percent as usize {
                let j = self.rng.usize_below(batch.len());
                batch.swap(i, j);
            }
        }
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

    /// Asymmetrically isolate `node`: drop only its *outgoing* messages to
    /// `peers`, while incoming messages still arrive. Models a one-way link
    /// failure, where a node believes it can reach peers but its replies (or
    /// heartbeats) are lost — a classic split-brain trigger.
    pub fn isolate_outgoing(&mut self, node: NodeId, peers: &[NodeId]) {
        for &peer in peers {
            self.partition(node, peer);
        }
    }

    /// Asymmetrically isolate `node`: drop only its *incoming* messages from
    /// `peers`, while its outgoing messages still arrive.
    pub fn isolate_incoming(&mut self, node: NodeId, peers: &[NodeId]) {
        for &peer in peers {
            self.partition(peer, node);
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
    /// Per-node replicated state machines (mirrors server engine apply path).
    state_machines: HashMap<NodeId, RefModel>,
    /// Per-node real engines on SimDisk (engine-backed snapshot path).
    engines: HashMap<NodeId, Engine<SimDisk>>,
    _disks: HashMap<NodeId, Arc<SimDisk>>,
    /// In-memory Raft index ↔ engine LSN correlation (mirrors server apply index).
    apply_records: HashMap<NodeId, Vec<RaftApplyCommand>>,
    runtime: tokio::runtime::Runtime,
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
        let state_machines = all_ids.iter().map(|&id| (id, RefModel::new())).collect();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio runtime for ClusterSim");

        let mut engines = HashMap::new();
        let mut disks = HashMap::new();
        let engine_cfg = EngineConfig {
            disable_locking: true,
            ..EngineConfig::default()
        };
        for &id in &all_ids {
            let disk = Arc::new(SimDisk::new());
            let engine = runtime
                .block_on(Engine::open(engine_cfg.clone(), disk.clone()))
                .expect("engine open in ClusterSim");
            disks.insert(id, disk);
            engines.insert(id, engine);
        }

        Self {
            nodes,
            state_machines,
            engines,
            _disks: disks,
            apply_records: HashMap::new(),
            runtime,
            all_ids,
            network: SimNetwork::new(seed, net_config),
            ticks: 0,
            violations: Vec::new(),
        }
    }

    /// Execute one logical tick across all nodes.
    pub fn step(&mut self) {
        self.ticks += 1;
        self.network.advance_tick();

        let ids: Vec<NodeId> = self.all_ids.clone();
        for &id in &ids {
            self.tick_node(id);
        }
        self.deliver_network_rounds();
    }

    /// Run the cluster for `ticks` logical ticks.
    pub fn run_ticks(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Propose a command to the current leader and immediately broadcast replication
    /// (mirrors the server Raft loop's propose + broadcast path).
    ///
    /// Returns `(leader_id, log_index)` or `None` if there is no unique leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<(NodeId, LogIndex)> {
        let leader = self.current_leader()?;
        let node = self.nodes.get_mut(&leader)?;
        let idx = node.propose(command)?;
        let out = node.broadcast();
        self.network.inject(out);
        Some((leader, idx))
    }

    /// Propose a [`RaftCommand::Put`] through the leader.
    pub fn propose_put(&mut self, key: &[u8], value: &[u8]) -> Option<(NodeId, LogIndex)> {
        let cmd = RaftCommand::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.propose(cmd.encode())
    }

    /// Propose a [`RaftCommand::Delete`] through the leader.
    pub fn propose_delete(&mut self, key: &[u8]) -> Option<(NodeId, LogIndex)> {
        let cmd = RaftCommand::Delete { key: key.to_vec() };
        self.propose(cmd.encode())
    }

    /// Propose a linearizable read on the leader (ReadIndex path).
    ///
    /// Returns the request id when accepted, or `None` if there is no unique leader.
    pub fn propose_read(&mut self, request_id: u64) -> Option<NodeId> {
        let leader = self.current_leader()?;
        let node = self.nodes.get_mut(&leader)?;
        node.propose_read(request_id)?;
        let out = node.broadcast();
        self.network.inject(out);
        Some(leader)
    }

    /// Drain ready read request ids from the leader after quorum confirmation.
    pub fn drain_ready_reads(&mut self) -> Vec<u64> {
        self.current_leader()
            .and_then(|leader| self.nodes.get_mut(&leader))
            .map(|node| node.drain_ready_reads())
            .unwrap_or_default()
    }

    /// Read-only access to a node's replicated state machine.
    pub fn state_machine(&self, id: NodeId) -> Option<&RefModel> {
        self.state_machines.get(&id)
    }

    /// Raft index ↔ engine LSN correlation records captured during apply.
    pub fn apply_records(&self, id: NodeId) -> &[RaftApplyCommand] {
        self.apply_records
            .get(&id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
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

    /// Take an engine-backed snapshot on the given node (compacts its Raft log).
    ///
    /// Uses the same `Engine::create_snapshot` path as the live server.
    /// Returns the (last_included_index, term) if a snapshot was created.
    pub fn take_snapshot(&mut self, id: NodeId) -> Option<(LogIndex, Term)> {
        let node = self.nodes.get_mut(&id)?;
        let status = node.status();
        let last = status.last_applied;
        if last.0 == 0 {
            return None;
        }

        let engine = self.engines.get_mut(&id)?;
        let engine_data = self
            .runtime
            .block_on(engine.create_snapshot())
            .map_err(|e| e.to_string())
            .ok()?;
        if engine_data.is_empty() {
            return None;
        }

        // Embed membership config (consistent with server) so that when a
        // follower/new node catches up via InstallSnapshot after a membership
        // change + snapshot, its Raft effective_config is restored correctly.
        let voters: Vec<NodeId> = node
            .effective_config()
            .stable_config()
            .voters
            .iter()
            .copied()
            .collect();
        let members = Self::sim_members(&voters);
        let combined = kaya_raft::build_snapshot_payload(&engine_data, &members);
        node.compact(last, status.current_term, combined);
        Some((last, status.current_term))
    }

    /// Add a new node to the simulation (not yet a voter until membership change).
    pub fn add_node(&mut self, id: NodeId) {
        if self.all_ids.contains(&id) {
            return;
        }
        let peers: Vec<NodeId> = self.all_ids.iter().copied().filter(|&p| p != id).collect();
        let timeout = 10 + (id.0.saturating_sub(1)) * 3;
        let config = RaftConfig {
            id,
            peers,
            election_timeout_ticks: timeout,
            heartbeat_interval_ticks: 3,
        };
        self.nodes.insert(id, RaftNode::new(config));
        self.state_machines.insert(id, RefModel::new());

        let engine_cfg = EngineConfig {
            disable_locking: true,
            ..EngineConfig::default()
        };
        let disk = Arc::new(SimDisk::new());
        let engine = self
            .runtime
            .block_on(Engine::open(engine_cfg, disk.clone()))
            .expect("engine open for added node");
        self._disks.insert(id, disk);
        self.engines.insert(id, engine);
        self.all_ids.push(id);
    }

    fn sim_members(voter_ids: &[NodeId]) -> Vec<ClusterMember> {
        voter_ids
            .iter()
            .map(|&id| ClusterMember {
                id,
                raft_addr: format!("sim://raft/{}", id.0),
                client_addr: format!("sim://client/{}", id.0),
                is_learner: false,
            })
            .collect()
    }

    /// Propose a joint-consensus membership change through the current leader.
    pub fn propose_membership_change(
        &mut self,
        new_voters: Vec<NodeId>,
    ) -> Option<(NodeId, LogIndex)> {
        let leader = self.current_leader()?;
        let node = self.nodes.get_mut(&leader)?;
        let idx = node.propose_membership_change(Self::sim_members(&new_voters))?;
        let out = node.broadcast();
        self.network.inject(out);
        Some((leader, idx))
    }

    /// Add `id` to the voter set via joint consensus.
    pub fn add_voter(&mut self, id: NodeId) -> Option<(NodeId, LogIndex)> {
        let leader = self.current_leader()?;
        let mut voters: Vec<NodeId> = self
            .nodes
            .get(&leader)?
            .effective_config()
            .stable_config()
            .voters
            .iter()
            .copied()
            .collect();
        if voters.contains(&id) {
            return None;
        }
        voters.push(id);
        voters.sort_by_key(|n| n.0);
        self.propose_membership_change(voters)
    }

    /// Remove `id` from the voter set via joint consensus.
    pub fn remove_voter(&mut self, id: NodeId) -> Option<(NodeId, LogIndex)> {
        let leader = self.current_leader()?;
        let voters: Vec<NodeId> = self
            .nodes
            .get(&leader)?
            .effective_config()
            .stable_config()
            .voters
            .iter()
            .copied()
            .filter(|&v| v != id)
            .collect();
        if voters.len() < 2 {
            return None;
        }
        self.propose_membership_change(voters)
    }

    /// Current voter set on a node (stable configuration).
    pub fn voter_ids(&self, id: NodeId) -> Vec<NodeId> {
        self.nodes
            .get(&id)
            .map(|n| {
                let mut ids: Vec<NodeId> = n
                    .effective_config()
                    .stable_config()
                    .voters
                    .iter()
                    .copied()
                    .collect();
                ids.sort_by_key(|n| n.0);
                ids
            })
            .unwrap_or_default()
    }

    /// Mutable access to the network for fault injection.
    pub fn network_mut(&mut self) -> &mut SimNetwork {
        &mut self.network
    }

    /// Tick a single node and inject its outgoing messages (used by clock-skew helper).
    pub(crate) fn tick_node(&mut self, node: NodeId) {
        let out = self.nodes.get_mut(&node).expect("node").tick();
        self.network.inject(out);
    }

    /// Deliver queued network messages (two rounds) and run invariant checks.
    pub(crate) fn deliver_network_rounds(&mut self) {
        let round1 = self.network.drain();
        for env in round1 {
            if let Some(n) = self.nodes.get_mut(&env.to) {
                let out = n.handle(env);
                self.network.inject(out);
            }
        }

        let round2 = self.network.drain();
        for env in round2 {
            if let Some(n) = self.nodes.get_mut(&env.to) {
                let out = n.handle(env);
                self.network.inject(out);
            }
        }

        self.apply_drained_entries();
        self.check_election_safety();
        self.check_state_machine_convergence();
    }

    /// Replace a node's engine disk (e.g. inject [`SimDisk::with_faults`] schedules).
    pub fn replace_node_disk(&mut self, id: NodeId, disk: Arc<SimDisk>) {
        let engine_cfg = EngineConfig {
            disable_locking: true,
            ..EngineConfig::default()
        };
        let engine = self
            .runtime
            .block_on(Engine::open(engine_cfg, disk.clone()))
            .unwrap_or_else(|e| panic!("engine open for node {}: {e}", id.0));
        self._disks.insert(id, disk);
        self.engines.insert(id, engine);
    }

    /// The highest log index covered by a snapshot on this node (0 if none).
    pub fn last_included(&self, id: NodeId) -> LogIndex {
        self.nodes
            .get(&id)
            .map(|n| n.last_included_index())
            .unwrap_or(LogIndex(0))
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

    // ── State machine apply path (mirrors server drain_and_apply) ─────────────

    pub(crate) fn apply_drained_entries(&mut self) {
        for &id in &self.all_ids {
            let node = match self.nodes.get_mut(&id) {
                Some(n) => n,
                None => continue,
            };

            if let Some((_idx, _term, data)) = node.drain_installed_snapshot() {
                if let Some(engine) = self.engines.get_mut(&id) {
                    // Parse combined payload (if present) to restore membership config
                    // on the RaftNode after snapshot (for dynamic membership + snapshot tests).
                    let engine_data =
                        if let Ok((eng, mems)) = kaya_raft::parse_snapshot_payload(&data) {
                            if !mems.is_empty() {
                                node.restore_config_from_snapshot(mems);
                            }
                            if !eng.is_empty() {
                                eng
                            } else {
                                data
                            }
                        } else {
                            data
                        };
                    match self.runtime.block_on(engine.install_snapshot(&engine_data)) {
                        Ok(()) => {
                            let model = self.runtime.block_on(sync_ref_model_from_engine(engine));
                            self.state_machines.insert(id, model);
                        }
                        Err(e) => {
                            let msg = format!(
                                "RAFT-INV-003: node {} failed engine snapshot install: {e}",
                                id.0
                            );
                            if !self.violations.contains(&msg) {
                                self.violations.push(msg);
                            }
                        }
                    }
                }
            }

            let applied = node.drain_applied();
            for (idx, term, command) in applied {
                if let Some(sm) = self.state_machines.get_mut(&id) {
                    if let Err(e) = sm.apply_log_entry(&command) {
                        let msg = format!("RAFT-INV-003: node {} corrupt log entry: {e}", id.0);
                        if !self.violations.contains(&msg) {
                            self.violations.push(msg);
                        }
                    }
                }

                let lsn = if command.is_empty() {
                    None
                } else if let Some(engine) = self.engines.get_mut(&id) {
                    match self
                        .runtime
                        .block_on(apply_command_to_engine(engine, &command))
                    {
                        Ok(lsn) => lsn,
                        Err(e) => {
                            let msg =
                                format!("RAFT-INV-003: node {} engine apply failed: {e}", id.0);
                            if !self.violations.contains(&msg) {
                                self.violations.push(msg);
                            }
                            None
                        }
                    }
                } else {
                    None
                };

                self.apply_records
                    .entry(id)
                    .or_default()
                    .push(RaftApplyCommand {
                        term,
                        index: idx,
                        engine_lsn_hint: lsn,
                    });
            }
        }
    }

    // ── Invariant checks ──────────────────────────────────────────────────────

    /// RAFT-INV-002: caught-up nodes share identical replicated state.
    pub(crate) fn check_state_machine_convergence(&mut self) {
        let max_applied = self
            .nodes
            .values()
            .map(|n| n.status().last_applied)
            .max()
            .unwrap_or(LogIndex(0));
        if max_applied.0 == 0 {
            return;
        }

        let caught_up: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.status().last_applied == max_applied)
            .map(|(&id, _)| id)
            .collect();
        if caught_up.len() < 2 {
            return;
        }

        let reference = caught_up[0];
        let ref_state = self
            .state_machines
            .get(&reference)
            .map(|m| m.scan_prefix(b""))
            .unwrap_or_default();

        for &id in &caught_up[1..] {
            let state = self
                .state_machines
                .get(&id)
                .map(|m| m.scan_prefix(b""))
                .unwrap_or_default();
            if state != ref_state {
                let msg = format!(
                    "RAFT-INV-002: state divergence at applied={}: node {} != node {}",
                    max_applied.0, reference.0, id.0
                );
                if !self.violations.contains(&msg) {
                    self.violations.push(msg);
                }
            }
        }
    }

    /// RAFT-INV-001: at most one leader per term.
    pub(crate) fn check_election_safety(&mut self) {
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

async fn apply_command_to_engine(
    engine: &mut Engine<SimDisk>,
    command: &[u8],
) -> Result<Option<Lsn>, String> {
    match RaftCommand::decode(command)? {
        RaftCommand::Put { key, value } => engine
            .put(key, value, WriteOptions::default())
            .await
            .map(|r| Some(r.lsn))
            .map_err(|e| e.to_string()),
        RaftCommand::Delete { key } => engine
            .delete(key, WriteOptions::default())
            .await
            .map(|r| Some(r.lsn))
            .map_err(|e| e.to_string()),
        RaftCommand::ConfigChange { .. } => Ok(None),
        RaftCommand::TxnCommit { mutations, .. } => {
            if mutations.is_empty() {
                return Ok(None);
            }
            engine
                .apply_mutations(mutations.into_iter().collect(), WriteOptions::default())
                .await
                .map(|_| None)
                .map_err(|e| e.to_string())
        }
        RaftCommand::TxnPrepare {
            txn_id, mutations, ..
        } => {
            let mutations: Vec<_> = mutations.into_iter().collect();
            engine
                .apply_txn_prepare(txn_id, &mutations)
                .await
                .map(|_| None)
                .map_err(|e| e.to_string())
        }
        RaftCommand::TxnCommit2pc { txn_id } => engine
            .apply_txn_commit_2pc(txn_id)
            .await
            .map(|_| None)
            .map_err(|e| e.to_string()),
        RaftCommand::TxnAbort2pc { txn_id } => engine
            .apply_txn_abort_2pc(txn_id)
            .await
            .map(|_| None)
            .map_err(|e| e.to_string()),
    }
}

async fn sync_ref_model_from_engine(engine: &mut Engine<SimDisk>) -> RefModel {
    let mut model = RefModel::new();
    if let Ok(kvs) = engine.scan_prefix(b"", ScanOptions::default()).await {
        // Latest-only rebuild: assign synthetic increasing seqs per key.
        let mut seq = 1u64;
        for kv in kvs {
            model.put(kv.key, kv.value, seq);
            seq = seq.saturating_add(1);
        }
    }
    model
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn no_fault_config() -> SimNetworkConfig {
        SimNetworkConfig {
            drop_percent: 0,
            dup_percent: 0,
            latency_ticks: 0,
            reorder_percent: 0,
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
                latency_ticks: 0,
                reorder_percent: 0,
            },
        );
        sim.run_ticks(300);
        assert!(
            sim.violations().is_empty(),
            "invariant violations: {:?}",
            sim.violations()
        );
    }

    /// A leader is still elected and safety holds when every message is delayed
    /// by a fixed number of logical ticks.
    #[test]
    fn election_safe_under_network_latency() {
        let mut sim = ClusterSim::new(
            3,
            31,
            SimNetworkConfig {
                drop_percent: 0,
                dup_percent: 0,
                latency_ticks: 2,
                reorder_percent: 0,
            },
        );
        sim.run_ticks(120);
        assert!(sim.current_leader().is_some(), "no leader under latency");
        assert!(
            sim.violations().is_empty(),
            "invariant violations under latency: {:?}",
            sim.violations()
        );
    }

    /// RAFT-INV-001 holds when delivery order is scrambled and messages are
    /// dropped, duplicated and delayed together.
    #[test]
    fn election_safe_under_reorder_and_latency() {
        let mut sim = ClusterSim::new(
            5,
            9001,
            SimNetworkConfig {
                drop_percent: 15,
                dup_percent: 10,
                latency_ticks: 1,
                reorder_percent: 50,
            },
        );
        sim.run_ticks(400);
        assert!(
            sim.violations().is_empty(),
            "invariant violations under reorder+latency: {:?}",
            sim.violations()
        );
    }

    /// A one-way (asymmetric) partition of the leader's outgoing links forces a
    /// re-election without ever producing two leaders in one term.
    #[test]
    fn asymmetric_partition_preserves_election_safety() {
        let mut sim = ClusterSim::new(3, 12345, no_fault_config());
        sim.run_ticks(40);
        let leader = sim.current_leader().expect("leader before partition");
        let peers: Vec<NodeId> = (1..=3).map(NodeId).filter(|&n| n != leader).collect();
        // Leader can still hear peers but its own messages never arrive.
        sim.network_mut().isolate_outgoing(leader, &peers);
        sim.run_ticks(200);
        assert!(
            sim.violations().is_empty(),
            "asymmetric partition caused a safety violation: {:?}",
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

        assert!(
            sim.propose_put(b"hello", b"world").is_some(),
            "propose returned None — no leader"
        );

        // Run to convergence.
        sim.run_ticks(30);

        // Every node should have the key in its replicated state machine.
        for id in (1u64..=3).map(NodeId) {
            let sm = sim.state_machine(id).expect("missing state machine");
            assert_eq!(sm.get(b"hello"), Some(&b"world".to_vec()));
        }
        assert!(sim.violations().is_empty(), "{:?}", sim.violations());
    }

    /// An isolated minority node does not become an additional leader.
    #[test]
    fn partition_isolated_minority_cannot_lead() {
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
            sim.propose_put(format!("k{i}").as_bytes(), &[i]);
            sim.run_ticks(5);
        }

        // Heal and converge.
        sim.network_mut().reconnect(follower, &others);
        sim.run_ticks(60);

        let leader_state = sim
            .state_machine(leader)
            .expect("leader state machine")
            .scan_prefix(b"k");
        let follower_state = sim
            .state_machine(follower)
            .expect("follower state machine")
            .scan_prefix(b"k");
        assert_eq!(
            leader_state, follower_state,
            "state machines differ after rejoin"
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

    /// Snapshot can be taken and compacts the log (last_included advances).
    #[test]
    fn snapshot_compacts_log() {
        let mut sim = ClusterSim::new(3, 123, no_fault_config());

        // Get a leader and apply some commands
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader should exist");

        for i in 0u8..=20 {
            let _ = sim.propose_put(format!("snap-{i}").as_bytes(), &[i]);
            sim.run_ticks(3);
        }

        let before = sim.last_included(leader);
        assert_eq!(before, LogIndex(0), "no snapshot yet");

        // Take snapshot
        let snap = sim.take_snapshot(leader);
        assert!(snap.is_some(), "snapshot should be created");

        let after = sim.last_included(leader);
        assert!(after.0 > 0, "last_included should advance after snapshot");

        // The applied history should be trimmed in the node (prefix removed)
        let applied_after = sim.applied_entries(leader).len();
        // We expect some trimming happened (not the full 21+ entries kept if they were before snapshot)
        // Exact count depends on when snapshot was taken; just ensure it didn't grow unbounded in the snapshot case.
        assert!(
            applied_after < 30,
            "applied entries list should be reasonable after trim"
        );
    }

    /// A 5-node cluster requires a 3-node quorum to commit entries.
    #[test]
    fn quorum_commit_requires_majority_in_five_node_cluster() {
        let mut sim = ClusterSim::new(5, 88, no_fault_config());
        sim.run_ticks(80);
        let leader = sim.current_leader().expect("leader should exist");

        // Isolate two followers — majority (3/5) remains.
        let followers: Vec<NodeId> = (1u64..=5)
            .map(NodeId)
            .filter(|&id| id != leader)
            .take(2)
            .collect();
        for &follower in &followers {
            let peers: Vec<NodeId> = (1u64..=5)
                .map(NodeId)
                .filter(|&id| id != follower)
                .collect();
            sim.network_mut().isolate(follower, &peers);
        }

        assert!(sim.propose_put(b"quorum-key", b"quorum-val").is_some());
        sim.run_ticks(40);

        let leader_sm = sim.state_machine(leader).expect("leader sm");
        assert_eq!(leader_sm.get(b"quorum-key"), Some(&b"quorum-val".to_vec()));

        // Now isolate two more followers so only leader + one follower remain (2/5 < quorum).
        let remaining_followers: Vec<NodeId> = (1u64..=5)
            .map(NodeId)
            .filter(|&id| id != leader && !followers.contains(&id))
            .collect();
        for &follower in &remaining_followers {
            let peers: Vec<NodeId> = (1u64..=5)
                .map(NodeId)
                .filter(|&id| id != follower)
                .collect();
            sim.network_mut().isolate(follower, &peers);
        }

        assert!(sim.propose_put(b"blocked-key", b"blocked-val").is_some());
        sim.run_ticks(40);

        // Minority partition should not have committed the second write cluster-wide.
        let caught_up = (1u64..=5)
            .map(NodeId)
            .filter(|&id| {
                sim.state_machine(id)
                    .and_then(|sm| sm.get(b"blocked-key"))
                    .is_some()
            })
            .count();
        assert!(
            caught_up < 3,
            "minority partition committed blocked-key on {caught_up} nodes"
        );
    }

    /// ReadIndex path: linearizable read becomes ready after quorum heartbeat acks.
    #[test]
    fn read_index_quorum_confirmation() {
        let mut sim = ClusterSim::new(3, 314, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader should exist");

        sim.propose_put(b"rkey", b"rval").unwrap();
        sim.run_ticks(20);

        assert!(sim.propose_read(42).is_some());
        sim.run_ticks(20);

        let ready = sim.drain_ready_reads();
        assert!(ready.contains(&42), "read 42 not ready; ready={ready:?}");

        let sm = sim.state_machine(leader).expect("leader sm");
        assert_eq!(sm.get(b"rkey"), Some(&b"rval".to_vec()));
    }

    /// Put + delete through real RaftCommand entries converges on all nodes.
    #[test]
    fn raft_command_put_delete_converges() {
        let mut sim = ClusterSim::new(3, 515, no_fault_config());
        sim.run_ticks(60);
        assert!(sim.current_leader().is_some());

        sim.propose_put(b"temp", b"value").unwrap();
        sim.run_ticks(20);
        sim.propose_delete(b"temp").unwrap();
        sim.run_ticks(20);

        for id in (1u64..=3).map(NodeId) {
            let sm = sim.state_machine(id).expect("state machine");
            assert!(sm.get(b"temp").is_none());
        }
        assert!(sim.violations().is_empty(), "{:?}", sim.violations());
    }

    /// Applied Raft entries record engine LSN hints (WAL↔Raft correlation).
    #[test]
    fn apply_records_capture_engine_lsn() {
        let mut sim = ClusterSim::new(3, 616, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader");

        sim.propose_put(b"lsn-key", b"lsn-val").unwrap();
        sim.run_ticks(30);

        let records = sim.apply_records(leader);
        let write_records: Vec<_> = records
            .iter()
            .filter(|r| r.engine_lsn_hint.is_some())
            .collect();
        assert!(
            !write_records.is_empty(),
            "expected at least one LSN-correlated apply record"
        );
        assert!(write_records.last().unwrap().index.0 > 0);
    }

    /// Engine-backed snapshot replicates to a lagging follower via InstallSnapshot.
    #[test]
    fn engine_backed_snapshot_catches_up_follower() {
        let mut sim = ClusterSim::new(3, 717, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader");

        let follower = (1u64..=3).map(NodeId).find(|&id| id != leader).unwrap();
        let peers: Vec<NodeId> = (1u64..=3)
            .map(NodeId)
            .filter(|&id| id != follower)
            .collect();
        sim.network_mut().isolate(follower, &peers);

        for i in 0u8..=10 {
            sim.propose_put(format!("eng-{i}").as_bytes(), &[i])
                .unwrap();
            sim.run_ticks(4);
        }

        sim.take_snapshot(leader);
        sim.network_mut().reconnect(follower, &peers);
        sim.run_ticks(80);

        let leader_state = sim
            .state_machine(leader)
            .expect("leader sm")
            .scan_prefix(b"eng-");
        let follower_state = sim
            .state_machine(follower)
            .expect("follower sm")
            .scan_prefix(b"eng-");
        assert_eq!(leader_state, follower_state);
        assert!(sim.violations().is_empty(), "{:?}", sim.violations());
    }

    /// SimDisk DiskFull on a follower engine does not violate election safety.
    #[test]
    fn cluster_disk_full_election_stable() {
        use kaya_io::{FaultKind, FaultRule, FaultSchedule, SimSeed};

        let mut sim = ClusterSim::new(3, 505, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader");

        let follower = (1u64..=3)
            .map(NodeId)
            .find(|&id| id != leader)
            .expect("follower");
        let schedule = FaultSchedule {
            seed: SimSeed(505),
            rules: vec![FaultRule {
                operation_index: 0,
                kind: FaultKind::DiskFull,
            }],
        };
        sim.replace_node_disk(follower, Arc::new(SimDisk::with_faults(schedule)));

        sim.propose_put(b"disk-key", b"disk-val");
        sim.run_ticks(80);

        let election_violations: Vec<_> = sim
            .violations()
            .iter()
            .filter(|v| v.contains("RAFT-INV-001"))
            .collect();
        assert!(
            election_violations.is_empty(),
            "election safety violated under disk full: {:?}",
            sim.violations()
        );
        assert!(
            sim.current_leader().is_some(),
            "cluster should retain a leader after disk-full injection"
        );
    }

    /// Joint consensus expands the voter set from 3 to 4 nodes.
    #[test]
    fn joint_consensus_adds_fourth_voter() {
        let mut sim = ClusterSim::new(3, 818, no_fault_config());
        sim.run_ticks(60);
        let leader = sim.current_leader().expect("leader");

        sim.add_node(NodeId(4));
        assert!(sim.add_voter(NodeId(4)).is_some());
        sim.run_ticks(120);

        for id in [leader, NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
            let voters = sim.voter_ids(id);
            assert!(
                voters.contains(&NodeId(4)),
                "node {} voters {:?} missing node 4",
                id.0,
                voters
            );
        }

        sim.propose_put(b"after-join", b"ok").unwrap();
        sim.run_ticks(40);
        for id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
            let val = sim
                .state_machine(id)
                .and_then(|m| m.get(b"after-join"))
                .cloned();
            assert_eq!(val.as_deref(), Some(b"ok".as_ref()));
        }
        assert!(sim.violations().is_empty(), "{:?}", sim.violations());
    }
}
