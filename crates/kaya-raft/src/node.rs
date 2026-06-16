use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{
    cluster_config::{ClusterConfiguration, EffectiveConfig},
    command::{ClusterMember, ConfigChangePhase, RaftCommand},
    log::{LogEntry, MemLog},
    message::{AppendRequest, AppendResponse, Envelope, Message, VoteRequest, VoteResponse},
    types::{LogIndex, NodeId, Term},
};

/// Static configuration for a single Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's identity.
    pub id: NodeId,
    /// All other nodes in the cluster (excluding self).
    pub peers: Vec<NodeId>,
    /// Election timeout in logical ticks. Should be staggered across nodes.
    pub election_timeout_ticks: u64,
    /// How often the leader sends heartbeats (in logical ticks).
    pub heartbeat_interval_ticks: u64,
}

/// Current role of a Raft node within a term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Observable snapshot of a node's state (no mutable access).
#[derive(Debug, Clone)]
pub struct RaftStatus {
    pub id: NodeId,
    pub role: Role,
    pub current_term: Term,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub leader_id: Option<NodeId>,
}

#[derive(Debug, Clone)]
struct PendingRead {
    request_id: u64,
    read_index: LogIndex,
    term: Term,
    ack_seq: u64,
    acks: HashSet<NodeId>,
}

/// A complete Raft state-machine node.
///
/// All I/O is removed from this struct. The caller drives logical time via
/// [`RaftNode::tick`] and delivers incoming messages via [`RaftNode::handle`].
/// Both return the set of outgoing [`Envelope`]s to dispatch.
pub struct RaftNode {
    config: RaftConfig,

    // ── Persistent state (in-memory for prototype) ────────────────────────────
    current_term: Term,
    voted_for: Option<NodeId>,
    log: MemLog,

    // ── Volatile state ────────────────────────────────────────────────────────
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,
    leader_id: Option<NodeId>,

    // ── Election timer (follower / candidate) ─────────────────────────────────
    election_ticks: u64,

    // ── Candidate state ───────────────────────────────────────────────────────
    votes_received: HashSet<NodeId>,

    // ── Leader state ──────────────────────────────────────────────────────────
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    heartbeat_ticks: u64,

    // ── Applied entries (visible to the cluster simulator) ────────────────────
    /// Every entry that has been applied to this node's state machine, in order.
    /// Each tuple is `(log_index, term, command)`.
    pub applied_entries: Vec<(LogIndex, Term, Vec<u8>)>,

    // ── ReadIndex state ───────────────────────────────────────────────────────
    pending_reads: Vec<PendingRead>,
    current_ack_seq: u64,
    last_sent_ack_seq: HashMap<NodeId, u64>,
    ready_reads: Vec<u64>,

    // ── Snapshot state (for state machine to consume) ─────────────────────────
    pending_snapshot: Option<(LogIndex, Term, Vec<u8>)>,

    // ── Joint-consensus membership ────────────────────────────────────────────
    effective_config: EffectiveConfig,
    /// Target member set while a joint→final change is in flight.
    pending_membership: Option<Vec<ClusterMember>>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        let mut voters: BTreeSet<NodeId> = config.peers.iter().copied().collect();
        voters.insert(config.id);
        let effective_config = EffectiveConfig::stable(voters);
        Self {
            config,
            current_term: Term(0),
            voted_for: None,
            log: MemLog::new(),
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            role: Role::Follower,
            leader_id: None,
            election_ticks: 0,
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            heartbeat_ticks: 0,
            applied_entries: Vec::new(),
            pending_reads: Vec::new(),
            current_ack_seq: 0,
            last_sent_ack_seq: HashMap::new(),
            ready_reads: Vec::new(),
            pending_snapshot: None,
            effective_config,
            pending_membership: None,
        }
    }

    /// This node's identity.
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// Returns `true` if this node currently believes itself to be the leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Snapshot of the node's observable state.
    pub fn status(&self) -> RaftStatus {
        RaftStatus {
            id: self.config.id,
            role: self.role,
            current_term: self.current_term,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            leader_id: self.leader_id,
        }
    }

    /// Propose a command for replication. Only valid when this node is the leader.
    ///
    /// Returns the log index assigned to the entry, or `None` if not the leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.log.append(LogEntry {
            term: self.current_term,
            command,
        });
        self.match_index.insert(self.config.id, index);
        self.next_index
            .insert(self.config.id, LogIndex(index.0 + 1));
        Some(index)
    }

    /// Propose a read query. Only valid when this node is the leader.
    ///
    /// Returns the commit index at the time of proposal (ReadIndex), or `None` if not the leader.
    pub fn propose_read(&mut self, request_id: u64) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        self.current_ack_seq += 1;

        let mut acks = HashSet::new();
        acks.insert(self.config.id);

        self.pending_reads.push(PendingRead {
            request_id,
            read_index: self.commit_index,
            term: self.current_term,
            ack_seq: self.current_ack_seq,
            acks,
        });

        Some(self.commit_index)
    }

    /// Drain all ready reads that have been confirmed by a majority of the cluster
    /// and have had their ReadIndex surpassed by `last_applied`.
    pub fn drain_ready_reads(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.ready_reads)
    }

    /// Advance logical time by one tick. Returns outgoing messages.
    pub fn tick(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_ticks += 1;
                if self.election_ticks >= self.config.election_timeout_ticks {
                    self.start_election(&mut out);
                }
            }
            Role::Leader => {
                self.heartbeat_ticks += 1;
                if self.heartbeat_ticks >= self.config.heartbeat_interval_ticks {
                    self.heartbeat_ticks = 0;
                    self.send_append_to_all(&mut out);
                }
            }
        }
        out.extend(self.try_advance_apply());
        out
    }

    /// Process an incoming message envelope. Returns outgoing messages.
    pub fn handle(&mut self, env: Envelope) -> Vec<Envelope> {
        let mut out = Vec::new();

        // If the message carries a higher term, revert to follower immediately.
        let msg_term = match &env.message {
            Message::VoteRequest(m) => m.term,
            Message::VoteResponse(m) => m.term,
            Message::AppendRequest(m) => m.term,
            Message::AppendResponse(m) => m.term,
            Message::InstallSnapshotRequest(m) => m.term,
            Message::InstallSnapshotResponse(m) => m.term,
            Message::ConfigChangeRequest(m) => m.term,
            Message::ConfigChangeResponse(m) => m.term,
        };
        if msg_term > self.current_term {
            self.step_down(msg_term);
        }

        match env.message {
            Message::VoteRequest(m) => self.on_vote_request(env.from, m, &mut out),
            Message::VoteResponse(m) => self.on_vote_response(env.from, m, &mut out),
            Message::AppendRequest(m) => self.on_append_request(env.from, m, &mut out),
            Message::AppendResponse(m) => self.on_append_response(env.from, m, &mut out),
            Message::InstallSnapshotRequest(m) => {
                self.on_install_snapshot_request(env.from, m, &mut out)
            }
            Message::InstallSnapshotResponse(m) => {
                self.on_install_snapshot_response(env.from, m, &mut out)
            }
            Message::ConfigChangeRequest(m) => self.on_config_change_request(env.from, m, &mut out),
            Message::ConfigChangeResponse(m) => {
                self.on_config_change_response(env.from, m, &mut out)
            }
        }

        out.extend(self.try_advance_apply());
        out
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Majority quorum size for the current effective configuration.
    fn quorum(&self) -> usize {
        self.effective_config.stable_config().quorum()
    }

    /// Cluster size (self + peers).
    pub fn cluster_size(&self) -> usize {
        self.effective_config.stable_config().voters.len()
    }

    /// Required majority vote/replication count for this cluster.
    pub fn quorum_size(&self) -> usize {
        self.quorum()
    }

    /// Current effective configuration (stable or joint).
    pub fn effective_config(&self) -> &EffectiveConfig {
        &self.effective_config
    }

    fn sync_peers_from_effective_config(&mut self) {
        self.config.peers = self
            .effective_config
            .stable_config()
            .peers_of(self.config.id);
    }

    fn apply_config_command(
        &mut self,
        phase: ConfigChangePhase,
        members: Vec<ClusterMember>,
        out: &mut Vec<Envelope>,
    ) {
        let voter_set: BTreeSet<NodeId> = members.iter().map(|m| m.id).collect();
        match phase {
            ConfigChangePhase::Joint => {
                let outgoing = self.effective_config.stable_config().clone();
                let incoming = ClusterConfiguration::from_voters(voter_set);
                self.effective_config = EffectiveConfig::Joint { outgoing, incoming };
                self.sync_peers_from_effective_config();
                if self.role == Role::Leader {
                    if let Some(final_members) = self.pending_membership.take() {
                        let cmd = RaftCommand::ConfigChange {
                            phase: ConfigChangePhase::Final,
                            members: final_members,
                        };
                        let idx = self.log.append(LogEntry {
                            term: self.current_term,
                            command: cmd.encode(),
                        });
                        self.match_index.insert(self.config.id, idx);
                        self.next_index.insert(self.config.id, LogIndex(idx.0 + 1));
                        self.send_append_to_all(out);
                    }
                }
            }
            ConfigChangePhase::Final => {
                self.effective_config =
                    EffectiveConfig::Stable(ClusterConfiguration::from_voters(voter_set));
                self.sync_peers_from_effective_config();
                self.pending_membership = None;
                let last = self.log.last_index();
                for &peer in &self.config.peers {
                    self.next_index.entry(peer).or_insert(LogIndex(last.0 + 1));
                    self.match_index.entry(peer).or_insert(LogIndex(0));
                }
            }
        }
    }

    /// Revert to follower, adopting `new_term`.
    fn step_down(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader_id = None;
        self.election_ticks = 0;
        self.votes_received.clear();
        self.pending_reads.clear();
    }

    /// Increment term, become candidate, vote for self, send RequestVote to peers.
    fn start_election(&mut self, out: &mut Vec<Envelope>) {
        self.current_term = Term(self.current_term.0 + 1);
        self.role = Role::Candidate;
        self.voted_for = Some(self.config.id);
        self.election_ticks = 0;
        self.votes_received.clear();
        self.votes_received.insert(self.config.id);

        let req = VoteRequest {
            term: self.current_term,
            candidate_id: self.config.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for &peer in &self.config.peers {
            out.push(Envelope::new(
                self.config.id,
                peer,
                Message::VoteRequest(req.clone()),
            ));
        }
    }

    /// Transition to leader, initialise per-peer state, append no-op, send heartbeat.
    fn become_leader(&mut self, out: &mut Vec<Envelope>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.heartbeat_ticks = 0;

        // Initialise next/match indexes (Raft §5.3).
        let last = self.log.last_index();
        for &peer in &self.config.peers {
            self.next_index.insert(peer, LogIndex(last.0 + 1));
            self.match_index.insert(peer, LogIndex(0));
        }
        // Own match index before no-op.
        self.match_index.insert(self.config.id, last);
        self.next_index.insert(self.config.id, LogIndex(last.0 + 1));

        // Append a no-op entry to establish a commit barrier for entries from
        // previous terms (Raft §5.4.2).
        let noop = self.log.append(LogEntry {
            term: self.current_term,
            command: vec![],
        });
        self.match_index.insert(self.config.id, noop);
        self.next_index.insert(self.config.id, LogIndex(noop.0 + 1));

        // Broadcast the no-op immediately.
        self.send_append_to_all(out);
    }

    fn send_append_to_all(&mut self, out: &mut Vec<Envelope>) {
        let peers: Vec<NodeId> = self.config.peers.clone();
        for peer in peers {
            self.send_append_to(peer, out);
        }
    }

    fn send_append_to(&mut self, peer: NodeId, out: &mut Vec<Envelope>) {
        self.last_sent_ack_seq.insert(peer, self.current_ack_seq);

        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(LogIndex(self.log.last_index().0 + 1));

        // If the follower is behind our snapshot, send a snapshot instead of log entries.
        let snap_idx = self.log.last_included_index();
        if next <= snap_idx && snap_idx.0 > 0 {
            if let Some((idx, term, data)) = self.snapshot() {
                out.push(Envelope::new(
                    self.config.id,
                    peer,
                    Message::InstallSnapshotRequest(crate::InstallSnapshotRequest {
                        term: self.current_term,
                        leader_id: self.config.id,
                        last_included_index: idx,
                        last_included_term: term,
                        data,
                    }),
                ));
                return;
            }
        }

        let prev_idx = LogIndex(next.0.saturating_sub(1));
        let prev_term = self.log.term_at(prev_idx).unwrap_or(Term(0));
        let entries = self.log.entries_from(next).to_vec();

        out.push(Envelope::new(
            self.config.id,
            peer,
            Message::AppendRequest(AppendRequest {
                term: self.current_term,
                leader_id: self.config.id,
                prev_log_index: prev_idx,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
            }),
        ));
    }

    fn on_vote_request(&mut self, from: NodeId, req: VoteRequest, out: &mut Vec<Envelope>) {
        // Raft §5.2, §5.4.1: grant if haven't voted (or voted for this candidate)
        // and candidate log is at least as up-to-date as ours.
        let log_ok = (req.last_log_term, req.last_log_index)
            >= (self.log.last_term(), self.log.last_index());

        let vote_granted = req.term >= self.current_term
            && log_ok
            && (self.voted_for.is_none() || self.voted_for == Some(from));

        if vote_granted {
            self.voted_for = Some(from);
            self.election_ticks = 0; // reset timer to avoid competing election
        }

        out.push(Envelope::new(
            self.config.id,
            from,
            Message::VoteResponse(VoteResponse {
                term: self.current_term,
                vote_granted,
            }),
        ));
    }

    fn on_vote_response(&mut self, from: NodeId, resp: VoteResponse, out: &mut Vec<Envelope>) {
        if self.role != Role::Candidate || resp.term < self.current_term {
            return;
        }
        if resp.vote_granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.effective_config.election_quorum() {
                self.become_leader(out);
            }
        }
    }

    fn on_append_request(&mut self, from: NodeId, req: AppendRequest, out: &mut Vec<Envelope>) {
        // Stale leader: reject.
        if req.term < self.current_term {
            out.push(Envelope::new(
                self.config.id,
                from,
                Message::AppendResponse(AppendResponse {
                    term: self.current_term,
                    success: false,
                    match_index: LogIndex(0),
                }),
            ));
            return;
        }

        // Valid leader: reset election timer and record leader.
        self.role = Role::Follower;
        self.leader_id = Some(req.leader_id);
        self.election_ticks = 0;

        // Check previous log consistency (Raft §5.3).
        let prev_ok = if req.prev_log_index.0 == 0 {
            true
        } else {
            self.log.term_at(req.prev_log_index) == Some(req.prev_log_term)
        };

        if !prev_ok {
            out.push(Envelope::new(
                self.config.id,
                from,
                Message::AppendResponse(AppendResponse {
                    term: self.current_term,
                    success: false,
                    match_index: self.log.last_index(), // hint for leader back-off
                }),
            ));
            return;
        }

        // Append new entries, resolving conflicts (Raft §5.3 rule 3 & 4).
        for (offset, entry) in req.entries.iter().enumerate() {
            let idx = LogIndex(req.prev_log_index.0 + 1 + offset as u64);
            match self.log.term_at(idx) {
                Some(existing_term) if existing_term != entry.term => {
                    // Conflict: truncate and replace.
                    self.log.truncate_from(idx);
                    self.log.append(entry.clone());
                }
                None => {
                    self.log.append(entry.clone());
                }
                Some(_) => {} // Already present and consistent; skip.
            }
        }

        // Advance commit index (Raft §5.3 rule 5).
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        out.push(Envelope::new(
            self.config.id,
            from,
            Message::AppendResponse(AppendResponse {
                term: self.current_term,
                success: true,
                match_index: self.log.last_index(),
            }),
        ));
    }

    fn on_append_response(&mut self, from: NodeId, resp: AppendResponse, out: &mut Vec<Envelope>) {
        if self.role != Role::Leader || resp.term < self.current_term {
            return;
        }

        // Process ReadIndex acknowledgements
        if resp.term == self.current_term {
            if let Some(&last_sent) = self.last_sent_ack_seq.get(&from) {
                for read in &mut self.pending_reads {
                    if read.ack_seq <= last_sent && read.term == self.current_term {
                        read.acks.insert(from);
                    }
                }
            }
        }

        if resp.success {
            // Advance match/next index for this peer (only move forward).
            let mi = resp.match_index;
            let old_mi = self.match_index.get(&from).copied().unwrap_or(LogIndex(0));
            if mi > old_mi {
                self.match_index.insert(from, mi);
                self.next_index.insert(from, LogIndex(mi.0 + 1));
            }

            // Try to advance commit_index (Raft §5.3, §5.4).
            // Only entries from the *current* term can be committed by counting.
            let last = self.log.last_index();
            for n in (self.commit_index.0 + 1)..=last.0 {
                let n_idx = LogIndex(n);
                if self.log.term_at(n_idx) != Some(self.current_term) {
                    continue;
                }
                let met = self.effective_config.commit_quorum_met(|id| {
                    self.match_index.get(id).copied().unwrap_or(LogIndex(0)) >= n_idx
                });
                if met {
                    self.commit_index = n_idx;
                }
            }
        } else {
            // Back off next_index for this peer (use peer's hint if useful).
            let ni = self.next_index.get(&from).copied().unwrap_or(LogIndex(1));
            let backed = if resp.match_index.0 > 0 && resp.match_index < ni {
                LogIndex(resp.match_index.0)
            } else {
                LogIndex(ni.0.saturating_sub(1).max(1))
            };
            self.next_index.insert(from, backed);
            // Retry immediately so convergence is fast in simulation.
            self.send_append_to(from, out);
        }
    }

    /// Drain and return all entries applied since the last call to this method.
    ///
    /// The caller is responsible for executing each command against the state machine.
    pub fn drain_applied(&mut self) -> Vec<(LogIndex, Term, Vec<u8>)> {
        self.applied_entries.drain(..).collect()
    }

    /// If a snapshot was installed on this node (via InstallSnapshot RPC or direct compact),
    /// return it so the state machine can be brought to that point.
    /// The data is opaque to Raft; the state machine (Engine) must know how to interpret it.
    pub fn drain_installed_snapshot(&mut self) -> Option<(LogIndex, Term, Vec<u8>)> {
        self.pending_snapshot.take()
    }

    /// Immediately send `AppendEntries` to all peers (only when leader).
    ///
    /// Useful after [`propose`] to trigger replication without waiting for the
    /// next heartbeat tick.
    pub fn broadcast(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        if self.role == Role::Leader {
            self.send_append_to_all(&mut out);
        }
        out
    }

    /// Compact the local log up to (and including) the given index by installing a snapshot.
    ///
    /// This is called by the state machine applier (Engine / simulator RefModel) once it has
    /// produced a durable snapshot of its state at `up_to_index`.
    /// The Raft node will drop the corresponding prefix of the log.
    pub fn compact(&mut self, up_to_index: LogIndex, up_to_term: Term, data: Vec<u8>) {
        self.log.install_snapshot(up_to_index, up_to_term, data);
        if up_to_index > self.commit_index {
            self.commit_index = up_to_index;
        }
        if up_to_index > self.last_applied {
            self.last_applied = up_to_index;
        }

        // Trim applied_entries history that is now covered by the snapshot.
        // Keep only entries after the snapshot for visibility to the caller.
        if !self.applied_entries.is_empty() {
            let keep_from = self
                .applied_entries
                .iter()
                .position(|(i, _, _)| *i > up_to_index)
                .unwrap_or(self.applied_entries.len());
            self.applied_entries.drain(0..keep_from);
        }
    }

    /// Return the currently installed snapshot (if any) as (index, term, data).
    pub fn snapshot(&self) -> Option<(LogIndex, Term, Vec<u8>)> {
        self.log.snapshot().map(|(i, t, d)| (i, t, d.to_vec()))
    }

    /// Highest log index covered by the current snapshot (0 if no snapshot has been installed).
    pub fn last_included_index(&self) -> LogIndex {
        self.log.last_included_index()
    }

    /// Propose a joint-consensus membership change. Only valid on leader.
    ///
    /// Appends a joint-configuration log entry; once committed and applied the
    /// leader automatically appends the final-configuration entry.
    pub fn propose_membership_change(
        &mut self,
        new_members: Vec<ClusterMember>,
    ) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        if matches!(self.effective_config, EffectiveConfig::Joint { .. }) {
            return None;
        }
        if self.pending_membership.is_some() {
            return None;
        }
        let mut by_id: BTreeSet<NodeId> = new_members.iter().map(|m| m.id).collect();
        by_id.insert(self.config.id);
        let mut members: Vec<ClusterMember> = new_members;
        if !members.iter().any(|m| m.id == self.config.id) {
            members.push(ClusterMember {
                id: self.config.id,
                raft_addr: String::new(),
                client_addr: String::new(),
            });
        }
        members.sort_by_key(|m| m.id.0);
        members.retain(|m| by_id.contains(&m.id));
        self.pending_membership = Some(members.clone());
        let cmd = RaftCommand::ConfigChange {
            phase: ConfigChangePhase::Joint,
            members,
        };
        let idx = self.log.append(LogEntry {
            term: self.current_term,
            command: cmd.encode(),
        });
        self.match_index.insert(self.config.id, idx);
        self.next_index.insert(self.config.id, LogIndex(idx.0 + 1));
        Some(idx)
    }

    // Dynamic membership stubs (prototype scaffolding)
    fn on_config_change_request(
        &mut self,
        from: NodeId,
        req: crate::ConfigChangeRequest,
        out: &mut Vec<Envelope>,
    ) {
        if req.term < self.current_term {
            out.push(Envelope::new(
                self.config.id,
                from,
                Message::ConfigChangeResponse(crate::ConfigChangeResponse {
                    term: self.current_term,
                    success: false,
                }),
            ));
            return;
        }
        if req.term > self.current_term {
            self.step_down(req.term);
        }
        if self.role == Role::Leader {
            self.config.peers = req.new_peers.clone();
        }
        out.push(Envelope::new(
            self.config.id,
            from,
            Message::ConfigChangeResponse(crate::ConfigChangeResponse {
                term: self.current_term,
                success: true,
            }),
        ));
    }

    fn on_config_change_response(
        &mut self,
        _from: NodeId,
        _resp: crate::ConfigChangeResponse,
        _out: &mut Vec<Envelope>,
    ) {
        // TODO: track for quorum on config change
    }

    /// Apply all committed but not yet applied log entries.
    ///
    /// Returns outbound envelopes produced while applying membership changes
    /// (e.g. replication of the final-configuration entry).
    fn try_advance_apply(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied = LogIndex(self.last_applied.0 + 1);
            let entry = self
                .log
                .get(self.last_applied)
                .map(|e| (e.term, e.command.clone()));
            if let Some((term, command)) = entry {
                if let Ok(RaftCommand::ConfigChange { phase, members }) =
                    RaftCommand::decode(&command)
                {
                    self.apply_config_command(phase, members, &mut out);
                }
                self.applied_entries
                    .push((self.last_applied, term, command));
            }
        }
        self.check_pending_reads();
        out
    }

    fn check_pending_reads(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        let quorum_size = self.quorum();
        let last_applied = self.last_applied;

        let mut ready = Vec::new();
        self.pending_reads.retain(|read| {
            if read.acks.len() >= quorum_size && last_applied >= read.read_index {
                ready.push(read.request_id);
                false
            } else {
                true
            }
        });

        self.ready_reads.extend(ready);
    }

    // ── Snapshot support (MVP scaffolding) ─────────────────────────────────────

    fn on_install_snapshot_request(
        &mut self,
        from: NodeId,
        req: crate::InstallSnapshotRequest,
        out: &mut Vec<Envelope>,
    ) {
        // Basic term handling and snapshot installation (to be expanded).
        if req.term < self.current_term {
            out.push(Envelope::new(
                self.config.id,
                from,
                Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
                    term: self.current_term,
                    success: false,
                }),
            ));
            return;
        }
        if req.term > self.current_term {
            self.step_down(req.term);
        }

        // Accept the snapshot and truncate our log accordingly.
        self.log.install_snapshot(
            req.last_included_index,
            req.last_included_term,
            req.data.clone(),
        );

        // Advance our tracking.
        if req.last_included_index > self.commit_index {
            self.commit_index = req.last_included_index;
        }
        if req.last_included_index > self.last_applied {
            self.last_applied = req.last_included_index;
        }

        // Make the snapshot data available for the state machine (engine) to consume.
        self.pending_snapshot = Some((req.last_included_index, req.last_included_term, req.data));

        out.push(Envelope::new(
            self.config.id,
            from,
            Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
                term: self.current_term,
                success: true,
            }),
        ));
    }

    fn on_install_snapshot_response(
        &mut self,
        from: NodeId,
        resp: crate::InstallSnapshotResponse,
        _out: &mut Vec<Envelope>,
    ) {
        if self.role != Role::Leader || resp.term < self.current_term {
            return;
        }
        if resp.success {
            // Follower now has at least up to our current snapshot.
            let snap_idx = self.log.last_included_index();
            self.match_index.insert(from, snap_idx);
            self.next_index.insert(from, LogIndex(snap_idx.0 + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: u64, peers: Vec<u64>) -> RaftNode {
        RaftNode::new(RaftConfig {
            id: NodeId(id),
            peers: peers.into_iter().map(NodeId).collect(),
            election_timeout_ticks: 10,
            heartbeat_interval_ticks: 3,
        })
    }

    #[test]
    fn test_read_index_propose_on_follower_fails() {
        let mut node = make_node(1, vec![2, 3]);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.propose_read(123), None);
    }

    #[test]
    fn test_read_index_leader_lifecycle() {
        let mut node = make_node(1, vec![2, 3]);
        let mut out = Vec::new();

        // Force election
        node.start_election(&mut out);
        assert_eq!(node.role, Role::Candidate);

        // Grant votes
        node.handle(Envelope::new(
            NodeId(2),
            NodeId(1),
            Message::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
        ));
        assert_eq!(node.role, Role::Leader);

        // Propose a read
        let read_index = node
            .propose_read(456)
            .expect("Proposing read should succeed on leader");
        assert_eq!(read_index, node.commit_index);

        // At this point, only leader (node 1) has acknowledged it. Quorum is 2 (majority of 3).
        // It is not ready yet.
        assert!(node.drain_ready_reads().is_empty());

        // Broadcast heartbeats
        let heartbeats = node.broadcast();
        assert_eq!(heartbeats.len(), 2);

        // Simulate AppendResponse from node 2
        let resp_env = Envelope::new(
            NodeId(2),
            NodeId(1),
            Message::AppendResponse(AppendResponse {
                term: Term(1),
                success: true,
                match_index: LogIndex(1), // no-op index
            }),
        );
        node.handle(resp_env);

        // Quorum is satisfied (node 1 and node 2 acknowledged).
        // Since last_applied is 1 (after no-op is applied on leader during try_advance_apply)
        // and read_index is 1 (the no-op), last_applied >= read_index is true!
        let ready = node.drain_ready_reads();
        assert_eq!(ready, vec![456]);
    }

    #[test]
    fn quorum_size_matches_cluster_majority() {
        let three = make_node(1, vec![2, 3]);
        assert_eq!(three.cluster_size(), 3);
        assert_eq!(three.quorum_size(), 2);

        let five = make_node(1, vec![2, 3, 4, 5]);
        assert_eq!(five.cluster_size(), 5);
        assert_eq!(five.quorum_size(), 3);
    }

    #[test]
    fn five_node_election_requires_three_votes() {
        let mut node = make_node(1, vec![2, 3, 4, 5]);
        let mut out = Vec::new();
        node.start_election(&mut out);
        assert_eq!(node.role, Role::Candidate);

        node.handle(Envelope::new(
            NodeId(2),
            NodeId(1),
            Message::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
        ));
        assert_eq!(
            node.role,
            Role::Candidate,
            "self + one peer is not a majority of five"
        );

        for peer in [3u64, 4] {
            node.handle(Envelope::new(
                NodeId(peer),
                NodeId(1),
                Message::VoteResponse(VoteResponse {
                    term: Term(1),
                    vote_granted: true,
                }),
            ));
        }
        assert_eq!(node.role, Role::Leader);
    }

    #[test]
    fn test_read_index_cleared_on_step_down() {
        let mut node = make_node(1, vec![2, 3]);
        let mut out = Vec::new();
        node.start_election(&mut out);

        node.handle(Envelope::new(
            NodeId(2),
            NodeId(1),
            Message::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
        ));
        assert_eq!(node.role, Role::Leader);

        node.propose_read(789);
        assert_eq!(node.pending_reads.len(), 1);

        // Step down due to higher term
        node.handle(Envelope::new(
            NodeId(3),
            NodeId(1),
            Message::AppendRequest(AppendRequest {
                term: Term(2),
                leader_id: NodeId(3),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: vec![],
                leader_commit: LogIndex(0),
            }),
        ));

        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.pending_reads.len(), 0);
    }
}
