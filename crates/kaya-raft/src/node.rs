use std::collections::{HashMap, HashSet};

use crate::{
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
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
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
        self.try_advance_apply();
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
        };
        if msg_term > self.current_term {
            self.step_down(msg_term);
        }

        match env.message {
            Message::VoteRequest(m) => self.on_vote_request(env.from, m, &mut out),
            Message::VoteResponse(m) => self.on_vote_response(env.from, m, &mut out),
            Message::AppendRequest(m) => self.on_append_request(env.from, m, &mut out),
            Message::AppendResponse(m) => self.on_append_response(env.from, m, &mut out),
        }

        self.try_advance_apply();
        out
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Majority quorum size for this cluster (including self).
    fn quorum(&self) -> usize {
        self.config.peers.len().div_ceil(2)
    }

    /// Revert to follower, adopting `new_term`.
    fn step_down(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader_id = None;
        self.election_ticks = 0;
        self.votes_received.clear();
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

    fn send_append_to(&self, peer: NodeId, out: &mut Vec<Envelope>) {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(LogIndex(self.log.last_index().0 + 1));
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
            if self.votes_received.len() >= self.quorum() {
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
                let count = self.match_index.values().filter(|&&m| m >= n_idx).count();
                if count >= self.quorum() {
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

    /// Apply all committed but not yet applied log entries.
    fn try_advance_apply(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied = LogIndex(self.last_applied.0 + 1);
            if let Some(entry) = self.log.get(self.last_applied) {
                self.applied_entries
                    .push((self.last_applied, entry.term, entry.command.clone()));
            }
        }
    }
}
