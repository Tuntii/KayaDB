use crate::{
    log::LogEntry,
    types::{LogIndex, NodeId, Term},
};

/// RequestVote RPC — sent by a candidate to request a vote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// AppendEntries RPC — sent by the leader for heartbeats and log replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub term: Term,
    pub success: bool,
    /// Highest log index stored on the responder after this RPC.
    /// On failure: the follower's current `last_index` (back-off hint for leader).
    pub match_index: LogIndex,
}

/// All messages exchanged between Raft nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendRequest(AppendRequest),
    AppendResponse(AppendResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    // Dynamic membership (prototype scaffolding)
    ConfigChangeRequest(ConfigChangeRequest),
    ConfigChangeResponse(ConfigChangeResponse),
}

/// Simple membership change request (prototype; full joint consensus later).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChangeRequest {
    pub term: Term,
    pub old_peers: Vec<NodeId>,
    pub new_peers: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChangeResponse {
    pub term: Term,
    pub success: bool,
}

/// Sent by leader to install a snapshot on a follower that is far behind
/// or is a new node (Raft §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    /// Opaque bytes representing the state machine snapshot at `last_included_index`.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub success: bool,
}

/// A directed message between two Raft nodes.
///
/// `group_id` multiplexes multi-raft traffic on a shared transport (M20).
/// Group `0` is the legacy single-group default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    /// Raft group this message belongs to (`0` = single-group / legacy).
    pub group_id: u64,
    pub message: Message,
}

impl Envelope {
    /// Construct an envelope for the legacy single group (`group_id = 0`).
    pub fn new(from: NodeId, to: NodeId, message: Message) -> Self {
        Self {
            from,
            to,
            group_id: 0,
            message,
        }
    }

    /// Construct an envelope for an explicit multi-raft group.
    pub fn with_group(from: NodeId, to: NodeId, group_id: u64, message: Message) -> Self {
        Self {
            from,
            to,
            group_id,
            message,
        }
    }
}
