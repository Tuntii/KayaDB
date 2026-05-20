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
}

/// A directed message between two Raft nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

impl Envelope {
    pub fn new(from: NodeId, to: NodeId, message: Message) -> Self {
        Self { from, to, message }
    }
}
