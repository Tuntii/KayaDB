use kaya_core::Lsn;

/// Raft term number. Increases monotonically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

/// Index into the Raft log. 1-based; `LogIndex(0)` means "no entry".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogIndex(pub u64);

/// Unique identity of a node within a Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// A command committed by Raft that the storage engine should apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftApplyCommand {
    pub term: Term,
    pub index: LogIndex,
    pub engine_lsn_hint: Option<Lsn>,
}
