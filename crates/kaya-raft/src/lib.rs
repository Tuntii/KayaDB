use kaya_core::Lsn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftApplyCommand {
    pub term: Term,
    pub index: LogIndex,
    pub engine_lsn_hint: Option<Lsn>,
}
