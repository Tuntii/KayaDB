use serde::{Deserialize, Serialize};

/// Durability-relevant syscall observed by an eBPF probe or userspace tap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyscallKind {
    Fsync,
    Fdatasync,
}

impl SyscallKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fsync => "fsync",
            Self::Fdatasync => "fdatasync",
        }
    }
}

/// Normalized probe event emitted on the internal channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeEvent {
    FsyncLatency {
        seq: u64,
        syscall: SyscallKind,
        latency_us: u64,
        ts_ns: u64,
    },
}

impl ProbeEvent {
    pub fn seq(&self) -> u64 {
        match self {
            Self::FsyncLatency { seq, .. } => *seq,
        }
    }

    pub fn is_wal_relevant(&self) -> bool {
        matches!(self, Self::FsyncLatency { .. })
    }
}