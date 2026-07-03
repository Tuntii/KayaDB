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

/// Userspace USDT-shaped marker site (WAL fsync or memtable flush publish).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerSite {
    WalFsync,
    Flush,
}

/// Enter/exit phase for a userspace marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerPhase {
    Enter,
    Exit,
}

/// LSM publish syscalls correlated with flush/compaction (kernel-shaped).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishSyscallKind {
    Write,
    Rename,
    Unlink,
    FsyncDir,
}

impl PublishSyscallKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Rename => "rename",
            Self::Unlink => "unlink",
            Self::FsyncDir => "fsync_dir",
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
    UsdtMarker {
        seq: u64,
        site: MarkerSite,
        phase: MarkerPhase,
        #[serde(skip_serializing_if = "Option::is_none")]
        duration_us: Option<u64>,
        ts_ns: u64,
    },
    PublishSyscall {
        seq: u64,
        syscall: PublishSyscallKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        latency_us: Option<u64>,
        ts_ns: u64,
    },
}

impl ProbeEvent {
    pub fn seq(&self) -> u64 {
        match self {
            Self::FsyncLatency { seq, .. }
            | Self::UsdtMarker { seq, .. }
            | Self::PublishSyscall { seq, .. } => *seq,
        }
    }

    pub fn set_seq(&mut self, seq: u64) {
        match self {
            Self::FsyncLatency { seq: s, .. }
            | Self::UsdtMarker { seq: s, .. }
            | Self::PublishSyscall { seq: s, .. } => *s = seq,
        }
    }

    pub fn is_wal_relevant(&self) -> bool {
        matches!(
            self,
            Self::FsyncLatency { .. }
                | Self::UsdtMarker {
                    site: MarkerSite::WalFsync,
                    ..
                }
        )
    }

    pub fn is_durability_event(&self) -> bool {
        matches!(
            self,
            Self::FsyncLatency { .. } | Self::UsdtMarker { .. } | Self::PublishSyscall { .. }
        )
    }

    pub fn is_publish_relevant(&self) -> bool {
        matches!(
            self,
            Self::UsdtMarker {
                site: MarkerSite::Flush,
                ..
            } | Self::PublishSyscall { .. }
        )
    }
}
