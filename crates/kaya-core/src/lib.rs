mod probe_markers;

use std::fmt;
use std::num::NonZeroU64;
use std::path::PathBuf;

pub use probe_markers::{
    emit_probe_marker, set_probe_marker_callback, set_probe_span_callback, ProbeMarkerPhase,
    ProbeMarkerSite,
};

pub type Bytes = Vec<u8>;
pub type Result<T> = std::result::Result<T, KayaError>;

pub const DEFAULT_MAX_KEY_LEN: usize = 4 * 1024;
pub const DEFAULT_MAX_VALUE_LEN: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_SCAN_RESULTS: usize = 100_000;
pub const DEFAULT_MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MEMTABLE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_SSTABLE_BLOCK_TARGET_BYTES: usize = 32 * 1024;
pub const DEFAULT_SSTABLE_BLOOM_BITS_PER_KEY: u32 = 10;
pub const DEFAULT_SSTABLE_BLOCK_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KayaError {
    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },
    #[error("not found")]
    NotFound,
    #[error("corruption: {message}")]
    Corruption { message: String },
    #[error("io error: {message}")]
    Io { message: String },
    #[error("disk full")]
    DiskFull,
    #[error("fsync failed")]
    FsyncFailed,
    #[error("unsupported version: {found}")]
    UnsupportedVersion { found: u16 },
    #[error("data directory lock conflict")]
    LockConflict,
    #[error("invariant violation {id}: {message}")]
    InvariantViolation { id: String, message: String },
    #[error("internal error: {message}")]
    Internal { message: String },
}

impl KayaError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::NotFound => 2,
            Self::Corruption { .. } => 3,
            Self::InvalidArgument { .. } | Self::UnsupportedVersion { .. } => 4,
            Self::InvariantViolation { .. } => 5,
            Self::LockConflict => 6,
            Self::Io { .. } | Self::DiskFull | Self::FsyncFailed | Self::Internal { .. } => 1,
        }
    }
}

impl From<std::io::Error> for KayaError {
    fn from(value: std::io::Error) -> Self {
        match value.kind() {
            std::io::ErrorKind::NotFound => Self::NotFound,
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::WriteZero => Self::DiskFull,
            _ => Self::Io {
                message: value.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for SequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityMode {
    #[default]
    Strict,
    Relaxed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityConfig {
    pub mode: DurabilityMode,
    pub fsync_every_n_records: NonZeroU64,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::Strict,
            fsync_every_n_records: NonZeroU64::new(1).expect("1 is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatchConfig {
    /// Maximum strict records to buffer before a group fsync. `1` disables record-count batching.
    pub batch_max_records: usize,
    /// Maximum encoded bytes to buffer before a group fsync. `0` disables byte-limit batching.
    pub batch_max_bytes: usize,
    /// Maximum time (microseconds) to hold a partial batch before flushing. `0` disables time-based flush.
    pub batch_flush_interval_us: u64,
}

impl Default for WalBatchConfig {
    fn default() -> Self {
        Self {
            batch_max_records: 1,
            batch_max_bytes: 0,
            batch_flush_interval_us: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    pub segment_max_bytes: u64,
    pub max_record_bytes: u32,
    pub batch: WalBatchConfig,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            max_record_bytes: DEFAULT_MAX_PAYLOAD_LEN,
            batch: WalBatchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemtableConfig {
    pub max_bytes: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MEMTABLE_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstableConfig {
    pub block_target_bytes: usize,
    /// Bloom filter bits per key; `0` disables the filter.
    pub bloom_bits_per_key: u32,
    /// Decoded data blocks cached per open `SstableReader`; `0` disables the cache.
    pub block_cache_capacity: usize,
    /// When true, new SSTables use LZ4-compressed data blocks (format v3).
    pub compression_lz4: bool,
    /// When true, new SSTables use ZSTD-compressed data blocks (format v3; takes precedence over LZ4).
    pub compression_zstd: bool,
    /// When true, data blocks use prefix compression with restart points.
    pub prefix_compression: bool,
}

impl Default for SstableConfig {
    fn default() -> Self {
        Self {
            block_target_bytes: DEFAULT_SSTABLE_BLOCK_TARGET_BYTES,
            bloom_bits_per_key: DEFAULT_SSTABLE_BLOOM_BITS_PER_KEY,
            block_cache_capacity: DEFAULT_SSTABLE_BLOCK_CACHE_CAPACITY,
            compression_lz4: false,
            compression_zstd: false,
            prefix_compression: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,
    /// Hard cap on entries a single scan may return, even without a client limit.
    /// Bounds merge memory as well: at most this many keys are held during the merge.
    pub max_scan_results: usize,
    /// Hard cap on total key+value bytes a single scan may return. The first
    /// entry is always allowed so a scan can make progress.
    pub max_scan_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_key_len: DEFAULT_MAX_KEY_LEN,
            max_value_len: DEFAULT_MAX_VALUE_LEN,
            max_scan_results: DEFAULT_MAX_SCAN_RESULTS,
            max_scan_bytes: DEFAULT_MAX_SCAN_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompactionPolicyKind {
    #[default]
    L0Merge,
    Leveled,
    SizeTiered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeveledCompactionConfig {
    pub level_count: u32,
    pub l0_compaction_trigger: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            level_count: 7,
            l0_compaction_trigger: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeTieredCompactionConfig {
    pub min_tables: usize,
    /// Size ratio as fixed-point thousandths (1500 = 1.5).
    pub ratio_x1000: u32,
}

impl Default for SizeTieredCompactionConfig {
    fn default() -> Self {
        Self {
            min_tables: 4,
            ratio_x1000: 1500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionConfig {
    pub policy: CompactionPolicyKind,
    pub leveled: LeveledCompactionConfig,
    pub tiered: SizeTieredCompactionConfig,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            policy: CompactionPolicyKind::L0Merge,
            leveled: LeveledCompactionConfig::default(),
            tiered: SizeTieredCompactionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub durability: DurabilityConfig,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
    pub sstable: SstableConfig,
    pub limits: LimitsConfig,
    pub compaction: CompactionConfig,
    pub disable_locking: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            durability: DurabilityConfig::default(),
            wal: WalConfig::default(),
            memtable: MemtableConfig::default(),
            sstable: SstableConfig::default(),
            limits: LimitsConfig::default(),
            compaction: CompactionConfig::default(),
            disable_locking: false,
        }
    }
}

pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn default_durability_is_strict() {
        assert_eq!(
            EngineConfig::default().durability.mode,
            DurabilityMode::Strict
        );
    }

    #[test]
    fn default_compaction_policy_is_l0_merge() {
        let config = EngineConfig::default();
        assert_eq!(config.compaction.policy, CompactionPolicyKind::L0Merge);
        assert_eq!(config.compaction.leveled.level_count, 7);
        assert_eq!(config.compaction.leveled.l0_compaction_trigger, 4);
        assert_eq!(config.compaction.tiered.min_tables, 4);
        assert_eq!(config.compaction.tiered.ratio_x1000, 1500);
    }
}
