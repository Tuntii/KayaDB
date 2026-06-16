use std::fmt;
use std::num::NonZeroU64;
use std::path::PathBuf;

pub type Bytes = Vec<u8>;
pub type Result<T> = std::result::Result<T, KayaError>;

pub const DEFAULT_MAX_KEY_LEN: usize = 4 * 1024;
pub const DEFAULT_MAX_VALUE_LEN: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MEMTABLE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_SSTABLE_BLOCK_TARGET_BYTES: usize = 32 * 1024;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBytes(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueBytes(pub Bytes);

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
pub struct WalConfig {
    pub segment_max_bytes: u64,
    pub max_record_bytes: u32,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            max_record_bytes: DEFAULT_MAX_PAYLOAD_LEN,
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
}

impl Default for SstableConfig {
    fn default() -> Self {
        Self {
            block_target_bytes: DEFAULT_SSTABLE_BLOCK_TARGET_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_key_len: DEFAULT_MAX_KEY_LEN,
            max_value_len: DEFAULT_MAX_VALUE_LEN,
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
}
