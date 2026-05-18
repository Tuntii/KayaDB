mod codec;
mod inspect;
mod recovery;
mod writer;

pub use codec::{
    decode_record, encode_record, DecodeRecordResult, WalPayload, WalRecord, WalRecordType,
    WalWarning, WAL_HEADER_LEN, WAL_MAGIC, WAL_VERSION,
};
pub use inspect::{inspect_wal_path, WalInspection, WalInspectionRow};
pub use recovery::{recover_wal, RecoveredRecord, WalRecoveryReport};
pub use writer::{AppendResult, SegmentId, WalWriter};
