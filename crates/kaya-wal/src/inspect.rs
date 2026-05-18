use std::fs;
use std::path::Path;

use kaya_core::Result;

use crate::{decode_record, DecodeRecordResult, WalRecordType, WalWarning};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalInspectionRow {
    pub offset: u64,
    pub lsn: u64,
    pub sequence: u64,
    pub record_type: WalRecordType,
    pub key_len: Option<usize>,
    pub value_len: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalInspection {
    pub segment: String,
    pub rows: Vec<WalInspectionRow>,
    pub warnings: Vec<WalWarning>,
}

pub fn inspect_wal_path(path: impl AsRef<Path>, max_record_bytes: u32) -> Result<WalInspection> {
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    let segment = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());
    let mut offset = 0_usize;
    let mut rows = Vec::new();
    let mut warnings = Vec::new();

    while offset < bytes.len() {
        match decode_record(&bytes[offset..], offset as u64, max_record_bytes) {
            DecodeRecordResult::Complete { record, bytes_read } => {
                rows.push(WalInspectionRow {
                    offset: offset as u64,
                    lsn: record.lsn.get(),
                    sequence: record.sequence.get(),
                    record_type: record.record_type(),
                    key_len: record.payload.key_len(),
                    value_len: record.payload.value_len(),
                });
                offset += bytes_read;
            }
            DecodeRecordResult::Incomplete { warning }
            | DecodeRecordResult::Invalid { warning } => {
                warnings.push(warning);
                break;
            }
        }
    }

    Ok(WalInspection {
        segment,
        rows,
        warnings,
    })
}
