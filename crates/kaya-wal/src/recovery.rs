use std::sync::Arc;

use kaya_core::{KayaError, Lsn, Result, WalConfig};
use kaya_io::{Disk, RelativePath};

use crate::codec::DecodeRecordResult;
use crate::writer::parse_segment_id;
use crate::{decode_record, SegmentId, WalRecord, WalWarning};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredRecord {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub record: WalRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalRecoveryReport {
    pub records: Vec<RecoveredRecord>,
    pub last_lsn: Option<Lsn>,
    pub valid_bytes: u64,
    pub truncated_bytes: u64,
    pub warnings: Vec<WalWarning>,
}

pub async fn recover_wal<D: Disk>(config: WalConfig, disk: Arc<D>) -> Result<WalRecoveryReport> {
    let wal_dir = RelativePath::new("wal")?;
    let mut segments = disk
        .list_dir(&wal_dir)
        .await?
        .into_iter()
        .filter_map(|entry| parse_segment_id(&entry.path).map(|id| (SegmentId(id), entry.path)))
        .collect::<Vec<_>>();
    segments.sort_by_key(|(segment_id, _)| segment_id.0);

    let mut report = WalRecoveryReport::default();
    let mut expected_lsn = None::<u64>;
    let mut stop_index = None::<usize>;

    for (index, (segment_id, path)) in segments.iter().enumerate() {
        let len = match disk.file_len(path).await {
            Ok(len) => len,
            Err(KayaError::NotFound) => 0,
            Err(error) => return Err(error),
        };
        if len == 0 {
            continue;
        }

        let mut bytes = vec![0_u8; len as usize];
        let read = disk.read_at(path, 0, &mut bytes).await?;
        bytes.truncate(read);

        let mut offset = 0_usize;
        while offset < bytes.len() {
            match decode_record(&bytes[offset..], offset as u64, config.max_record_bytes) {
                DecodeRecordResult::Complete { record, bytes_read } => {
                    if let Some(expected) = expected_lsn {
                        if record.lsn.get() != expected {
                            report.warnings.push(WalWarning::NonMonotonicLsn {
                                offset: offset as u64,
                                expected,
                                found: record.lsn.get(),
                            });
                            stop_index = Some(index);
                            break;
                        }
                    }
                    expected_lsn = Some(record.lsn.next().get());
                    report.last_lsn = Some(record.lsn);
                    report.records.push(RecoveredRecord {
                        segment_id: *segment_id,
                        offset: offset as u64,
                        record,
                    });
                    offset += bytes_read;
                }
                DecodeRecordResult::Incomplete { warning }
                | DecodeRecordResult::Invalid { warning } => {
                    report.warnings.push(warning);
                    stop_index = Some(index);
                    break;
                }
            }
        }

        report.valid_bytes += offset as u64;

        if stop_index == Some(index) {
            let valid_len = offset as u64;
            if len > valid_len {
                disk.truncate(path, valid_len).await?;
                let truncated = len - valid_len;
                report.truncated_bytes += truncated;
                report.warnings.push(WalWarning::TailTruncated {
                    path: path.as_str().to_owned(),
                    valid_len,
                    truncated_bytes: truncated,
                });
            }
            break;
        }
    }

    if let Some(index) = stop_index {
        let ignored = segments.len().saturating_sub(index + 1);
        if ignored > 0 {
            report
                .warnings
                .push(WalWarning::TrailingSegmentsIgnored { count: ignored });
        }
    }

    Ok(report)
}
