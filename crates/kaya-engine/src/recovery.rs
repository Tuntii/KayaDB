//! Recovery helpers and the standalone `recover` entry point (used by CLI dry-run etc).
//! Extracted from the monolithic lib.rs to improve code organization.

use std::sync::Arc;

use kaya_core::{EngineConfig, KayaError, Result, SequenceNumber};
use kaya_io::{Disk, RelativePath};
use kaya_lsm::{
    CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME, ManifestState, ManifestWarning, SstableReader,
    TableMetadata,
};
use kaya_wal::{recover_wal, WalPayload};

use super::{RecoveryReport, RecoveryWarning};
use kaya_lsm::Memtable;

pub async fn recover<D: Disk>(config: EngineConfig, disk: Arc<D>) -> Result<RecoveryReport> {
    // Scan (but DO NOT delete) leftover temporary files.
    let temp_files = scan_temp_files(&disk).await?;
    let tmp_files_removed = temp_files.len();

    let wal_report = recover_wal(config.wal.clone(), disk.clone()).await?;

    let next_sequence = wal_report
        .records
        .last()
        .map_or(SequenceNumber::FIRST, |record| {
            record.record.sequence.next()
        });

    // Replay manifest (without opening SSTable readers to be fast and safe during dry run)
    let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
    let (manifest_records_replayed, live_sstable_count, manifest_warnings) =
        match disk.file_len(&current_rel).await {
            Ok(len) if len > 0 => {
                let mut current_buf = vec![0u8; len as usize];
                disk.read_at(&current_rel, 0, &mut current_buf).await?;
                let manifest_name = std::str::from_utf8(&current_buf)
                    .map_err(|_| KayaError::corruption("CURRENT file is not valid UTF-8"))?
                    .trim();
                let manifest_rel = RelativePath::new(manifest_name)?;
                match disk.file_len(&manifest_rel).await {
                    Ok(m_len) if m_len > 0 => {
                        let mut manifest_buf = vec![0u8; m_len as usize];
                        disk.read_at(&manifest_rel, 0, &mut manifest_buf).await?;
                        let (state, replayed_count, warnings) = kaya_lsm::replay_manifest(&manifest_buf);
                        (replayed_count, state.live_tables.len(), warnings)
                    }
                    _ => (0, 0, Vec::new()),
                }
            }
            _ => (0, 0, Vec::new()),
        };

    let mut warnings = Vec::new();
    for w in &wal_report.warnings {
        warnings.push(RecoveryWarning::Wal(w.clone()));
    }
    for mw in &manifest_warnings {
        warnings.push(RecoveryWarning::Manifest(mw.clone()));
    }

    let wal_records_replayed = wal_report.records.len();
    Ok(RecoveryReport {
        manifest_records_replayed,
        live_sstable_count,
        wal_records_replayed,
        wal_truncated_bytes: wal_report.truncated_bytes,
        tmp_files_removed,
        last_lsn: wal_report.last_lsn,
        last_sequence: if next_sequence > SequenceNumber::FIRST {
            Some(SequenceNumber::new(next_sequence.get().saturating_sub(1)))
        } else {
            None
        },
        warnings,
        wal: wal_report,
        records_replayed: wal_records_replayed,
    })
}

/// Read the CURRENT file → manifest → live SSTables from disk.
/// Returns `(ManifestState, live_sstables sorted newest-first, manifest_records_replayed, manifest_warnings)`.
pub(crate) async fn load_manifest_and_sstables<D: Disk>(
    disk: Arc<D>,
) -> Result<(
    ManifestState,
    Vec<(TableMetadata, SstableReader)>,
    usize,
    Vec<ManifestWarning>,
)> {
    let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
    let current_len = match disk.file_len(&current_rel).await {
        Ok(len) => len,
        Err(KayaError::NotFound) => {
            return Ok((ManifestState::default(), Vec::new(), 0, Vec::new()))
        }
        Err(e) => return Err(e),
    };
    if current_len == 0 {
        return Ok((ManifestState::default(), Vec::new(), 0, Vec::new()));
    }
    let mut current_buf = vec![0u8; current_len as usize];
    disk.read_at(&current_rel, 0, &mut current_buf).await?;
    let manifest_name = std::str::from_utf8(&current_buf)
        .map_err(|_| KayaError::corruption("CURRENT file is not valid UTF-8"))?
        .trim();
    let manifest_rel = RelativePath::new(manifest_name)?;
    let manifest_len = match disk.file_len(&manifest_rel).await {
        Ok(len) => len,
        Err(KayaError::NotFound) => {
            return Ok((ManifestState::default(), Vec::new(), 0, Vec::new()))
        }
        Err(e) => return Err(e),
    };
    let mut manifest_buf = vec![0u8; manifest_len as usize];
    disk.read_at(&manifest_rel, 0, &mut manifest_buf).await?;
    let (state, replayed_count, warnings) = kaya_lsm::replay_manifest(&manifest_buf);

    // Load each live SSTable into memory.
    let mut live_sstables: Vec<(TableMetadata, SstableReader)> = Vec::new();
    for meta in &state.live_tables {
        let sst_rel = RelativePath::new(&meta.path)?;
        let sst_len = disk.file_len(&sst_rel).await?;
        let mut sst_buf = vec![0u8; sst_len as usize];
        disk.read_at(&sst_rel, 0, &mut sst_buf).await?;
        let reader = SstableReader::open(sst_buf)?;
        live_sstables.push((meta.clone(), reader));
    }
    // Sort newest-first (highest table_id first).
    live_sstables.sort_by_key(|b| std::cmp::Reverse(b.0.table_id));
    Ok((state, live_sstables, replayed_count, warnings))
}

pub(crate) fn apply_payload(
    memtable: &mut Memtable,
    payload: &WalPayload,
    sequence: SequenceNumber,
) -> Result<()> {
    match payload {
        WalPayload::Put { key, value } => memtable.put(key.clone(), value.clone(), sequence),
        WalPayload::Delete { key } => memtable.delete(key.clone(), sequence),
        WalPayload::Noop => {}
    }
    Ok(())
}

pub(crate) async fn scan_temp_files<D: Disk>(disk: &Arc<D>) -> Result<Vec<RelativePath>> {
    let mut temps = Vec::new();

    // Check CURRENT.tmp in root.
    let current_tmp = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
    if disk.file_len(&current_tmp).await.is_ok() {
        temps.push(current_tmp);
    }

    // Check for *.tmp in sst/ directory.
    let sst_dir = RelativePath::new("sst")?;
    match disk.list_dir(&sst_dir).await {
        Ok(entries) => {
            for entry in entries {
                if !entry.is_dir && entry.path.as_str().ends_with(".tmp") {
                    temps.push(entry.path);
                }
            }
        }
        Err(KayaError::NotFound) => {}
        Err(e) => return Err(e),
    }

    Ok(temps)
}
