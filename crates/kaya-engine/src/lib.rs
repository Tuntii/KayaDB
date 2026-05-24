use std::collections::BTreeMap;
use std::sync::Arc;

use kaya_core::{
    Bytes, DurabilityMode, EngineConfig, KayaError, KeyValue, Lsn, Result, SequenceNumber,
};
use kaya_io::{Disk, RelativePath};
use kaya_lsm::{
    decode_footer, encode_manifest_edit, replay_manifest, ManifestEdit, ManifestState,
    ManifestWarning, Memtable, SstEntry, SstableBuilder, SstableReader, TableMetadata,
    ValueRecordRef, CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME, MANIFEST_FILE_NAME, SST_FOOTER_LEN,
};
use kaya_wal::{recover_wal, WalPayload, WalRecoveryReport, WalWarning, WalWriter};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub durability: Option<DurabilityMode>,
    pub idempotency_key: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadTimestamp {
    #[default]
    Latest,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub read_at: ReadTimestamp,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub sequence: SequenceNumber,
    pub lsn: Lsn,
    pub durable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushResult {
    pub memtable_entries: u64,
    pub sstable_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionResult {
    pub input_tables: u64,
    pub output_tables: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub put_count: u64,
    pub get_count: u64,
    pub delete_count: u64,
    pub scan_count: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_count: u64,
    pub memtable_entries: u64,
    pub sstable_count: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryWarning {
    Wal(WalWarning),
    Manifest(ManifestWarning),
}

impl std::fmt::Display for RecoveryWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wal(w) => write!(f, "wal warning: {w}"),
            Self::Manifest(m) => write!(f, "manifest warning: {m}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub manifest_records_replayed: usize,
    pub live_sstable_count: usize,
    pub wal_records_replayed: usize,
    pub wal_truncated_bytes: u64,
    pub tmp_files_removed: usize,
    pub last_lsn: Option<Lsn>,
    pub last_sequence: Option<SequenceNumber>,
    pub warnings: Vec<RecoveryWarning>,
    pub wal: WalRecoveryReport,
    pub records_replayed: usize,
}

#[derive(Debug)]
pub struct Engine<D: Disk> {
    config: EngineConfig,
    disk: Arc<D>,
    wal: WalWriter<D>,
    memtable: Memtable,
    stats: EngineStats,
    last_recovery: RecoveryReport,
    manifest_state: ManifestState,
    next_table_id: u64,
    next_manifest_edit_seq: u64,
    /// Live SSTables sorted newest-first (highest table_id first).
    live_sstables: Vec<(TableMetadata, SstableReader)>,
    #[allow(dead_code)]
    lock_file: Option<std::fs::File>,
}

fn acquire_directory_lock(config: &EngineConfig) -> Result<Option<std::fs::File>> {
    if config.disable_locking {
        return Ok(None);
    }

    #[cfg(test)]
    {
        Ok(None)
    }

    #[cfg(not(test))]
    {
        use std::fs::OpenOptions;
        let data_dir = &config.data_dir;

        // If the data_dir path is empty, skip locking
        if data_dir.as_os_str().is_empty() {
            return Ok(None);
        }

        // Ensure the data directory exists
        std::fs::create_dir_all(data_dir)?;

        let lock_path = data_dir.join("KAYA_LOCK");

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // share_mode(0) opens the file with exclusive access, preventing other processes from opening it
            options.share_mode(0);
        }

        let file = match options.open(&lock_path) {
            Ok(f) => f,
            Err(e) => {
                return Err(KayaError::internal(format!(
                    "Could not acquire exclusive directory lock on KAYA_LOCK: {}. Is another instance of KayaDB running on this directory?",
                    e
                )));
            }
        };

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let res = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if res != 0 {
                return Err(KayaError::internal("Could not acquire exclusive directory lock on KAYA_LOCK. Is another instance of KayaDB running on this directory?"));
            }
        }

        Ok(Some(file))
    }
}

impl<D: Disk> Engine<D> {
    pub async fn open(config: EngineConfig, disk: Arc<D>) -> Result<Self> {
        let lock_file = acquire_directory_lock(&config)?;
        // Clean up leftover temporary files.
        let temp_files = scan_temp_files(&disk).await?;
        let tmp_files_removed = temp_files.len();
        for path in &temp_files {
            disk.remove_file(path).await?;
        }

        let wal_report = recover_wal(config.wal.clone(), disk.clone()).await?;
        let mut memtable = Memtable::new();

        for recovered in &wal_report.records {
            apply_payload(
                &mut memtable,
                &recovered.record.payload,
                recovered.record.sequence,
            )?;
        }

        let next_lsn = wal_report.last_lsn.map_or(Lsn::FIRST, Lsn::next);
        let next_sequence = wal_report
            .records
            .last()
            .map_or(SequenceNumber::FIRST, |record| {
                record.record.sequence.next()
            });
        let wal =
            WalWriter::open_at(config.wal.clone(), disk.clone(), next_lsn, next_sequence).await?;

        // Load manifest + live SSTables.
        let (manifest_state, live_sstables, manifest_records_replayed, manifest_warnings) =
            load_manifest_and_sstables(disk.clone()).await?;
        let next_table_id = manifest_state
            .live_tables
            .iter()
            .map(|t| t.table_id + 1)
            .max()
            .unwrap_or(1);
        let next_manifest_edit_seq = manifest_state.last_edit_seq + 1;

        let stats = EngineStats {
            memtable_entries: memtable.len() as u64,
            sstable_count: live_sstables.len() as u64,
            last_sequence: next_sequence.get().saturating_sub(1),
            ..EngineStats::default()
        };

        let mut warnings = Vec::new();
        for w in &wal_report.warnings {
            warnings.push(RecoveryWarning::Wal(w.clone()));
        }
        for mw in &manifest_warnings {
            warnings.push(RecoveryWarning::Manifest(mw.clone()));
        }

        let wal_records_replayed = wal_report.records.len();
        let last_recovery = RecoveryReport {
            manifest_records_replayed,
            live_sstable_count: live_sstables.len(),
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
        };

        Ok(Self {
            config,
            disk,
            wal,
            memtable,
            stats,
            last_recovery,
            manifest_state,
            next_table_id,
            next_manifest_edit_seq,
            live_sstables,
            lock_file,
        })
    }

    pub async fn close(&mut self) -> Result<()> {
        let _ = &self.disk;
        Ok(())
    }

    pub async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        opts: WriteOptions,
    ) -> Result<WriteResult> {
        self.validate_key(&key)?;
        self.validate_value(&value)?;
        let durability = opts.durability.unwrap_or(self.config.durability.mode);
        let append = self
            .wal
            .append(
                WalPayload::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                durability,
            )
            .await?;
        self.memtable.put(key, value, append.sequence);
        self.stats.put_count += 1;
        self.stats.memtable_entries = self.memtable.len() as u64;
        self.stats.wal_bytes_written += u64::from(append.encoded_len);
        self.stats.wal_fsync_count += u64::from(append.durable);
        self.stats.last_sequence = append.sequence.get();
        Ok(WriteResult {
            sequence: append.sequence,
            lsn: append.lsn,
            durable: append.durable,
        })
    }

    pub async fn delete(&mut self, key: Bytes, opts: WriteOptions) -> Result<WriteResult> {
        self.validate_key(&key)?;
        let durability = opts.durability.unwrap_or(self.config.durability.mode);
        let append = self
            .wal
            .append(WalPayload::Delete { key: key.clone() }, durability)
            .await?;
        self.memtable.delete(key, append.sequence);
        self.stats.delete_count += 1;
        self.stats.memtable_entries = self.memtable.len() as u64;
        self.stats.wal_bytes_written += u64::from(append.encoded_len);
        self.stats.wal_fsync_count += u64::from(append.durable);
        self.stats.last_sequence = append.sequence.get();
        Ok(WriteResult {
            sequence: append.sequence,
            lsn: append.lsn,
            durable: append.durable,
        })
    }

    pub async fn get(&mut self, key: &[u8], opts: ReadOptions) -> Result<Option<Bytes>> {
        let _ = opts;
        self.stats.get_count += 1;
        // Memtable is always checked first (newest data).
        match self.memtable.get(key) {
            Some(ValueRecordRef::Put { value, .. }) => return Ok(Some(value.to_vec())),
            Some(ValueRecordRef::Delete { .. }) => return Ok(None),
            None => {}
        }
        // Fall back to SSTables, newest-first (highest table_id first).
        for (_, reader) in &self.live_sstables {
            if let Some(entry) = reader.get(key)? {
                return Ok(entry.value);
            }
        }
        Ok(None)
    }

    pub async fn scan_prefix(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        self.stats.scan_count += 1;
        // Merge all sources into a BTreeMap.  SSTables oldest-first so that
        // newer tables overwrite older ones, then memtable always overwrites.
        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        for (_, reader) in self.live_sstables.iter().rev() {
            for entry in reader.scan_prefix(prefix)? {
                let seq = entry.sequence.get();
                match merged.get(&entry.key) {
                    Some((s, _)) if *s >= seq => {}
                    _ => {
                        merged.insert(entry.key, (seq, entry.value));
                    }
                }
            }
        }
        // Memtable overrides everything including tombstones.
        for (key, value, seq) in self.memtable.raw_scan_prefix(prefix) {
            merged.insert(key, (seq.get(), value));
        }
        let mut result: Vec<KeyValue> = merged
            .into_iter()
            .filter_map(|(key, (_, v))| v.map(|value| KeyValue { key, value }))
            .collect();
        if let Some(limit) = opts.limit {
            result.truncate(limit);
        }
        Ok(result)
    }

    pub async fn flush(&mut self) -> Result<FlushResult> {
        if self.memtable.is_empty() {
            return Ok(FlushResult {
                memtable_entries: 0,
                sstable_count: self.live_sstables.len() as u64,
            });
        }
        let entry_count = self.memtable.len() as u64;
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        // Build SSTable from all memtable entries (including tombstones).
        let mut builder = SstableBuilder::new(self.config.sstable.block_target_bytes);
        for (key, value, sequence) in self.memtable.raw_scan_prefix(b"") {
            builder.add(SstEntry {
                key,
                value,
                sequence,
            });
        }
        let sst_bytes = builder.finish()?;
        let sst_file_size = sst_bytes.len() as u64;
        // Derive true min/max from builder before consuming.
        let (sst_table_min_seq, sst_table_max_seq, smallest_key, largest_key) = {
            let footer = decode_footer(&sst_bytes)?;
            let reader_tmp = SstableReader::open(sst_bytes.clone())?;
            let entries = reader_tmp.all_entries()?;
            let sk = entries.first().map(|e| e.key.clone()).unwrap_or_default();
            let lk = entries.last().map(|e| e.key.clone()).unwrap_or_default();
            (footer.table_min_seq, footer.table_max_seq, sk, lk)
        };

        // Write tmp → rename → fsync (atomic SSTable publication).
        let sst_path = format!("sst/{table_id:016x}.sst");
        let tmp_path = format!("sst/{table_id:016x}.tmp");
        let sst_rel = RelativePath::new(&sst_path)?;
        let tmp_rel = RelativePath::new(&tmp_path)?;
        let sst_dir_rel = RelativePath::new("sst")?;
        self.disk.write_at(&tmp_rel, 0, &sst_bytes).await?;
        self.disk.fsync_file(&tmp_rel).await?;
        self.disk.rename(&tmp_rel, &sst_rel).await?;
        self.disk.fsync_dir(&sst_dir_rel).await?;

        // Build table metadata for manifest.
        let footer_crc = {
            let len = sst_bytes.len();
            if len >= SST_FOOTER_LEN {
                let fb = &sst_bytes[len - SST_FOOTER_LEN..];
                // footer_crc32c is at offset 40 within the footer bytes
                u32::from_le_bytes(fb[40..44].try_into().unwrap_or([0u8; 4]))
            } else {
                0
            }
        };
        let meta = TableMetadata {
            table_id,
            level: 0,
            path: sst_path,
            smallest_key,
            largest_key,
            min_sequence: SequenceNumber::new(sst_table_min_seq),
            max_sequence: SequenceNumber::new(sst_table_max_seq),
            entry_count,
            file_size: sst_file_size,
            footer_checksum: footer_crc,
        };
        let last_seq = SequenceNumber::new(self.stats.last_sequence);

        // Append edits to manifest.
        let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME)?;
        let edit_create = encode_manifest_edit(
            &ManifestEdit::CreateTable(meta.clone()),
            self.next_manifest_edit_seq,
        );
        self.next_manifest_edit_seq += 1;
        let edit_seq = encode_manifest_edit(
            &ManifestEdit::SetLastSequence { sequence: last_seq },
            self.next_manifest_edit_seq,
        );
        self.next_manifest_edit_seq += 1;
        self.disk.append(&manifest_rel, &edit_create).await?;
        self.disk.append(&manifest_rel, &edit_seq).await?;
        self.disk.fsync_file(&manifest_rel).await?;

        // Atomically update CURRENT → points to our single manifest.
        let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
        let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
        let root_rel = RelativePath::root();
        self.disk
            .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
            .await?;
        self.disk.fsync_file(&current_tmp_rel).await?;
        self.disk.rename(&current_tmp_rel, &current_rel).await?;
        self.disk.fsync_dir(&root_rel).await?;

        // Update in-memory state.
        let reader = SstableReader::open(sst_bytes)?;
        self.live_sstables.insert(0, (meta.clone(), reader));
        self.manifest_state.live_tables.push(meta);
        self.manifest_state.last_sequence = last_seq;
        self.memtable = Memtable::new();
        self.stats.sstable_count = self.live_sstables.len() as u64;
        self.stats.memtable_entries = 0;

        Ok(FlushResult {
            memtable_entries: entry_count,
            sstable_count: self.live_sstables.len() as u64,
        })
    }

    pub async fn compact(&mut self) -> Result<CompactionResult> {
        let input_count = self.live_sstables.len() as u64;
        if input_count < 2 {
            // Nothing to compact — 0 or 1 table.
            return Ok(CompactionResult {
                input_tables: 0,
                output_tables: 0,
            });
        }

        // Merge all entries from all L0 tables.  Iterate oldest-first so that
        // entries from newer tables (higher table_id) overwrite older ones.
        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        for (_, reader) in self.live_sstables.iter().rev() {
            for entry in reader.all_entries()? {
                let seq = entry.sequence.get();
                match merged.get(&entry.key) {
                    Some((s, _)) if *s >= seq => {}
                    _ => {
                        merged.insert(entry.key, (seq, entry.value));
                    }
                }
            }
        }

        let new_table_id = self.next_table_id;
        self.next_table_id += 1;

        // Build compacted SSTable.  Tombstones are kept because there is no
        // lower level to compact against yet.
        let mut builder = SstableBuilder::new(self.config.sstable.block_target_bytes);
        for (key, (seq, value)) in &merged {
            builder.add(SstEntry {
                key: key.clone(),
                value: value.clone(),
                sequence: SequenceNumber::new(*seq),
            });
        }
        let sst_bytes = builder.finish()?;
        let sst_file_size = sst_bytes.len() as u64;

        let (sst_table_min_seq, sst_table_max_seq, smallest_key, largest_key) = {
            let footer = decode_footer(&sst_bytes)?;
            let reader_tmp = SstableReader::open(sst_bytes.clone())?;
            let entries = reader_tmp.all_entries()?;
            let sk = entries.first().map(|e| e.key.clone()).unwrap_or_default();
            let lk = entries.last().map(|e| e.key.clone()).unwrap_or_default();
            (footer.table_min_seq, footer.table_max_seq, sk, lk)
        };

        // Write new SSTable atomically: tmp → rename → fsync dir.
        let sst_path = format!("sst/{new_table_id:016x}.sst");
        let tmp_path = format!("sst/{new_table_id:016x}.tmp");
        let sst_rel = RelativePath::new(&sst_path)?;
        let tmp_rel = RelativePath::new(&tmp_path)?;
        let sst_dir_rel = RelativePath::new("sst")?;
        self.disk.write_at(&tmp_rel, 0, &sst_bytes).await?;
        self.disk.fsync_file(&tmp_rel).await?;
        self.disk.rename(&tmp_rel, &sst_rel).await?;
        self.disk.fsync_dir(&sst_dir_rel).await?;

        let footer_crc = {
            let len = sst_bytes.len();
            if len >= SST_FOOTER_LEN {
                let fb = &sst_bytes[len - SST_FOOTER_LEN..];
                u32::from_le_bytes(fb[40..44].try_into().unwrap_or([0u8; 4]))
            } else {
                0
            }
        };
        let new_meta = TableMetadata {
            table_id: new_table_id,
            level: 0,
            path: sst_path,
            smallest_key,
            largest_key,
            min_sequence: SequenceNumber::new(sst_table_min_seq),
            max_sequence: SequenceNumber::new(sst_table_max_seq),
            entry_count: merged.len() as u64,
            file_size: sst_file_size,
            footer_checksum: footer_crc,
        };

        // Collect input table IDs before mutating state.
        let input_ids: Vec<u64> = self.live_sstables.iter().map(|(m, _)| m.table_id).collect();
        let last_seq = SequenceNumber::new(self.stats.last_sequence);

        // Append manifest edits: CreateTable(output) first — safe to have it
        // live before inputs are removed.  Then DeleteTable for each input.
        // Then SetLastSequence.  Fsync once after all edits.
        let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME)?;
        let edit_create = encode_manifest_edit(
            &ManifestEdit::CreateTable(new_meta.clone()),
            self.next_manifest_edit_seq,
        );
        self.next_manifest_edit_seq += 1;
        self.disk.append(&manifest_rel, &edit_create).await?;

        for &id in &input_ids {
            let edit_del = encode_manifest_edit(
                &ManifestEdit::DeleteTable { table_id: id },
                self.next_manifest_edit_seq,
            );
            self.next_manifest_edit_seq += 1;
            self.disk.append(&manifest_rel, &edit_del).await?;
        }

        let edit_seq = encode_manifest_edit(
            &ManifestEdit::SetLastSequence { sequence: last_seq },
            self.next_manifest_edit_seq,
        );
        self.next_manifest_edit_seq += 1;
        self.disk.append(&manifest_rel, &edit_seq).await?;
        self.disk.fsync_file(&manifest_rel).await?;

        // Atomically update CURRENT.
        let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
        let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
        let root_rel = RelativePath::root();
        self.disk
            .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
            .await?;
        self.disk.fsync_file(&current_tmp_rel).await?;
        self.disk.rename(&current_tmp_rel, &current_rel).await?;
        self.disk.fsync_dir(&root_rel).await?;

        // Update in-memory state.
        let new_reader = SstableReader::open(sst_bytes)?;
        self.live_sstables = vec![(new_meta.clone(), new_reader)];
        for &id in &input_ids {
            self.manifest_state.live_tables.retain(|t| t.table_id != id);
        }
        self.manifest_state.live_tables.push(new_meta);
        self.manifest_state.last_sequence = last_seq;
        self.stats.sstable_count = 1;

        Ok(CompactionResult {
            input_tables: input_count,
            output_tables: 1,
        })
    }

    pub fn stats(&self) -> EngineStats {
        self.stats
    }

    pub fn last_recovery(&self) -> &RecoveryReport {
        &self.last_recovery
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(KayaError::invalid_argument(
                "empty keys are not supported in MVP",
            ));
        }
        if key.len() > self.config.limits.max_key_len {
            return Err(KayaError::invalid_argument(format!(
                "key length {} exceeds max {}",
                key.len(),
                self.config.limits.max_key_len
            )));
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > self.config.limits.max_value_len {
            return Err(KayaError::invalid_argument(format!(
                "value length {} exceeds max {}",
                value.len(),
                self.config.limits.max_value_len
            )));
        }
        Ok(())
    }
}

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
                        let (state, replayed_count, warnings) = replay_manifest(&manifest_buf);
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
async fn load_manifest_and_sstables<D: Disk>(
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
    let (state, replayed_count, warnings) = replay_manifest(&manifest_buf);

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

fn apply_payload(
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

async fn scan_temp_files<D: Disk>(disk: &Arc<D>) -> Result<Vec<RelativePath>> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{DurabilityMode, EngineConfig};
    use kaya_io::SimDisk;

    use super::*;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn strict_opts() -> WriteOptions {
        WriteOptions {
            durability: Some(DurabilityMode::Strict),
            ..WriteOptions::default()
        }
    }

    fn relaxed_opts() -> WriteOptions {
        WriteOptions {
            durability: Some(DurabilityMode::Relaxed),
            ..WriteOptions::default()
        }
    }

    // KD-0303 / engine restart — strictly ACKed puts survive crash+reopen.
    #[test]
    fn engine_restart_recovers_strict_puts() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"key1".to_vec(), b"val1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine
                    .put(b"key2".to_vec(), b"val2".to_vec(), strict_opts())
                    .await
                    .unwrap();
            }

            disk.crash();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"key1", ReadOptions::default()).await.unwrap(),
                Some(b"val1".to_vec())
            );
            assert_eq!(
                engine2.get(b"key2", ReadOptions::default()).await.unwrap(),
                Some(b"val2".to_vec())
            );
            assert_eq!(engine2.last_recovery().records_replayed, 2);
        });
    }

    // Writes made with Relaxed durability (no fsync) are lost after a crash.
    #[test]
    fn engine_crash_discards_relaxed_puts() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"key".to_vec(), b"value".to_vec(), relaxed_opts())
                    .await
                    .unwrap();
                // No explicit flush / fsync — data lives in volatile only.
            }

            disk.crash();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"key", ReadOptions::default()).await.unwrap(),
                None,
                "relaxed write should be lost after crash"
            );
            assert_eq!(engine2.last_recovery().records_replayed, 0);
        });
    }

    // A deleted key must not be visible after restart.
    #[test]
    fn engine_restart_propagates_delete() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"key".to_vec(), b"val".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.delete(b"key".to_vec(), strict_opts()).await.unwrap();
            }

            disk.crash();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"key", ReadOptions::default()).await.unwrap(),
                None,
                "deleted key must not be visible after restart"
            );
        });
    }

    // scan_prefix must return consistent results after restart.
    #[test]
    fn engine_restart_scan_prefix_consistent() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                for i in 0_u8..3 {
                    let mut key = b"prefix:".to_vec();
                    key.push(i);
                    engine.put(key, vec![i], strict_opts()).await.unwrap();
                }
            }

            disk.crash();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            let items = engine2
                .scan_prefix(b"prefix:", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(items.len(), 3);
        });
    }

    // KD-0403: flush writes an SSTable; reopen finds it and serves reads.
    #[test]
    fn engine_flush_writes_sstable_and_reopen_reads_it() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"sst:a".to_vec(), b"alpha".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine
                    .put(b"sst:b".to_vec(), b"beta".to_vec(), strict_opts())
                    .await
                    .unwrap();
                let result = engine.flush().await.unwrap();
                assert_eq!(result.memtable_entries, 2);
                assert_eq!(result.sstable_count, 1);
                // After flush memtable is empty; reads still work from SSTable.
                assert_eq!(
                    engine.get(b"sst:a", ReadOptions::default()).await.unwrap(),
                    Some(b"alpha".to_vec()),
                    "must read from SSTable after flush"
                );
            }

            // Reopen (simulating clean shutdown, no crash).
            let mut engine2 = Engine::open(config, disk.clone()).await.unwrap();
            assert_eq!(engine2.stats().sstable_count, 1);
            assert_eq!(
                engine2.get(b"sst:a", ReadOptions::default()).await.unwrap(),
                Some(b"alpha".to_vec())
            );
            assert_eq!(
                engine2.get(b"sst:b", ReadOptions::default()).await.unwrap(),
                Some(b"beta".to_vec())
            );
        });
    }

    // KD-0403: delete after flush is visible via memtable tombstone.
    #[test]
    fn engine_delete_after_flush_is_visible() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();
            engine
                .put(b"k".to_vec(), b"v".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            engine.delete(b"k".to_vec(), strict_opts()).await.unwrap();
            assert_eq!(
                engine.get(b"k", ReadOptions::default()).await.unwrap(),
                None,
                "memtable tombstone must shadow SSTable entry"
            );
        });
    }

    // KD-0501: compaction merges two SSTables and preserves visible state.
    #[test]
    fn compaction_preserves_visible_state() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();

            // Two flushes produce two L0 SSTables.
            engine
                .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"b".to_vec(), b"old".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            // Second flush overwrites "b" with a higher sequence.
            engine
                .put(b"b".to_vec(), b"new".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"c".to_vec(), b"3".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            assert_eq!(engine.stats().sstable_count, 2);

            let r = engine.compact().await.unwrap();
            assert_eq!(r.input_tables, 2);
            assert_eq!(r.output_tables, 1);
            assert_eq!(engine.stats().sstable_count, 1);

            // All visible state preserved; newest "b" wins.
            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine.get(b"b", ReadOptions::default()).await.unwrap(),
                Some(b"new".to_vec()),
                "compaction must keep the highest-sequence value"
            );
            assert_eq!(
                engine.get(b"c", ReadOptions::default()).await.unwrap(),
                Some(b"3".to_vec())
            );
        });
    }

    // KD-0502: flush crash recovery is idempotent.
    #[test]
    fn flush_crash_recovery_idempotent() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"k1".to_vec(), b"v1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine
                    .put(b"k2".to_vec(), b"v2".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
            }

            disk.crash();

            let mut engine2 = Engine::open(config.clone(), disk.clone()).await.unwrap();
            assert_eq!(
                engine2.get(b"k1", ReadOptions::default()).await.unwrap(),
                Some(b"v1".to_vec())
            );
            assert_eq!(
                engine2.get(b"k2", ReadOptions::default()).await.unwrap(),
                Some(b"v2".to_vec())
            );

            disk.crash();

            // Second recovery from same flushed state — must be identical.
            let mut engine3 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine3.get(b"k1", ReadOptions::default()).await.unwrap(),
                Some(b"v1".to_vec())
            );
            assert_eq!(
                engine3.get(b"k2", ReadOptions::default()).await.unwrap(),
                Some(b"v2".to_vec())
            );
        });
    }

    // KD-0502: compaction crash recovery is idempotent.
    #[test]
    fn compaction_crash_recovery_idempotent() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
                engine
                    .put(b"b".to_vec(), b"2".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
                let r = engine.compact().await.unwrap();
                assert_eq!(r.input_tables, 2);
                assert_eq!(r.output_tables, 1);
            }

            disk.crash();

            let mut engine2 = Engine::open(config.clone(), disk.clone()).await.unwrap();
            assert_eq!(
                engine2.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine2.get(b"b", ReadOptions::default()).await.unwrap(),
                Some(b"2".to_vec())
            );
            assert_eq!(
                engine2.stats().sstable_count,
                1,
                "compacted state must survive crash+reopen"
            );

            disk.crash();

            // Second recovery — idempotent.
            let mut engine3 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine3.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine3.get(b"b", ReadOptions::default()).await.unwrap(),
                Some(b"2".to_vec())
            );
        });
    }

    // KD-0502: manifest tail corruption is recovered gracefully.
    #[test]
    fn manifest_tail_corruption_recovers_gracefully() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"x".to_vec(), b"y".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
            }

            // Simulate a torn write that was partially fsynced by appending
            // garbage to the manifest and making it stable.
            let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME).unwrap();
            disk.append(&manifest_rel, b"garbage_corruption\x00\xff\x80")
                .await
                .unwrap();
            disk.fsync_file(&manifest_rel).await.unwrap();

            // Reopen — corrupt tail must be silently truncated.
            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"x", ReadOptions::default()).await.unwrap(),
                Some(b"y".to_vec()),
                "data before corruption must still be visible"
            );
            assert_eq!(engine2.stats().sstable_count, 1);
        });
    }

    // KD-0403: scan_prefix merges memtable and SSTable results correctly.
    #[test]
    fn engine_scan_prefix_merges_sstable_and_memtable() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();
            // Write two keys and flush.
            engine
                .put(b"u:1".to_vec(), b"one".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"u:2".to_vec(), b"two".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            // Write one more key to the new memtable.
            engine
                .put(b"u:3".to_vec(), b"three".to_vec(), strict_opts())
                .await
                .unwrap();
            let items = engine
                .scan_prefix(b"u:", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(items.len(), 3, "must return SSTable + memtable entries");
            assert_eq!(items[0].key, b"u:1");
            assert_eq!(items[2].key, b"u:3");
        });
    }

    #[test]
    fn test_engine_temp_file_cleanup_on_open() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            let current_tmp = RelativePath::new(CURRENT_TMP_FILE_NAME).unwrap();
            let sst_tmp = RelativePath::new("sst/0000000000000001.tmp").unwrap();

            disk.write_at(&current_tmp, 0, b"some content")
                .await
                .unwrap();
            disk.write_at(&sst_tmp, 0, b"some other content")
                .await
                .unwrap();

            assert!(disk.file_len(&current_tmp).await.is_ok());
            assert!(disk.file_len(&sst_tmp).await.is_ok());

            let engine = Engine::open(config, disk.clone()).await.unwrap();

            assert_eq!(engine.last_recovery().tmp_files_removed, 2);

            assert!(disk.file_len(&current_tmp).await.is_err());
            assert!(disk.file_len(&sst_tmp).await.is_err());
        });
    }

    #[test]
    fn test_engine_recover_temp_file_scan_dry_run() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            let current_tmp = RelativePath::new(CURRENT_TMP_FILE_NAME).unwrap();
            let sst_tmp = RelativePath::new("sst/0000000000000001.tmp").unwrap();

            disk.write_at(&current_tmp, 0, b"some content")
                .await
                .unwrap();
            disk.write_at(&sst_tmp, 0, b"some other content")
                .await
                .unwrap();

            assert!(disk.file_len(&current_tmp).await.is_ok());
            assert!(disk.file_len(&sst_tmp).await.is_ok());

            let report = recover(config, disk.clone()).await.unwrap();

            assert_eq!(report.tmp_files_removed, 2);

            assert!(disk.file_len(&current_tmp).await.is_ok());
            assert!(disk.file_len(&sst_tmp).await.is_ok());
        });
    }

    #[test]
    fn test_stable_warning_enums() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"key1".to_vec(), b"val1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
            }

            // Let's corrupt both WAL and Manifest.
            let wal_rel = RelativePath::new("wal/0000000000000001.wal").unwrap();
            disk.append(&wal_rel, b"partial_corrupt_wal_bytes")
                .await
                .unwrap();

            let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME).unwrap();
            disk.append(&manifest_rel, b"corrupted_manifest_tail_bytes")
                .await
                .unwrap();

            // Reopen engine and inspect warning enums.
            let engine = Engine::open(config, disk).await.unwrap();
            let warnings = &engine.last_recovery().warnings;

            let mut has_wal_warning = false;
            let mut has_manifest_warning = false;

            for warning in warnings {
                match warning {
                    RecoveryWarning::Wal(WalWarning::PartialHeader { .. })
                    | RecoveryWarning::Wal(WalWarning::BadMagic { .. })
                    | RecoveryWarning::Wal(WalWarning::PartialPayload { .. }) => {
                        has_wal_warning = true;
                    }
                    RecoveryWarning::Manifest(ManifestWarning::Truncated { .. })
                    | RecoveryWarning::Manifest(ManifestWarning::Invalid { .. }) => {
                        has_manifest_warning = true;
                    }
                    _ => {}
                }
            }

            assert!(has_wal_warning, "Should have WAL warning");
            assert!(has_manifest_warning, "Should have Manifest warning");
        });
    }

    #[test]
    fn engine_disk_full_resilience() {
        block_on(async {
            use kaya_io::{FaultKind, FaultRule, FaultSchedule, SimSeed};

            // Inject DiskFull fault at operation index 3 (which will be the SSTable write_at during flush)
            let schedule = FaultSchedule {
                seed: SimSeed(42),
                rules: vec![FaultRule {
                    operation_index: 3,
                    kind: FaultKind::DiskFull,
                }],
            };

            let disk = Arc::new(SimDisk::with_faults(schedule));
            let config = EngineConfig::default();

            let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
            engine
                .put(b"key1".to_vec(), b"val1".to_vec(), strict_opts())
                .await
                .unwrap();

            // Flush should fail due to simulated DiskFull
            let flush_res = engine.flush().await;
            assert!(
                matches!(flush_res, Err(KayaError::DiskFull)),
                "flush must fail with DiskFull, got: {:?}",
                flush_res
            );

            // Verify that in-memory stats are still intact and key1 is still readable from memtable
            assert_eq!(
                engine.stats().sstable_count,
                0,
                "no SSTables should be committed"
            );
            assert_eq!(
                engine.stats().memtable_entries,
                1,
                "memtable should retain the entry"
            );

            assert_eq!(
                engine.get(b"key1", ReadOptions::default()).await.unwrap(),
                Some(b"val1".to_vec()),
                "data must still be readable from memtable"
            );
        });
    }
}
