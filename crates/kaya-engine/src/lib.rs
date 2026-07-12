use std::collections::BTreeMap;
use std::sync::Arc;

use kaya_core::{Bytes, DurabilityMode, EngineConfig, KayaError, Lsn, Result, SequenceNumber};
use kaya_io::{Disk, RelativePath};
use kaya_lsm::{
    decode_footer, encode_manifest_edit, footer_stored_crc, CompactionPolicy, ManifestEdit,
    ManifestState, ManifestWarning, Memtable, SstEntry, SstableBuilder, SstableReader,
    TableMetadata, CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME, MANIFEST_FILE_NAME,
};
use kaya_wal::{recover_wal, WalRecoveryReport, WalWarning, WalWriter};

mod compaction;
mod flush;
mod memtable;
mod recovery;
mod snapshot;
mod stats;

pub use recovery::recover;
pub use snapshot::SnapshotView;
pub use stats::{CompactionResult, EngineStats, FlushResult, WriteResult};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub durability: Option<DurabilityMode>,
    pub idempotency_key: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadTimestamp {
    #[default]
    Latest,
    /// Inclusive upper bound on commit_ts / sequence.
    At(u64),
}

impl ReadTimestamp {
    pub fn as_u64(self) -> u64 {
        match self {
            Self::Latest => u64::MAX,
            Self::At(ts) => ts,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub read_at: ReadTimestamp,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub limit: Option<usize>,
    pub read_at: ReadTimestamp,
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
    histograms: stats::EngineHistograms,
    last_recovery: RecoveryReport,
    manifest_state: ManifestState,
    next_table_id: u64,
    next_manifest_edit_seq: u64,
    /// Live SSTables sorted newest-first (highest table_id first).
    live_sstables: Vec<(TableMetadata, SstableReader)>,
    /// Reference counts for pinned SSTables (used for snapshots).
    /// table_id -> refcount. When >0 the table is pinned and should not be deleted by compaction.
    sstable_refcounts: std::collections::HashMap<u64, u32>,
    /// GC lower bound: compaction may drop versions with seq < watermark under Rules A–C.
    gc_watermark: u64,
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

        if data_dir.as_os_str().is_empty() {
            return Ok(None);
        }

        std::fs::create_dir_all(data_dir)?;

        let lock_path = data_dir.join("KAYA_LOCK");

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }

        let file = match options.open(&lock_path) {
            Ok(f) => f,
            // Structural lock-conflict error; `KayaError::guidance()` supplies
            // the "another instance is running" operator hint uniformly.
            Err(_) => return Err(KayaError::LockConflict),
        };

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let res = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if res != 0 {
                return Err(KayaError::LockConflict);
            }
        }

        Ok(Some(file))
    }
}

impl<D: Disk> Engine<D> {
    pub async fn open(config: EngineConfig, disk: Arc<D>) -> Result<Self> {
        let recovery_started = std::time::Instant::now();
        let lock_file = acquire_directory_lock(&config)?;
        let temp_files = recovery::scan_temp_files(&disk).await?;
        let tmp_files_removed = temp_files.len();
        for path in &temp_files {
            disk.remove_file(path).await?;
        }

        let wal_report = recover_wal(config.wal.clone(), disk.clone()).await?;
        let mut memtable = Memtable::new();

        for recovered in &wal_report.records {
            recovery::apply_payload(
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

        let (manifest_state, live_sstables, manifest_records_replayed, manifest_warnings) =
            recovery::load_manifest_and_sstables(disk.clone(), config.sstable.block_cache_capacity)
                .await?;
        let next_table_id = manifest_state
            .live_tables
            .iter()
            .map(|t| t.table_id + 1)
            .max()
            .unwrap_or(1);
        let next_manifest_edit_seq = manifest_state.last_edit_seq + 1;

        let recovery_duration_us = recovery_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let stats = EngineStats {
            memtable_entries: memtable.len() as u64,
            sstable_count: live_sstables.len() as u64,
            last_sequence: next_sequence.get().saturating_sub(1),
            recovery_duration_us,
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
            histograms: stats::EngineHistograms::default(),
            last_recovery,
            manifest_state,
            next_table_id,
            next_manifest_edit_seq,
            live_sstables,
            sstable_refcounts: std::collections::HashMap::new(),
            gc_watermark: 0,
            lock_file,
        })
    }

    pub async fn close(&mut self) -> Result<()> {
        let _ = &self.disk;
        Ok(())
    }

    /// Current GC watermark (versions with seq < watermark may be dropped by compaction).
    pub fn gc_watermark(&self) -> u64 {
        self.gc_watermark
    }

    /// Advance the GC watermark (non-decreasing).
    pub fn set_gc_watermark(&mut self, ts: u64) {
        if ts > self.gc_watermark {
            self.gc_watermark = ts;
        }
    }

    pub async fn compact(&mut self) -> Result<CompactionResult> {
        let mut pinned_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (&id, &cnt) in &self.sstable_refcounts {
            if cnt > 0 {
                pinned_ids.insert(id);
            }
        }

        let policy = compaction::compaction_policy_from_config(&self.config.compaction);
        let candidate = match policy.pick_compaction(&self.manifest_state.live_tables, &pinned_ids)
        {
            Some(c) => c,
            None => {
                return Ok(CompactionResult {
                    input_tables: 0,
                    output_tables: 0,
                });
            }
        };
        let input_ids: Vec<u64> = candidate.input_table_ids;

        let compact_start = std::time::Instant::now();

        // Collect ALL versions from input SSTs (v4 multi-version or legacy single).
        // Group by user_key; keep multi-version order (seq DESC).
        let input_set: std::collections::HashSet<u64> = input_ids.iter().copied().collect();
        let mut by_key: BTreeMap<Bytes, Vec<(u64, Option<Bytes>)>> = BTreeMap::new();
        for (meta, reader) in self.live_sstables.iter().rev() {
            if !input_set.contains(&meta.table_id) {
                continue;
            }
            for entry in reader.all_entries()? {
                by_key
                    .entry(entry.key)
                    .or_default()
                    .push((entry.sequence.get(), entry.value));
            }
        }

        // Apply GC watermark (Rules A/B/C) and emit in InternalKey order
        // (user_key ASC, seq DESC) for SST v4 builder.
        let watermark = self.gc_watermark;
        let mut out_entries: Vec<SstEntry> = Vec::new();
        for (key, mut versions) in by_key {
            versions.sort_by(|a, b| b.0.cmp(&a.0));
            // Same seq from overlapping inputs: keep first (already seq DESC).
            versions.dedup_by(|a, b| a.0 == b.0);
            for (seq, value) in select_versions_for_gc(versions, watermark) {
                out_entries.push(SstEntry {
                    key: key.clone(),
                    value,
                    sequence: SequenceNumber::new(seq),
                });
            }
        }

        let last_seq = SequenceNumber::new(self.stats.last_sequence);
        let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME)?;

        // Empty output after GC: delete inputs only (cannot build empty SSTable).
        if out_entries.is_empty() {
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

            let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
            let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
            let root_rel = RelativePath::root();
            self.disk
                .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
                .await?;
            self.disk.fsync_file(&current_tmp_rel).await?;
            self.disk.rename(&current_tmp_rel, &current_rel).await?;
            self.disk.fsync_dir(&root_rel).await?;

            let mut new_live: Vec<(TableMetadata, SstableReader)> = Vec::new();
            for (meta, reader) in self.live_sstables.drain(..) {
                if !input_set.contains(&meta.table_id) {
                    new_live.push((meta, reader));
                }
            }
            new_live.sort_by_key(|b| std::cmp::Reverse(b.0.table_id));
            self.live_sstables = new_live;
            for &id in &input_ids {
                self.manifest_state.live_tables.retain(|t| t.table_id != id);
            }
            self.manifest_state.last_sequence = last_seq;
            self.stats.sstable_count = self.live_sstables.len() as u64;

            let compact_us = compact_start.elapsed().as_micros() as u64;
            self.stats.compaction_total_us += compact_us;
            if compact_us > self.stats.compaction_max_us {
                self.stats.compaction_max_us = compact_us;
            }
            self.stats.compaction_count += 1;
            self.histograms.compaction_us.observe(compact_us);

            return Ok(CompactionResult {
                input_tables: input_ids.len() as u64,
                output_tables: 0,
            });
        }

        let new_table_id = self.next_table_id;
        self.next_table_id += 1;

        let mut build_opts = kaya_lsm::SstableBuildOptions::from(&self.config.sstable);
        build_opts.mvcc = true;
        let mut builder = SstableBuilder::with_options(build_opts);
        for entry in &out_entries {
            builder.add(entry.clone());
        }
        let sst_bytes = builder.finish()?;
        let sst_file_size = sst_bytes.len() as u64;
        let entry_count = out_entries.len() as u64;

        let (sst_table_min_seq, sst_table_max_seq, smallest_key, largest_key) = {
            let footer = decode_footer(&sst_bytes)?;
            let reader_tmp = SstableReader::open_with_cache(
                sst_bytes.clone(),
                self.config.sstable.block_cache_capacity,
            )?;
            let entries = reader_tmp.all_entries()?;
            let sk = entries.first().map(|e| e.key.clone()).unwrap_or_default();
            let lk = entries.last().map(|e| e.key.clone()).unwrap_or_default();
            (footer.table_min_seq, footer.table_max_seq, sk, lk)
        };

        let sst_path = format!("sst/{new_table_id:016x}.sst");
        let tmp_path = format!("sst/{new_table_id:016x}.tmp");
        let sst_rel = RelativePath::new(&sst_path)?;
        let tmp_rel = RelativePath::new(&tmp_path)?;
        let sst_dir_rel = RelativePath::new("sst")?;
        self.disk.write_at(&tmp_rel, 0, &sst_bytes).await?;
        self.disk.fsync_file(&tmp_rel).await?;
        self.disk.rename(&tmp_rel, &sst_rel).await?;
        self.disk.fsync_dir(&sst_dir_rel).await?;

        let footer_crc = footer_stored_crc(&sst_bytes).unwrap_or(0);
        let new_meta = TableMetadata {
            table_id: new_table_id,
            level: candidate.output_level,
            path: sst_path,
            smallest_key,
            largest_key,
            min_sequence: SequenceNumber::new(sst_table_min_seq),
            max_sequence: SequenceNumber::new(sst_table_max_seq),
            entry_count,
            file_size: sst_file_size,
            footer_checksum: footer_crc,
        };

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

        let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
        let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
        let root_rel = RelativePath::root();
        self.disk
            .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
            .await?;
        self.disk.fsync_file(&current_tmp_rel).await?;
        self.disk.rename(&current_tmp_rel, &current_rel).await?;
        self.disk.fsync_dir(&root_rel).await?;

        let new_reader =
            SstableReader::open_with_cache(sst_bytes, self.config.sstable.block_cache_capacity)?;
        let mut new_live: Vec<(TableMetadata, SstableReader)> = Vec::new();
        for (meta, reader) in self.live_sstables.drain(..) {
            if !input_set.contains(&meta.table_id) {
                new_live.push((meta, reader));
            }
        }
        new_live.push((new_meta.clone(), new_reader));
        new_live.sort_by_key(|b| std::cmp::Reverse(b.0.table_id));

        self.live_sstables = new_live;

        for &id in &input_ids {
            self.manifest_state.live_tables.retain(|t| t.table_id != id);
        }
        self.manifest_state.live_tables.push(new_meta);
        self.manifest_state.last_sequence = last_seq;
        self.stats.sstable_count = self.live_sstables.len() as u64;

        let compact_us = compact_start.elapsed().as_micros() as u64;
        self.stats.compaction_total_us += compact_us;
        if compact_us > self.stats.compaction_max_us {
            self.stats.compaction_max_us = compact_us;
        }
        self.stats.compaction_count += 1;
        self.histograms.compaction_us.observe(compact_us);

        let actual_input = input_ids.len() as u64;
        Ok(CompactionResult {
            input_tables: actual_input,
            output_tables: 1,
        })
    }
}

/// Select versions to retain under GC watermark Rules A/B/C (mvcc-spec §7.2).
///
/// `versions` must be sorted by sequence descending (newest first).
/// Safer minimal policy:
/// - keep all versions with `seq >= watermark`
/// - if newest overall is a Put with `seq < watermark`, keep it (Rule C)
/// - drop obsolete tombstones and superseded older versions (Rules A/B)
fn select_versions_for_gc(
    versions: Vec<(u64, Option<Bytes>)>,
    watermark: u64,
) -> Vec<(u64, Option<Bytes>)> {
    if versions.is_empty() {
        return versions;
    }
    // Rule B: newest is tombstone below watermark → drop tombstone and all older.
    if versions[0].1.is_none() && versions[0].0 < watermark {
        return Vec::new();
    }

    let mut retained = Vec::new();
    for (i, (seq, val)) in versions.into_iter().enumerate() {
        if seq >= watermark {
            retained.push((seq, val));
        } else if i == 0 && val.is_some() {
            // Rule C: sole covering Put below watermark.
            retained.push((seq, val));
        }
        // else Rule A / obsolete: drop
    }
    retained
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{
        CompactionConfig, CompactionPolicyKind, DurabilityMode, EngineConfig, KeyValue,
        LeveledCompactionConfig,
    };
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

    #[test]
    fn engine_zstd_prefix_cache_flush_get_path() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut config = EngineConfig::default();
            config.sstable.compression_zstd = true;
            config.sstable.prefix_compression = true;
            config.sstable.block_cache_capacity = 32;

            let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
            for i in 0_u8..24 {
                let key = format!("metric:{i:02}").into_bytes();
                let val = vec![i; 64];
                engine.put(key, val, strict_opts()).await.unwrap();
            }
            engine.flush().await.unwrap();

            let key = b"metric:05".to_vec();
            assert_eq!(
                engine.get(&key, ReadOptions::default()).await.unwrap(),
                Some(vec![5u8; 64])
            );
            let misses_after_first = engine.stats().block_cache_misses;
            assert!(misses_after_first > 0, "first get should miss block cache");

            assert_eq!(
                engine.get(&key, ReadOptions::default()).await.unwrap(),
                Some(vec![5u8; 64])
            );
            assert!(
                engine.stats().block_cache_hits > 0,
                "second get should hit block cache"
            );

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2
                    .get(b"metric:10", ReadOptions::default())
                    .await
                    .unwrap(),
                Some(vec![10u8; 64]),
                "reopen must read ZSTD+prefix SSTable"
            );
        });
    }

    #[test]
    fn engine_flush_reopen_proper_prefix_user_keys() {
        // Regression: user keys where one is a proper prefix of another must
        // survive flush (sorted SST emission) and reopen.
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"aa".to_vec(), b"2".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine
                    .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
                assert_eq!(
                    engine.get(b"a", ReadOptions::default()).await.unwrap(),
                    Some(b"1".to_vec())
                );
                assert_eq!(
                    engine.get(b"aa", ReadOptions::default()).await.unwrap(),
                    Some(b"2".to_vec())
                );
            }

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine2.get(b"aa", ReadOptions::default()).await.unwrap(),
                Some(b"2".to_vec())
            );
            let items = engine2
                .scan_prefix(b"a", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(items.len(), 2);
            assert_eq!(items[0].key, b"a");
            assert_eq!(items[1].key, b"aa");
        });
    }

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
                assert_eq!(
                    engine.get(b"sst:a", ReadOptions::default()).await.unwrap(),
                    Some(b"alpha".to_vec()),
                    "must read from SSTable after flush"
                );
            }

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

    #[test]
    fn compaction_preserves_visible_state() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();

            engine
                .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"b".to_vec(), b"old".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

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

    #[test]
    fn leveled_policy_waits_for_l0_trigger() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig {
                compaction: CompactionConfig {
                    policy: CompactionPolicyKind::Leveled,
                    leveled: LeveledCompactionConfig {
                        level_count: 7,
                        l0_compaction_trigger: 4,
                    },
                    ..CompactionConfig::default()
                },
                ..EngineConfig::default()
            };
            let mut engine = Engine::open(config, disk).await.unwrap();

            for i in 0..3 {
                engine
                    .put(format!("k{i}").into_bytes(), b"v".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
            }
            assert_eq!(engine.stats().sstable_count, 3);

            let below_threshold = engine.compact().await.unwrap();
            assert_eq!(below_threshold.input_tables, 0);
            assert_eq!(below_threshold.output_tables, 0);
            assert_eq!(engine.stats().sstable_count, 3);

            engine
                .put(b"k3".to_vec(), b"v".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            assert_eq!(engine.stats().sstable_count, 4);

            let at_threshold = engine.compact().await.unwrap();
            assert_eq!(at_threshold.input_tables, 4);
            assert_eq!(at_threshold.output_tables, 1);
            assert_eq!(engine.stats().sstable_count, 1);
        });
    }

    #[test]
    fn flush_and_compaction_stats_are_recorded() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();

            engine
                .put(b"k1".to_vec(), b"v1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            let s1 = engine.stats();
            assert!(s1.flush_count >= 1);
            let _ = s1.flush_total_us;

            engine
                .put(b"k2".to_vec(), b"v2".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            let s2 = engine.stats();
            assert!(s2.flush_count >= 2);

            engine
                .put(b"k3".to_vec(), b"v3".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            let _r = engine.compact().await.unwrap();
            let s3 = engine.stats();
            assert!(s3.compaction_count >= 1);
            let _ = s3.compaction_total_us;
        });
    }

    #[test]
    fn read_path_latency_histograms_are_populated() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            for i in 0..20u8 {
                engine
                    .put(vec![b'k', i], vec![b'v', i], strict_opts())
                    .await
                    .unwrap();
            }
            for i in 0..20u8 {
                let _ = engine
                    .get(&[b'k', i], ReadOptions::default())
                    .await
                    .unwrap();
            }
            let _ = engine
                .scan_prefix(b"k", ScanOptions::default())
                .await
                .unwrap();

            let h = engine.histograms();
            assert_eq!(h.get_us.count(), 20, "each get records a latency sample");
            assert_eq!(h.scan_us.count(), 1);
            assert!(
                h.wal_fsync_us.count() >= 20,
                "strict puts record fsync latency"
            );
            // Percentile is a finite bucket bound (or max), never a panic.
            let p99 = h.get_us.percentile_us(0.99);
            assert!(p99 >= h.get_us.percentile_us(0.5));

            // Scalar counters mirror the histograms.
            let s = engine.stats();
            assert_eq!(s.get_count, 20);
            assert!(s.get_total_us >= s.get_max_us);
        });
    }

    #[test]
    fn put_auto_flushes_when_memtable_exceeds_max_bytes() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut config = EngineConfig::default();
            config.memtable.max_bytes = 6;
            let mut engine = Engine::open(config, disk).await.unwrap();

            engine
                .put(b"k".to_vec(), b"value".to_vec(), strict_opts())
                .await
                .unwrap();

            assert_eq!(engine.stats().flush_count, 1);
            assert_eq!(engine.stats().memtable_entries, 0);
            assert_eq!(engine.stats().sstable_count, 1);
        });
    }

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

            let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME).unwrap();
            disk.append(&manifest_rel, b"garbage_corruption\x00\xff\x80")
                .await
                .unwrap();
            disk.fsync_file(&manifest_rel).await.unwrap();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"x", ReadOptions::default()).await.unwrap(),
                Some(b"y".to_vec()),
                "data before corruption must still be visible"
            );
            assert_eq!(engine2.stats().sstable_count, 1);
        });
    }

    #[test]
    fn engine_scan_prefix_merges_sstable_and_memtable() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let mut engine = Engine::open(config, disk).await.unwrap();
            engine
                .put(b"u:1".to_vec(), b"one".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"u:2".to_vec(), b"two".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
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

            let wal_rel = RelativePath::new("wal/0000000000000001.wal").unwrap();
            disk.append(&wal_rel, b"partial_corrupt_wal_bytes")
                .await
                .unwrap();

            let manifest_rel = RelativePath::new(MANIFEST_FILE_NAME).unwrap();
            disk.append(&manifest_rel, b"corrupted_manifest_tail_bytes")
                .await
                .unwrap();

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

            let flush_res = engine.flush().await;
            assert!(
                matches!(flush_res, Err(KayaError::DiskFull)),
                "flush must fail with DiskFull, got: {:?}",
                flush_res
            );

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

    fn scan_limits_config(max_scan_results: usize, max_scan_bytes: usize) -> EngineConfig {
        let mut config = EngineConfig::default();
        config.limits.max_scan_results = max_scan_results;
        config.limits.max_scan_bytes = max_scan_bytes;
        config
    }

    #[test]
    fn scan_hard_cap_bounds_unlimited_scan() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(scan_limits_config(5, usize::MAX), disk)
                .await
                .unwrap();
            for i in 0..10u8 {
                engine
                    .put(vec![b'k', b'0' + i], b"v".to_vec(), strict_opts())
                    .await
                    .unwrap();
            }
            let result = engine
                .scan_prefix(b"k", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(result.len(), 5, "hard cap must bound unlimited scans");
            assert_eq!(result[0].key, b"k0".to_vec());
            assert_eq!(result[4].key, b"k4".to_vec());
        });
    }

    #[test]
    fn scan_user_limit_capped_by_hard_cap() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(scan_limits_config(3, usize::MAX), disk)
                .await
                .unwrap();
            for i in 0..6u8 {
                engine
                    .put(vec![b'k', b'0' + i], b"v".to_vec(), strict_opts())
                    .await
                    .unwrap();
            }
            let result = engine
                .scan_prefix(b"k", ScanOptions { limit: Some(10), ..Default::default() })
                .await
                .unwrap();
            assert_eq!(result.len(), 3, "user limit above hard cap is clamped");
        });
    }

    #[test]
    fn scan_byte_cap_truncates_but_returns_first_entry() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            // Each entry is 2 (key) + 100 (value) bytes; cap allows one entry only,
            // and the first entry must always pass even when it alone exceeds the cap.
            let mut engine = Engine::open(scan_limits_config(usize::MAX, 10), disk)
                .await
                .unwrap();
            for i in 0..4u8 {
                engine
                    .put(vec![b'k', b'0' + i], vec![b'v'; 100], strict_opts())
                    .await
                    .unwrap();
            }
            let result = engine
                .scan_prefix(b"k", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(result.len(), 1, "byte cap should stop after first entry");
            assert_eq!(result[0].key, b"k0".to_vec());
        });
    }

    #[test]
    fn scan_rejects_oversized_prefix() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            let oversized = vec![b'p'; kaya_core::DEFAULT_MAX_KEY_LEN + 1];
            let result = engine.scan_prefix(&oversized, ScanOptions::default()).await;
            assert!(
                matches!(result, Err(KayaError::InvalidArgument { .. })),
                "prefix longer than max key length must be rejected, got: {result:?}"
            );
        });
    }

    #[test]
    fn scan_hard_cap_keeps_newest_values_across_flush() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(scan_limits_config(3, usize::MAX), disk)
                .await
                .unwrap();
            for i in 0..6u8 {
                engine
                    .put(vec![b'k', b'0' + i], b"old".to_vec(), strict_opts())
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
            engine
                .put(b"k0".to_vec(), b"new".to_vec(), strict_opts())
                .await
                .unwrap();
            let result = engine
                .scan_prefix(b"k", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(
                result[0],
                KeyValue {
                    key: b"k0".to_vec(),
                    value: b"new".to_vec()
                },
                "capped scan must not resurrect stale values"
            );
            assert_eq!(result[1].key, b"k1".to_vec());
            assert_eq!(result[2].key, b"k2".to_vec());
        });
    }

    fn read_at(ts: u64) -> ReadOptions {
        ReadOptions {
            read_at: ReadTimestamp::At(ts),
        }
    }

    #[test]
    fn snapshot_get_at_sees_older_version_in_memtable() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let w1 = engine
                .put(b"k".to_vec(), b"v1".to_vec(), strict_opts())
                .await
                .unwrap();
            let w2 = engine
                .put(b"k".to_vec(), b"v2".to_vec(), strict_opts())
                .await
                .unwrap();
            assert!(w2.sequence.get() > w1.sequence.get());

            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v1".to_vec())
            );
            assert_eq!(
                engine.get(b"k", ReadOptions::default()).await.unwrap(),
                Some(b"v2".to_vec())
            );
            assert_eq!(
                engine
                    .get(b"k", read_at(w2.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v2".to_vec())
            );
        });
    }

    #[test]
    fn snapshot_get_at_survives_flush_and_reopen() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();
            let (s1, s2) = {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                let w1 = engine
                    .put(b"k".to_vec(), b"v1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                let w2 = engine
                    .put(b"k".to_vec(), b"v2".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine.flush().await.unwrap();
                assert_eq!(engine.stats().sstable_count, 1);
                assert_eq!(engine.stats().memtable_entries, 0);
                // Still visible from SST after flush
                assert_eq!(
                    engine
                        .get(b"k", read_at(w1.sequence.get()))
                        .await
                        .unwrap(),
                    Some(b"v1".to_vec())
                );
                assert_eq!(
                    engine.get(b"k", ReadOptions::default()).await.unwrap(),
                    Some(b"v2".to_vec())
                );
                (w1.sequence.get(), w2.sequence.get())
            };

            disk.crash();

            let mut engine2 = Engine::open(config, disk).await.unwrap();
            assert_eq!(
                engine2.get(b"k", read_at(s1)).await.unwrap(),
                Some(b"v1".to_vec())
            );
            assert_eq!(
                engine2.get(b"k", read_at(s2)).await.unwrap(),
                Some(b"v2".to_vec())
            );
            assert_eq!(
                engine2.get(b"k", ReadOptions::default()).await.unwrap(),
                Some(b"v2".to_vec())
            );
        });
    }

    #[test]
    fn delete_then_get_at_older_sees_put() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let w1 = engine
                .put(b"k".to_vec(), b"v1".to_vec(), strict_opts())
                .await
                .unwrap();
            let _w2 = engine.delete(b"k".to_vec(), strict_opts()).await.unwrap();

            assert_eq!(
                engine.get(b"k", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v1".to_vec())
            );

            engine.flush().await.unwrap();
            assert_eq!(
                engine.get(b"k", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v1".to_vec())
            );
        });
    }

    #[test]
    fn compact_with_watermark_drops_old_versions() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let w1 = engine
                .put(b"k".to_vec(), b"v1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            let w2 = engine
                .put(b"k".to_vec(), b"v2".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            assert_eq!(engine.stats().sstable_count, 2);

            // Before GC: both versions visible
            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v1".to_vec())
            );

            // Watermark at v2: drop v1 under Rule A (superseded by retained N >= wm)
            engine.set_gc_watermark(w2.sequence.get());
            assert_eq!(engine.gc_watermark(), w2.sequence.get());

            let r = engine.compact().await.unwrap();
            assert_eq!(r.input_tables, 2);
            assert_eq!(r.output_tables, 1);
            assert_eq!(engine.stats().sstable_count, 1);

            assert_eq!(
                engine.get(b"k", ReadOptions::default()).await.unwrap(),
                Some(b"v2".to_vec())
            );
            assert_eq!(
                engine
                    .get(b"k", read_at(w2.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v2".to_vec())
            );
            // Old snapshot bound below watermark no longer sees dropped version
            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn compact_preserves_versions_when_watermark_zero() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let w1 = engine
                .put(b"k".to_vec(), b"v1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();
            let w2 = engine
                .put(b"k".to_vec(), b"v2".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            assert_eq!(engine.gc_watermark(), 0);
            engine.compact().await.unwrap();

            assert_eq!(
                engine
                    .get(b"k", read_at(w1.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v1".to_vec())
            );
            assert_eq!(
                engine
                    .get(b"k", read_at(w2.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"v2".to_vec())
            );
        });
    }

    #[test]
    fn proper_prefix_keys_after_multi_version_flush() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let wa_old = engine
                .put(b"a".to_vec(), b"va-old".to_vec(), strict_opts())
                .await
                .unwrap();
            let _wa = engine
                .put(b"a".to_vec(), b"va".to_vec(), strict_opts())
                .await
                .unwrap();
            let _waa = engine
                .put(b"aa".to_vec(), b"vaa".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.flush().await.unwrap();

            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"va".to_vec())
            );
            assert_eq!(
                engine.get(b"aa", ReadOptions::default()).await.unwrap(),
                Some(b"vaa".to_vec())
            );
            assert_eq!(
                engine
                    .get(b"a", read_at(wa_old.sequence.get()))
                    .await
                    .unwrap(),
                Some(b"va-old".to_vec())
            );

            let scan = engine
                .scan_prefix(b"a", ScanOptions::default())
                .await
                .unwrap();
            assert_eq!(scan.len(), 2);
            assert_eq!(scan[0].key, b"a");
            assert_eq!(scan[0].value, b"va");
            assert_eq!(scan[1].key, b"aa");
            assert_eq!(scan[1].value, b"vaa");

            let scan_at = engine
                .scan_prefix(
                    b"a",
                    ScanOptions {
                        limit: None,
                        read_at: ReadTimestamp::At(wa_old.sequence.get()),
                    },
                )
                .await
                .unwrap();
            assert_eq!(scan_at.len(), 1);
            assert_eq!(scan_at[0].key, b"a");
            assert_eq!(scan_at[0].value, b"va-old");
        });
    }

    #[test]
    fn gc_watermark_is_non_decreasing() {
        let disk = Arc::new(SimDisk::new());
        let mut engine = block_on(Engine::open(EngineConfig::default(), disk)).unwrap();
        assert_eq!(engine.gc_watermark(), 0);
        engine.set_gc_watermark(10);
        assert_eq!(engine.gc_watermark(), 10);
        engine.set_gc_watermark(5);
        assert_eq!(engine.gc_watermark(), 10);
        engine.set_gc_watermark(20);
        assert_eq!(engine.gc_watermark(), 20);
    }
}
