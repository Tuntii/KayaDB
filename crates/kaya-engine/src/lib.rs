use std::collections::BTreeMap;
use std::sync::Arc;

use kaya_core::{
    Bytes, DurabilityMode, EngineConfig, KayaError, Lsn, Result, SequenceNumber,
};
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
pub use stats::{
    CompactionResult, EngineStats, FlushResult, WriteResult,
};

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
    /// Reference counts for pinned SSTables (used for snapshots).
    /// table_id -> refcount. When >0 the table is pinned and should not be deleted by compaction.
    sstable_refcounts: std::collections::HashMap<u64, u32>,
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
            recovery::load_manifest_and_sstables(disk.clone()).await?;
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
            sstable_refcounts: std::collections::HashMap::new(),
            lock_file,
        })
    }

    pub async fn close(&mut self) -> Result<()> {
        let _ = &self.disk;
        Ok(())
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

        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        let input_set: std::collections::HashSet<u64> = input_ids.iter().copied().collect();
        for (meta, reader) in self.live_sstables.iter().rev() {
            if !input_set.contains(&meta.table_id) {
                continue;
            }
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

        let mut builder = SstableBuilder::new(
            self.config.sstable.block_target_bytes,
            self.config.sstable.bloom_bits_per_key,
        );
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
            entry_count: merged.len() as u64,
            file_size: sst_file_size,
            footer_checksum: footer_crc,
        };

        let last_seq = SequenceNumber::new(self.stats.last_sequence);

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

        let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
        let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
        let root_rel = RelativePath::root();
        self.disk
            .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
            .await?;
        self.disk.fsync_file(&current_tmp_rel).await?;
        self.disk.rename(&current_tmp_rel, &current_rel).await?;
        self.disk.fsync_dir(&root_rel).await?;

        let new_reader = SstableReader::open(sst_bytes)?;
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

        let actual_input = input_ids.len() as u64;
        Ok(CompactionResult {
            input_tables: actual_input,
            output_tables: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{
        CompactionConfig, CompactionPolicyKind, DurabilityMode, EngineConfig,
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
}