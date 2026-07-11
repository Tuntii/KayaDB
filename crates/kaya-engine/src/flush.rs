use kaya_core::{Result, SequenceNumber};
use kaya_io::{Disk, RelativePath};
use kaya_lsm::{
    decode_footer, encode_manifest_edit, footer_stored_crc, ManifestEdit, Memtable, SstEntry,
    SstableBuilder, SstableReader, TableMetadata, ValueRecord, CURRENT_FILE_NAME,
    CURRENT_TMP_FILE_NAME, MANIFEST_FILE_NAME,
};

use super::{Engine, FlushResult};

impl<D: Disk> Engine<D> {
    /// Flush memtable to L0 when it reaches `config.memtable.max_bytes` (if non-zero).
    pub(crate) async fn maybe_auto_flush(&mut self) -> Result<()> {
        let max_bytes = self.config.memtable.max_bytes;
        if max_bytes > 0 && self.memtable.approximate_bytes() >= max_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<FlushResult> {
        if self.memtable.is_empty() {
            return Ok(FlushResult {
                memtable_entries: 0,
                sstable_count: self.live_sstables.len() as u64,
            });
        }

        kaya_core::emit_probe_marker(
            kaya_core::ProbeMarkerSite::Flush,
            kaya_core::ProbeMarkerPhase::Enter,
            None,
        );
        let flush_start = std::time::Instant::now();
        let result = self.flush_nonempty(flush_start).await;
        match &result {
            Ok(_) => {
                let flush_us = flush_start.elapsed().as_micros() as u64;
                kaya_core::emit_probe_marker(
                    kaya_core::ProbeMarkerSite::Flush,
                    kaya_core::ProbeMarkerPhase::Exit,
                    Some(flush_us),
                );
            }
            Err(_) => {
                kaya_core::emit_probe_marker(
                    kaya_core::ProbeMarkerSite::Flush,
                    kaya_core::ProbeMarkerPhase::Exit,
                    None,
                );
            }
        }
        result
    }

    async fn flush_nonempty(&mut self, flush_start: std::time::Instant) -> Result<FlushResult> {
        let entry_count = self.memtable.len() as u64;
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        let mut builder =
            SstableBuilder::with_options(kaya_lsm::SstableBuildOptions::from(&self.config.sstable));
        for (key, record) in self.memtable.iter() {
            match record {
                ValueRecord::Put { value, sequence } => {
                    builder.add(SstEntry {
                        key: key.clone(),
                        value: Some(value.clone()),
                        sequence: *sequence,
                    });
                }
                ValueRecord::Delete { sequence } => {
                    builder.add(SstEntry {
                        key: key.clone(),
                        value: None,
                        sequence: *sequence,
                    });
                }
            }
        }
        let sst_bytes = builder.finish()?;
        let sst_file_size = sst_bytes.len() as u64;
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

        let sst_path = format!("sst/{table_id:016x}.sst");
        let tmp_path = format!("sst/{table_id:016x}.tmp");
        let sst_rel = RelativePath::new(&sst_path)?;
        let tmp_rel = RelativePath::new(&tmp_path)?;
        let sst_dir_rel = RelativePath::new("sst")?;
        self.disk.write_at(&tmp_rel, 0, &sst_bytes).await?;
        self.disk.fsync_file(&tmp_rel).await?;
        self.disk.rename(&tmp_rel, &sst_rel).await?;
        self.disk.fsync_dir(&sst_dir_rel).await?;

        let footer_crc = footer_stored_crc(&sst_bytes).unwrap_or(0);
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

        let current_tmp_rel = RelativePath::new(CURRENT_TMP_FILE_NAME)?;
        let current_rel = RelativePath::new(CURRENT_FILE_NAME)?;
        let root_rel = RelativePath::root();
        self.disk
            .write_at(&current_tmp_rel, 0, MANIFEST_FILE_NAME.as_bytes())
            .await?;
        self.disk.fsync_file(&current_tmp_rel).await?;
        self.disk.rename(&current_tmp_rel, &current_rel).await?;
        self.disk.fsync_dir(&root_rel).await?;

        let reader =
            SstableReader::open_with_cache(sst_bytes, self.config.sstable.block_cache_capacity)?;
        self.live_sstables.insert(0, (meta.clone(), reader));
        self.manifest_state.live_tables.push(meta);
        self.manifest_state.last_sequence = last_seq;
        self.memtable = Memtable::new();
        self.stats.sstable_count = self.live_sstables.len() as u64;
        self.stats.memtable_entries = 0;

        let flush_us = flush_start.elapsed().as_micros() as u64;
        self.stats.flush_total_us += flush_us;
        if flush_us > self.stats.flush_max_us {
            self.stats.flush_max_us = flush_us;
        }
        self.stats.flush_count += 1;
        self.histograms.flush_us.observe(flush_us);

        Ok(FlushResult {
            memtable_entries: entry_count,
            sstable_count: self.live_sstables.len() as u64,
        })
    }
}
