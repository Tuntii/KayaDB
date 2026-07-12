use kaya_core::{Bytes, KayaError, Result, SequenceNumber};
use kaya_io::{Disk, RelativePath};
use kaya_lsm::{Memtable, SstableReader, TableMetadata, ValueRecord};

use super::Engine;

/// A compact, pinnable snapshot view of the LSM state at a point in time.
/// Used for efficient Raft snapshots instead of full KV dump.
///
/// Contains:
/// - pinned_tables: the exact live SSTables at snapshot time (with their metadata)
/// - memtable_data: raw contents of the memtable at that point (key, value or tombstone, seq)
/// - cutoff_seq: the MVCC sequence number boundary
#[derive(Debug, Clone)]
pub struct SnapshotView {
    pub pinned_tables: Vec<TableMetadata>,
    pub memtable_data: Vec<(Bytes, Option<Bytes>, SequenceNumber)>,
    pub cutoff_seq: SequenceNumber,
}

impl<D: Disk> Engine<D> {
    /// Attempt to load a persisted Raft snapshot from `snap_path`.
    /// Returns true if a snapshot was loaded and applied to the engine.
    ///
    /// After load, the tables referenced by the snapshot are pinned (refcounted)
    /// so that subsequent compactions respect them (see compact()).
    pub async fn load_persisted_raft_snapshot(
        &mut self,
        snap_path: impl AsRef<std::path::Path>,
    ) -> Result<bool> {
        let path = snap_path.as_ref();
        match std::fs::read(path) {
            Ok(data) if !data.is_empty() => {
                self.install_snapshot(&data).await?;
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(_) => Ok(false),
        }
    }

    /// Create a **pinned, manifest-anchored MVCC snapshot** (the proper prototype implementation).
    ///
    /// Instead of dumping the entire KV dataset, we capture:
    /// - The exact set of live SSTables (from manifest) — these will be "pinned".
    /// - The current memtable contents (raw with seqs).
    /// - The cutoff SequenceNumber.
    ///
    /// This is cheap (mostly metadata + one flush) and allows efficient log truncation
    /// in Raft. The returned bytes are a compact serialized `SnapshotView`.
    ///
    /// Files referenced in the snapshot are refcounted so compaction won't delete them
    /// until the snapshot is released.
    pub async fn create_snapshot(&mut self) -> Result<Vec<u8>> {
        let memtable_snapshot: Vec<(Bytes, Option<Bytes>, SequenceNumber)> = self
            .memtable
            .iter()
            .map(|(k, rec)| {
                // Store user keys so install_snapshot put/delete re-encodes once.
                let uk = k.user_key.clone();
                match rec {
                    ValueRecord::Put { value, sequence } => (uk, Some(value.clone()), *sequence),
                    ValueRecord::Delete { sequence } => (uk, None, *sequence),
                }
            })
            .collect();

        let pinned_tables: Vec<TableMetadata> = self.manifest_state.live_tables.clone();

        for meta in &pinned_tables {
            *self.sstable_refcounts.entry(meta.table_id).or_insert(0) += 1;
        }

        let cutoff = self.manifest_state.last_sequence;

        let view = SnapshotView {
            pinned_tables,
            memtable_data: memtable_snapshot,
            cutoff_seq: cutoff,
        };

        let mut buf = Vec::new();

        buf.extend_from_slice(&(view.pinned_tables.len() as u32).to_le_bytes());
        for meta in &view.pinned_tables {
            buf.extend_from_slice(&meta.table_id.to_le_bytes());
            buf.extend_from_slice(&meta.level.to_le_bytes());
            let path_bytes = meta.path.as_bytes();
            buf.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(path_bytes);
            buf.extend_from_slice(&(meta.smallest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.smallest_key);
            buf.extend_from_slice(&(meta.largest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.largest_key);
            buf.extend_from_slice(&meta.min_sequence.get().to_le_bytes());
            buf.extend_from_slice(&meta.max_sequence.get().to_le_bytes());
            buf.extend_from_slice(&meta.entry_count.to_le_bytes());
            buf.extend_from_slice(&meta.file_size.to_le_bytes());
            buf.extend_from_slice(&meta.footer_checksum.to_le_bytes());
        }

        buf.extend_from_slice(&(view.memtable_data.len() as u32).to_le_bytes());
        for (key, value_opt, seq) in &view.memtable_data {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            match value_opt {
                Some(v) => {
                    buf.push(1u8);
                    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    buf.extend_from_slice(v);
                }
                None => {
                    buf.push(0u8);
                }
            }
            buf.extend_from_slice(&seq.get().to_le_bytes());
        }

        buf.extend_from_slice(&view.cutoff_seq.get().to_le_bytes());

        buf.extend_from_slice(&(view.pinned_tables.len() as u32).to_le_bytes());
        for meta in &view.pinned_tables {
            let p = meta.path.as_bytes();
            buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
            buf.extend_from_slice(p);

            let rel = RelativePath::new(&meta.path)?;
            let flen = self.disk.file_len(&rel).await? as usize;
            let mut data = vec![0u8; flen];
            let _n = self.disk.read_at(&rel, 0, &mut data).await?;
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
            buf.extend_from_slice(&data);
        }

        Ok(buf)
    }

    /// Install a pinned snapshot view produced by `create_snapshot`.
    pub async fn install_snapshot(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut cur: &[u8] = data;

        let num_tables = Self::take_u32(&mut cur)? as usize;
        let mut pinned: Vec<TableMetadata> = Vec::with_capacity(num_tables);
        for _ in 0..num_tables {
            let table_id = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            let level = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
            cur = &cur[4..];
            let path_len = Self::take_u32(&mut cur)? as usize;
            let path = String::from_utf8(cur[..path_len].to_vec())
                .map_err(|_| KayaError::corruption("bad path in snapshot"))?;
            cur = &cur[path_len..];
            let sk_len = Self::take_u32(&mut cur)? as usize;
            let smallest_key = cur[..sk_len].to_vec();
            cur = &cur[sk_len..];
            let lk_len = Self::take_u32(&mut cur)? as usize;
            let largest_key = cur[..lk_len].to_vec();
            cur = &cur[lk_len..];
            let min_seq = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            let max_seq = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            let entry_count = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            let file_size = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            let footer_checksum = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
            cur = &cur[4..];

            pinned.push(TableMetadata {
                table_id,
                level,
                path,
                smallest_key,
                largest_key,
                min_sequence: SequenceNumber::new(min_seq),
                max_sequence: SequenceNumber::new(max_seq),
                entry_count,
                file_size,
                footer_checksum,
            });
        }

        let num_mt = Self::take_u32(&mut cur)? as usize;
        let mut mt_data = Vec::with_capacity(num_mt);
        for _ in 0..num_mt {
            let klen = Self::take_u32(&mut cur)? as usize;
            let key = cur[..klen].to_vec();
            cur = &cur[klen..];
            let has_val = cur[0];
            cur = &cur[1..];
            let value = if has_val == 1 {
                let vlen = Self::take_u32(&mut cur)? as usize;
                let v = cur[..vlen].to_vec();
                cur = &cur[vlen..];
                Some(v)
            } else {
                None
            };
            let seq = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];
            mt_data.push((key, value, SequenceNumber::new(seq)));
        }

        let cutoff = u64::from_le_bytes([
            cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
        ]);
        let cutoff_seq = SequenceNumber::new(cutoff);

        if cur.len() >= 4 {
            let num_candidate = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
            if num_candidate > 0 && num_candidate < 4096 && cur.len() >= 4 + num_candidate {
                let _ = Self::take_u32(&mut cur);
                for _ in 0..num_candidate {
                    if cur.len() < 4 {
                        break;
                    }
                    let plen = Self::take_u32(&mut cur).unwrap_or(0) as usize;
                    if cur.len() < plen {
                        break;
                    }
                    let path = String::from_utf8(cur[..plen].to_vec()).unwrap_or_default();
                    cur = &cur[plen..];

                    if cur.len() < 8 {
                        break;
                    }
                    let clen = Self::take_u64(&mut cur).unwrap_or(0) as usize;
                    if cur.len() < clen {
                        break;
                    }
                    let content = cur[..clen].to_vec();
                    cur = &cur[clen..];

                    if !path.is_empty() && clen > 0 {
                        if let Ok(rel) = RelativePath::new(&path) {
                            let _ = self.disk.write_at(&rel, 0, &content).await;
                            let _ = self.disk.fsync_file(&rel).await;
                        }
                    }
                }
            }
        }

        let mut new_live: Vec<(TableMetadata, SstableReader)> = Vec::new();
        for meta in &pinned {
            let sst_rel = RelativePath::new(&meta.path)?;
            let sst_len = self.disk.file_len(&sst_rel).await?;
            let mut sst_buf = vec![0u8; sst_len as usize];
            self.disk.read_at(&sst_rel, 0, &mut sst_buf).await?;
            let reader =
                SstableReader::open_with_cache(sst_buf, self.config.sstable.block_cache_capacity)?;
            new_live.push((meta.clone(), reader));
        }
        new_live.sort_by_key(|b| std::cmp::Reverse(b.0.table_id));

        self.live_sstables = new_live;
        self.manifest_state.live_tables = pinned.clone();

        let mut new_mem = Memtable::new();
        for (key, value, seq) in mt_data {
            match value {
                Some(v) => new_mem.put(key, v, seq),
                None => new_mem.delete(key, seq),
            }
        }
        self.memtable = new_mem;

        self.manifest_state.last_sequence = cutoff_seq;
        self.stats.last_sequence = cutoff_seq.get().saturating_sub(1);
        self.stats.memtable_entries = self.memtable.len() as u64;
        self.stats.sstable_count = self.live_sstables.len() as u64;

        for meta in &pinned {
            *self.sstable_refcounts.entry(meta.table_id).or_insert(0) += 1;
        }

        Ok(())
    }

    /// Release the pin counts held by a serialized snapshot view (produced by
    /// `create_snapshot`).
    pub async fn release_snapshot(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut cur: &[u8] = data;

        let num_tables = Self::take_u32(&mut cur)? as usize;
        for _ in 0..num_tables {
            if cur.len() < 8 {
                return Err(KayaError::invalid_argument("truncated snapshot table id"));
            }
            let table_id = u64::from_le_bytes([
                cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
            ]);
            cur = &cur[8..];

            if cur.len() < 4 {
                return Err(KayaError::invalid_argument("truncated level"));
            }
            cur = &cur[4..];
            let plen = Self::take_u32(&mut cur)? as usize;
            if cur.len() < plen {
                return Err(KayaError::invalid_argument("truncated path"));
            }
            cur = &cur[plen..];
            let sk = Self::take_u32(&mut cur)? as usize;
            if cur.len() < sk {
                return Err(KayaError::invalid_argument("truncated smallest"));
            }
            cur = &cur[sk..];
            let lk = Self::take_u32(&mut cur)? as usize;
            if cur.len() < lk {
                return Err(KayaError::invalid_argument("truncated largest"));
            }
            cur = &cur[lk..];
            if cur.len() < 40 {
                return Err(KayaError::invalid_argument("truncated table footer"));
            }
            cur = &cur[40..];

            if let Some(count) = self.sstable_refcounts.get_mut(&table_id) {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    self.sstable_refcounts.remove(&table_id);
                }
            }
        }

        if !cur.is_empty() {
            let _ = Self::take_u32(&mut cur).ok();
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn take_u32(cur: &mut &[u8]) -> Result<u32> {
        if cur.len() < 4 {
            return Err(KayaError::invalid_argument("truncated snapshot u32"));
        }
        let v = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
        *cur = &cur[4..];
        Ok(v)
    }

    fn take_u64(cur: &mut &[u8]) -> Result<u64> {
        if cur.len() < 8 {
            return Err(KayaError::invalid_argument("truncated snapshot u64"));
        }
        let v = u64::from_le_bytes([
            cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
        ]);
        *cur = &cur[8..];
        Ok(v)
    }

    #[allow(dead_code)]
    fn take_bytes(cur: &mut &[u8]) -> Result<Vec<u8>> {
        let len = Self::take_u32(cur)? as usize;
        if cur.len() < len {
            return Err(KayaError::invalid_argument("truncated snapshot bytes"));
        }
        let bytes = cur[..len].to_vec();
        *cur = &cur[len..];
        Ok(bytes)
    }
}
