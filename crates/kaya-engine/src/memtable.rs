use std::collections::BTreeMap;

use kaya_core::{Bytes, KayaError, KeyValue, Result};
use kaya_io::Disk;
use kaya_lsm::ValueRecordRef;
use kaya_wal::WalPayload;

use super::{Engine, ReadOptions, ScanOptions, WriteOptions, WriteResult};

impl<D: Disk> Engine<D> {
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
        if let Some(us) = append.fsync_duration_us {
            self.stats.wal_fsync_total_us += us;
            if us > self.stats.wal_fsync_max_us {
                self.stats.wal_fsync_max_us = us;
            }
        }
        self.stats.last_sequence = append.sequence.get();
        self.maybe_auto_flush().await?;
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
        if let Some(us) = append.fsync_duration_us {
            self.stats.wal_fsync_total_us += us;
            if us > self.stats.wal_fsync_max_us {
                self.stats.wal_fsync_max_us = us;
            }
        }
        self.stats.last_sequence = append.sequence.get();
        self.maybe_auto_flush().await?;
        Ok(WriteResult {
            sequence: append.sequence,
            lsn: append.lsn,
            durable: append.durable,
        })
    }

    pub async fn get(&mut self, key: &[u8], opts: ReadOptions) -> Result<Option<Bytes>> {
        let _ = opts;
        self.stats.get_count += 1;
        match self.memtable.get(key) {
            Some(ValueRecordRef::Put { value, .. }) => return Ok(Some(value.to_vec())),
            Some(ValueRecordRef::Delete { .. }) => return Ok(None),
            None => {}
        }
        for (_, reader) in &self.live_sstables {
            if let Some(entry) = reader.get(key)? {
                self.sync_block_cache_stats();
                return Ok(entry.value);
            }
        }
        self.sync_block_cache_stats();
        Ok(None)
    }

    pub async fn scan_prefix(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        self.validate_scan_prefix(prefix)?;
        self.stats.scan_count += 1;
        let max_results = self.config.limits.max_scan_results;
        let max_bytes = self.config.limits.max_scan_bytes;
        // Merge window is bounded to `max_scan_results` keys (tombstones included):
        // the map always holds the smallest keys seen so far, so pruning the
        // largest key never resurrects a stale version of a surviving key.
        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        for (_, reader) in self.live_sstables.iter().rev() {
            for entry in reader.scan_prefix(prefix)? {
                let seq = entry.sequence.get();
                match merged.get(&entry.key) {
                    Some((s, _)) if *s >= seq => {}
                    _ => {
                        merged.insert(entry.key, (seq, entry.value));
                        if merged.len() > max_results {
                            merged.pop_last();
                        }
                    }
                }
            }
        }
        for (key, value, seq) in self.memtable.raw_scan_prefix(prefix) {
            merged.insert(key, (seq.get(), value));
            if merged.len() > max_results {
                merged.pop_last();
            }
        }
        let effective_limit = opts.limit.map_or(max_results, |l| l.min(max_results));
        let mut result: Vec<KeyValue> = Vec::new();
        let mut total_bytes = 0usize;
        for (key, (_, v)) in merged {
            let Some(value) = v else { continue };
            let entry_bytes = key.len() + value.len();
            if !result.is_empty() && total_bytes + entry_bytes > max_bytes {
                break;
            }
            total_bytes += entry_bytes;
            result.push(KeyValue { key, value });
            if result.len() >= effective_limit {
                break;
            }
        }
        self.sync_block_cache_stats();
        Ok(result)
    }

    fn sync_block_cache_stats(&mut self) {
        let mut hits = 0u64;
        let mut misses = 0u64;
        for (_, reader) in &self.live_sstables {
            let s = reader.block_cache_stats();
            hits += s.hits;
            misses += s.misses;
        }
        self.stats.block_cache_hits = hits;
        self.stats.block_cache_misses = misses;
    }

    pub(crate) fn validate_key(&self, key: &[u8]) -> Result<()> {
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

    pub(crate) fn validate_scan_prefix(&self, prefix: &[u8]) -> Result<()> {
        if prefix.len() > self.config.limits.max_key_len {
            return Err(KayaError::invalid_argument(format!(
                "scan prefix length {} exceeds max key length {}",
                prefix.len(),
                self.config.limits.max_key_len
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_value(&self, value: &[u8]) -> Result<()> {
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
