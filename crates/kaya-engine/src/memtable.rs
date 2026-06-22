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
                return Ok(entry.value);
            }
        }
        Ok(None)
    }

    pub async fn scan_prefix(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        self.stats.scan_count += 1;
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
