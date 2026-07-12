use std::collections::BTreeMap;

use kaya_core::{Bytes, KayaError, KeyValue, Result};
use kaya_io::Disk;
use kaya_lsm::ValueRecordRef;

use super::{Engine, ReadOptions, ReadTimestamp, ScanOptions, WriteOptions, WriteResult};

impl<D: Disk> Engine<D> {
    pub async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        opts: WriteOptions,
    ) -> Result<WriteResult> {
        self.validate_key(&key)?;
        crate::index::reject_if_system_key(&key)?;
        self.validate_value(&value)?;
        let old = self.get_inner(&key, ReadTimestamp::Latest)?;
        let wr = self
            .write_put(key.clone(), value.clone(), opts.clone())
            .await?;
        self.maintain_indexes_after_put(&key, old.as_deref(), &value, &opts)
            .await?;
        self.append_cdc_event(crate::cdc::CdcEvent {
            seq: wr.sequence.get(),
            key: key.clone(),
            value: Some(value),
            op: crate::cdc::CdcOp::Put,
        })
        .await?;
        Ok(wr)
    }

    pub async fn delete(&mut self, key: Bytes, opts: WriteOptions) -> Result<WriteResult> {
        self.validate_key(&key)?;
        crate::index::reject_if_system_key(&key)?;
        let old = self.get_inner(&key, ReadTimestamp::Latest)?;
        let wr = self.write_delete(key.clone(), opts.clone()).await?;
        self.maintain_indexes_after_delete(&key, old.as_deref(), &opts)
            .await?;
        self.append_cdc_event(crate::cdc::CdcEvent {
            seq: wr.sequence.get(),
            key: key.clone(),
            value: None,
            op: crate::cdc::CdcOp::Delete,
        })
        .await?;
        Ok(wr)
    }

    pub async fn get(&mut self, key: &[u8], opts: ReadOptions) -> Result<Option<Bytes>> {
        let start = std::time::Instant::now();
        self.stats.get_count += 1;
        let result = self.get_inner(key, opts.read_at);
        let us = start.elapsed().as_micros() as u64;
        self.stats.record_get_latency(us);
        self.histograms.get_us.observe(us);
        result
    }

    pub(crate) fn get_inner(&mut self, key: &[u8], read_at: ReadTimestamp) -> Result<Option<Bytes>> {
        let read_ts = read_at.as_u64();
        // Memtable first: a hit (Put or Delete) at read_ts short-circuits.
        // No visible version → fall through to SSTs (may hold older versions).
        match self.memtable.get_at(key, read_ts) {
            Some(ValueRecordRef::Put { value, .. }) => return Ok(Some(value.to_vec())),
            Some(ValueRecordRef::Delete { .. }) => return Ok(None),
            None => {}
        }
        // SSTs newest-first: first get_at hit wins (Put returns value, Delete → missing).
        for (_, reader) in &self.live_sstables {
            if let Some(entry) = reader.get_at(key, read_ts)? {
                self.sync_block_cache_stats();
                return Ok(entry.value);
            }
        }
        self.sync_block_cache_stats();
        Ok(None)
    }

    pub async fn scan_prefix(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        let start = std::time::Instant::now();
        let result = self.scan_prefix_inner(prefix, opts);
        let us = start.elapsed().as_micros() as u64;
        self.stats.record_scan_latency(us);
        self.histograms.scan_us.observe(us);
        result
    }

    pub(crate) fn scan_prefix_inner(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        self.validate_scan_prefix(prefix)?;
        self.stats.scan_count += 1;
        let max_results = self.config.limits.max_scan_results;
        let max_bytes = self.config.limits.max_scan_bytes;
        let read_ts = opts.read_at.as_u64();
        // Merge window is bounded to `max_scan_results` keys (tombstones included):
        // the map always holds the smallest keys seen so far, so pruning the
        // largest key never resurrects a stale version of a surviving key.
        // Per key: keep highest sequence visible at read_ts across all sources.
        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        for (_, reader) in self.live_sstables.iter().rev() {
            for entry in reader.scan_prefix_at(prefix, read_ts)? {
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
            let seq_n = seq.get();
            if seq_n > read_ts {
                continue;
            }
            // Multi-version memtable may yield several seqs per user key;
            // keep the higher sequence (max seq wins), not last insert.
            match merged.get(&key) {
                Some((s, _)) if *s >= seq_n => {}
                _ => {
                    merged.insert(key, (seq_n, value));
                    if merged.len() > max_results {
                        merged.pop_last();
                    }
                }
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
