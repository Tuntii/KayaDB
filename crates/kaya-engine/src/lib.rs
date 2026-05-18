use std::sync::Arc;

use kaya_core::{
    Bytes, DurabilityMode, EngineConfig, KayaError, KeyValue, Lsn, Result, SequenceNumber,
};
use kaya_io::Disk;
use kaya_lsm::{Memtable, ValueRecordRef};
use kaya_wal::{recover_wal, WalPayload, WalRecoveryReport, WalWriter};

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
pub struct RecoveryReport {
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
}

impl<D: Disk> Engine<D> {
    pub async fn open(config: EngineConfig, disk: Arc<D>) -> Result<Self> {
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
        let stats = EngineStats {
            memtable_entries: memtable.len() as u64,
            last_sequence: next_sequence.get().saturating_sub(1),
            ..EngineStats::default()
        };
        let last_recovery = RecoveryReport {
            records_replayed: wal_report.records.len(),
            wal: wal_report,
        };

        Ok(Self {
            config,
            disk,
            wal,
            memtable,
            stats,
            last_recovery,
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
        Ok(match self.memtable.get(key) {
            Some(ValueRecordRef::Put { value, .. }) => Some(value.to_vec()),
            Some(ValueRecordRef::Delete { .. }) | None => None,
        })
    }

    pub async fn scan_prefix(&mut self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>> {
        self.stats.scan_count += 1;
        let mut items = self.memtable.scan_prefix(prefix);
        if let Some(limit) = opts.limit {
            items.truncate(limit);
        }
        Ok(items)
    }

    pub async fn flush(&mut self) -> Result<FlushResult> {
        Ok(FlushResult {
            memtable_entries: self.memtable.len() as u64,
            sstable_count: 0,
        })
    }

    pub async fn compact(&mut self) -> Result<CompactionResult> {
        Ok(CompactionResult {
            input_tables: 0,
            output_tables: 0,
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
    let wal = recover_wal(config.wal, disk).await?;
    Ok(RecoveryReport {
        records_replayed: wal.records.len(),
        wal,
    })
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
