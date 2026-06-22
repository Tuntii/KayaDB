use std::sync::Arc;
use std::time::Instant;

use kaya_core::{DurabilityMode, KayaError, Lsn, Result, SequenceNumber, WalConfig};
use kaya_io::{Disk, RelativePath};
use tokio::sync::Mutex;

use crate::batch::{BatchAction, WalBatchWriter};
use crate::{encode_record, WalPayload, WalRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub lsn: Lsn,
    pub sequence: SequenceNumber,
    pub segment_id: SegmentId,
    pub offset: u64,
    pub encoded_len: u32,
    pub durable: bool,
    /// Duration of the fsync_file that made this append durable (Strict mode only).
    /// None for Relaxed or if timing was not captured.
    pub fsync_duration_us: Option<u64>,
}

#[derive(Debug)]
struct WalWriterInner<D: Disk> {
    disk: Arc<D>,
    config: WalConfig,
    active_segment_id: SegmentId,
    active_path: RelativePath,
    active_len: u64,
    next_lsn: Lsn,
    next_sequence: SequenceNumber,
    batch: WalBatchWriter,
}

#[derive(Debug, Clone)]
pub struct WalWriter<D: Disk> {
    inner: Arc<Mutex<WalWriterInner<D>>>,
}

impl<D: Disk> WalWriter<D> {
    pub async fn open(config: WalConfig, disk: Arc<D>) -> Result<Self> {
        Self::open_at(config, disk, Lsn::FIRST, SequenceNumber::FIRST).await
    }

    pub async fn open_at(
        config: WalConfig,
        disk: Arc<D>,
        next_lsn: Lsn,
        next_sequence: SequenceNumber,
    ) -> Result<Self> {
        let wal_dir = RelativePath::new("wal")?;
        let segments = disk.list_dir(&wal_dir).await?;
        let active_segment_id = segments
            .iter()
            .filter_map(|entry| parse_segment_id(&entry.path))
            .max()
            .map(SegmentId)
            .unwrap_or(SegmentId(1));
        let active_path = segment_path(active_segment_id)?;
        let active_len = match disk.file_len(&active_path).await {
            Ok(len) => len,
            Err(KayaError::NotFound) => 0,
            Err(error) => return Err(error),
        };
        let batch = WalBatchWriter::new(&config.batch);

        Ok(Self {
            inner: Arc::new(Mutex::new(WalWriterInner {
                disk,
                config,
                active_segment_id,
                active_path,
                active_len,
                next_lsn,
                next_sequence,
                batch,
            })),
        })
    }

    pub async fn append(&self, payload: WalPayload, mode: DurabilityMode) -> Result<AppendResult> {
        let mut inner = self.inner.lock().await;

        if inner.batch.enabled() && inner.batch.has_pending() && inner.batch.interval_expired() {
            inner.flush_strict_batch().await?;
        }

        let record = WalRecord::new(inner.next_lsn, inner.next_sequence, payload);
        let encoded = encode_record(&record)?;
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| {
            KayaError::invalid_argument("encoded WAL record length does not fit into u32")
        })?;
        if encoded_len > inner.config.max_record_bytes {
            return Err(KayaError::invalid_argument(format!(
                "encoded WAL record exceeds configured max: {encoded_len} > {}",
                inner.config.max_record_bytes
            )));
        }

        if inner.active_len > 0
            && inner.active_len + u64::from(encoded_len) > inner.config.segment_max_bytes
        {
            inner.flush_pending_batch().await?;
            inner.rotate().await?;
        }

        let lsn = inner.next_lsn;
        let sequence = inner.next_sequence;
        let segment_id = inner.active_segment_id;
        let offset = inner.disk.append(&inner.active_path, &encoded).await?;

        match mode {
            DurabilityMode::Relaxed => {
                inner.active_len = offset + u64::from(encoded_len);
                inner.next_lsn = inner.next_lsn.next();
                inner.next_sequence = inner.next_sequence.next();
                Ok(AppendResult {
                    lsn,
                    sequence,
                    segment_id,
                    offset,
                    encoded_len,
                    durable: false,
                    fsync_duration_us: None,
                })
            }
            DurabilityMode::Strict => {
                if !inner.batch.enabled() {
                    let fsync_duration_us = inner.flush_strict_batch().await?;
                    inner.active_len = offset + u64::from(encoded_len);
                    inner.next_lsn = inner.next_lsn.next();
                    inner.next_sequence = inner.next_sequence.next();
                    return Ok(AppendResult {
                        lsn,
                        sequence,
                        segment_id,
                        offset,
                        encoded_len,
                        durable: true,
                        fsync_duration_us: Some(fsync_duration_us),
                    });
                }

                let action = inner.batch.after_record_appended(encoded.len());
                inner.active_len = offset + u64::from(encoded_len);
                inner.next_lsn = inner.next_lsn.next();
                inner.next_sequence = inner.next_sequence.next();

                match action {
                    BatchAction::FlushNow => {
                        let fsync_duration_us = inner.flush_strict_batch().await?;
                        Ok(AppendResult {
                            lsn,
                            sequence,
                            segment_id,
                            offset,
                            encoded_len,
                            durable: true,
                            fsync_duration_us: Some(fsync_duration_us),
                        })
                    }
                    BatchAction::WaitForFlush(rx) => {
                        drop(inner);
                        let fsync_duration_us = rx.await.map_err(|_| {
                            KayaError::internal("WAL batch waiter dropped before group commit")
                        })?;
                        Ok(AppendResult {
                            lsn,
                            sequence,
                            segment_id,
                            offset,
                            encoded_len,
                            durable: true,
                            fsync_duration_us: Some(fsync_duration_us),
                        })
                    }
                }
            }
        }
    }
}

impl<D: Disk> WalWriterInner<D> {
    async fn flush_pending_batch(&mut self) -> Result<()> {
        if self.batch.has_pending() {
            self.flush_strict_batch().await?;
        }
        Ok(())
    }

    async fn flush_strict_batch(&mut self) -> Result<u64> {
        let start = Instant::now();
        match self.disk.fsync_file(&self.active_path).await {
            Ok(()) => {
                let duration_us = start.elapsed().as_micros() as u64;
                self.batch.complete_flush(duration_us);
                Ok(duration_us)
            }
            Err(error) => {
                self.batch.fail_flush();
                Err(error)
            }
        }
    }

    async fn rotate(&mut self) -> Result<()> {
        self.active_segment_id = SegmentId(self.active_segment_id.0 + 1);
        self.active_path = segment_path(self.active_segment_id)?;
        self.active_len = 0;
        self.disk.fsync_dir(&RelativePath::new("wal")?).await?;
        Ok(())
    }
}

pub fn segment_path(segment_id: SegmentId) -> Result<RelativePath> {
    RelativePath::new(format!("wal/{:016x}.wal", segment_id.0))
}

pub fn parse_segment_id(path: &RelativePath) -> Option<u64> {
    let name = path.file_name()?;
    let hex = name.strip_suffix(".wal")?;
    u64::from_str_radix(hex, 16).ok()
}
