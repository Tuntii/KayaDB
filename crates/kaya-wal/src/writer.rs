use std::sync::Arc;

use kaya_core::{DurabilityMode, KayaError, Lsn, Result, SequenceNumber, WalConfig};
use kaya_io::{Disk, RelativePath};

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
pub struct WalWriter<D: Disk> {
    disk: Arc<D>,
    config: WalConfig,
    active_segment_id: SegmentId,
    active_path: RelativePath,
    active_len: u64,
    next_lsn: Lsn,
    next_sequence: SequenceNumber,
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

        Ok(Self {
            disk,
            config,
            active_segment_id,
            active_path,
            active_len,
            next_lsn,
            next_sequence,
        })
    }

    pub async fn append(
        &mut self,
        payload: WalPayload,
        mode: DurabilityMode,
    ) -> Result<AppendResult> {
        let record = WalRecord::new(self.next_lsn, self.next_sequence, payload);
        let encoded = encode_record(&record)?;
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| {
            KayaError::invalid_argument("encoded WAL record length does not fit into u32")
        })?;
        if encoded_len > self.config.max_record_bytes {
            return Err(KayaError::invalid_argument(format!(
                "encoded WAL record exceeds configured max: {encoded_len} > {}",
                self.config.max_record_bytes
            )));
        }

        if self.active_len > 0
            && self.active_len + u64::from(encoded_len) > self.config.segment_max_bytes
        {
            self.rotate().await?;
        }

        let offset = self.disk.append(&self.active_path, &encoded).await?;
        let durable = match mode {
            DurabilityMode::Strict => {
                let start = std::time::Instant::now();
                self.disk.fsync_file(&self.active_path).await?;
                let us = start.elapsed().as_micros() as u64;
                self.active_len = offset + u64::from(encoded_len);
                let result = AppendResult {
                    lsn: self.next_lsn,
                    sequence: self.next_sequence,
                    segment_id: self.active_segment_id,
                    offset,
                    encoded_len,
                    durable: true,
                    fsync_duration_us: Some(us),
                };
                self.next_lsn = self.next_lsn.next();
                self.next_sequence = self.next_sequence.next();
                return Ok(result);
            }
            DurabilityMode::Relaxed => false,
        };

        self.active_len = offset + u64::from(encoded_len);
        let result = AppendResult {
            lsn: self.next_lsn,
            sequence: self.next_sequence,
            segment_id: self.active_segment_id,
            offset,
            encoded_len,
            durable,
            fsync_duration_us: None,
        };
        self.next_lsn = self.next_lsn.next();
        self.next_sequence = self.next_sequence.next();
        Ok(result)
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
