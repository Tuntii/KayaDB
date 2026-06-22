use std::time::{Duration, Instant};

use kaya_core::WalBatchConfig;
use tokio::sync::oneshot;

/// Action to take after a strict WAL record has been appended to the active segment.
pub(crate) enum BatchAction {
    /// Flush the active segment now (single group fsync for the pending batch).
    FlushNow,
    /// Wait until a group fsync covering this record completes.
    WaitForFlush(oneshot::Receiver<u64>),
}

/// Buffers strict append completions for WAL group commit.
#[derive(Debug)]
pub(crate) struct WalBatchWriter {
    max_records: usize,
    max_bytes: usize,
    flush_interval: Duration,
    pending_count: usize,
    pending_bytes: usize,
    batch_started_at: Option<Instant>,
    waiters: Vec<oneshot::Sender<u64>>,
}

impl WalBatchWriter {
    pub(crate) fn new(config: &WalBatchConfig) -> Self {
        Self {
            max_records: config.batch_max_records,
            max_bytes: config.batch_max_bytes,
            flush_interval: Duration::from_micros(config.batch_flush_interval_us),
            pending_count: 0,
            pending_bytes: 0,
            batch_started_at: None,
            waiters: Vec::new(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.max_records > 1 || self.max_bytes > 0 || !self.flush_interval.is_zero()
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending_count > 0
    }

    pub(crate) fn interval_expired(&self) -> bool {
        if self.flush_interval.is_zero() {
            return false;
        }
        self.batch_started_at
            .is_some_and(|started| started.elapsed() >= self.flush_interval)
    }

    pub(crate) fn after_record_appended(&mut self, encoded_len: usize) -> BatchAction {
        if !self.enabled() {
            return BatchAction::FlushNow;
        }

        self.pending_count += 1;
        self.pending_bytes += encoded_len;
        if self.batch_started_at.is_none() {
            self.batch_started_at = Some(Instant::now());
        }

        if self.should_flush_now() {
            BatchAction::FlushNow
        } else {
            let (tx, rx) = oneshot::channel();
            self.waiters.push(tx);
            BatchAction::WaitForFlush(rx)
        }
    }

    pub(crate) fn complete_flush(&mut self, duration_us: u64) {
        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(duration_us);
        }
        self.reset();
    }

    pub(crate) fn fail_flush(&mut self) {
        self.waiters.clear();
        self.reset();
    }

    fn should_flush_now(&self) -> bool {
        let count_limit = if self.max_records > 1 {
            self.max_records
        } else {
            usize::MAX
        };
        let count_full = self.pending_count >= count_limit;
        let bytes_full = self.max_bytes > 0 && self.pending_bytes >= self.max_bytes;
        let time_full = self.interval_expired();
        count_full || bytes_full || time_full
    }

    fn reset(&mut self) {
        self.pending_count = 0;
        self.pending_bytes = 0;
        self.batch_started_at = None;
    }
}