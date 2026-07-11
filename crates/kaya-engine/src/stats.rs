use super::RecoveryReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub sequence: kaya_core::SequenceNumber,
    pub lsn: kaya_core::Lsn,
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
    /// Cumulative microseconds spent in WAL fsync_file calls for Strict appends.
    /// Pairs with eBPF kernel-side histograms from scripts/ebpf/fsync-latency.bt.
    pub wal_fsync_total_us: u64,
    /// Maximum single WAL fsync duration observed (us).
    pub wal_fsync_max_us: u64,
    pub memtable_entries: u64,
    pub sstable_count: u64,
    pub last_sequence: u64,
    /// Cumulative microseconds spent inside flush() calls (full operation wall time).
    /// Track A observability addition (pairs with eBPF syscall timelines).
    pub flush_total_us: u64,
    /// Maximum single flush() duration observed (us).
    pub flush_max_us: u64,
    /// Number of flush() operations performed.
    pub flush_count: u64,
    /// Cumulative microseconds spent inside compact() calls (full operation wall time).
    pub compaction_total_us: u64,
    /// Maximum single compact() duration observed (us).
    pub compaction_max_us: u64,
    /// Number of compact() operations performed.
    pub compaction_count: u64,
    /// Aggregate decoded block-cache hits across open SSTable readers.
    pub block_cache_hits: u64,
    /// Aggregate decoded block-cache misses across open SSTable readers.
    pub block_cache_misses: u64,
    /// Wall-clock microseconds spent in the last engine open/recovery path.
    pub recovery_duration_us: u64,
    /// Cumulative microseconds spent in `get()` read-path lookups (memtable +
    /// live SSTables). Previously the read path was entirely unmeasured.
    pub get_total_us: u64,
    /// Maximum single `get()` duration observed (us).
    pub get_max_us: u64,
    /// Cumulative microseconds spent in `scan_prefix()` merges.
    pub scan_total_us: u64,
    /// Maximum single `scan_prefix()` duration observed (us).
    pub scan_max_us: u64,
}

impl EngineStats {
    /// Record one `get()` latency sample (microseconds).
    pub fn record_get_latency(&mut self, us: u64) {
        self.get_total_us += us;
        if us > self.get_max_us {
            self.get_max_us = us;
        }
    }

    /// Record one `scan_prefix()` latency sample (microseconds).
    pub fn record_scan_latency(&mut self, us: u64) {
        self.scan_total_us += us;
        if us > self.scan_max_us {
            self.scan_max_us = us;
        }
    }
}

/// Per-operation latency distributions (percentile-capable), complementing the
/// `total/max/count` scalars in [`EngineStats`]. Held separately from
/// `EngineStats` because histograms are not `Copy`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineHistograms {
    pub get_us: kaya_core::LatencyHistogram,
    pub scan_us: kaya_core::LatencyHistogram,
    pub wal_fsync_us: kaya_core::LatencyHistogram,
    pub flush_us: kaya_core::LatencyHistogram,
    pub compaction_us: kaya_core::LatencyHistogram,
}

use kaya_io::Disk;

use super::Engine;

impl<D: Disk> Engine<D> {
    pub fn stats(&self) -> EngineStats {
        self.stats
    }

    /// Per-operation latency histograms (p50/p99-capable distributions).
    pub fn histograms(&self) -> &EngineHistograms {
        &self.histograms
    }

    pub fn last_recovery(&self) -> &RecoveryReport {
        &self.last_recovery
    }
}
