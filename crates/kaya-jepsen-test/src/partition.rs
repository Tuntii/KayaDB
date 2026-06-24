//! Partition nemesis observability for scenario verification.

use std::sync::atomic::{AtomicU32, Ordering};

/// Tracks partition nemesis attempts and outcomes during a scenario run.
#[derive(Debug, Default)]
pub struct PartitionTracker {
    attempted: AtomicU32,
    applied: AtomicU32,
    failed: AtomicU32,
}

impl PartitionTracker {
    pub fn record_attempt(&self) {
        self.attempted.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn attempted(&self) -> u32 {
        self.attempted.load(Ordering::SeqCst)
    }

    pub fn applied(&self) -> u32 {
        self.applied.load(Ordering::SeqCst)
    }

    pub fn failed(&self) -> u32 {
        self.failed.load(Ordering::SeqCst)
    }

    pub fn summary(&self) -> String {
        format!(
            "partition_attempted={} applied={} failed={}",
            self.attempted(),
            self.applied(),
            self.failed()
        )
    }
}
