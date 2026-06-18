use crate::types::{LogIndex, Term};

/// A single entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    /// Opaque payload. An empty command represents a no-op (leader commit barrier).
    pub command: Vec<u8>,
}

/// In-memory Raft log. Entries are 1-indexed (logical).
///
/// Supports log compaction via snapshots (Raft §7):
/// - `last_included_index` / `last_included_term` record the prefix that has been
///   snapshotted and removed from `entries`.
/// - The first real entry in `entries` (if any) is at logical index `last_included_index + 1`.
/// - `snapshot` holds opaque state-machine snapshot bytes (optional, for transfer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemLog {
    entries: Vec<LogEntry>,
    last_included_index: LogIndex,
    last_included_term: Term,
    snapshot: Option<Vec<u8>>,
}

impl Default for MemLog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
            snapshot: None,
        }
    }

    /// Logical index of the last entry in the (possibly compacted) log.
    /// Returns the snapshot's last_included_index if there are no trailing entries.
    pub fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            self.last_included_index
        } else {
            LogIndex(self.last_included_index.0 + self.entries.len() as u64)
        }
    }

    /// Term of the last entry, or the snapshot term if log has been compacted to a snapshot.
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.last_included_term)
    }

    /// Term of the entry at `index`, taking snapshots into account.
    /// `LogIndex(0)` returns `Some(Term(0))`.
    /// If `index` is covered by the current snapshot, returns the snapshot's term (for prev-log checks).
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.0 == 0 {
            return Some(Term(0));
        }
        if index <= self.last_included_index {
            // Anything at or before the snapshot boundary uses the snapshot term for consistency checks.
            return Some(self.last_included_term);
        }
        let offset = (index.0 - self.last_included_index.0 - 1) as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    /// Return the entry at logical `index`, or `None` (entries before/ in snapshot are not stored).
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index.0 == 0 || index <= self.last_included_index {
            return None;
        }
        let offset = (index.0 - self.last_included_index.0 - 1) as usize;
        self.entries.get(offset)
    }

    /// Append a new entry and return its assigned (logical) log index.
    pub fn append(&mut self, entry: LogEntry) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Remove all entries from `from_index` onwards (inclusive). Respects snapshot boundary.
    pub fn truncate_from(&mut self, from_index: LogIndex) {
        if from_index.0 == 0 {
            self.entries.clear();
            return;
        }
        if from_index <= self.last_included_index {
            // Truncating into or before the snapshot — keep the snapshot, clear trailing entries.
            self.entries.clear();
            return;
        }
        let offset = (from_index.0 - self.last_included_index.0 - 1) as usize;
        if offset < self.entries.len() {
            self.entries.truncate(offset);
        }
    }

    /// Return a slice of logical entries starting from `from_index`.
    /// Skips anything covered by the snapshot.
    pub fn entries_from(&self, from_index: LogIndex) -> &[LogEntry] {
        if from_index.0 == 0 || from_index <= self.last_included_index {
            return &[];
        }
        let start = (from_index.0 - self.last_included_index.0 - 1) as usize;
        if start >= self.entries.len() {
            return &[];
        }
        &self.entries[start..]
    }

    /// Compact the log up to (and including) `up_to_index` by installing a snapshot.
    ///
    /// After this call:
    /// - All entries at or before `up_to_index` are removed.
    /// - `last_included_index` and `last_included_term` are updated.
    /// - `snapshot` holds the provided opaque bytes (state machine image at that point).
    ///
    /// The caller is responsible for producing a correct `data` snapshot of the applied state
    /// at `up_to_index`.
    pub fn install_snapshot(&mut self, up_to_index: LogIndex, up_to_term: Term, data: Vec<u8>) {
        if up_to_index.0 <= self.last_included_index.0 {
            // Older or equal snapshot — keep the newer one.
            return;
        }

        // Remove all entries up to and including the snapshot point.
        let first_to_keep = up_to_index.0 + 1;
        let current_logical_first = self.last_included_index.0 + 1;

        if first_to_keep > current_logical_first {
            let drain_count = (first_to_keep - current_logical_first) as usize;
            let drain = drain_count.min(self.entries.len());
            self.entries.drain(0..drain);
        } else {
            self.entries.clear();
        }

        self.last_included_index = up_to_index;
        self.last_included_term = up_to_term;
        self.snapshot = Some(data);
    }

    /// Returns the currently installed snapshot, if any.
    pub fn snapshot(&self) -> Option<(LogIndex, Term, &[u8])> {
        self.snapshot.as_ref().map(|d| {
            (
                self.last_included_index,
                self.last_included_term,
                d.as_slice(),
            )
        })
    }

    /// Clear any installed snapshot (used after state machine has consumed it, or for tests).
    pub fn clear_snapshot(&mut self) {
        self.snapshot = None;
    }

    /// Highest index covered by the current snapshot (0 if none).
    pub fn last_included_index(&self) -> LogIndex {
        self.last_included_index
    }

    /// Term of the snapshot boundary.
    pub fn last_included_term(&self) -> Term {
        self.last_included_term
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_log_sentinels() {
        let log = MemLog::new();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert_eq!(log.term_at(LogIndex(0)), Some(Term(0)));
        assert_eq!(log.term_at(LogIndex(1)), None);
        assert_eq!(log.entries_from(LogIndex(1)), &[]);
    }

    #[test]
    fn append_and_read() {
        let mut log = MemLog::new();
        let idx = log.append(LogEntry {
            term: Term(1),
            command: b"a".to_vec(),
        });
        assert_eq!(idx, LogIndex(1));
        assert_eq!(log.last_index(), LogIndex(1));
        assert_eq!(log.last_term(), Term(1));
        assert_eq!(log.term_at(LogIndex(1)), Some(Term(1)));
        assert_eq!(log.get(LogIndex(1)).unwrap().command, b"a");
    }

    #[test]
    fn truncate_and_reappend() {
        let mut log = MemLog::new();
        for i in 1u64..=5 {
            log.append(LogEntry {
                term: Term(i),
                command: vec![i as u8],
            });
        }
        assert_eq!(log.last_index(), LogIndex(5));

        log.truncate_from(LogIndex(3));
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.term_at(LogIndex(3)), None);

        log.append(LogEntry {
            term: Term(9),
            command: b"new".to_vec(),
        });
        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.term_at(LogIndex(3)), Some(Term(9)));
    }

    #[test]
    fn entries_from_slice() {
        let mut log = MemLog::new();
        for i in 1u64..=4 {
            log.append(LogEntry {
                term: Term(i),
                command: vec![i as u8],
            });
        }
        let slice = log.entries_from(LogIndex(3));
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].term, Term(3));
        assert_eq!(slice[1].term, Term(4));

        assert_eq!(log.entries_from(LogIndex(5)), &[]);
    }

    #[test]
    fn snapshot_compaction_basic() {
        let mut log = MemLog::new();
        for i in 1u64..=10 {
            log.append(LogEntry {
                term: Term(i),
                command: vec![i as u8],
            });
        }
        assert_eq!(log.last_index(), LogIndex(10));

        // Install snapshot at 6
        let snap_data = b"snapshot-at-6".to_vec();
        log.install_snapshot(LogIndex(6), Term(6), snap_data.clone());

        assert_eq!(log.last_included_index(), LogIndex(6));
        assert_eq!(log.last_included_term(), Term(6));
        assert_eq!(log.last_index(), LogIndex(10)); // still have 7..10

        // Entries before or at snapshot are gone
        assert!(log.get(LogIndex(6)).is_none());
        assert!(log.get(LogIndex(3)).is_none());

        // Remaining entries are correct
        let tail = log.entries_from(LogIndex(7));
        assert_eq!(tail.len(), 4);
        assert_eq!(tail[0].term, Term(7));

        // term_at for snapshot boundary
        assert_eq!(log.term_at(LogIndex(6)), Some(Term(6)));
        assert_eq!(log.term_at(LogIndex(5)), Some(Term(6))); // treated as snapshot term

        // Snapshot data roundtrips
        let (idx, term, data) = log.snapshot().unwrap();
        assert_eq!(idx, LogIndex(6));
        assert_eq!(term, Term(6));
        assert_eq!(data, &snap_data[..]);
    }

    #[test]
    fn append_after_snapshot() {
        let mut log = MemLog::new();
        for i in 1u64..=5 {
            log.append(LogEntry {
                term: Term(1),
                command: vec![i as u8],
            });
        }
        log.install_snapshot(LogIndex(3), Term(1), b"s3".to_vec());

        // After snapshot at 3 we still have logical 4 and 5 (2 entries in the vec).
        // Next append should be logical 6.
        let new_idx = log.append(LogEntry {
            term: Term(2),
            command: b"new".to_vec(),
        });
        assert_eq!(new_idx, LogIndex(6));
        assert_eq!(log.last_index(), LogIndex(6));
        assert_eq!(log.get(LogIndex(6)).unwrap().command, b"new");

        // Logical 4 and 5 should still be retrievable
        assert_eq!(log.get(LogIndex(4)).unwrap().command, vec![4]);
    }
}
