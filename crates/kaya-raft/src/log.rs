use crate::types::{LogIndex, Term};

/// A single entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    /// Opaque payload. An empty command represents a no-op (leader commit barrier).
    pub command: Vec<u8>,
}

/// In-memory Raft log. Entries are 1-indexed.
///
/// Internal storage is 0-based: entry at log index `n` lives at vec index `n − 1`.
pub struct MemLog {
    entries: Vec<LogEntry>,
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
        }
    }

    /// Index of the last entry, or `LogIndex(0)` if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        LogIndex(self.entries.len() as u64)
    }

    /// Term of the last entry, or `Term(0)` if the log is empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(Term(0))
    }

    /// Term of the entry at `index`.
    /// `LogIndex(0)` returns `Some(Term(0))` (sentinel for "before start of log").
    /// Returns `None` if `index` exceeds the last stored index.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.0 == 0 {
            return Some(Term(0));
        }
        self.entries.get((index.0 - 1) as usize).map(|e| e.term)
    }

    /// Return the entry at `index`, or `None`.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index.0 == 0 {
            return None;
        }
        self.entries.get((index.0 - 1) as usize)
    }

    /// Append a new entry and return its assigned log index.
    pub fn append(&mut self, entry: LogEntry) -> LogIndex {
        self.entries.push(entry);
        LogIndex(self.entries.len() as u64)
    }

    /// Remove all entries from `from_index` onwards (inclusive).
    pub fn truncate_from(&mut self, from_index: LogIndex) {
        if from_index.0 == 0 {
            self.entries.clear();
            return;
        }
        self.entries.truncate((from_index.0 - 1) as usize);
    }

    /// Return a slice of all entries starting from `from_index`.
    pub fn entries_from(&self, from_index: LogIndex) -> &[LogEntry] {
        if from_index.0 == 0 {
            return &[];
        }
        let start = (from_index.0 - 1) as usize;
        if start >= self.entries.len() {
            return &[];
        }
        &self.entries[start..]
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
}
