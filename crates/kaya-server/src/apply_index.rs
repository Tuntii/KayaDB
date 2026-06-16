//! Append-only persistence for Raft log index ↔ engine LSN correlation.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use kaya_raft::{LogIndex, RaftApplyCommand};

/// Append-only WAL↔Raft correlation log stored at `data_dir/raft-apply-index.jsonl`.
pub struct RaftApplyIndex {
    path: PathBuf,
    writer: File,
    seen_indices: HashSet<u64>,
}

impl RaftApplyIndex {
    /// Open (or create) the correlation index under `data_dir`.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let path = data_dir.join("raft-apply-index.jsonl");
        let mut seen_indices = HashSet::new();
        if path.exists() {
            let contents = std::fs::read_to_string(&path)?;
            for line in contents.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(record) = RaftApplyCommand::from_jsonl(line) {
                    seen_indices.insert(record.index.0);
                }
            }
        }
        let writer = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            writer,
            seen_indices,
        })
    }

    /// Append a correlation record. Duplicate indices are ignored (idempotent).
    pub fn append(&mut self, record: &RaftApplyCommand) -> io::Result<()> {
        if !self.seen_indices.insert(record.index.0) {
            return Ok(());
        }
        self.writer.write_all(record.to_jsonl().as_bytes())?;
        self.writer.sync_data()?;
        Ok(())
    }

    /// Look up the persisted record for a Raft log index.
    pub fn lookup(&self, index: LogIndex) -> Option<RaftApplyCommand> {
        Self::load_all(&self.path)
            .ok()?
            .into_iter()
            .find(|r| r.index == index)
    }

    /// Load every persisted correlation record (for tests and recovery tooling).
    pub fn load_all(path: &Path) -> io::Result<Vec<RaftApplyCommand>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for line in std::fs::read_to_string(path)?.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = RaftApplyCommand::from_jsonl(line) {
                out.push(record);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_core::Lsn;
    use kaya_raft::Term;

    #[test]
    fn append_and_reload_correlation() {
        let dir = std::env::temp_dir().join(format!(
            "kayadb_apply_index_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut index = RaftApplyIndex::open(&dir).unwrap();
        index
            .append(&RaftApplyCommand {
                term: Term(1),
                index: LogIndex(5),
                engine_lsn_hint: Some(Lsn::new(100)),
            })
            .unwrap();
        index
            .append(&RaftApplyCommand {
                term: Term(1),
                index: LogIndex(6),
                engine_lsn_hint: None,
            })
            .unwrap();
        // Duplicate append is a no-op.
        index
            .append(&RaftApplyCommand {
                term: Term(1),
                index: LogIndex(5),
                engine_lsn_hint: Some(Lsn::new(999)),
            })
            .unwrap();

        let path = dir.join("raft-apply-index.jsonl");
        let records = RaftApplyIndex::load_all(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].index, LogIndex(5));
        assert_eq!(records[0].engine_lsn_hint, Some(Lsn::new(100)));
        assert_eq!(records[1].index, LogIndex(6));
        assert!(records[1].engine_lsn_hint.is_none());

        let reopened = RaftApplyIndex::open(&dir).unwrap();
        assert_eq!(
            reopened.lookup(LogIndex(5)).unwrap().engine_lsn_hint,
            Some(Lsn::new(100))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
