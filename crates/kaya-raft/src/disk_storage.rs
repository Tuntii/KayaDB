use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::log::MemLog;
use crate::storage::{
    decode_hard_state, decode_log_file, default_hard_state, encode_hard_state, encode_log_file,
    frames_to_memlog, memlog_to_frames, HardState, PersistedRaftState, RaftStorage,
    RaftStorageError,
};


/// Directory for a Raft group's persistent state under `data_dir`.
///
/// - Group `0` keeps the legacy layout at `data_dir` root (`raft-hard-state`, `raft-log`)
///   for single-group backward compatibility.
/// - Non-zero groups use `data_dir/groups/{group_id}/`.
pub fn raft_group_dir(data_dir: impl AsRef<Path>, group_id: u64) -> PathBuf {
    let data_dir = data_dir.as_ref();
    if group_id == 0 {
        data_dir.to_path_buf()
    } else {
        data_dir.join("groups").join(group_id.to_string())
    }
}

/// On-disk Raft persistence under `data_dir/raft-hard-state` and `data_dir/raft-log`.
pub struct DiskRaftStorage {
    data_dir: PathBuf,
}

impl DiskRaftStorage {
    pub fn open(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    /// Open storage for a multi-raft group under the group-specific directory.
    ///
    /// Equivalent to `DiskRaftStorage::open(raft_group_dir(data_dir, group_id))`.
    pub fn open_group(data_dir: impl AsRef<Path>, group_id: u64) -> Self {
        Self::open(raft_group_dir(data_dir, group_id))
    }

    fn hard_state_path(&self) -> PathBuf {
        self.data_dir.join("raft-hard-state")
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.join("raft-log")
    }
}

impl RaftStorage for DiskRaftStorage {
    fn load(&self) -> Result<PersistedRaftState, RaftStorageError> {
        let hs_path = self.hard_state_path();
        let log_path = self.log_path();

        let hard_state = if hs_path.exists() {
            let bytes = std::fs::read(&hs_path).map_err(RaftStorageError::Io)?;
            decode_hard_state(&bytes).map_err(RaftStorageError::Corrupt)?
        } else {
            default_hard_state()
        };

        let log = if log_path.exists() {
            let bytes = std::fs::read(&log_path).map_err(RaftStorageError::Io)?;
            let frames = decode_log_file(&bytes).map_err(RaftStorageError::Corrupt)?;
            frames_to_memlog(&hard_state, frames)
        } else if hard_state.last_included_index.0 > 0 {
            frames_to_memlog(&hard_state, Vec::new())
        } else {
            MemLog::new()
        };

        Ok(PersistedRaftState { hard_state, log })
    }

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), RaftStorageError> {
        std::fs::create_dir_all(&self.data_dir).map_err(RaftStorageError::Io)?;
        let bytes = encode_hard_state(hs);
        atomic_write(&self.hard_state_path(), &bytes).map_err(RaftStorageError::Io)
    }

    fn save_log(&mut self, log: &MemLog, hs: &HardState) -> Result<(), RaftStorageError> {
        std::fs::create_dir_all(&self.data_dir).map_err(RaftStorageError::Io)?;
        let frames = memlog_to_frames(log);
        let bytes = encode_log_file(&frames);
        atomic_write(&self.log_path(), &bytes).map_err(RaftStorageError::Io)?;
        // Hard-state carries snapshot boundary metadata used when rebuilding the log.
        let _ = hs;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), RaftStorageError> {
        std::fs::create_dir_all(&self.data_dir).map_err(RaftStorageError::Io)?;
        if let Ok(dirf) = File::open(&self.data_dir) {
            let _ = dirf.sync_all();
        }
        Ok(())
    }
}

/// Write `bytes` to `path` atomically: tmp → fsync → rename → fsync(parent dir).
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dirf) = File::open(parent) {
            let _ = dirf.sync_all();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{LogEntry, MemLog};
    use crate::types::{LogIndex, NodeId, Term};

    fn temp_data_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kayadb_{name}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn raft_group_dir_legacy_group_zero_at_root() {
        let root = PathBuf::from("/tmp/kayadb-data");
        assert_eq!(raft_group_dir(&root, 0), root);
        assert_eq!(
            raft_group_dir(&root, 7),
            root.join("groups").join("7")
        );
    }

    #[test]
    fn open_group_uses_per_group_path() {
        let dir = temp_data_dir("open_group_paths");
        let mut storage = DiskRaftStorage::open_group(&dir, 3);
        let hs = HardState {
            current_term: Term(2),
            voted_for: Some(NodeId(1)),
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        };
        storage.save_hard_state(&hs).unwrap();
        let expected = dir.join("groups").join("3").join("raft-hard-state");
        assert!(expected.exists(), "expected {}", expected.display());
        // legacy root path must not be used for group 3
        assert!(!dir.join("raft-hard-state").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_storage_roundtrip() {
        let dir = temp_data_dir("disk_storage_roundtrip");
        let mut storage = DiskRaftStorage::open(&dir);

        let mut log = MemLog::new();
        log.append(LogEntry {
            term: Term(1),
            command: b"a".to_vec(),
        });
        log.append(LogEntry {
            term: Term(1),
            command: b"b".to_vec(),
        });

        let hs = HardState {
            current_term: Term(1),
            voted_for: Some(NodeId(1)),
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        };

        storage.save_hard_state(&hs).unwrap();
        storage.save_log(&log, &hs).unwrap();
        storage.sync().unwrap();

        let loaded = storage.load().unwrap();
        assert_eq!(loaded.hard_state, hs);
        assert_eq!(loaded.log.last_index(), log.last_index());
        for i in 1..=log.last_index().0 {
            let idx = LogIndex(i);
            assert_eq!(
                loaded.log.get(idx).unwrap().command,
                log.get(idx).unwrap().command
            );
            assert_eq!(
                loaded.log.get(idx).unwrap().term,
                log.get(idx).unwrap().term
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_storage_load_empty_when_missing() {
        let dir = temp_data_dir("disk_storage_load_empty");
        let storage = DiskRaftStorage::open(&dir);
        let loaded = storage.load().unwrap();
        assert_eq!(loaded.hard_state, default_hard_state());
        assert_eq!(loaded.log.last_index(), LogIndex(0));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
