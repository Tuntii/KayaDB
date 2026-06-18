//! Disk persistence crash/restart property tests for [`DiskRaftStorage`].
//!
//! Run with: `cargo test -p kaya-raft --features disk-storage --test raft_persist_crash`

use kaya_raft::{
    default_hard_state, encode_hard_state, DiskRaftStorage, HardState, LogEntry, LogIndex,
    MemLog, NodeId, RaftStorage, RaftStorageError, Term,
};
use std::path::PathBuf;

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

fn assert_log_matches(loaded: &MemLog, expected: &MemLog) {
    assert_eq!(loaded.last_index(), expected.last_index());
    for i in 1..=expected.last_index().0 {
        let idx = LogIndex(i);
        assert_eq!(
            loaded.get(idx).unwrap().command,
            expected.get(idx).unwrap().command
        );
        assert_eq!(
            loaded.get(idx).unwrap().term,
            expected.get(idx).unwrap().term
        );
    }
}

#[test]
fn raft_persist_roundtrip() {
    let dir = temp_data_dir("raft_persist_roundtrip");
    let mut storage = DiskRaftStorage::open(&dir);

    let mut log = MemLog::new();
    log.append(LogEntry {
        term: Term(1),
        command: b"alpha".to_vec(),
    });
    log.append(LogEntry {
        term: Term(2),
        command: b"beta".to_vec(),
    });

    let hs = HardState {
        current_term: Term(2),
        voted_for: Some(NodeId(3)),
        last_included_index: LogIndex(0),
        last_included_term: Term(0),
    };

    storage.save_hard_state(&hs).unwrap();
    storage.save_log(&log, &hs).unwrap();
    storage.sync().unwrap();

    let loaded = storage.load().unwrap();
    assert_eq!(loaded.hard_state, hs);
    assert_log_matches(&loaded.log, &log);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn disk_storage_survives_truncated_hard_state() {
    let dir = temp_data_dir("truncated_hard_state");
    let mut storage = DiskRaftStorage::open(&dir);

    let hs = HardState {
        current_term: Term(5),
        voted_for: Some(NodeId(1)),
        last_included_index: LogIndex(0),
        last_included_term: Term(0),
    };
    storage.save_hard_state(&hs).unwrap();

    // Corrupt CRC: flip a payload byte without updating checksum.
    let path = dir.join("raft-hard-state");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[10] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let err = storage.load().unwrap_err();
    match err {
        RaftStorageError::Corrupt(msg) => assert!(msg.contains("crc mismatch")),
        other => panic!("expected Corrupt, got {other:?}"),
    }

    // Truncated file is also rejected.
    std::fs::write(&path, &bytes[..32]).unwrap();
    let err = storage.load().unwrap_err();
    match err {
        RaftStorageError::Corrupt(msg) => assert!(msg.contains("wrong len")),
        other => panic!("expected Corrupt, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn disk_storage_atomic_replace() {
    let dir = temp_data_dir("atomic_replace");
    let mut storage = DiskRaftStorage::open(&dir);

    let hs1 = HardState {
        current_term: Term(1),
        voted_for: Some(NodeId(1)),
        last_included_index: LogIndex(0),
        last_included_term: Term(0),
    };
    storage.save_hard_state(&hs1).unwrap();

    let hs2 = HardState {
        current_term: Term(7),
        voted_for: None,
        last_included_index: LogIndex(10),
        last_included_term: Term(6),
    };
    storage.save_hard_state(&hs2).unwrap();
    storage.sync().unwrap();

    let loaded = storage.load().unwrap();
    assert_eq!(loaded.hard_state, hs2);
    assert_ne!(loaded.hard_state, hs1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn disk_storage_log_rewrite_after_append() {
    let dir = temp_data_dir("log_rewrite");
    let mut storage = DiskRaftStorage::open(&dir);
    let hs = default_hard_state();

    let mut log = MemLog::new();
    log.append(LogEntry {
        term: Term(1),
        command: b"first".to_vec(),
    });
    storage.save_log(&log, &hs).unwrap();
    storage.sync().unwrap();

    let loaded = storage.load().unwrap();
    assert_log_matches(&loaded.log, &log);

    log.append(LogEntry {
        term: Term(1),
        command: b"second".to_vec(),
    });
    log.append(LogEntry {
        term: Term(2),
        command: b"third".to_vec(),
    });
    storage.save_log(&log, &hs).unwrap();
    storage.sync().unwrap();

    let loaded = storage.load().unwrap();
    assert_log_matches(&loaded.log, &log);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn raft_crash_mid_save() {
    let dir = temp_data_dir("crash_mid_save");
    let mut storage = DiskRaftStorage::open(&dir);

    let hs_good = HardState {
        current_term: Term(3),
        voted_for: Some(NodeId(2)),
        last_included_index: LogIndex(0),
        last_included_term: Term(0),
    };
    storage.save_hard_state(&hs_good).unwrap();

    // Simulate crash during the next atomic write: partial tmp, main file unchanged.
    let hs_pending = HardState {
        current_term: Term(99),
        voted_for: Some(NodeId(99)),
        last_included_index: LogIndex(0),
        last_included_term: Term(0),
    };
    let bytes = encode_hard_state(&hs_pending);
    let tmp_path = dir.join("raft-hard-state.tmp");
    std::fs::write(&tmp_path, &bytes[..32]).unwrap();

    let loaded = storage.load().unwrap();
    assert_eq!(loaded.hard_state, hs_good);

    let _ = std::fs::remove_dir_all(&dir);
}