//! Durable range meta table: disk snapshot helpers (issue #25).
//!
//! Layout is committed as [`kaya_raft::RaftCommand::RangeMeta`] on group 0 and
//! mirrored to `{data_dir}/range-table.bin` so process restart restores the last
//! committed split/merge layout without replaying the full Raft log.
//!
//! Apply (CAS + host groups) lives in `cluster::replication` so this module
//! stays free of the multi-raft runtime.

use std::path::{Path, PathBuf};

use kaya_raft::StaticRangeTable;

/// Load a previously persisted range table from `data_dir/range-table.bin`.
///
/// Returns `None` when the file is missing or unreadable.
pub fn load_persisted_range_table(data_dir: &Path) -> Option<StaticRangeTable> {
    let path = range_table_path(data_dir);
    let bytes = std::fs::read(&path).ok()?;
    match StaticRangeTable::decode(&bytes) {
        Ok(t) => Some(t),
        Err(e) => {
            eprintln!(
                "warning: failed to decode range table at {}: {e}",
                path.display()
            );
            None
        }
    }
}

/// Persist the current range table to `data_dir/range-table.bin`.
pub fn persist_range_table(data_dir: &Path, table: &StaticRangeTable) -> Result<(), String> {
    let path = range_table_path(data_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let tmp = path.with_extension("bin.tmp");
    std::fs::write(&tmp, table.encode()).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
}

fn range_table_path(data_dir: &Path) -> PathBuf {
    data_dir.join("range-table.bin")
}

/// Decode helper for drain_and_apply (mirrors membership::decode_config_change).
pub fn decode_range_meta(command: &[u8]) -> Option<(u64, Vec<u8>)> {
    match kaya_raft::RaftCommand::decode(command) {
        Ok(kaya_raft::RaftCommand::RangeMeta {
            base_epoch,
            snapshot,
        }) => Some((base_epoch, snapshot)),
        _ => None,
    }
}

/// Build a RangeMeta command from a post-mutation table snapshot.
pub fn encode_range_meta_command(base_epoch: u64, table: &StaticRangeTable) -> Vec<u8> {
    kaya_raft::RaftCommand::RangeMeta {
        base_epoch,
        snapshot: table.encode(),
    }
    .encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_raft::GroupId;

    #[test]
    fn persist_and_load_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "kaya-range-meta-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut t = StaticRangeTable::single_group(GroupId::ZERO);
        t.split_at(b"m").unwrap();
        let epoch = t.meta_epoch();
        persist_range_table(&dir, &t).unwrap();

        let loaded = load_persisted_range_table(&dir).expect("file present");
        assert_eq!(loaded.meta_epoch(), epoch);
        assert_eq!(loaded.ranges().len(), 2);
        assert_eq!(loaded.lookup(b"a"), Some(GroupId::ZERO));
        assert_eq!(loaded.lookup(b"m"), Some(GroupId(1)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_returns_none() {
        let dir =
            std::env::temp_dir().join(format!("kaya-range-meta-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(load_persisted_range_table(&dir).is_none());
    }

    #[test]
    fn encode_range_meta_command_round_trips() {
        let t = StaticRangeTable::single_group(GroupId::ZERO);
        let cmd = encode_range_meta_command(1, &t);
        let (base, snap) = decode_range_meta(&cmd).unwrap();
        assert_eq!(base, 1);
        assert_eq!(StaticRangeTable::decode(&snap).unwrap().meta_epoch(), 1);
    }
}
