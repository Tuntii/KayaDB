mod cluster_config;
mod command;
mod log;
mod message;
mod multi_raft;
mod node;
mod storage;
mod types;

#[cfg(feature = "disk-storage")]
mod disk_storage;

pub use cluster_config::{ClusterConfiguration, EffectiveConfig};
pub use command::{ClusterMember, ConfigChangePhase, RaftCommand};
#[cfg(feature = "disk-storage")]
pub use disk_storage::{raft_group_dir, DiskRaftStorage};
pub use log::{LogEntry, MemLog};
pub use message::{
    AppendRequest, AppendResponse, ConfigChangeRequest, ConfigChangeResponse, Envelope,
    InstallSnapshotRequest, InstallSnapshotResponse, Message, VoteRequest, VoteResponse,
};
pub use multi_raft::{
    multi_raft_group_dir, GroupId, MultiRaftHost, RangeDescriptor, RangeTable, StaticRange,
    StaticRangeTable,
};
pub use node::{RaftConfig, RaftNode, RaftStatus, Role};
pub use storage::{
    decode_hard_state, decode_log_file, default_hard_state, encode_hard_state, encode_log_file,
    frames_to_memlog, memlog_to_frames, HardState, LogFrame, PersistedRaftState, RaftStorage,
    RaftStorageError, RAFT_HARD_STATE_LEN, RAFT_LOG_FRAME_HEADER_LEN, RAFT_LOG_FRAME_MAGIC,
    RAFT_LOG_FRAME_VERSION,
};
pub use types::{LogIndex, NodeId, RaftApplyCommand, Term};

/// Build a combined snapshot payload (engine data + membership members).
///
/// Writes snapshot **version 2** (engine + members + optional range table).
/// Pass an empty `range_table` via [`build_snapshot_payload`]; use
/// [`build_snapshot_payload_v2`] to embed [`StaticRangeTable::encode`].
pub fn build_snapshot_payload(engine_data: &[u8], members: &[ClusterMember]) -> Vec<u8> {
    build_snapshot_payload_v2(engine_data, members, &[])
}

/// Snapshot v2: engine + membership + range-table bytes (`StaticRangeTable::encode`).
pub fn build_snapshot_payload_v2(
    engine_data: &[u8],
    members: &[ClusterMember],
    range_table: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&2u32.to_le_bytes()); // version 2
    buf.extend_from_slice(&(engine_data.len() as u32).to_le_bytes());
    buf.extend_from_slice(engine_data);
    buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for m in members {
        buf.extend_from_slice(&m.id.0.to_le_bytes());
        push_len_prefixed(&mut buf, m.raft_addr.as_bytes());
        push_len_prefixed(&mut buf, m.client_addr.as_bytes());
        buf.push(if m.is_learner { 1u8 } else { 0u8 });
    }
    buf.extend_from_slice(&(range_table.len() as u32).to_le_bytes());
    buf.extend_from_slice(range_table);
    buf
}

/// Parse combined snapshot payload. Returns (engine_data, members).
/// Falls back to treating data as pure engine for legacy payloads.
pub fn parse_snapshot_payload(data: &[u8]) -> Result<(Vec<u8>, Vec<ClusterMember>), String> {
    let (engine, members, _) = parse_snapshot_payload_v2(data)?;
    Ok((engine, members))
}

/// `(engine_data, members, range_table_bytes)` from a combined snapshot.
pub type CombinedSnapshot = (Vec<u8>, Vec<ClusterMember>, Vec<u8>);

/// Parse snapshot v1 or v2. Third tuple is range-table bytes (empty when absent).
pub fn parse_snapshot_payload_v2(data: &[u8]) -> Result<CombinedSnapshot, String> {
    if data.is_empty() {
        return Ok((vec![], vec![], vec![]));
    }
    if data.len() < 4 {
        return Ok((data.to_vec(), vec![], vec![]));
    }
    let ver = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let mut cur = &data[4..];
    if ver != 1 && ver != 2 {
        return Ok((data.to_vec(), vec![], vec![]));
    }
    if cur.len() < 4 {
        return Err("bad engine len".into());
    }
    let elen = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
    cur = &cur[4..];
    if cur.len() < elen {
        return Err("truncated engine".into());
    }
    let engine = cur[..elen].to_vec();
    cur = &cur[elen..];
    if cur.len() < 4 {
        return Ok((engine, vec![], vec![]));
    }
    let mcnt = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
    cur = &cur[4..];
    let (members, rest) = if let Ok((m, r)) = decode_snapshot_members(cur, mcnt, true) {
        (m, r)
    } else {
        decode_snapshot_members(cur, mcnt, false).unwrap_or((vec![], cur))
    };
    if ver == 1 {
        return Ok((engine, members, vec![]));
    }
    if rest.len() < 4 {
        return Ok((engine, members, vec![]));
    }
    let rlen = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    let rest = &rest[4..];
    if rest.len() < rlen {
        return Err("truncated range table in snapshot".into());
    }
    Ok((engine, members, rest[..rlen].to_vec()))
}

fn decode_snapshot_members(
    data: &[u8],
    mcnt: usize,
    with_learner_flag: bool,
) -> Result<(Vec<ClusterMember>, &[u8]), String> {
    let mut cur = data;
    let mut members = Vec::with_capacity(mcnt);
    for _ in 0..mcnt {
        if cur.len() < 8 {
            return Err("truncated member id".into());
        }
        let id = NodeId(u64::from_le_bytes([
            cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
        ]));
        cur = &cur[8..];
        let raft = take_len_prefixed(&mut cur)?;
        let client = take_len_prefixed(&mut cur)?;
        let is_learner = if with_learner_flag {
            if cur.is_empty() {
                return Err("missing learner flag".into());
            }
            let flag = cur[0];
            cur = &cur[1..];
            flag != 0
        } else {
            false
        };
        members.push(ClusterMember {
            id,
            raft_addr: raft,
            client_addr: client,
            is_learner,
        });
    }
    Ok((members, cur))
}

fn push_len_prefixed(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn take_len_prefixed(cur: &mut &[u8]) -> Result<String, String> {
    if cur.len() < 4 {
        return Err("bad len".into());
    }
    let l = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
    *cur = &cur[4..];
    if cur.len() < l {
        return Err("truncated".into());
    }
    let s = String::from_utf8(cur[..l].to_vec()).map_err(|e| e.to_string())?;
    *cur = &cur[l..];
    Ok(s)
}

#[cfg(test)]
mod snapshot_payload_tests {
    use super::*;

    #[test]
    fn snapshot_v2_round_trips_range_table() {
        let members = vec![ClusterMember::voter(NodeId(1), "r", "c")];
        let mut table = StaticRangeTable::single_group(GroupId::ZERO);
        table.split_at(b"m").unwrap();
        let snap = table.encode();
        let payload = build_snapshot_payload_v2(b"eng", &members, &snap);
        let (eng, mems, ranges) = parse_snapshot_payload_v2(&payload).unwrap();
        assert_eq!(eng, b"eng");
        assert_eq!(mems, members);
        let restored = StaticRangeTable::decode(&ranges).unwrap();
        assert_eq!(restored.meta_epoch(), table.meta_epoch());
        assert_eq!(restored.ranges().len(), 2);
        // 2-tuple parser still works.
        let (eng2, mems2) = parse_snapshot_payload(&payload).unwrap();
        assert_eq!(eng2, eng);
        assert_eq!(mems2, mems);
    }

    #[test]
    fn snapshot_v1_legacy_has_empty_range_table() {
        // Hand-build version 1 (no range section).
        let mut v1 = Vec::new();
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&3u32.to_le_bytes());
        v1.extend_from_slice(b"eng");
        v1.extend_from_slice(&0u32.to_le_bytes());
        let (eng, mems, ranges) = parse_snapshot_payload_v2(&v1).unwrap();
        assert_eq!(eng, b"eng");
        assert!(mems.is_empty());
        assert!(ranges.is_empty());
    }
}
