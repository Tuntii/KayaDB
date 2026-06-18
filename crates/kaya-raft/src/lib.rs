mod cluster_config;
mod command;
mod log;
mod message;
mod node;
mod types;

pub use cluster_config::{ClusterConfiguration, EffectiveConfig};
pub use command::{ClusterMember, ConfigChangePhase, RaftCommand};
pub use log::{LogEntry, MemLog};
pub use message::{
    AppendRequest, AppendResponse, ConfigChangeRequest, ConfigChangeResponse, Envelope,
    InstallSnapshotRequest, InstallSnapshotResponse, Message, VoteRequest, VoteResponse,
};
pub use node::{RaftConfig, RaftNode, RaftStatus, Role};
pub use types::{LogIndex, NodeId, RaftApplyCommand, Term};

/// Build a combined snapshot payload (engine data + membership members).
/// Used by higher layers (server, sim) so that InstallSnapshot carries
/// the membership configuration active at the snapshot index.
pub fn build_snapshot_payload(engine_data: &[u8], members: &[ClusterMember]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u32.to_le_bytes()); // version
    buf.extend_from_slice(&(engine_data.len() as u32).to_le_bytes());
    buf.extend_from_slice(engine_data);
    buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for m in members {
        buf.extend_from_slice(&m.id.0.to_le_bytes());
        push_len_prefixed(&mut buf, m.raft_addr.as_bytes());
        push_len_prefixed(&mut buf, m.client_addr.as_bytes());
    }
    buf
}

/// Parse combined snapshot payload. Returns (engine_data, members).
/// Falls back to treating data as pure engine for legacy payloads.
pub fn parse_snapshot_payload(data: &[u8]) -> Result<(Vec<u8>, Vec<ClusterMember>), String> {
    if data.is_empty() {
        return Ok((vec![], vec![]));
    }
    if data.len() < 4 {
        return Ok((data.to_vec(), vec![]));
    }
    let ver = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let mut cur = &data[4..];
    if ver != 1 {
        return Ok((data.to_vec(), vec![]));
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
        return Ok((engine, vec![]));
    }
    let mcnt = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
    cur = &cur[4..];
    let mut members = Vec::with_capacity(mcnt);
    for _ in 0..mcnt {
        if cur.len() < 8 {
            break;
        }
        let id = NodeId(u64::from_le_bytes([
            cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
        ]));
        cur = &cur[8..];
        let raft = take_len_prefixed(&mut cur).unwrap_or_default();
        let client = take_len_prefixed(&mut cur).unwrap_or_default();
        members.push(ClusterMember {
            id,
            raft_addr: raft,
            client_addr: client,
        });
    }
    Ok((engine, members))
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
