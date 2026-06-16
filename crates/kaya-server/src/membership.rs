//! Dynamic cluster membership: roster persistence and config-change sync.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;

use kaya_net::NodeRoster;
use kaya_raft::{ClusterMember, ConfigChangePhase, NodeId, RaftCommand};

pub type SharedRoster = Arc<RwLock<NodeRoster>>;

pub fn shared_roster(initial: NodeRoster) -> SharedRoster {
    Arc::new(RwLock::new(initial))
}

/// Load persisted roster entries from `data_dir/cluster-roster.json` if present.
pub fn load_persisted_roster(data_dir: &Path, roster: &mut NodeRoster) {
    let path = roster_path(data_dir);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(record) = serde_json_parse_line(line) {
            if let (Ok(raft), Ok(client)) = (
                record.raft_addr.parse::<SocketAddr>(),
                record.client_addr.parse::<SocketAddr>(),
            ) {
                roster.upsert(NodeId(record.id), raft, client);
            }
        }
    }
}

/// Persist the current roster to `data_dir/cluster-roster.json`.
pub fn persist_roster(data_dir: &Path, roster: &NodeRoster) -> Result<(), String> {
    let path = roster_path(data_dir);
    let mut lines = Vec::new();
    for (id, raft_addr) in roster.all_entries() {
        let client = roster.client_addr(id).unwrap_or(raft_addr).to_string();
        lines.push(format!(
            r#"{{"id":{},"raft_addr":"{raft_addr}","client_addr":"{client}"}}"#,
            id.0
        ));
    }
    std::fs::write(path, lines.join("\n")).map_err(|e| e.to_string())
}

fn roster_path(data_dir: &Path) -> PathBuf {
    data_dir.join("cluster-roster.json")
}

#[derive(Debug)]
struct RosterRecord {
    id: u64,
    raft_addr: String,
    client_addr: String,
}

fn serde_json_parse_line(line: &str) -> Result<RosterRecord, String> {
    // Minimal JSON parser for one-line roster records (no extra deps).
    let id = extract_json_u64(line, "id")?;
    let raft_addr = extract_json_str(line, "raft_addr")?;
    let client_addr = extract_json_str(line, "client_addr")?;
    Ok(RosterRecord {
        id,
        raft_addr,
        client_addr,
    })
}

fn extract_json_u64(json: &str, key: &str) -> Result<u64, String> {
    let needle = format!("\"{key}\":");
    let start = json.find(&needle).ok_or_else(|| format!("missing {key}"))? + needle.len();
    let rest = json[start..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().map_err(|e| format!("{key}: {e}"))
}

fn extract_json_str(json: &str, key: &str) -> Result<String, String> {
    let needle = format!("\"{key}\":\"");
    let start = json.find(&needle).ok_or_else(|| format!("missing {key}"))? + needle.len();
    let rest = &json[start..];
    let end = rest
        .find('"')
        .ok_or_else(|| format!("unterminated {key}"))?;
    Ok(rest[..end].to_owned())
}

/// Apply a replicated config-change entry to the hot roster.
pub async fn apply_config_change_to_roster(
    data_dir: &Path,
    roster: &SharedRoster,
    phase: ConfigChangePhase,
    members: &[ClusterMember],
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    let tuples: Vec<(NodeId, String, String)> = members
        .iter()
        .map(|m| (m.id, m.raft_addr.clone(), m.client_addr.clone()))
        .collect();

    let mut guard = roster.write().await;
    guard.merge_member_addresses(&tuples);
    guard.upsert(self_id, self_raft, self_client);

    if phase == ConfigChangePhase::Final {
        let has_addrs = members
            .iter()
            .all(|m| !m.raft_addr.is_empty() && !m.client_addr.is_empty());
        if has_addrs {
            if let Ok(()) = guard.replace_from_members(&tuples) {
                guard.upsert(self_id, self_raft, self_client);
            }
        } else {
            let voter_ids: BTreeSet<_> = members.iter().map(|m| m.id).collect();
            guard.retain_voters(&voter_ids, self_id, self_raft, self_client);
        }
    }

    if let Err(e) = persist_roster(data_dir, &guard) {
        eprintln!("warning: failed to persist cluster roster: {e}");
    }
}

/// Build the next member set when adding `new_member` to the cluster.
pub fn members_for_add(
    roster: &NodeRoster,
    current_voters: &[NodeId],
    new_member: ClusterMember,
    self_entry: ClusterMember,
) -> Vec<ClusterMember> {
    let mut by_id: std::collections::BTreeMap<NodeId, ClusterMember> =
        std::collections::BTreeMap::new();

    for &id in current_voters {
        if id == self_entry.id {
            by_id.insert(id, self_entry.clone());
        } else if let (Some(raft), Some(client)) = (roster.addr(id), roster.client_addr(id)) {
            by_id.insert(
                id,
                ClusterMember {
                    id,
                    raft_addr: raft.to_string(),
                    client_addr: client.to_string(),
                },
            );
        }
    }
    by_id.insert(new_member.id, new_member);
    by_id.into_values().collect()
}

/// Build the next member set when removing `remove_id` from the cluster.
pub fn members_for_remove(
    roster: &NodeRoster,
    current_voters: &[NodeId],
    remove_id: NodeId,
    self_entry: ClusterMember,
) -> Option<Vec<ClusterMember>> {
    if current_voters.len() <= 2 || !current_voters.contains(&remove_id) {
        return None;
    }
    if remove_id == self_entry.id {
        return None;
    }

    let mut by_id: std::collections::BTreeMap<NodeId, ClusterMember> =
        std::collections::BTreeMap::new();
    for &id in current_voters {
        if id == remove_id {
            continue;
        }
        if id == self_entry.id {
            by_id.insert(id, self_entry.clone());
        } else if let (Some(raft), Some(client)) = (roster.addr(id), roster.client_addr(id)) {
            by_id.insert(
                id,
                ClusterMember {
                    id,
                    raft_addr: raft.to_string(),
                    client_addr: client.to_string(),
                },
            );
        }
    }
    if by_id.len() < 2 {
        return None;
    }
    Some(by_id.into_values().collect())
}

/// Decode config change from a drained applied command payload.
pub fn decode_config_change(command: &[u8]) -> Option<(ConfigChangePhase, Vec<ClusterMember>)> {
    match RaftCommand::decode(command) {
        Ok(RaftCommand::ConfigChange { phase, members }) => Some((phase, members)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn persist_and_load_round_trip() {
        let dir = std::env::temp_dir().join(format!("kaya-roster-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut roster = NodeRoster::new_with_client([(NodeId(1), addr(7481), addr(7379))]);
        persist_roster(&dir, &roster).unwrap();

        roster = NodeRoster::new_with_client([]);
        load_persisted_roster(&dir, &mut roster);
        assert_eq!(roster.addr(NodeId(1)), Some(addr(7481)));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
