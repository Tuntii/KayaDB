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

/// Resolve a member entry, preferring known membership metadata then roster addrs.
fn resolve_member(
    id: NodeId,
    known: Option<&ClusterMember>,
    roster: &NodeRoster,
    self_entry: &ClusterMember,
) -> Option<ClusterMember> {
    if id == self_entry.id {
        return Some(self_entry.clone());
    }
    if let Some(m) = known {
        let mut m = m.clone();
        // Fill empty addresses from the live roster when available.
        if m.raft_addr.is_empty() {
            if let Some(raft) = roster.addr(id) {
                m.raft_addr = raft.to_string();
            }
        }
        if m.client_addr.is_empty() {
            if let Some(client) = roster.client_addr(id) {
                m.client_addr = client.to_string();
            }
        }
        return Some(m);
    }
    if let (Some(raft), Some(client)) = (roster.addr(id), roster.client_addr(id)) {
        return Some(ClusterMember {
            id,
            raft_addr: raft.to_string(),
            client_addr: client.to_string(),
            is_learner: false,
        });
    }
    None
}

/// Build the next member set when adding `new_member` to the cluster.
///
/// `current_members` should be the full membership (voters + learners). When
/// empty, falls back to treating `current_voters` as voters-only.
pub fn members_for_add(
    roster: &NodeRoster,
    current_voters: &[NodeId],
    current_members: &[ClusterMember],
    new_member: ClusterMember,
    self_entry: ClusterMember,
) -> Vec<ClusterMember> {
    let mut by_id: std::collections::BTreeMap<NodeId, ClusterMember> =
        std::collections::BTreeMap::new();

    if current_members.is_empty() {
        for &id in current_voters {
            if let Some(m) = resolve_member(id, None, roster, &self_entry) {
                by_id.insert(id, m);
            }
        }
    } else {
        for m in current_members {
            if let Some(resolved) = resolve_member(m.id, Some(m), roster, &self_entry) {
                by_id.insert(m.id, resolved);
            }
        }
    }
    by_id.insert(new_member.id, new_member);
    by_id.into_values().collect()
}

/// Build the next member set when removing `remove_id` from the cluster.
///
/// Voters only are subject to the "must keep >= 2 voters" rule; removing a
/// learner is always allowed when the node is present.
pub fn members_for_remove(
    roster: &NodeRoster,
    current_voters: &[NodeId],
    current_members: &[ClusterMember],
    remove_id: NodeId,
    self_entry: ClusterMember,
) -> Option<Vec<ClusterMember>> {
    if remove_id == self_entry.id {
        return None;
    }

    let is_learner_removal = current_members
        .iter()
        .find(|m| m.id == remove_id)
        .map(|m| m.is_learner)
        .unwrap_or(false);

    if !is_learner_removal {
        if current_voters.len() <= 2 || !current_voters.contains(&remove_id) {
            return None;
        }
    } else {
        // Learner must exist in membership.
        if !current_members.iter().any(|m| m.id == remove_id) {
            return None;
        }
    }

    let mut by_id: std::collections::BTreeMap<NodeId, ClusterMember> =
        std::collections::BTreeMap::new();

    let source: Vec<ClusterMember> = if current_members.is_empty() {
        current_voters
            .iter()
            .filter_map(|&id| resolve_member(id, None, roster, &self_entry))
            .collect()
    } else {
        current_members.to_vec()
    };

    for m in source {
        if m.id == remove_id {
            continue;
        }
        if let Some(resolved) = resolve_member(m.id, Some(&m), roster, &self_entry) {
            by_id.insert(m.id, resolved);
        }
    }

    // Min-voter floor applies only when removing a voter; learner removal is
    // always allowed when the node was present (checked above).
    if !is_learner_removal {
        let remaining_voters = by_id.values().filter(|m| !m.is_learner).count();
        if remaining_voters < 2 {
            return None;
        }
    }
    Some(by_id.into_values().collect())
}

/// Build the next member set promoting `promote_id` from learner to voter.
pub fn members_for_promote(
    roster: &NodeRoster,
    current_members: &[ClusterMember],
    promote_id: NodeId,
    self_entry: ClusterMember,
) -> Option<Vec<ClusterMember>> {
    let target = current_members.iter().find(|m| m.id == promote_id)?;
    if !target.is_learner {
        return None;
    }

    let mut out = Vec::with_capacity(current_members.len());
    for m in current_members {
        let mut resolved = resolve_member(m.id, Some(m), roster, &self_entry)?;
        if resolved.id == promote_id {
            resolved.is_learner = false;
        }
        out.push(resolved);
    }
    Some(out)
}

/// Decode config change from a drained applied command payload.
pub fn decode_config_change(command: &[u8]) -> Option<(ConfigChangePhase, Vec<ClusterMember>)> {
    match RaftCommand::decode(command) {
        Ok(RaftCommand::ConfigChange { phase, members }) => Some((phase, members)),
        _ => None,
    }
}

// Re-export the payload helpers from kaya-raft for server use (avoids duplication).
pub use kaya_raft::{
    build_snapshot_payload as build_raft_snapshot_payload,
    parse_snapshot_payload as parse_raft_snapshot_payload,
};

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

    #[test]
    fn members_for_promote_flips_learner_flag() {
        let roster = NodeRoster::new_with_client([
            (NodeId(1), addr(7481), addr(7379)),
            (NodeId(2), addr(7482), addr(7380)),
            (NodeId(3), addr(7483), addr(7381)),
        ]);
        let current = vec![
            ClusterMember::voter(NodeId(1), "127.0.0.1:7481", "127.0.0.1:7379"),
            ClusterMember::voter(NodeId(2), "127.0.0.1:7482", "127.0.0.1:7380"),
            ClusterMember::learner(NodeId(3), "127.0.0.1:7483", "127.0.0.1:7381"),
        ];
        let self_entry = current[0].clone();
        let promoted = members_for_promote(&roster, &current, NodeId(3), self_entry).unwrap();
        assert_eq!(promoted.len(), 3);
        let p3 = promoted.iter().find(|m| m.id == NodeId(3)).unwrap();
        assert!(!p3.is_learner);
        assert_eq!(
            ClusterMember::voter_ids(&promoted),
            vec![NodeId(1), NodeId(2), NodeId(3)]
        );
        // Promoting a non-learner fails.
        assert!(members_for_promote(&roster, &promoted, NodeId(3), current[0].clone()).is_none());
    }

    #[test]
    fn members_for_add_preserves_existing_learners() {
        let roster = NodeRoster::new_with_client([
            (NodeId(1), addr(7481), addr(7379)),
            (NodeId(2), addr(7482), addr(7380)),
            (NodeId(3), addr(7483), addr(7381)),
        ]);
        let current = vec![
            ClusterMember::voter(NodeId(1), "127.0.0.1:7481", "127.0.0.1:7379"),
            ClusterMember::learner(NodeId(3), "127.0.0.1:7483", "127.0.0.1:7381"),
        ];
        let self_entry = current[0].clone();
        let next = members_for_add(
            &roster,
            &[NodeId(1)],
            &current,
            ClusterMember::voter(NodeId(2), "127.0.0.1:7482", "127.0.0.1:7380"),
            self_entry,
        );
        assert!(next.iter().any(|m| m.id == NodeId(3) && m.is_learner));
        assert!(next.iter().any(|m| m.id == NodeId(2) && !m.is_learner));
    }

    #[test]
    fn members_for_remove_allows_learner_even_with_single_voter() {
        let roster = NodeRoster::new_with_client([
            (NodeId(1), addr(7481), addr(7379)),
            (NodeId(3), addr(7483), addr(7381)),
        ]);
        let current = vec![
            ClusterMember::voter(NodeId(1), "127.0.0.1:7481", "127.0.0.1:7379"),
            ClusterMember::learner(NodeId(3), "127.0.0.1:7483", "127.0.0.1:7381"),
        ];
        let self_entry = current[0].clone();
        let next = members_for_remove(&roster, &[NodeId(1)], &current, NodeId(3), self_entry)
            .expect("removing a learner must succeed even with one voter left");
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].id, NodeId(1));
        assert!(!next[0].is_learner);
    }

    #[test]
    fn members_for_remove_blocks_voter_when_floor_would_break() {
        let roster = NodeRoster::new_with_client([
            (NodeId(1), addr(7481), addr(7379)),
            (NodeId(2), addr(7482), addr(7380)),
            (NodeId(3), addr(7483), addr(7381)),
        ]);
        let current = vec![
            ClusterMember::voter(NodeId(1), "127.0.0.1:7481", "127.0.0.1:7379"),
            ClusterMember::voter(NodeId(2), "127.0.0.1:7482", "127.0.0.1:7380"),
            ClusterMember::learner(NodeId(3), "127.0.0.1:7483", "127.0.0.1:7381"),
        ];
        let self_entry = current[0].clone();
        // Two voters: removing a voter would leave < 2.
        assert!(members_for_remove(
            &roster,
            &[NodeId(1), NodeId(2)],
            &current,
            NodeId(2),
            self_entry.clone(),
        )
        .is_none());
        // Learner still removable.
        assert!(members_for_remove(
            &roster,
            &[NodeId(1), NodeId(2)],
            &current,
            NodeId(3),
            self_entry,
        )
        .is_some());
    }
}
