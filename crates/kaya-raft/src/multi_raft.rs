//! Multi-raft host and static range table foundation (M20).
//!
//! One process may host N independent [`RaftNode`]s (one per [`GroupId`]).
//! Transport multiplexing uses [`crate::Envelope::group_id`]; this module
//! stamps outgoing envelopes and demuxes by group.
//!
//! **Production note:** `kaya-server::ClusterNode` always hosts a
//! [`MultiRaftHost`] with at least group 0. Multi-group static ranges are
//! configured via `ClusterConfig::with_static_ranges`. Dynamic splits and
//! per-range Jepsen remain follow-on work.
//!
//! **Tracing (v1 stub):** when OTel is enabled at the server layer, attach a
//! `kaya.raft.group_id` attribute on spans that touch multi-raft propose/handle
//! paths so node↔node↔client correlation can demux by group. Full
//! W3C trace-context propagation remains M20/M24 follow-on.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::message::Envelope;
use crate::node::{RaftConfig, RaftNode};
use crate::types::LogIndex;

/// Identifier of a Raft group (range) within a multi-raft process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GroupId(pub u64);

impl GroupId {
    /// Legacy single-group id used by ClusterNode today.
    pub const ZERO: GroupId = GroupId(0);
}

/// One static key-range assignment: `[start_key, end_key)` → group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRange {
    /// Inclusive start key.
    pub start_key: Vec<u8>,
    /// Exclusive end key. Empty means "unbounded upper" (all keys ≥ start).
    pub end_key: Vec<u8>,
    pub group_id: GroupId,
}

/// Static range table: ordered list of non-overlapping ranges.
///
/// Lookup is linear scan for the foundation (N is expected small until M21).
#[derive(Debug, Clone, Default)]
pub struct StaticRangeTable {
    ranges: Vec<StaticRange>,
}

impl StaticRangeTable {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Build from a list of ranges. Ranges are sorted by `start_key`.
    pub fn from_ranges(mut ranges: Vec<StaticRange>) -> Self {
        ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        Self { ranges }
    }

    /// Single-range whole keyspace → `group_id`.
    pub fn single_group(group_id: GroupId) -> Self {
        Self::from_ranges(vec![StaticRange {
            start_key: vec![],
            end_key: vec![],
            group_id,
        }])
    }

    pub fn ranges(&self) -> &[StaticRange] {
        &self.ranges
    }

    /// Look up the group for `key`. Returns `None` if no range covers the key.
    pub fn lookup(&self, key: &[u8]) -> Option<GroupId> {
        for r in &self.ranges {
            if key < r.start_key.as_slice() {
                continue;
            }
            // empty end_key ⇒ unbounded upper
            if r.end_key.is_empty() || key < r.end_key.as_slice() {
                return Some(r.group_id);
            }
        }
        None
    }
}

/// Hosts multiple independent Raft nodes in one process.
///
/// Ticks are coalesced: a single `tick_all` advances every group once and
/// returns stamped envelopes. Proposals and message handling are per-group.
pub struct MultiRaftHost {
    groups: HashMap<GroupId, RaftNode>,
}

impl MultiRaftHost {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Insert (or replace) a group with an already-constructed node.
    pub fn insert(&mut self, group_id: GroupId, node: RaftNode) {
        self.groups.insert(group_id, node);
    }

    /// Convenience: create a single-node Raft group (self-electing after enough ticks).
    pub fn insert_single_node(&mut self, group_id: GroupId, node_id: crate::types::NodeId) {
        let cfg = RaftConfig {
            id: node_id,
            peers: vec![],
            election_timeout_ticks: 1,
            heartbeat_interval_ticks: 1,
        };
        self.insert(group_id, RaftNode::new(cfg));
    }

    pub fn group_ids(&self) -> impl Iterator<Item = GroupId> + '_ {
        self.groups.keys().copied()
    }

    pub fn get(&self, group_id: GroupId) -> Option<&RaftNode> {
        self.groups.get(&group_id)
    }

    pub fn get_mut(&mut self, group_id: GroupId) -> Option<&mut RaftNode> {
        self.groups.get_mut(&group_id)
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Tick every group once; stamp `group_id` on all outgoing envelopes.
    pub fn tick_all(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        // Stable order for determinism in tests.
        let mut ids: Vec<GroupId> = self.groups.keys().copied().collect();
        ids.sort();
        for gid in ids {
            let Some(node) = self.groups.get_mut(&gid) else {
                continue;
            };
            for mut env in node.tick() {
                env.group_id = gid.0;
                out.push(env);
            }
        }
        out
    }

    /// Propose `cmd` on `group_id`. Returns the log index if this node is leader.
    pub fn propose(&mut self, group_id: GroupId, cmd: Vec<u8>) -> Option<LogIndex> {
        self.groups.get_mut(&group_id)?.propose(cmd)
    }

    /// Deliver an envelope to the group identified by `env.group_id` (or override).
    ///
    /// Outgoing replies are stamped with the same group id.
    pub fn handle(&mut self, env: Envelope) -> Vec<Envelope> {
        let gid = GroupId(env.group_id);
        let Some(node) = self.groups.get_mut(&gid) else {
            return Vec::new();
        };
        node.handle(env)
            .into_iter()
            .map(|mut e| {
                e.group_id = gid.0;
                e
            })
            .collect()
    }

    /// Handle with an explicit group id (overrides `env.group_id` for routing).
    pub fn handle_group(&mut self, group_id: GroupId, mut env: Envelope) -> Vec<Envelope> {
        env.group_id = group_id.0;
        self.handle(env)
    }

    /// Unique group ids currently hosted, sorted.
    pub fn sorted_group_ids(&self) -> Vec<GroupId> {
        let mut ids: Vec<GroupId> = self.groups.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Whether the local node is leader of `group_id`.
    pub fn is_leader_of(&self, group_id: GroupId) -> bool {
        self.groups
            .get(&group_id)
            .map(|n| n.is_leader())
            .unwrap_or(false)
    }

    /// True if this process is leader of any hosted group.
    pub fn is_leader_any(&self) -> bool {
        self.groups.values().any(|n| n.is_leader())
    }

    /// Status of group 0 when present (legacy single-group metrics/health).
    pub fn primary_status(&self) -> Option<crate::node::RaftStatus> {
        self.groups.get(&GroupId::ZERO).map(|n| n.status())
    }

    /// Status of an arbitrary group.
    pub fn status_of(&self, group_id: GroupId) -> Option<crate::node::RaftStatus> {
        self.groups.get(&group_id).map(|n| n.status())
    }

    /// Propose on `group_id`.
    pub fn propose_group(&mut self, group_id: GroupId, cmd: Vec<u8>) -> Option<LogIndex> {
        self.propose(group_id, cmd)
    }

    /// Read-index on `group_id`.
    pub fn propose_read_group(&mut self, group_id: GroupId, request_id: u64) -> Option<LogIndex> {
        self.groups.get_mut(&group_id)?.propose_read(request_id)
    }

    /// Broadcast AppendEntries for one group (stamps group_id).
    pub fn broadcast_group(&mut self, group_id: GroupId) -> Vec<Envelope> {
        let Some(node) = self.groups.get_mut(&group_id) else {
            return Vec::new();
        };
        node.broadcast()
            .into_iter()
            .map(|mut e| {
                e.group_id = group_id.0;
                e
            })
            .collect()
    }

    /// Drain applied entries from every group as `(group_id, index, term, cmd)`.
    pub fn drain_all_applied(&mut self) -> Vec<(GroupId, LogIndex, crate::types::Term, Vec<u8>)> {
        let mut out = Vec::new();
        for gid in self.sorted_group_ids() {
            let Some(node) = self.groups.get_mut(&gid) else {
                continue;
            };
            for (idx, term, cmd) in node.drain_applied() {
                out.push((gid, idx, term, cmd));
            }
        }
        out
    }

    /// Drain ready read-index ids from every group as `(group_id, request_id)`.
    pub fn drain_all_ready_reads(&mut self) -> Vec<(GroupId, u64)> {
        let mut out = Vec::new();
        for gid in self.sorted_group_ids() {
            let Some(node) = self.groups.get_mut(&gid) else {
                continue;
            };
            for req_id in node.drain_ready_reads() {
                out.push((gid, req_id));
            }
        }
        out
    }

    /// Persist views for every group via the provided callback.
    pub fn for_each_persist_view(&self, mut f: impl FnMut(GroupId, crate::storage::PersistedRaftState)) {
        for gid in self.sorted_group_ids() {
            if let Some(node) = self.groups.get(&gid) {
                f(gid, node.persist_view());
            }
        }
    }

    /// True if `from` is a voter in any group's effective config.
    pub fn is_voter_anywhere(&self, from: crate::types::NodeId) -> bool {
        self.groups
            .values()
            .any(|n| n.effective_config().all_voters().contains(&from))
    }
}

impl Default for MultiRaftHost {
    fn default() -> Self {
        Self::new()
    }
}

/// Re-export path helper for callers that depend on multi-raft layout without
/// enabling disk-storage feature (always available as pure path logic).
pub fn multi_raft_group_dir(data_dir: impl AsRef<Path>, group_id: GroupId) -> PathBuf {
    let data_dir = data_dir.as_ref();
    if group_id.0 == 0 {
        data_dir.to_path_buf()
    } else {
        data_dir.join("groups").join(group_id.0.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;
    use crate::Role;

    fn elect_single(host: &mut MultiRaftHost, gid: GroupId) {
        // election_timeout_ticks = 1 → one tick starts election; single node wins.
        for _ in 0..5 {
            let _ = host.tick_all();
            if host.get(gid).map(|n| n.status().role == Role::Leader) == Some(true) {
                return;
            }
        }
        panic!("group {} failed to elect leader", gid.0);
    }

    #[test]
    fn static_range_lookup() {
        let table = StaticRangeTable::from_ranges(vec![
            StaticRange {
                start_key: b"a".to_vec(),
                end_key: b"m".to_vec(),
                group_id: GroupId(1),
            },
            StaticRange {
                start_key: b"m".to_vec(),
                end_key: b"z".to_vec(),
                group_id: GroupId(2),
            },
        ]);
        assert_eq!(table.lookup(b"a"), Some(GroupId(1)));
        assert_eq!(table.lookup(b"hello"), Some(GroupId(1)));
        assert_eq!(table.lookup(b"m"), Some(GroupId(2)));
        assert_eq!(table.lookup(b"xyz"), Some(GroupId(2)));
        assert_eq!(table.lookup(b"z"), None);
        assert_eq!(table.lookup(b"0"), None);
    }

    #[test]
    fn single_group_table_covers_all_keys() {
        let table = StaticRangeTable::single_group(GroupId::ZERO);
        assert_eq!(table.lookup(b""), Some(GroupId::ZERO));
        assert_eq!(table.lookup(b"anything"), Some(GroupId::ZERO));
    }

    #[test]
    fn two_groups_propose_and_apply_independently() {
        let mut host = MultiRaftHost::new();
        let g1 = GroupId(1);
        let g2 = GroupId(2);
        host.insert_single_node(g1, NodeId(1));
        host.insert_single_node(g2, NodeId(1));

        elect_single(&mut host, g1);
        elect_single(&mut host, g2);

        let idx1 = host.propose(g1, b"cmd-g1".to_vec()).expect("g1 leader");
        let idx2 = host.propose(g2, b"cmd-g2".to_vec()).expect("g2 leader");
        assert!(idx1.0 >= 1);
        assert!(idx2.0 >= 1);

        // Drive apply (1-node commits immediately on propose, but tick is harmless).
        let _ = host.tick_all();

        let n1 = host.get(g1).unwrap();
        let n2 = host.get(g2).unwrap();

        let applied1: Vec<&[u8]> = n1
            .applied_entries
            .iter()
            .map(|(_, _, c)| c.as_slice())
            .collect();
        let applied2: Vec<&[u8]> = n2
            .applied_entries
            .iter()
            .map(|(_, _, c)| c.as_slice())
            .collect();

        assert!(
            applied1.iter().any(|c| *c == b"cmd-g1".as_slice()),
            "group1 applied: {applied1:?}"
        );
        assert!(
            !applied1.iter().any(|c| *c == b"cmd-g2".as_slice()),
            "group1 must not see group2 cmd"
        );
        assert!(
            applied2.iter().any(|c| *c == b"cmd-g2".as_slice()),
            "group2 applied: {applied2:?}"
        );
        assert!(
            !applied2.iter().any(|c| *c == b"cmd-g1".as_slice()),
            "group2 must not see group1 cmd"
        );
    }

    #[test]
    fn tick_all_stamps_group_id_on_envelopes() {
        // Two-node configs so ticks produce vote requests with group stamps.
        let mut host = MultiRaftHost::new();
        let g1 = GroupId(10);
        let g2 = GroupId(20);
        let cfg = |id| RaftConfig {
            id: NodeId(id),
            peers: vec![NodeId(id + 1)],
            election_timeout_ticks: 1,
            heartbeat_interval_ticks: 1,
        };
        host.insert(g1, RaftNode::new(cfg(1)));
        host.insert(g2, RaftNode::new(cfg(1)));

        let mut saw_g1 = false;
        let mut saw_g2 = false;
        for _ in 0..5 {
            for env in host.tick_all() {
                if env.group_id == 10 {
                    saw_g1 = true;
                }
                if env.group_id == 20 {
                    saw_g2 = true;
                }
            }
        }
        assert!(saw_g1, "expected envelopes for group 10");
        assert!(saw_g2, "expected envelopes for group 20");
    }

    #[test]
    fn multi_raft_group_dir_matches_layout() {
        let root = PathBuf::from("/data");
        assert_eq!(multi_raft_group_dir(&root, GroupId(0)), root);
        assert_eq!(
            multi_raft_group_dir(&root, GroupId(5)),
            root.join("groups").join("5")
        );
    }
}
