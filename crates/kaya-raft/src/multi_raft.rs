//! Multi-raft host and static range table foundation (M20).
//!
//! One process may host N independent [`RaftNode`]s (one per [`GroupId`]).
//! Transport multiplexing uses [`crate::Envelope::group_id`]; this module
//! stamps outgoing envelopes and demuxes by group.
//!
//! **Production note:** `kaya-server::ClusterNode` always hosts a
//! [`MultiRaftHost`] with at least group 0. Multi-group static ranges are
//! configured via `ClusterConfig::with_static_ranges`. Dynamic splits/merges
//! are committed as `RaftCommand::RangeMeta` (group 0) and snapshotted to
//! disk (`range-table.bin`); see issue #25.
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

/// One key-range assignment: `[start_key, end_key)` → group (M20 static / M21 meta).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRange {
    /// Inclusive start key.
    pub start_key: Vec<u8>,
    /// Exclusive end key. Empty means "unbounded upper" (all keys ≥ start).
    pub end_key: Vec<u8>,
    pub group_id: GroupId,
    /// Stable range identity (bumped only when a new range is created by split).
    pub range_id: u64,
    /// Per-range epoch; increments when this range is split or otherwise mutated.
    pub epoch: u64,
}

impl StaticRange {
    /// Construct a range without meta fields (M20-compatible helper).
    pub fn new(start_key: Vec<u8>, end_key: Vec<u8>, group_id: GroupId) -> Self {
        Self {
            start_key,
            end_key,
            group_id,
            range_id: 0,
            epoch: 0,
        }
    }

    /// True if `key` is in `[start_key, end_key)`.
    pub fn contains(&self, key: &[u8]) -> bool {
        if key < self.start_key.as_slice() {
            return false;
        }
        self.end_key.is_empty() || key < self.end_key.as_slice()
    }
}

/// Alias used in docs / M21 wording.
pub type RangeDescriptor = StaticRange;

/// Range / meta table: ordered non-overlapping ranges + cluster meta epoch (M21).
///
/// Lookup is linear (N is expected small until large-scale sharding).
/// Dynamic [`Self::split_at`] / [`Self::merge_with_next`] update routing; engine
/// data stays shared across groups.
#[derive(Debug, Clone, Default)]
pub struct StaticRangeTable {
    ranges: Vec<StaticRange>,
    /// Global meta epoch; increments on every split/merge (client cache invalidation).
    meta_epoch: u64,
    next_range_id: u64,
    next_group_id: u64,
}

/// Alias for the M21 meta range table.
pub type RangeTable = StaticRangeTable;

impl StaticRangeTable {
    pub fn new() -> Self {
        Self {
            ranges: Vec::new(),
            meta_epoch: 0,
            next_range_id: 1,
            next_group_id: 1,
        }
    }

    /// Build from a list of ranges. Ranges are sorted by `start_key`.
    /// Assigns sequential `range_id`s when callers left them at 0.
    pub fn from_ranges(mut ranges: Vec<StaticRange>) -> Self {
        ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        let mut next_range_id = 1u64;
        let mut max_group = 0u64;
        for r in &mut ranges {
            if r.range_id == 0 {
                r.range_id = next_range_id;
                next_range_id += 1;
            } else {
                next_range_id = next_range_id.max(r.range_id + 1);
            }
            max_group = max_group.max(r.group_id.0);
            if r.epoch == 0 {
                r.epoch = 1;
            }
        }
        Self {
            ranges,
            meta_epoch: 1,
            next_range_id,
            next_group_id: max_group.saturating_add(1).max(1),
        }
    }

    /// Single-range whole keyspace → `group_id`.
    pub fn single_group(group_id: GroupId) -> Self {
        let mut t = Self::from_ranges(vec![StaticRange {
            start_key: vec![],
            end_key: vec![],
            group_id,
            range_id: 1,
            epoch: 1,
        }]);
        t.next_group_id = group_id.0.saturating_add(1).max(1);
        t.next_range_id = 2;
        t
    }

    pub fn ranges(&self) -> &[StaticRange] {
        &self.ranges
    }

    pub fn meta_epoch(&self) -> u64 {
        self.meta_epoch
    }

    /// Look up the group for `key`. Returns `None` if no range covers the key.
    pub fn lookup(&self, key: &[u8]) -> Option<GroupId> {
        self.lookup_range(key).map(|r| r.group_id)
    }

    /// Look up the full descriptor for `key`.
    pub fn lookup_range(&self, key: &[u8]) -> Option<&StaticRange> {
        self.ranges.iter().find(|r| r.contains(key))
    }

    /// Split the range that contains `split_key` into
    /// `[start, split_key)` (keeps old group) and `[split_key, end)` (new group).
    ///
    /// Returns `(left, right, new_group_id)`. `split_key` must be strictly inside
    /// the range (not equal to start; not ≥ end when end is bounded).
    pub fn split_at(
        &mut self,
        split_key: &[u8],
    ) -> Result<(StaticRange, StaticRange, GroupId), String> {
        if split_key.is_empty() {
            return Err("split_key must be non-empty".into());
        }
        let idx = self
            .ranges
            .iter()
            .position(|r| r.contains(split_key))
            .ok_or_else(|| "split_key is not covered by any range".to_string())?;
        let old = self.ranges[idx].clone();
        if split_key == old.start_key.as_slice() {
            return Err("split_key must be strictly greater than range start".into());
        }
        if !old.end_key.is_empty() && split_key >= old.end_key.as_slice() {
            return Err("split_key must be strictly less than range end".into());
        }

        let new_group = GroupId(self.next_group_id);
        self.next_group_id = self.next_group_id.saturating_add(1);
        let new_range_id = self.next_range_id;
        self.next_range_id = self.next_range_id.saturating_add(1);
        self.meta_epoch = self.meta_epoch.saturating_add(1);

        let left = StaticRange {
            start_key: old.start_key.clone(),
            end_key: split_key.to_vec(),
            group_id: old.group_id,
            range_id: old.range_id,
            epoch: old.epoch.saturating_add(1),
        };
        let right = StaticRange {
            start_key: split_key.to_vec(),
            end_key: old.end_key.clone(),
            group_id: new_group,
            range_id: new_range_id,
            epoch: 1,
        };

        self.ranges[idx] = left.clone();
        self.ranges.insert(idx + 1, right.clone());
        Ok((left, right, new_group))
    }

    /// Merge the range whose `start_key` equals `left_start` with its right neighbor.
    ///
    /// The merged range keeps `L.group_id` and `L.range_id`, takes `R.end_key`, and
    /// sets `epoch = max(L.epoch, R.epoch) + 1`. `R` is dropped from the table and
    /// `meta_epoch` is bumped. The Raft group that owned `R` is **not** torn down
    /// here (orphan group may stay hosted and idle; reclaim is follow-on work).
    pub fn merge_with_next(&mut self, left_start: &[u8]) -> Result<StaticRange, String> {
        let idx = self
            .ranges
            .iter()
            .position(|r| r.start_key.as_slice() == left_start)
            .ok_or_else(|| "no range with the given left_start".to_string())?;
        if idx + 1 >= self.ranges.len() {
            return Err("left range has no right neighbor to merge".into());
        }
        let left = &self.ranges[idx];
        let right = &self.ranges[idx + 1];
        if left.end_key != right.start_key {
            return Err("left and right ranges are not adjacent".into());
        }

        let merged = StaticRange {
            start_key: left.start_key.clone(),
            end_key: right.end_key.clone(),
            group_id: left.group_id,
            range_id: left.range_id,
            epoch: left.epoch.max(right.epoch).saturating_add(1),
        };
        self.ranges[idx] = merged.clone();
        self.ranges.remove(idx + 1);
        self.meta_epoch = self.meta_epoch.saturating_add(1);
        Ok(merged)
    }

    /// Allocate the next free group id without splitting (tests / bootstrap).
    pub fn peek_next_group_id(&self) -> GroupId {
        GroupId(self.next_group_id)
    }

    /// Binary snapshot for disk / Raft meta replication (issue #25).
    ///
    /// Format (version 1):
    /// ```text
    /// version(u8=1) | meta_epoch(u64 LE) | next_range_id(u64) | next_group_id(u64)
    /// | count(u32 LE) | repeated:
    ///     range_id(u64) | epoch(u64) | group_id(u64)
    ///     | start_len(u32) | start | end_len(u32) | end
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(1u8); // version
        out.extend_from_slice(&self.meta_epoch.to_le_bytes());
        out.extend_from_slice(&self.next_range_id.to_le_bytes());
        out.extend_from_slice(&self.next_group_id.to_le_bytes());
        out.extend_from_slice(&(self.ranges.len() as u32).to_le_bytes());
        for r in &self.ranges {
            out.extend_from_slice(&r.range_id.to_le_bytes());
            out.extend_from_slice(&r.epoch.to_le_bytes());
            out.extend_from_slice(&r.group_id.0.to_le_bytes());
            out.extend_from_slice(&(r.start_key.len() as u32).to_le_bytes());
            out.extend_from_slice(&r.start_key);
            out.extend_from_slice(&(r.end_key.len() as u32).to_le_bytes());
            out.extend_from_slice(&r.end_key);
        }
        out
    }

    /// Decode a snapshot produced by [`Self::encode`].
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        let mut cur = data;
        if cur.is_empty() {
            return Err("empty range table snapshot".into());
        }
        let version = cur[0];
        cur = &cur[1..];
        if version != 1 {
            return Err(format!("unsupported range table version: {version}"));
        }
        let meta_epoch = take_u64(&mut cur, "meta_epoch")?;
        let next_range_id = take_u64(&mut cur, "next_range_id")?;
        let next_group_id = take_u64(&mut cur, "next_group_id")?;
        let count = take_u32(&mut cur, "range count")? as usize;
        let mut ranges = Vec::with_capacity(count);
        for _ in 0..count {
            let range_id = take_u64(&mut cur, "range_id")?;
            let epoch = take_u64(&mut cur, "epoch")?;
            let group_id = GroupId(take_u64(&mut cur, "group_id")?);
            let start_key = take_bytes(&mut cur, "start_key")?;
            let end_key = take_bytes(&mut cur, "end_key")?;
            ranges.push(StaticRange {
                start_key,
                end_key,
                group_id,
                range_id,
                epoch,
            });
        }
        if !cur.is_empty() {
            return Err(format!(
                "trailing {} bytes after range table snapshot",
                cur.len()
            ));
        }
        Ok(Self {
            ranges,
            meta_epoch,
            next_range_id,
            next_group_id,
        })
    }

    /// Replace this table with `other` (used when applying a committed meta snapshot).
    pub fn restore(&mut self, other: Self) {
        *self = other;
    }

    /// Group ids present in the table (sorted, unique).
    pub fn group_ids(&self) -> Vec<GroupId> {
        let mut ids: Vec<GroupId> = self.ranges.iter().map(|r| r.group_id).collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

fn take_u64(cur: &mut &[u8], label: &str) -> Result<u64, String> {
    if cur.len() < 8 {
        return Err(format!("truncated range table ({label})"));
    }
    let v = u64::from_le_bytes([
        cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
    ]);
    *cur = &cur[8..];
    Ok(v)
}

fn take_u32(cur: &mut &[u8], label: &str) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err(format!("truncated range table ({label})"));
    }
    let v = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
    *cur = &cur[4..];
    Ok(v)
}

fn take_bytes(cur: &mut &[u8], label: &str) -> Result<Vec<u8>, String> {
    let len = take_u32(cur, label)? as usize;
    if cur.len() < len {
        return Err(format!(
            "truncated range table ({label}): need {len}, have {}",
            cur.len()
        ));
    }
    let bytes = cur[..len].to_vec();
    *cur = &cur[len..];
    Ok(bytes)
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

    /// Transfer leadership of `group_id` to `target` (M22).
    ///
    /// Returns an error if the group is not hosted or the local node cannot
    /// transfer (not leader / non-voter target). See
    /// [`RaftNode::transfer_leadership`].
    pub fn transfer_leadership(
        &mut self,
        group_id: GroupId,
        target: crate::types::NodeId,
    ) -> Result<(), String> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("group {} not hosted", group_id.0))?;
        node.transfer_leadership(target)
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
    pub fn for_each_persist_view(
        &self,
        mut f: impl FnMut(GroupId, crate::storage::PersistedRaftState),
    ) {
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
            StaticRange::new(b"a".to_vec(), b"m".to_vec(), GroupId(1)),
            StaticRange::new(b"m".to_vec(), b"z".to_vec(), GroupId(2)),
        ]);
        assert_eq!(table.lookup(b"a"), Some(GroupId(1)));
        assert_eq!(table.lookup(b"hello"), Some(GroupId(1)));
        assert_eq!(table.lookup(b"m"), Some(GroupId(2)));
        assert_eq!(table.lookup(b"xyz"), Some(GroupId(2)));
        assert_eq!(table.lookup(b"z"), None);
        assert_eq!(table.lookup(b"0"), None);
        assert_eq!(table.meta_epoch(), 1);
    }

    #[test]
    fn single_group_table_covers_all_keys() {
        let table = StaticRangeTable::single_group(GroupId::ZERO);
        assert_eq!(table.lookup(b""), Some(GroupId::ZERO));
        assert_eq!(table.lookup(b"anything"), Some(GroupId::ZERO));
    }

    #[test]
    fn split_at_updates_routing_and_epoch() {
        let mut table = StaticRangeTable::single_group(GroupId::ZERO);
        let epoch0 = table.meta_epoch();
        let (left, right, new_gid) = table.split_at(b"m").unwrap();
        assert_eq!(left.group_id, GroupId::ZERO);
        assert_eq!(left.end_key, b"m");
        assert_eq!(right.start_key, b"m");
        assert_eq!(right.group_id, new_gid);
        assert_eq!(table.lookup(b"a"), Some(GroupId::ZERO));
        assert_eq!(table.lookup(b"m"), Some(new_gid));
        assert_eq!(table.lookup(b"z"), Some(new_gid));
        assert!(table.meta_epoch() > epoch0);
        assert_eq!(table.ranges().len(), 2);
    }

    #[test]
    fn split_rejects_boundary_and_uncovered() {
        let mut table = StaticRangeTable::from_ranges(vec![StaticRange::new(
            b"a".to_vec(),
            b"z".to_vec(),
            GroupId(1),
        )]);
        assert!(table.split_at(b"a").is_err());
        assert!(table.split_at(b"z").is_err());
        assert!(table.split_at(b"0").is_err());
    }

    /// Mirrors the server SPLIT_RANGE path: peek + (host) + split_at under
    /// exclusive access so the hosted id matches the allocated id.
    #[test]
    fn merge_with_next_recombines_split_ranges() {
        let mut t = StaticRangeTable::single_group(GroupId(0));
        t.split_at(b"m").unwrap();
        assert_eq!(t.ranges().len(), 2);
        let merged = t.merge_with_next(b"").unwrap(); // left starts at empty
        assert_eq!(t.ranges().len(), 1);
        assert!(merged.end_key.is_empty());
        assert_eq!(t.meta_epoch(), 3); // start 1 + split + merge
        assert_eq!(merged.group_id, GroupId(0));
        assert_eq!(merged.range_id, 1);
        assert_eq!(t.lookup(b"a"), Some(GroupId(0)));
        assert_eq!(t.lookup(b"m"), Some(GroupId(0)));
        assert_eq!(t.lookup(b"z"), Some(GroupId(0)));
    }

    #[test]
    fn merge_with_next_rejects_missing_and_last() {
        let mut t = StaticRangeTable::single_group(GroupId(0));
        assert!(t.merge_with_next(b"").is_err()); // only one range
        assert!(t.merge_with_next(b"nope").is_err());
        t.split_at(b"m").unwrap();
        // Right half has no neighbor.
        assert!(t.merge_with_next(b"m").is_err());
    }

    #[test]
    fn range_table_encode_decode_preserves_epochs_and_counters() {
        let mut t = StaticRangeTable::single_group(GroupId::ZERO);
        t.split_at(b"m").unwrap();
        t.split_at(b"t").unwrap();
        let epoch = t.meta_epoch();
        let peek_g = t.peek_next_group_id();
        let encoded = t.encode();
        let restored = StaticRangeTable::decode(&encoded).unwrap();
        assert_eq!(restored.meta_epoch(), epoch);
        assert_eq!(restored.peek_next_group_id(), peek_g);
        assert_eq!(restored.ranges(), t.ranges());
        assert_eq!(restored.lookup(b"a"), Some(GroupId::ZERO));
        assert_eq!(restored.lookup(b"m"), Some(GroupId(1)));
        assert_eq!(restored.lookup(b"t"), Some(GroupId(2)));
        // Round-trip again after restore (bytes + further split allocation).
        assert_eq!(
            StaticRangeTable::decode(&restored.encode())
                .unwrap()
                .ranges(),
            t.ranges()
        );
        // Counters preserved: next split allocates the same group id as pre-encode.
        let mut again = restored;
        let (_, right, gid) = again.split_at(b"z").unwrap();
        assert_eq!(gid, peek_g);
        assert_eq!(right.group_id, peek_g);
    }

    #[test]
    fn range_table_decode_rejects_bad_version() {
        let err = StaticRangeTable::decode(&[99u8]).unwrap_err();
        assert!(err.contains("version"), "err={err}");
    }

    #[test]
    fn from_ranges_does_not_match_encode_restore_semantics() {
        // from_ranges resets meta_epoch to 1 — durable restore must use decode.
        let mut t = StaticRangeTable::single_group(GroupId::ZERO);
        t.split_at(b"m").unwrap();
        assert!(t.meta_epoch() > 1);
        let via_from = StaticRangeTable::from_ranges(t.ranges().to_vec());
        assert_eq!(via_from.meta_epoch(), 1);
        let via_decode = StaticRangeTable::decode(&t.encode()).unwrap();
        assert_eq!(via_decode.meta_epoch(), t.meta_epoch());
    }

    #[test]
    fn peek_then_split_allocates_stable_group_ids() {
        let mut table = StaticRangeTable::single_group(GroupId::ZERO);
        let mut hosted = vec![GroupId::ZERO];

        for key in [b"m".as_slice(), b"t".as_slice()] {
            let peek = table.peek_next_group_id();
            // Server hosts `peek` before split_at; table mutation is exclusive.
            hosted.push(peek);
            let (_, right, gid) = table.split_at(key).unwrap();
            assert_eq!(gid, peek, "hosted peek must match split_at allocation");
            assert_eq!(right.group_id, gid);
        }
        assert_eq!(hosted, vec![GroupId(0), GroupId(1), GroupId(2)]);
        assert_eq!(table.lookup(b"a"), Some(GroupId::ZERO));
        assert_eq!(table.lookup(b"m"), Some(GroupId(1)));
        assert_eq!(table.lookup(b"t"), Some(GroupId(2)));
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
