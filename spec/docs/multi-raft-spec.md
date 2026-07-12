# Multi-Raft Foundation Spec (M20)

**Status:** Draft v0.2 (ClusterNode production path)  
**Scope:** N Raft groups per process, envelope multiplexing, static ranges, HLC  
**Milestone:** M20 (foundation + ClusterNode multi-group wiring)

---

## 1. Purpose

Enable **many independent Raft groups** in one process so the key space can
later be partitioned into ranges (M21+). This foundation provides:

- Transport multiplexing via `Envelope.group_id`
- Per-group on-disk Raft state paths
- Coalesced ticks across groups (`MultiRaftHost::tick_all`)
- Static key → group routing (`StaticRangeTable`)
- Hybrid logical clocks (`kaya_core::Hlc`) wired as optional engine `commit_ts`
- **`ClusterNode` multi-group production path** via `MultiRaftHost` (always ≥ group 0)

Dynamic splits, RANGE_MOVED, and per-range Jepsen remain follow-on work.

---

## 2. Group identity

```rust
pub struct GroupId(pub u64);
// GroupId(0) / GroupId::ZERO — legacy single-group default + membership group
```

Each group is an independent Raft state machine (log, term, leadership).

---

## 3. Wire format

Raft envelope body (after `frame_len` u32 LE):

```text
from_id   : u64 LE
to_id     : u64 LE
group_id  : u64 LE   // M20; 0 = single-group / membership
msg_type  : u8
…message-specific fields…
```

**Compatibility:** this is a deliberate wire break vs pre-M20 peers. All nodes
in a cluster must speak the same layout. Group `0` traffic is identical to a
single-group deployment on the new format.

`Envelope::new` defaults `group_id = 0`. `Envelope::with_group` sets an explicit
group. `MultiRaftHost` stamps `group_id` on all outgoing envelopes.

---

## 4. On-disk layout

```text
{data_dir}/raft-hard-state          # group 0 (legacy root paths)
{data_dir}/raft-log
{data_dir}/raft-apply-index.jsonl
{data_dir}/groups/{group_id}/raft-hard-state   # group_id != 0
{data_dir}/groups/{group_id}/raft-log
{data_dir}/groups/{group_id}/raft-apply-index.jsonl
```

Helpers:

- `kaya_raft::raft_group_dir(data_dir, group_id)` (disk-storage feature)
- `kaya_raft::multi_raft_group_dir(data_dir, GroupId)` (always available)
- `DiskRaftStorage::open_group(data_dir, group_id)`

---

## 5. Static range table

```rust
pub struct StaticRange {
    pub start_key: Vec<u8>, // inclusive
    pub end_key: Vec<u8>,   // exclusive; empty = unbounded upper
    pub group_id: GroupId,
}

impl StaticRangeTable {
    pub fn lookup(&self, key: &[u8]) -> Option<GroupId>;
}
```

Ranges are sorted by `start_key`. Lookup is linear (fine while N is small).
Dynamic splits / meta range descriptors arrive in M21.

Configure on the server:

```rust
ClusterConfig::new(...)
    .with_static_ranges(vec![
        StaticRange { start_key: b"a".into(), end_key: b"m".into(), group_id: GroupId(1) },
        StaticRange { start_key: b"m".into(), end_key: b"z".into(), group_id: GroupId(2) },
    ]);
```

Default: `StaticRangeTable::single_group(GroupId::ZERO)` (whole keyspace → group 0).

---

## 6. MultiRaftHost

```rust
pub struct MultiRaftHost { /* HashMap<GroupId, RaftNode> */ }

impl MultiRaftHost {
    pub fn insert(&mut self, group_id: GroupId, node: RaftNode);
    pub fn tick_all(&mut self) -> Vec<Envelope>;  // coalesced ticks; stamps group_id
    pub fn propose(&mut self, group_id: GroupId, cmd: Vec<u8>) -> Option<LogIndex>;
    pub fn handle(&mut self, env: Envelope) -> Vec<Envelope>; // demux by env.group_id
    pub fn drain_all_applied(&mut self) -> Vec<(GroupId, LogIndex, Term, Vec<u8>)>;
    pub fn broadcast_group(&mut self, group_id: GroupId) -> Vec<Envelope>;
}
```

Foundation tests: two single-node groups elect, propose independently, and
never apply each other's commands.

---

## 7. ClusterNode production path

`ClusterNode` **always** builds a `MultiRaftHost` with at least group 0.

Startup:

1. Collect unique `group_id`s from `ClusterConfig.range_table` (insert 0 if missing).
2. For each group, recover `RaftNode` from `multi_raft_group_dir` + per-group
   `RaftPersister` / apply-index.
3. Raft event loop: `tick_all`, demux inbound envelopes by `env.group_id`,
   propose with `ProposeReq.group_id`.
4. Client ops: `range_table.lookup(key)` (default group 0) before propose /
   read-index.
5. Membership ADD/REMOVE remains on **group 0**.
6. Engine state machine is **shared** across groups (range routing keeps key
   ownership disjoint). Multi-group auto-enables `EngineConfig.use_hlc`.

Pending client writes are keyed by `(group_id, LogIndex)` so indices never
collide across groups.

Cross-group transactions return `STATUS_INVALID_ARGUMENT` in this foundation.

Integration test: `test_multi_raft_static_ranges_put_get` (single-node, two ranges).

---

## 8. HLC

`kaya_core::Hlc { physical_ms, logical }` with:

- `update(now_ms, remote: Option<Hlc>)` — standard HLC merge
- `tick(now_ms)` — local advance
- `to_u64` / `from_u64` — pack as `(physical_ms << 16) | logical` for `commit_ts`

When `EngineConfig.use_hlc` is true (or multi-group ClusterNode config), each
`put`/`delete` ticks the HLC and calls `WalWriter::ensure_min_sequence` so the
WAL sequence / commit_ts is HLC-derived and monotonic when wall time stalls.

---

## 9. Observability (stub)

When OTel is enabled at the server layer, multi-raft code paths should attach
attribute `kaya.raft.group_id` so traces demux by group. Full W3C trace-context
propagation node↔node↔client remains M20/M24 follow-on.

STATS JSON includes `raft_groups` (count of hosted groups). HEALTH reports
`leader` if this process is leader of **any** group.

---

## 10. Guarantees

| Guarantee | Status |
|---|---|
| Envelope carries group_id | Yes |
| Per-group disk paths | Yes |
| N groups in one process (host) | Yes |
| Coalesced tick_all | Yes |
| Static key → group lookup | Yes |
| HLC type + pack | Yes |
| HLC as engine commit_ts (opt-in) | Yes |
| Independent apply per group (unit) | Yes |
| ClusterNode multi-group production loop | Yes |
| Client key routing (static table) | Yes |
| Cross-group txn / 2PC | No (rejected) |
| RANGE_MOVED | No (M21) |
| Dynamic splits / meta range | No (M21) |
| Per-range Jepsen / 3×N chaos | No |
| Live clock-skew nemesis | No |
| Full OTel context propagation | No (stub only) |

---

## 11. Out of scope → later M20 / M21+

- Dynamic splits / meta range descriptors
- Client RANGE_MOVED + range cache invalidation
- Production tick/heartbeat batching under load (benchmarks)
- OTel trace-context propagation v1 end-to-end
- Live-cluster clock-skew nemesis paired with HLC
- Jepsen per-range green on 3 nodes × N ranges
- Cross-group transactions (2PC / atomic commit)
