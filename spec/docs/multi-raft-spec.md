# Multi-Raft Foundation Spec (M20)

**Status:** Draft v0.1 (foundation)  
**Scope:** N Raft groups per process, envelope multiplexing, static ranges, HLC  
**Milestone:** M20 (foundation; not full production multi-raft / sharding)

---

## 1. Purpose

Enable **many independent Raft groups** in one process so the key space can
later be partitioned into ranges (M21+). This foundation provides:

- Transport multiplexing via `Envelope.group_id`
- Per-group on-disk Raft state paths
- Coalesced ticks across groups (`MultiRaftHost::tick_all`)
- Static key → group routing (`StaticRangeTable`)
- Hybrid logical clocks (`kaya_core::Hlc`) for future commit timestamps

`ClusterNode` continues to run a **single group 0** by default. Full production
wiring (client routing, multi-group persistence in the server loop, per-range
Jepsen) is follow-on work.

---

## 2. Group identity

```rust
pub struct GroupId(pub u64);
// GroupId(0) / GroupId::ZERO — legacy single-group default
```

Each group is an independent Raft state machine (log, term, leadership).

---

## 3. Wire format

Raft envelope body (after `frame_len` u32 LE):

```text
from_id   : u64 LE
to_id     : u64 LE
group_id  : u64 LE   // NEW (M20); 0 = single-group
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
{data_dir}/groups/{group_id}/raft-hard-state   # group_id != 0
{data_dir}/groups/{group_id}/raft-log
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

---

## 6. MultiRaftHost

```rust
pub struct MultiRaftHost { /* HashMap<GroupId, RaftNode> */ }

impl MultiRaftHost {
    pub fn insert(&mut self, group_id: GroupId, node: RaftNode);
    pub fn tick_all(&mut self) -> Vec<Envelope>;  // coalesced ticks; stamps group_id
    pub fn propose(&mut self, group_id: GroupId, cmd: Vec<u8>) -> Option<LogIndex>;
    pub fn handle(&mut self, env: Envelope) -> Vec<Envelope>; // demux by env.group_id
}
```

Foundation tests: two single-node groups elect, propose independently, and
never apply each other's commands.

---

## 7. HLC

`kaya_core::Hlc { physical_ms, logical }` with:

- `update(now_ms, remote: Option<Hlc>)` — standard HLC merge
- `tick(now_ms)` — local advance
- `to_u64` / `from_u64` — pack as `(physical_ms << 16) | logical` for `commit_ts`

Monotonic when wall clock stalls (logical increments). Engine commit_ts source
still uses sequence numbers until an integration pass swaps the assignment.

---

## 8. Observability (stub)

When OTel is enabled at the server layer, multi-raft code paths should attach
attribute `kaya.raft.group_id` so traces demux by group. Full W3C trace-context
propagation node↔node↔client remains M20/M24 follow-on.

---

## 9. Guarantees (foundation)

| Guarantee | Status |
|---|---|
| Envelope carries group_id | Yes |
| Per-group disk paths | Yes |
| N groups in one process (host) | Yes |
| Coalesced tick_all | Yes |
| Static key → group lookup | Yes |
| HLC type + pack | Yes |
| Independent apply per group (unit) | Yes |
| ClusterNode multi-group production loop | No |
| Client key routing / RANGE_MOVED | No (M21) |
| Dynamic splits / meta range | No (M21) |
| Per-range Jepsen / 3×N chaos | No |
| Live clock-skew nemesis | No |
| Full OTel context propagation | No (stub only) |

---

## 10. Out of scope → later M20 / M21+

- Wire `ClusterNode` to host N groups + demux inbound Raft by `group_id`
- Client ops route by `StaticRangeTable` / range cache
- Production tick/heartbeat batching under load (benchmarks)
- OTel trace-context propagation v1 end-to-end
- Live-cluster clock-skew nemesis paired with HLC
- Jepsen per-range green on 3 nodes × N ranges
- HLC as engine `commit_ts` assignment source
