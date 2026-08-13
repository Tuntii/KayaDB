# Range Metadata, Routing, Splits & Merges (M21/M22)

**Status:** Production path v1.1 (M21/M22 + durable meta #25; 2026-08-13)  
**Scope:** Meta range table with epochs, dynamic split/merge, client cache, RANGE_MOVED; advisory rebalance; Raft-replicated durable meta; no live migrate  
**Milestone:** M21 (split), M22 (merge, transfer, learners, advisory balancer, drain, dashboard v1), #25 (durable RangeMeta)

---

## 1. Purpose

Partition the keyspace across Raft groups with:

- Epoch’d range descriptors (meta table)
- Dynamic split at a key (shared engine; routing-only split)
- Dynamic merge of adjacent ranges (routing-only; no key moves)
- Client `list_ranges` cache + `RANGE_MOVED` status
- `kayactl range list|split|merge`

Engine data is **shared** across groups in a process; split/merge do not
physically move keys — they change which group commits future writes for a key
interval.

---

## 2. Descriptor model

```text
RangeDescriptor {
  range_id : u64
  epoch    : u64   // per-range; +1 on split/merge of this range
  group_id : u64
  start_key: bytes // inclusive
  end_key  : bytes // exclusive; empty = unbounded upper
}

RangeTable {
  meta_epoch : u64 // +1 on every split/merge (cache invalidation)
  ranges     : ordered non-overlapping descriptors
}
```

Lookup: linear scan; key `k` matches first range with `start ≤ k < end`
(or `end` empty).

---

## 3. Split algorithm

`split_at(split_key)`:

1. Find range `R` containing `split_key` (strictly inside: not start, not ≥ end).
2. Allocate `new_group_id`, host empty Raft group on this process.
3. Replace `R` with:
   - Left: `[R.start, split_key)` → keep `R.group_id`, `range_id`, `epoch+1`
   - Right: `[split_key, R.end)` → `new_group_id`, new `range_id`, `epoch=1`
4. Bump `meta_epoch`.
5. **Commit** the full table snapshot as `RaftCommand::RangeMeta` on group 0
   (`base_epoch` = pre-split `meta_epoch`). Apply is CAS: replace only if
   current `meta_epoch == base_epoch`, persist `{data_dir}/range-table.bin`,
   then host every group in the snapshot. Concurrent splits with the same
   `base_epoch` lose on CAS.

---

## 3b. Merge algorithm (M22)

`merge_with_next(left_start)`:

1. Find range `L` with `start_key == left_start`.
2. Right neighbor `R` must exist and be adjacent (`L.end_key == R.start_key`).
3. Merged range: keep `L.group_id`, `L.range_id`,
   `epoch = max(L.epoch, R.epoch) + 1`, `end_key = R.end_key`.
4. Drop `R` from the table; bump `meta_epoch`.
5. Do **not** tear down the Raft group that owned `R` in this path — the orphan
   group may stay hosted and idle. Reclaim / unhost is follow-on work.
6. Commit the merged table the same way as split (`RangeMeta` + disk file).

---

## 3c. Recovery (#25)

On `ClusterNode` start:

1. Load `{data_dir}/range-table.bin` if present (overrides configured defaults).
   Decode preserves `meta_epoch`, `next_range_id`, `next_group_id` — do **not**
   rebuild via `from_ranges` (that resets `meta_epoch` to 1).
2. Host every group referenced by the restored table (plus group 0).
3. Replay unapplied Raft log entries. A `RangeMeta` whose `base_epoch` does not
   match is ignored if the on-disk snapshot already matches the payload
   (idempotent re-apply after crash between persist and apply-index).
4. `raft-snapshot.bin` / InstallSnapshot payload is **v2**: engine + membership
   + `StaticRangeTable::encode`. A joiner after log compact restores the table
   from the snapshot when its `meta_epoch` is ≥ the live table.

Clients may prefix PUT/GET/DELETE/SCAN with `MEPO | meta_epoch(u64 LE)`. If
`client_epoch < server.meta_epoch`, the server returns `RANGE_MOVED` (11) with
a full list-ranges body. `kaya-client` attaches the cached epoch automatically.

---

## 4. Wire protocol

| Opcode | Name | Notes |
|---|---|---|
| 15 | `LIST_RANGES` | Response: meta_epoch + descriptors |
| 16 | `SPLIT_RANGE` | Request: split_key; response: two half descriptors |
| 17 | `MERGE_RANGE` | Request: left_start; response: one merged descriptor |
| 20 | `REBALANCE_PLAN` | **Advisory only.** Range-count heuristic; no live migrate |

**Status `RANGE_MOVED` (11):** body is a list-ranges payload (count≥1) for the
key’s current owner. Clients refresh cache and retry.

### REBALANCE_PLAN (advisory)

Opcode `20` is an admin op (operator token when configured). Request body empty.
Response:

```text
count(u32 LE) | repeated:
  range_id(u64 LE) | from_node(u64 LE) | to_node(u64 LE)
```

Heuristic (`plan_range_count`): while `max_count - min_count > 1`, move one range
from a richest node (group leader ownership) to a poorest node. **The plan does
not move data, transfer leases, or change the meta table.** Operators may use
it as a suggestion only; live placement / MOVE_RANGE is follow-on work.

### LIST_RANGES response

```text
meta_epoch(u64 LE) | count(u32 LE) | repeated:
  range_id(u64) | epoch(u64) | group_id(u64)
  | start_len(u32) | start | end_len(u32) | end
```

### SPLIT_RANGE request

```text
key_len(u32 LE) | key
```

### MERGE_RANGE request

```text
left_start_len(u32 LE) | left_start
```

Empty `left_start` is valid (left half after a whole-keyspace split). Response
uses the list-ranges layout with `count=1` for the merged descriptor.

---

## 5. CLI

```text
kayactl --server <addr> range list
kayactl --server <addr> range split <key>
kayactl --server <addr> range merge <left-start-hex-or-utf8>
kayactl --server <addr> [--operator-token <tok>] range rebalance-plan
```

`left-start`: empty string / `@empty` for empty start; `0x…` or `hex:…` for
raw bytes; otherwise UTF-8.

`rebalance-plan` prints suggested `(range_id, from, to)` moves; nothing is applied.

---

## 6. Exit criteria

| Gate | Status |
|---|---|
| Meta table + epoch | Yes |
| Dynamic split + host new group | Yes |
| LIST_RANGES / SPLIT_RANGE | Yes |
| merge_with_next + MERGE_RANGE (17) | Yes |
| Client range cache API | Yes (`list_ranges` / `split_range`) |
| No lost writes across split (IT) | Yes (`test_range_split_no_lost_writes`) |
| Split+merge round-trip (IT) | Yes (`test_range_merge_recombines`) |
| Multi-node range move / learner | Partial (learner + promote yes; live migrate no) |
| TRANSFER_LEADER (18) | Yes (step-down; no TimeoutNow) |
| PROMOTE_LEARNER (19) | Yes |
| Advisory REBALANCE_PLAN (20) | Yes (range-count; no live migrate) |
| Drain mode + decommission runbook | Yes |
| Dashboard v1 (read-only HTTP) | Yes (`/health`, `/v1/ranges`, `/v1/raft`) |
| Durable meta (RangeMeta + disk) | Yes (`range-table.bin`, IT restart) |
| Restart restores last committed layout | Yes (single + all-nodes ITs) |
| Range table inside Raft snapshot payload | Yes (snapshot v2; sim catch-up) |
| Stale client `meta_epoch` → RANGE_MOVED | Yes (`MEPO` prefix + IT) |
| Sim crash / snapshot restore | Yes (`range_meta_replicates_and_survives_crash`) |
| Auto size-threshold split | No (manual + API first) |
| Live range migrate / MOVE_RANGE | No (follow-on) |
| Orphan group reclaim after merge | No (follow-on) |

---

## 7. Related

- `spec/docs/multi-raft-spec.md` (M20 foundation)
- `kaya_raft::StaticRangeTable::split_at` / `merge_with_next`
- `kaya-server` opcodes 15/16/17/18/19/20
