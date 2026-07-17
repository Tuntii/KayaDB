# Range Metadata, Routing, Splits & Merges (M21/M22)

**Status:** Draft v0.2  
**Scope:** Meta range table with epochs, dynamic split/merge, client cache, RANGE_MOVED  
**Milestone:** M21 (split), M22 (merge)

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
| Multi-node range move / learner | Partial (learner promote yes; live migrate no) |
| Advisory REBALANCE_PLAN (20) | Yes (range-count; no live migrate) |
| Auto size-threshold split | No (manual + API first) |
| Orphan group reclaim after merge | No (follow-on) |

---

## 7. Related

- `spec/docs/multi-raft-spec.md` (M20 foundation)
- `kaya_raft::StaticRangeTable::split_at` / `merge_with_next`
- `kaya-server` opcodes 15/16/17
