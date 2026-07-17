# Range Metadata, Routing & Splits (M21)

**Status:** Draft v0.1  
**Scope:** Meta range table with epochs, dynamic split, client cache, RANGE_MOVED  
**Milestone:** M21

---

## 1. Purpose

Partition the keyspace across Raft groups with:

- Epoch’d range descriptors (meta table)
- Dynamic split at a key (shared engine; routing-only split)
- Client `list_ranges` cache + `RANGE_MOVED` status
- `kayactl range list|split`

Engine data is **shared** across groups in a process; split does not physically
move keys — it changes which group commits future writes for a key interval.

---

## 2. Descriptor model

```text
RangeDescriptor {
  range_id : u64
  epoch    : u64   // per-range; +1 on split of this range
  group_id : u64
  start_key: bytes // inclusive
  end_key  : bytes // exclusive; empty = unbounded upper
}

RangeTable {
  meta_epoch : u64 // +1 on every split (cache invalidation)
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

## 4. Wire protocol

| Opcode | Name | Notes |
|---|---|---|
| 15 | `LIST_RANGES` | Response: meta_epoch + descriptors |
| 16 | `SPLIT_RANGE` | Request: split_key; response: two half descriptors |

**Status `RANGE_MOVED` (11):** body is a list-ranges payload (count≥1) for the
key’s current owner. Clients refresh cache and retry.

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

---

## 5. CLI

```text
kayactl --server <addr> range list
kayactl --server <addr> range split <key>
```

---

## 6. Exit criteria

| Gate | Status |
|---|---|
| Meta table + epoch | Yes |
| Dynamic split + host new group | Yes |
| LIST_RANGES / SPLIT_RANGE | Yes |
| Client range cache API | Yes (`list_ranges` / `split_range`) |
| No lost writes across split (IT) | Yes (`test_range_split_no_lost_writes`) |
| Multi-node range move / learner | No (M22) |
| Auto size-threshold split | No (manual + API first) |

---

## 7. Related

- `spec/docs/multi-raft-spec.md` (M20 foundation)
- `kaya_raft::StaticRangeTable::split_at`
- `kaya-server` opcodes 15/16
