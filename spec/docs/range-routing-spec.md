# Range Metadata, Routing, Splits & Merges (M21/M22)

**Status:** Production path v1.3 (M21/M22 + durable meta #25 + live MOVE_RANGE #24 + orphan group reclaim #30; 2026-08-24)  
**Scope:** Meta range table with epochs, dynamic split/merge, live range migrate, client cache, RANGE_MOVED; advisory rebalance; Raft-replicated durable meta; orphan Raft group reclaim after merge  
**Milestone:** M21 (split), M22 (merge, transfer, learners, advisory balancer, drain, dashboard v1), #25 (durable RangeMeta), #24 (MOVE_RANGE), #30 (orphan group reclaim)

---

## 1. Purpose

Partition the keyspace across Raft groups with:

- Epoch’d range descriptors (meta table)
- Dynamic split at a key (shared engine; routing-only split)
- Dynamic merge of adjacent ranges (routing-only; no key moves)
- Live range migrate (`MOVE_RANGE`): reassign a range to another group under load
- Client `list_ranges` cache + `RANGE_MOVED` status
- `kayactl range list|split|merge|move`

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
5. Do **not** tear down the Raft group that owned `R` in this path — the merge
   only updates routing. `R`'s group becomes an *orphan* (hosted, no longer
   referenced by any range); see §3e for reclaim.
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

## 3d. Live range migrate — `MOVE_RANGE` (#24)

`move_range(range_start, target_group)` reassigns one range to another Raft
group **while the cluster serves traffic**. It is the product-side answer to
`REBALANCE_PLAN`: the plan suggests a move, `MOVE_RANGE` performs it.

### Descriptor change

```text
before: { range_id: R, epoch: E, group_id: S, [start, end) }
after:  { range_id: R, epoch: E+1, group_id: T, [start, end) }
        meta_epoch += 1;  next_group_id = max(next_group_id, T+1)
```

`range_id` and the bounds are preserved — a move is not a split and not a
merge. Both epochs advance so stale client caches take the `RANGE_MOVED`
refresh path, and `next_group_id` advances past `T` so a later split cannot
re-allocate the target id.

### Protocol (leader of group 0)

1. **Validate.** Exact `start_key` match; `T ≠ S`. A move onto the current
   owner is rejected (`INVALID_ARGUMENT`) so the meta epoch never churns for a
   no-op. Refused while the node is draining (same guard as `SPLIT_RANGE`).
2. **Host target.** `ensure_group_hosted(T)` before the cutover commits, so a
   write arriving immediately after cutover finds a hosted group instead of
   bouncing on `RANGE_MOVED`.
3. **Snapshot + delta catch-up.** See *Physical migration* below. In the
   shared-engine deployment this step is a no-op: the target group already
   reads and writes the same engine as the source.
4. **Quiesce the source (barrier).** Commit and apply an empty entry on `S`, so
   everything already committed on `S` is durable in the engine before
   ownership flips. Skipped when this node is not `S`'s leader (best-effort —
   see *Failure modes*).
5. **Cutover.** Commit one `RaftCommand::RangeMeta` on group 0 with
   `base_epoch = pre-move meta_epoch`. Apply is the same CAS + persist + host
   path as split/merge (§3c): replace the table only if
   `meta_epoch == base_epoch`, write `range-table.bin`, then host every group
   in the snapshot. A concurrent split/merge/move with the same `base_epoch`
   loses the CAS.
6. **Respond** with the moved descriptor (list-ranges layout, `count=1`).

The source group is **not** torn down; it may stay hosted and idle. That is the
same orphan situation merge leaves behind (reclaim is §3e, issue #30).

### Physical migration and dual-write

Today every Raft group in a process shares **one** engine, and every node hosts
every group in the table. A range's keys are therefore already present on the
target group's replicas: nothing to copy, nothing to lose, nothing to
duplicate. Copying keys through the target group's log would rewrite identical
bytes into the same store and open a real clobber window (a scan-then-propose
race can overwrite a newer concurrent write with the value read at scan time),
so the copy phase is deliberately **not** implemented.

When groups gain per-group engines, step 3 becomes:

- **Dual-write window.** From cutover-prepare, a mutation of a key in
  `[start, end)` is acknowledged only after it commits on **both** `S` and `T`.
  `S` stays the read authority for the whole window.
- **Snapshot.** Stream `[start, end)` from `S`'s engine to `T` in bounded
  batches, applied on `T` as put-if-absent so a dual-write never loses to a
  stale copied value.
- **Delta catch-up.** Repeat over the keys mutated since the snapshot started
  until the residue fits inside one short write fence.
- **Cutover.** Fence writes on the range, drain `S`, commit the meta entry,
  lift the fence. Reads move to `T` only after the meta entry applies.
- **Cleanup.** Drop `[start, end)` from `S`'s engine after the cutover entry is
  committed on a quorum — never before.

### Failure modes

| Failure | Outcome |
|---|---|
| Crash before the meta entry commits | Nothing changed: `S` still owns the range; retry the move |
| Crash after commit, before apply | Replay applies the entry; `T` owns the range (idempotent re-apply, §3c) |
| Crash between persist and apply-index | On-disk snapshot already matches the payload → re-apply is a no-op |
| Concurrent split / merge / move | CAS on `base_epoch` — exactly one wins; the loser gets `STATUS_ERROR` and retries against the fresh table |
| Move onto the current owner | `INVALID_ARGUMENT`; no epoch bump |
| Node is draining | `STATUS_ERROR` (refuses to host a new group) |
| Not leader of group 0 | `STATUS_NOT_LEADER` + leader hint |
| Group-0 leader is not `S`'s leader | Barrier is skipped; the cutover is still safe (no keys move). A read served by `T`'s leader may briefly trail `S`'s last pre-cutover apply — the same shared-engine freshness bound that `split_at` already has |
| Client holds a stale `meta_epoch` | `RANGE_MOVED` (11) + full list-ranges body; client refreshes and retries |

### Invariants

- **RANGE-INV-01 (single owner).** After every applied meta entry the table is
  an ordered, gapless, non-overlapping cover of the keyspace; every key resolves
  to exactly one range and one group.
- **RANGE-INV-02 (no lost / duplicated keys).** A move never creates, drops, or
  duplicates a user key. Verified in sim across a crash at every point
  mid-migrate (`move_range_no_lost_or_duplicated_keys_across_crash`).
- **RANGE-INV-03 (identity preserved).** `range_id` and `[start, end)` are
  unchanged by a move; only `group_id` and the epochs move.
- **RANGE-INV-04 (monotone epochs).** `meta_epoch` and the range `epoch`
  strictly increase on an applied move and never advance on a rejected one.
- **RANGE-INV-05 (total order).** All layout changes — split, merge, move — are
  totally ordered by the group-0 log and CAS'd on `meta_epoch`.

---

## 3e. Orphan Raft group reclaim (#30)

A merge (§3b) drops the right range from the table but does not tear down the
Raft group that owned it — that group is now *orphaned*: still hosted, no
longer referenced by any range. Reclaim runs as part of the normal drain pass
(`cluster::replication::drain_and_apply`, driven by the tick loop — no
separate background task) and unhosts + deletes the group's on-disk data.

**Candidate set:** `{ gid ∈ hosted_group_ids : gid != 0, gid < next_group_id,
gid ∉ referenced_group_ids }`, where `next_group_id` and `referenced_group_ids`
are read from the range table under one lock (`referenced_group_ids()` in
`replication.rs`) so both reflect the same snapshot.

**Invariants:**

1. **Never reclaim while meta still references the group.** A group only
   becomes a candidate after a committed `RangeMeta` snapshot has already
   dropped it from the table (§3b step 4 happens before reclaim ever
   considers the group; the CAS-apply in `apply_range_meta_entry` and the
   reclaim pass both run under `drain_and_apply`, so a group is either fully
   referenced or fully dropped from every reclaim pass's point of view — no
   window where a range still resolves to a group mid-reclaim).
2. **Never reclaim a group pre-hosted for a not-yet-committed split.**
   `SPLIT_RANGE` calls `ensure_group_hosted` for its new group *before*
   proposing the `RangeMeta` command that references it, so the leader can
   serve that group the instant the split succeeds. In the window between
   that call and the command committing, the new group is hosted,
   unreferenced (the old table is still live), and trivially "drained" (a
   fresh node has `commit_index == last_applied == 0`) — indistinguishable
   from a real orphan by invariants 1/3 alone. `next_group_id` only advances
   when a `RangeMeta` commits, so a not-yet-committed split's group id always
   equals the table's current `peek_next_group_id()`; excluding
   `gid >= next_group_id` from the candidate set closes the race (regression
   test: `reclaim_skips_group_pre_hosted_for_in_flight_split`).
   **Known bounded leak:** if that split's propose never commits (leader
   steps down, channel drop before commit), its pre-hosted group stays
   hosted — this rule can never reclaim it, since nothing advances
   `next_group_id` past an id that was never committed — until the *next
   successful* split on this table bumps the counter past it, at which point
   it becomes an ordinary referenced-check candidate (invariant 1). This
   mirrors the pre-existing "orphan stays hosted until reclaim runs" leak
   window merge already had; it is not a regression, and is bounded by the
   next successful split rather than unbounded.
3. **Never touch group 0.**
4. **Drain gate:** a candidate is only reclaimed once it is quiescent
   (`RaftStatus.commit_index == RaftStatus.last_applied` — nothing
   replicated-but-unapplied left in flight for that group). A candidate that
   isn't drained yet is skipped and retried on the next pass.
5. **Idempotent / crash-safe:** reclaim (a) removes the group from
   `MultiRaftHost`, (b) drops its `RaftPersister` / `RaftApplyIndex` map
   entries, (c) `remove_dir_all`s `{data_dir}/groups/{id}`, then (d)
   increments the reclaim counter. A crash between any of these steps is
   harmless: on restart, `ClusterNode` startup only (re)hosts groups the
   *persisted range table* still references (§3c step 2), so an orphan is
   never rehosted regardless of how far reclaim got; the next drain pass
   simply repeats step (c) (`remove_dir_all` on an already-missing directory
   is not an error) and, if the host entry still exists, (a)-(b) as well.

**Metrics** (`GET /metrics`, `kaya-server/src/metrics.rs`):
- `kaya_range_orphan_groups` (gauge) — live count of the candidate set.
- `kaya_range_orphan_groups_reclaimed_total` (counter) — cumulative reclaims
  since process start.

---

## 4. Wire protocol

| Opcode | Name | Notes |
|---|---|---|
| 15 | `LIST_RANGES` | Response: meta_epoch + descriptors |
| 16 | `SPLIT_RANGE` | Request: split_key; response: two half descriptors |
| 17 | `MERGE_RANGE` | Request: left_start; response: one merged descriptor |
| 20 | `REBALANCE_PLAN` | **Advisory only.** Range-count heuristic; suggests moves for opcode 21 |
| 21 | `MOVE_RANGE` | Live migrate: reassign a range to a target group (admin; operator token) |
| 22 | `TXN_FORWARD` | Internal. Cross-group 2PC coordinator → participant group leader; see `transactions-spec.md` §17.4 |

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
it as a suggestion only; apply a suggested move with `MOVE_RANGE` (21).

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

### MOVE_RANGE request

```text
start_len(u32 LE) | start_key | target_group(u64 LE)
```

Admin opcode: operator token via `ADMIN\x00` framing when the server is started
with `--operator-token`. `start_key` must match a range start exactly (empty is
valid for the first range). Response uses the list-ranges layout with `count=1`
for the moved descriptor.

---

## 5. CLI

```text
kayactl --server <addr> range list
kayactl --server <addr> range split <key>
kayactl --server <addr> range merge <left-start-hex-or-utf8>
kayactl --server <addr> [--operator-token <tok>] range move <range-start> <target-group>
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
| Multi-node range move / learner | Yes (learner + promote + MOVE_RANGE) |
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
| Orphan group reclaim after merge | Yes (§3e; `test_range_merge_reclaims_orphan_group`) |
| Auto size-threshold split | No (manual + API first) |
| Live range migrate / MOVE_RANGE (21) | Yes (routing cutover; `range move`) |
| No lost/duplicated keys across crash mid-migrate (sim) | Yes (`move_range_no_lost_or_duplicated_keys_across_crash`) |
| Migrate under concurrent load (IT) | Yes (`test_range_move_under_concurrent_load`) |
| Chaos: multi-range bank sum under move + kill | Yes (`bank-mr-move`, documented subset) |
| Physical key copy (snapshot + delta + dual-write) | No — shared engine makes it a no-op; spec'd in §3d for per-group engines |

---

## 7. Related

- `spec/docs/multi-raft-spec.md` (M20 foundation)
- `kaya_raft::StaticRangeTable::split_at` / `merge_with_next` / `move_range`
- `kaya-server` opcodes 15/16/17/18/19/20/21
- `kaya_server::cluster::replication::reclaim_orphan_groups` (§3e, issue #30)
- `docs/runbooks/move-range.md` (operator runbook)
