# Secondary Index Spec (M18 foundation)

**Status:** Draft v0.1  
**Scope:** Engine-local secondary indexes over primary KV data  
**Milestone:** M18 (foundation; not full production index suite)

---

## 1. Purpose

Secondary indexes let callers find primary keys by a secondary attribute without a
full table scan. This foundation ships a **minimal but real** maintenance path:

- Index definition stored under system keys
- Index entries stored under system keys
- Automatic maintenance on non-txn `put` / `delete`
- Same path covers `txn_commit` materialization (intents → put/delete)

Full M18 goals (online pause/resume backfill, chaos divergence gate, `kayactl index`,
conformance vectors v2, unique indexes) remain follow-on work.

---

## 2. MVP model

| Concept | MVP behavior |
|---|---|
| Indexed keys | User keys that start with `primary_prefix` |
| Secondary key | **Full primary value** (no field extraction) |
| Unique | Always `false` (duplicates allowed) |
| Atomicity with primary | Best-effort same write path; separate WAL records per index entry |
| Backfill | Synchronous scan of `primary_prefix` at `create_index` |

Example: index `by_email` with `primary_prefix = b"user:"`  
Primary put `user:42 → alice@x.com` yields index entry secondary=`alice@x.com`, primary=`user:42`.

---

## 3. System key layout

All index keys live under the reserved prefix `\x00idx/`. Public `put` / `delete` /
`txn_put` / `txn_delete` **reject** this prefix.

### 3.1 Metadata

```text
key   = "\x00idx/meta/" || name_utf8
value = version_u8(1) || unique_u8 || prefix_len_u32be || primary_prefix
```

### 3.2 Data entries

Prefix-scan friendly layout (secondary sorts first after the name header):

```text
key   = "\x00idx/data/"
        || name_len_u16be || name_utf8
        || secondary
        || primary
        || primary_len_u32be
value = empty (or future payload)
```

`scan_by_index(name, value_prefix)` scans:

```text
"\x00idx/data/" || name_len || name || value_prefix
```

and decodes `(secondary, primary)` pairs.

---

## 4. Engine API

```rust
impl Engine {
    pub async fn create_index(&mut self, name: &str, primary_prefix: &[u8]) -> Result<()>;
    pub fn list_indexes(&self) -> Vec<String>;
    pub async fn drop_index(&mut self, name: &str) -> Result<()>;
    pub async fn scan_by_index(
        &mut self,
        name: &str,
        value_prefix: &[u8],
    ) -> Result<Vec<(Bytes /*secondary*/, Bytes /*primary_key*/)>>;
}
```

### 4.1 Name rules

- Length 1..=64
- ASCII alphanumeric, `_`, or `-`

### 4.2 Maintenance rules

On user `put(key, value)` (key not system):

1. Read previous latest value `old`
2. Write primary
3. For each index whose `primary_prefix` matches `key`:
   - If `old` exists and `old != value`, delete data key for `(old, key)`
   - Put data key for `(value, key)`

On user `delete(key)`:

1. Read previous latest value `old`
2. Write primary tombstone
3. If `old` exists, delete data key for `(old, key)` on matching indexes

`txn_commit` materializes intents via `put`/`delete`, so indexes update then.

---

## 5. Recovery

Index metadata and entries are ordinary WAL-backed keys. After `Engine::open`,
metadata under `\x00idx/meta/` is scanned into an in-memory map. Data entries do
not need a separate rebuild if WAL + SST recovery is correct.

---

## 6. Invariants (foundation)

| ID | Invariant |
|---|---|
| IDX-I-001 | After successful `put` of an indexed key, `scan_by_index` finds `(value, key)` |
| IDX-I-002 | After successful `delete` of an indexed key, no entry remains for that primary |
| IDX-I-003 | Public API cannot write reserved `\x00idx/` keys |
| IDX-I-004 | Index metadata reloads after crash/reopen |

---

## 7. Limitations (explicit)

1. **Not multi-record atomic:** primary + index entries are separate WAL sequences; a crash mid-maintenance can leave temporary divergence (full M18 will tighten via same-txn intent batches / commit records).
2. **Value-as-secondary only** — no JSON path / column extraction.
3. **No unique enforcement.**
4. **No online backfill control** (pause/resume/progress).
5. **No wire opcodes / kayactl** in this foundation (engine API only).
6. **No chaos divergence gate** yet.
7. Empty `primary_prefix` is rejected (avoids accidental whole-space indexes).

---

## 8. Out of scope → later M18

- Transactional co-commit of primary + index as one logical unit under Raft
- `kayactl index create|list|scan|verify`
- Conformance vectors v2
- Automated index↔primary divergence checker under chaos
- Partial field extractors and unique indexes

---

## 9. Decisions log

| ID | Decision | Rationale |
|---|---|---|
| IDX-D-001 | System key prefix `\x00idx/` | Isolated from user space; easy reject |
| IDX-D-002 | Secondary = full value | Smallest useful MVP without a schema layer |
| IDX-D-003 | Maintain on put/delete | Covers non-txn path and txn materialization |
| IDX-D-004 | Length-suffixed data keys | Correct decode with arbitrary secondary/primary bytes |
