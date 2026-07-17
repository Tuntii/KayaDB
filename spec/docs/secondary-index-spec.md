# Secondary Index Spec (M18 + polish)

**Status:** Draft v0.2  
**Scope:** Engine-local secondary indexes over primary KV data  
**Milestone:** M18 (production path + polish)

---

## 1. Purpose

Secondary indexes let callers find primary keys by a secondary attribute without a
full table scan:

- Index definition stored under system keys
- Index entries stored under system keys
- Automatic maintenance on non-txn `put` / `delete` (and Raft apply of the same)
- Same path covers `txn_commit` materialization (intents → put/delete)
- Field extractors, online backfill pause/resume, `verify_index`, `kayactl index`

---

## 2. Model

| Concept | Behavior |
|---|---|
| Indexed keys | User keys that start with `primary_prefix` |
| Secondary key | From [`IndexExtractor`](#3-extractors) (default: full value) |
| Unique | Always `false` (duplicates allowed) |
| Atomicity with primary | Best-effort same write path; separate WAL records per index entry |
| Backfill | `Sync` (default) or `Online` with step/pause/resume |

Example: index `by_email` with `primary_prefix = b"user:"` and field extractor
delimiter `|`, index `1`.  
Primary put `user:42 → alice|alice@x.com` yields secondary=`alice@x.com`.

---

## 3. Extractors

| Kind | Wire tag | Secondary |
|---|---|---|
| `WholeValue` | 0 | Full primary value |
| `Prefix { len }` | 1 | First `len` bytes (or whole if shorter) |
| `Field { delimiter, index }` | 2 | Split on delimiter; 0-based field. Missing → skip entry |

---

## 4. System key layout

All index keys live under the reserved prefix `\x00idx/`. Public `put` / `delete` /
`txn_put` / `txn_delete` **reject** this prefix.

### 4.1 Metadata (v2)

```text
key   = "\x00idx/meta/" || name_utf8
value = version_u8(2) || unique_u8 || extractor_tag_u8
        || params_len_u16be || params
        || prefix_len_u32be || primary_prefix
```

v1 meta (whole-value only) is still decoded for recovery compatibility.

### 4.2 Data entries

```text
key   = "\x00idx/data/"
        || name_len_u16be || name_utf8
        || secondary
        || primary
        || primary_len_u32be
value = empty
```

---

## 5. Engine API

```rust
create_index(name, primary_prefix)                 // sync, WholeValue
create_index_with(name, primary_prefix, CreateIndexOptions { extractor, backfill })
list_indexes() / get_index(name) / drop_index(name)
scan_by_index(name, value_prefix)
index_backfill_step(name, batch) / pause / resume / status
verify_index(name) -> Vec<IndexDivergence>
```

### 5.1 Backfill modes

| Mode | Behavior |
|---|---|
| `Sync` | Scan prefix and index all keys before return |
| `Online` | Register meta immediately; operator drives `index_backfill_step` |

Live `put`/`delete` always maintain the index once registered (online backfill
catches historical keys).

### 5.2 Verify / divergence gate

`verify_index` compares expected secondaries from primary values against data
entries. Empty result ⇒ consistent for the current latest snapshot.

---

## 6. CLI

```text
kayactl index create <name> <prefix> [--online] [--extractor whole|prefix|field ...]
kayactl index list | drop <name> | scan <name> [value_prefix] | verify <name>
kayactl index backfill <pause|resume|step|status> <name> [--batch N]
```

---

## 7. Invariants

| ID | Invariant |
|---|---|
| IDX-I-001 | After successful `put` of an indexed key with extractable value, `scan_by_index` finds it |
| IDX-I-002 | After successful `delete`, no entry remains for that primary |
| IDX-I-003 | Public API cannot write reserved `\x00idx/` keys |
| IDX-I-004 | Index metadata reloads after crash/reopen |
| IDX-I-005 | After churn (put/update/delete), `verify_index` is empty |

---

## 8. Limitations

1. Primary + index entries are separate WAL sequences (not one logical Raft record).
2. No unique enforcement.
3. Online backfill progress is **not** durable across reopen (status resets to Complete for loaded indexes).
4. No wire opcodes for index admin (local/`kayactl` only).
5. Empty `primary_prefix` is rejected.
