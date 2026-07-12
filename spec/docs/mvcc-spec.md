# MVCC Spec

**Status:** Draft v0.1  
**Scope:** Multi-version concurrency control storage foundation (M16)  
**Primary crate:** `kaya-lsm` (codec), `kaya-engine` (visibility orchestration)  
**Depends on:** `lsm-storage-format-spec.md`, `wal-spec.md`, `engine-api-spec.md`  

---

## 1. Purpose

M16 introduces versioned storage so a key can retain multiple historical values
and snapshot reads can observe a consistent cut of the store at a chosen
timestamp. Everything above (transactions, indexes, CDC, multi-raft) builds on
this layer.

This spec defines:

- internal key encoding,
- visibility rules for point and scan reads,
- tombstone semantics,
- safe garbage-collection watermark rules,
- dual-read compatibility with pre-MVCC SSTables (v1–v3),
- relationship between WAL sequence and `commit_ts` for M16.

Memtable multi-version storage, SSTable v4, and engine `ReadOptions` are
specified here at the contract level; their implementation lands in subsequent
M16 tasks.

---

## 2. Terminology

| Term | Meaning |
|---|---|
| user_key | Logical key as seen by clients and the WAL |
| commit_ts | Monotonic timestamp of a version; for M16 equals `SequenceNumber` |
| internal_key | Physical key used in memtable/SST BTree order: user_key + inverted ts |
| read_ts | Timestamp bound for a snapshot read (`ReadTimestamp::At`) |
| Latest | Default visibility: highest committed version of each user_key |
| tombstone | A `Delete` version recorded at a `commit_ts` |
| GC watermark | Lower bound for versions that may still be required by open snapshots |
| dual-read | Ability to read v1–v3 SST entries alongside versioned (v4+) entries |

---

## 3. Commit timestamp assignment (M16)

**Decision (locked):** for M16, `commit_ts == SequenceNumber`.

- The WAL continues to assign a monotonic sequence on each durable write.
- The engine maps that WAL sequence into the version's `commit_ts` on apply.
- The WAL binary format is **unchanged**: records still carry user_keys and
  Put/Delete ops; encoding to internal keys happens above the WAL layer.
- HLC (hybrid logical clock) may replace the assignment source in M20; the
  internal_key wire layout remains the same (u64 commit_ts suffix).

Implications:

- Strict durability still means ACK only after WAL fsync of the record that
  established the sequence / commit_ts.
- Crash recovery rebuilds memtable versions from WAL using each record's
  sequence as commit_ts.

---

## 4. Internal key layout

### 4.1 Encoding

```text
internal_key = user_key ‖ (u64::MAX - commit_ts).to_be_bytes()
```

- `user_key`: raw bytes (may be empty; empty is legal but rare).
- Suffix: 8 bytes, big-endian encoding of `u64::MAX - commit_ts`.
- Constant: `COMMIT_TS_LEN = 8`.

Rust surface (`kaya-lsm`):

```rust
pub const COMMIT_TS_LEN: usize = 8;

pub fn encode_internal_key(user_key: &[u8], commit_ts: u64) -> Bytes;
pub fn encode_internal_key_seq(user_key: &[u8], seq: SequenceNumber) -> Bytes;
pub fn user_key_of(internal_key: &[u8]) -> &[u8];
pub fn commit_ts_of(internal_key: &[u8]) -> u64;
pub fn matches_user_key(internal_key: &[u8], user_key: &[u8]) -> bool;
```

Decoding rules for short / legacy keys:

- If `internal_key.len() < COMMIT_TS_LEN`, treat the whole slice as the user_key
  and return `commit_ts = 0` (legacy / dual-read path helper; production v4
  writers always append the full suffix).

### 4.2 Ordering properties

Because the inverted timestamp is appended as big-endian bytes:

1. **Different user_keys** order by user_key lexicographic ASC (unsigned bytes).
2. **Same user_key** orders by commit_ts DESC (newest first): higher commit_ts
   sorts **before** lower commit_ts in BTree / SSTable block order.

Example for user_key `k`:

| commit_ts | suffix bytes meaning | relative order |
|---|---|---|
| 20 | `u64::MAX - 20` | first (smaller internal key) |
| 10 | `u64::MAX - 10` | later |

This matches RocksDB-style "newest first" iteration within a key, so a point
lookup can stop at the first version with `commit_ts ≤ read_ts`.

### 4.3 Comparison contract

Ordering must be identical across:

- multi-version memtable keys,
- SSTable v4 data blocks (internal keys in entry key field),
- compaction merge iterators,
- scan / prefix bounds built from user_key prefixes.

Prefix scans over user_keys remain valid: every internal key for user_key `U`
starts with `U`, so a BTree range starting at `U` (or the first internal key of
`U`) and ending before the next user_key successor covers all versions of
matching user_keys. Implementations must not treat the inverted-ts suffix as
part of the logical prefix.

---

## 5. Value records and tombstones

Logical version payload (unchanged shape):

```rust
pub enum ValueRecord {
    Put { value: Bytes, sequence: SequenceNumber },
    Delete { sequence: SequenceNumber },
}
```

For versioned storage:

- The map key is the **internal key** (user_key + inverted commit_ts).
- `sequence` / commit_ts on the record must equal the commit_ts embedded in the
  internal key.
- A **tombstone** is a `Delete` version at some `commit_ts`. It does not remove
  older versions from storage; it only hides them from readers whose
  `read_ts ≥` that tombstone's commit_ts (see §6).

Same `(user_key, commit_ts)` must not be written twice with conflicting
payloads. Writers (WAL apply) assign unique sequences; compaction must treat
duplicate identical versions as idempotent.

---

## 6. Visibility rules

### 6.1 Point get

```text
get(user_key, read_ts) → Option<Value>
```

Algorithm:

1. Consider all versions of `user_key` (memtable + live SSTables).
2. Among versions with `commit_ts ≤ read_ts`, select the one with the **newest**
   `commit_ts` (equivalently: first in internal-key order for that user_key that
   satisfies the bound).
3. Result:
   - if that version is `Put { value, .. }` → return `value`
   - if that version is `Delete { .. }` → return not-found (Delete sentinel at
     the LSM layer; engine maps Delete / missing to the same client not-found)
   - if no version has `commit_ts ≤ read_ts` → not-found

### 6.2 Latest (default API)

Default client API remains LWW-style:

```text
get(user_key) ≡ get(user_key, read_ts = +∞ / max retained commit_ts)
```

Implementation may use `ReadTimestamp::Latest` which means "ignore upper bound
and take the newest version of the key across all sources."

### 6.3 Snapshot / At

```text
get_at(user_key, read_ts) ≡ get(user_key, read_ts)
```

`read_ts` is inclusive: a version committed exactly at `read_ts` is visible.

### 6.4 Scan

Prefix / range scans apply the same visibility rule **per user_key**:

- For each distinct user_key in range, emit at most one visible value under the
  chosen `read_ts` (or Latest).
- Tombstoned user_keys at the chosen bound are omitted from user-visible scans.
- Internal iterators used by flush/compaction/inspect may emit **all** versions
  including tombstones (`raw_scan` / `iter`).

### 6.5 Cross-source merge

When merging memtable, immutable memtables, and SST levels for the same
user_key, the global newest version with `commit_ts ≤ read_ts` wins. Sequence /
commit_ts is the sole ordering authority; file generation is not.

---

## 7. GC watermark

### 7.1 Definition

The **GC watermark** is a `u64` lower bound maintained by the engine:

- Starts at `0` in M16 (retain all versions) until transactions (M17) publish
  an active snapshot horizon.
- Compaction **must never drop** a version with `commit_ts ≥ watermark`.
- Versions with `commit_ts < watermark` may be dropped **only** under the safe
  rules below.

### 7.2 Safe drop rules

For a given user_key, during compaction merge of its versions, a version `V`
with `V.commit_ts < watermark` may be dropped only if one of the following
holds:

**Rule A — superseded by a retained newer version**

There exists another version `N` of the same user_key such that:

- `N.commit_ts ≥ watermark`, and
- `N.commit_ts > V.commit_ts`

(whether `N` is Put or Delete). Then `V` can never be chosen by any reader with
`read_ts ≥ watermark`, so it is safe to drop.

**Rule B — obsolete tombstone with no later version**

`V` is a tombstone (`Delete`) with `V.commit_ts < watermark`, and there is **no**
version of the same user_key with `commit_ts > V.commit_ts`. Then:

- No open snapshot needs to see an older put under this key (all such puts are
  also `< watermark` and hidden by the tombstone for any `read_ts ≥ watermark`).
- The tombstone and all older versions of the key may be dropped together.

**Rule C — never drop the sole covering version below watermark without Rule A/B**

If the newest version of a user_key has `commit_ts < watermark` and is a **Put**,
it must be **retained** until either a newer version/tombstone appears at or
above the watermark (Rule A path after that write) or the watermark advances
past it **and** a tombstone qualifies under Rule B. Compaction must not
silently delete the last visible value for Latest readers.

Summary table:

| Version V | Condition | Action |
|---|---|---|
| `commit_ts ≥ watermark` | always | **retain** |
| `commit_ts < watermark` | exists newer N with `N.commit_ts ≥ watermark` | **drop** V (Rule A) |
| tombstone, `commit_ts < watermark`, no newer version | Rule B | **drop** tombstone and older |
| Put, newest, `commit_ts < watermark` | no newer version | **retain** (Rule C) |

### 7.3 Invariants

1. Snapshot reads with `read_ts ≥ watermark` never observe a missing version that
   GC removed (GC only drops versions that cannot be selected under that bound).
2. Compaction is identity-preserving for all visible states at any
   `read_ts ≥ watermark` and for Latest.
3. Watermark only moves **forward** (non-decreasing).
4. M16 default watermark `0` means no GC drops (all commit_ts are ≥ 0; in
   practice treat as "retain everything" until M17 advances the horizon).

---

## 8. Dual-read: SSTable v1–v3 compatibility

Pre-MVCC SSTables store **user_key only** in the entry key field, with a
separate `sequence` field on the entry.

Dual-read rules:

1. A v1–v3 entry is treated as a **single version** of its user_key at
   `commit_ts = entry.sequence` (or the sequence carried on the ValueRecord).
2. Visibility selection treats that single version exactly like a v4 version
   with the same user_key and commit_ts.
3. Bloom filters on v1–v3 remain user_key based; v4 blooms also key on user_key
   (not internal key) so dual-read lookups stay efficient.
4. Compaction that rewrites v1–v3 data into v4 **must** encode internal keys
   using `encode_internal_key(user_key, sequence)` and preserve the sequence
   field for inspect/compat as commit_ts.
5. Short keys (`len < 8`) or keys that are not internal-encoded are never
   misinterpreted as inverted-ts suffixes when the table format version is
   v1–v3; format version is authoritative.

---

## 9. WAL interaction

| Concern | M16 policy |
|---|---|
| WAL record key | user_key (unchanged) |
| WAL record ops | Put / Delete (unchanged) |
| Sequence assignment | WAL / engine sequence (unchanged) |
| commit_ts source | same sequence (`commit_ts == SequenceNumber`) |
| Internal key encode | engine / LSM on apply, not on WAL write |
| Recovery | replay WAL → insert versioned memtable entries |

No WAL format version bump is required for M16.

---

## 10. SSTable v4 (preview)

SSTable format version v4 stores **internal keys** in the entry key field.
The entry `sequence` field still holds `commit_ts` for inspect tools and for
symmetry with dual-read.

Detailed binary layout (footer, block encoding, compression) remains owned by
`lsm-storage-format-spec.md` and is updated when v4 lands. This document owns
the **semantic** key/value versioning contract.

---

## 11. Engine API surface (preview)

```text
ReadTimestamp::Latest
ReadTimestamp::At(commit_ts)

get(key)                      // Latest
get_with_options(key, opts)   // may set read_ts
scan_prefix(prefix)           // Latest visible puts
scan_prefix_at(prefix, ts)    // snapshot scan
```

Default API remains Latest so existing clients keep LWW behavior without
changes.

---

## 12. Invariants checklist

| ID | Invariant |
|---|---|
| MVCC-1 | Internal key order is user_key ASC, commit_ts DESC |
| MVCC-2 | `encode`/`user_key_of`/`commit_ts_of` round-trip for all user_keys and ts |
| MVCC-3 | `get(k, t)` returns newest non-tombstone Put with `commit_ts ≤ t`, else not-found |
| MVCC-4 | Tombstone at `t` hides older puts for all `read_ts ≥ t` |
| MVCC-5 | Compaction never drops `commit_ts ≥ watermark` |
| MVCC-6 | Compaction drops only versions allowed by §7.2 Rules A–C |
| MVCC-7 | v1–v3 entries behave as single version at their sequence |
| MVCC-8 | WAL format unchanged; seq is commit_ts for M16 |
| MVCC-9 | Crash recovery rebuilds the same version set from durable WAL prefix |

---

## 13. Non-goals (M16)

- Interactive transactions / write intents (M17).
- HLC assignment (M20).
- Cross-shard timestamps (M20+).
- Changing the default client LWW API.
- Dropping support for reading v1–v3 tables.

---

## 14. Decision records (MVCC)

| ID | Decision | Rationale |
|---|---|---|
| MVCC-D-001 | `commit_ts == SequenceNumber` in M16 | WAL already assigns seq; avoid dual clocks |
| MVCC-D-002 | Inverted big-endian ts suffix | Newest-first BTree/block order without custom comparator |
| MVCC-D-003 | WAL stays user-key oriented | Avoid WAL version bump; encode at apply |
| MVCC-D-004 | Default read is Latest | Backward compatible LWW API |
| MVCC-D-005 | Watermark starts at 0 | No GC until snapshot horizon exists (M17) |
