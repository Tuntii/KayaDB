# M16–M20 Distributed Transactional KV Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take KayaDB from single-Raft LWW KV (v0.1.47) through MVCC (M16), single-group ACID txns (M17), secondary indexes (M18), CDC (M19), and multi-raft foundation (M20).

**Architecture:** Versioned internal keys (`user_key ‖ commit_ts`) under SSTable v4; snapshot reads + compaction GC watermark; then write intents + Raft commit records + TXN opcodes; indexes/CDC on the txn stack; finally multi-group Raft + HLC + static ranges. Discipline: spec → sim → implement → chaos/Jepsen → docs.

**Tech Stack:** Rust workspace (kaya-core, kaya-lsm, kaya-engine, kaya-wal, kaya-sim, kaya-raft, kaya-net, kaya-server, kaya-client, kaya-jepsen-test, kayactl).

**Design spec:** [`docs/superpowers/specs/2026-07-12-m16-m25-roadmap-design.md`](../specs/2026-07-12-m16-m25-roadmap-design.md)

**Worktree:** `.worktrees/feat-m16-m20` on branch `feat/m16-m20-transactional-kv`

**Validation (every landing task):**
```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1
```

**Key design decisions (locked):**
1. **commit_ts == SequenceNumber** for M16 (WAL already assigns seq). HLC replaces assignment source in M20.
2. **Internal key wire:** `user_key ‖ (u64::MAX - commit_ts).to_be_bytes()` so BTree/block order is user_key ASC, newest-ts first.
3. **SSTable v4** stores internal keys in entry `key` field; `sequence` field still holds commit_ts for inspect/compat. v1–v3 remain readable as single-version (legacy user-key only).
4. **WAL format unchanged** — still user-key Put/Delete; engine maps seq → internal key on apply.
5. **Default API remains LWW-visible** (`ReadTimestamp::Latest`); snapshot reads opt-in via `ReadOptions`.
6. **GC watermark** starts at 0 (retain all) until M17 sets active snapshot horizon; compaction never drops versions ≥ watermark.

---

## File map (new / primary touch)

| File | Responsibility |
|------|----------------|
| `crates/kaya-lsm/src/internal_key.rs` | encode/decode/compare/user_key extract |
| `crates/kaya-lsm/src/lib.rs` | multi-version Memtable + re-exports |
| `crates/kaya-lsm/src/sstable.rs` | SST_VERSION_V4, multi-version get/scan, bloom on user_key |
| `crates/kaya-engine/src/lib.rs` | ReadTimestamp::At, GC watermark, versioned paths |
| `crates/kaya-engine/src/memtable.rs` | engine-side get/scan with read_ts |
| `crates/kaya-engine/src/flush.rs` / compaction path in lib | multi-version flush + GC-aware compact |
| `crates/kaya-sim/src/model.rs` | versioned RefModel |
| `crates/kaya-sim/src/runner.rs` | snapshot-read invariants |
| `crates/kayactl/src/inspect.rs` | display user_key + commit_ts for v4 |
| `spec/docs/mvcc-spec.md` | M16 format + visibility rules |
| `spec/docs/transactions-spec.md` | M17 SI + conflict |
| later: txn/intent modules, multi-raft host, HLC |

---

# Phase M16 — MVCC storage foundation

### Task 1: MVCC spec + internal_key module

**Files:**
- Create: `spec/docs/mvcc-spec.md`
- Create: `crates/kaya-lsm/src/internal_key.rs`
- Modify: `crates/kaya-lsm/src/lib.rs` (mod + pub use)
- Modify: `spec/docs/00-spec-index.md` (link mvcc-spec if present)
- Test: unit tests inside `internal_key.rs`

- [ ] **Step 1: Write `spec/docs/mvcc-spec.md`** covering:
  - Internal key layout (user_key + descending commit_ts suffix)
  - Visibility: `get(user_key, read_ts)` returns newest version with `commit_ts ≤ read_ts` that is not a tombstone
  - Tombstones as Delete at commit_ts
  - GC watermark: drop versions with `commit_ts < watermark` only when a newer version or tombstone exists with `commit_ts ≥ watermark` OR when tombstone itself is `< watermark` and no newer version
  - Dual-read: v1–v3 SST entries are treated as single version at their `sequence`
  - WAL unchanged; engine uses WAL sequence as commit_ts

- [ ] **Step 2: Write failing tests for internal key codec**

```rust
// crates/kaya-lsm/src/internal_key.rs tests
#[test]
fn encode_decode_roundtrip() {
    let ik = encode_internal_key(b"abc", 42);
    assert_eq!(user_key_of(&ik), b"abc");
    assert_eq!(commit_ts_of(&ik), 42);
}

#[test]
fn newer_ts_sorts_before_older_for_same_user_key() {
    let a = encode_internal_key(b"k", 10);
    let b = encode_internal_key(b"k", 20);
    assert!(b < a); // descending ts in key order
}

#[test]
fn different_user_keys_order_by_user_key() {
    let a = encode_internal_key(b"a", 1);
    let b = encode_internal_key(b"b", 999);
    assert!(a < b);
}

#[test]
fn user_key_prefix_bound() {
    let k = encode_internal_key(b"user", 5);
    assert!(user_key_of(&k).starts_with(b"us"));
}
```

- [ ] **Step 3: Implement**

```rust
// crates/kaya-lsm/src/internal_key.rs
use kaya_core::{Bytes, SequenceNumber};

pub const COMMIT_TS_LEN: usize = 8;

pub fn encode_internal_key(user_key: &[u8], commit_ts: u64) -> Bytes {
    let mut out = Vec::with_capacity(user_key.len() + COMMIT_TS_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    out
}

pub fn encode_internal_key_seq(user_key: &[u8], seq: SequenceNumber) -> Bytes {
    encode_internal_key(user_key, seq.get())
}

pub fn user_key_of(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() < COMMIT_TS_LEN {
        return internal_key;
    }
    &internal_key[..internal_key.len() - COMMIT_TS_LEN]
}

pub fn commit_ts_of(internal_key: &[u8]) -> u64 {
    if internal_key.len() < COMMIT_TS_LEN {
        return 0;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&internal_key[internal_key.len() - COMMIT_TS_LEN..]);
    u64::MAX - u64::from_be_bytes(buf)
}

/// True if `internal_key` is a version of `user_key` (exact user_key match).
pub fn matches_user_key(internal_key: &[u8], user_key: &[u8]) -> bool {
    user_key_of(internal_key) == user_key
}
```

Export from `lib.rs`. Run tests.

- [ ] **Step 4: Commit**

```bash
git add spec/docs/mvcc-spec.md crates/kaya-lsm/src/internal_key.rs crates/kaya-lsm/src/lib.rs spec/docs/00-spec-index.md
git commit -m "$(cat <<'EOF'
feat(lsm): M16 internal key codec and mvcc-spec

Add user_key‖desc-commit_ts encoding and the MVCC visibility/GC
rules document. Foundation for versioned storage.
EOF
)"
```

---

### Task 2: Multi-version Memtable

**Files:**
- Modify: `crates/kaya-lsm/src/lib.rs` (Memtable)
- Keep: ValueRecord API; change map to multi-version

**Design:** Map key = internal key. `put`/`delete` insert a new version (never overwrite same ts; same user_key can have many). `get(user_key)` / `get_at(user_key, read_ts)` select visible version. Legacy `get` = Latest.

- [ ] **Step 1: Write failing tests** in `lib.rs` or `tests` module:

```rust
#[test]
fn memtable_keeps_two_versions() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v1".to_vec(), SequenceNumber::new(1));
    m.put(b"k".to_vec(), b"v2".to_vec(), SequenceNumber::new(2));
    assert_eq!(m.get_at(b"k", 1).unwrap().put_value(), Some(b"v1".as_ref()));
    assert_eq!(m.get_at(b"k", 2).unwrap().put_value(), Some(b"v2".as_ref()));
    assert_eq!(m.get(b"k").unwrap().put_value(), Some(b"v2".as_ref()));
}

#[test]
fn memtable_tombstone_hides_older() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v1".to_vec(), SequenceNumber::new(1));
    m.delete(b"k".to_vec(), SequenceNumber::new(2));
    assert!(matches!(m.get_at(b"k", 2), Some(ValueRecordRef::Delete { .. }) | None));
    // Latest delete → get returns Delete or treat as None at engine layer
    assert_eq!(m.get_at(b"k", 1).unwrap().put_value(), Some(b"v1".as_ref()));
}
```

Helper: implement `put_value()` on ValueRecordRef or match in tests.

- [ ] **Step 2: Implement multi-version Memtable**
  - Store `BTreeMap<Bytes /*internal*/, ValueRecord>` but put/delete take **user_key** and encode internally
  - OR keep user_key API that encodes to internal on insert
  - `raw_scan_prefix` / `iter` return all versions (internal keys or (user_key, seq, value))
  - `scan_prefix` returns Latest-visible puts only (user keys)
  - `scan_prefix_at(prefix, read_ts)` for snapshot scans
  - `approximate_bytes` still tracks all versions
  - Preserve `freeze()` if present

- [ ] **Step 3: Fix all kaya-lsm / engine compile breakages** from Memtable API changes. Engine put/delete still pass user keys.

- [ ] **Step 4: Run** `cargo test -p kaya-lsm --lib` and fix.

- [ ] **Step 5: Commit** `feat(lsm): multi-version memtable with snapshot get_at`

---

### Task 3: SSTable v4 multi-version reader/builder

**Files:**
- Modify: `crates/kaya-lsm/src/sstable.rs`
- Modify: `crates/kaya-lsm/tests/format_fixtures.rs`
- Create fixtures under `crates/kaya-lsm/tests/fixtures/sstable_v4_*.sst`

- [ ] **Step 1: Constants**
  - `SST_VERSION_V4: u16 = 4`
  - Accept v4 in `decode_footer`
  - Builder: when any entry has internal-key-length semantics OR always for new builds once engine enables MVCC — write `format_version = 4`
  - Prefer: **new builder default stays v2/v3 for non-MVCC path**; add `SstableBuildOptions { mvcc: bool }` or engine always builds v4 after Task 4

- [ ] **Step 2: Bloom on user_key** for v4 (hash `user_key_of(entry.key)`)

- [ ] **Step 3: `SstableReader::get_at(user_key, read_ts) -> Option<SstEntry>`**
  - Locate first candidate via index using seek key = `encode_internal_key(user_key, read_ts)` or lower-bound on user_key
  - Scan versions of that user_key; pick first (newest) with `commit_ts ≤ read_ts`
  - Legacy v1–v3: exact user_key match; treat sequence as commit_ts

- [ ] **Step 4: `get(user_key)` = `get_at(user_key, u64::MAX)`**

- [ ] **Step 5: `scan_prefix_at`** — for each user_key under prefix, emit visible version at read_ts

- [ ] **Step 6: Unit tests** multi-version roundtrip in sstable.rs tests

- [ ] **Step 7: Golden fixtures** — generate `sstable_v4_valid.sst` with 2 versions of one key + second key; register in format_fixtures

- [ ] **Step 8: Commit** `feat(lsm): SSTable v4 multi-version get/scan and fixtures`

---

### Task 4: Engine snapshot reads + GC watermark + versioned flush/compact

**Files:**
- Modify: `crates/kaya-engine/src/lib.rs`, `memtable.rs`, `flush.rs`, compact path, `stats.rs` if needed
- Modify: `crates/kaya-core/src/lib.rs` only if config field needed (`gc_watermark` on EngineConfig optional)

- [ ] **Step 1: Expand ReadTimestamp**

```rust
pub enum ReadTimestamp {
    #[default]
    Latest,
    At(u64), // commit_ts inclusive upper bound
}
```

Implement `get` / `scan_prefix` honoring `read_at`.

- [ ] **Step 2: Engine stores `gc_watermark: u64`** (default 0). Methods:
  - `set_gc_watermark(ts: u64)`
  - `gc_watermark() -> u64`

- [ ] **Step 3: Flush writes internal keys** (encode with sequence) and SST v4

- [ ] **Step 4: Compaction merges all versions; drops only when GC rules allow** (see mvcc-spec)

- [ ] **Step 5: Integration tests in kaya-engine**
  - put v1, put v2, get_at(v1) sees v1
  - delete then get_at older sees old put
  - flush + reopen preserves versions
  - compact with watermark drops old versions safely

- [ ] **Step 6: All existing engine tests stay green** (Latest semantics)

- [ ] **Step 7: Commit** `feat(engine): MVCC snapshot reads, flush/compact GC`

---

### Task 5: Versioned RefModel + sim property tests

**Files:**
- Modify: `crates/kaya-sim/src/model.rs`, `runner.rs`
- Tests: existing sim suite + new MVCC cases

- [ ] **Step 1: RefModel** stores `BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>` (user → ts → value/None)
- [ ] **Step 2: `get_at` / `put(seq)` / `delete(seq)` / `scan_prefix_at`**
- [ ] **Step 3: Runner tracks seq from engine WriteResult; optional read_at ops**
- [ ] **Step 4: Property test: after crash, get_at matches model for all written ts**
- [ ] **Step 5: Commit** `test(sim): versioned RefModel and MVCC crash properties`

---

### Task 6: kayactl inspect v4 + docs/CHANGELOG/ROADMAP M16 exit

**Files:**
- Modify: `crates/kayactl/src/inspect.rs`
- Modify: `ROADMAP.md` (M16 ✅ when gates met)
- Modify: `CHANGELOG.md` Unreleased
- Modify: `docs/architecture.md` brief MVCC note if needed

- [ ] **Step 1: inspect shows `user_key` + `commit_ts` for internal keys / v4**
- [ ] **Step 2: Mark M16 items done in ROADMAP when tests green**
- [ ] **Step 3: Commit** `docs: M16 MVCC exit notes and inspect v4`

**M16 exit gate:** snapshot-read + GC safety in sim; workspace tests green.

---

# Phase M17 — Single-group ACID transactions

### Task 7: transactions-spec.md

- [ ] Create `spec/docs/transactions-spec.md`: Snapshot Isolation, write-write conflict detection, intent lifecycle, commit record, rollback, crash recovery. Isolation default SI; serializable stretch.

### Task 8: Write intents in engine

- [ ] Intent key encoding: e.g. `\x00intent/` prefix or `ValueRecord::Intent { txn_id, value }`
- [ ] APIs: `put_intent`, `clear_intents(txn_id)`, `check_write_conflict(key, from_ts)`, `commit_intents(txn_id, commit_ts)`
- [ ] Persist via WAL/Raft as special records; crash-safe
- [ ] Unit + crash tests

### Task 9: RaftCommand + TXN opcodes + server dispatch

- [ ] `RaftCommand`: IntentWrite, TxnCommit, TxnAbort (or batch commit record)
- [ ] Opcodes 9–12: TXN_BEGIN, TXN_OP, TXN_COMMIT, TXN_ROLLBACK
- [ ] Server txn session map; commit proposes atomic record
- [ ] Status: TXN_CONFLICT

### Task 10: Rust client txn API

- [ ] `Transaction` with RYW buffer; begin/get/put/delete/commit/rollback
- [ ] Conformance vectors for txn payloads

### Task 11: TLA+ commit protocol model (deferred formal tooling OK)

- [ ] `spec/specs/txn/TxnCommit.tla` + `.cfg` minimal model of intents + commit
- [ ] README how to run TLC if available; otherwise model as documentation

### Task 12: Jepsen bank workload + exit gate

- [ ] `WorkloadType::Bank` with multi-key transfer via txn API
- [ ] Invariant: sum of balances constant
- [ ] Scenario under kill + partition
- [ ] ROADMAP M17 ✅ when green

---

# Phase M18 — Secondary indexes

### Task 13: Index metadata + transactional maintenance

- [ ] Index definition stored in system prefix; entries written in same txn as primary
- [ ] `kayactl index create/list/verify`
- [ ] Online backfill with pause/resume
- [ ] Index-driven scan opcode or engine API
- [ ] Divergence checker under chaos
- [ ] Conformance v2

---

# Phase M19 — CDC / changefeeds

### Task 14: Raft-log changefeed

- [ ] Per-consumer cursor/checkpoint; per-key order; at-least-once
- [ ] TCP + file sinks
- [ ] `backup --incremental` on CDC checkpoints
- [ ] Rust + Go subscribe API
- [ ] Chaos: no lost events across leader failover

---

# Phase M20 — Multi-raft foundation

### Task 15: HLC type + integration

- [ ] `kaya_core::Hlc { physical_ms, logical }` with `update` / `now`
- [ ] Use as commit_ts source (map HLC to u64 packing or replace SequenceNumber assignment carefully — prefer pack physical<<16|logical for M20)

### Task 16: Envelope group_id + per-group storage

- [ ] `Envelope.group_id`; codec multiplex; legacy group 0
- [ ] `data_dir/groups/{id}/raft-*`
- [ ] Transport demux to group channels

### Task 17: Multi-raft host + static ranges + coalesced ticks

- [ ] N RaftNode per process; shared tick/heartbeat coalescing
- [ ] Static range table: key → group
- [ ] Client ops route by key
- [ ] OTel trace-context propagation v1 (node↔node↔client)
- [ ] Live clock-skew nemesis hook
- [ ] Jepsen per-range green
- [ ] ROADMAP M20 ✅

---

## Execution notes for subagents

1. Work only in worktree: `C:\Users\tunay\Documents\GitHub\KayaDB\.worktrees\feat-m16-m20`
2. One task per implementer; TDD where steps say so
3. Do not change WAL golden fixtures for v1 formats
4. Do not claim production-ready
5. Prefer smallest change that keeps Latest LWW tests green
6. After M16 complete, continue M17 without waiting for human

## Status tracking

| Task | Milestone | Status |
|------|-----------|--------|
| 1 | M16 | pending |
| 2 | M16 | pending |
| 3 | M16 | pending |
| 4 | M16 | pending |
| 5 | M16 | pending |
| 6 | M16 | pending |
| 7–12 | M17 | pending |
| 13 | M18 | pending |
| 14 | M19 | pending |
| 15–17 | M20 | pending |
