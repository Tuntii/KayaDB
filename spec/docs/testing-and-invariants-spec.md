# Testing and Invariants Spec

**Status:** Draft v0.2  
**Scope:** Testing strategy, invariant catalog, regression policy  

---

## 1. Testing philosophy

KayaDB's testing strategy is based on this rule:

> If a bug can happen after crash, corruption, or reorder, there should eventually be a deterministic test for it.

Happy-path unit tests are necessary but not sufficient.

---

## 2. Test layers

```text
Unit tests
  ↓
Property tests
  ↓
Fault-injection tests
  ↓
Deterministic simulation
  ↓
Fuzzing
  ↓
Jepsen / distributed tests later
```

---

## 3. Unit test requirements

### 3.1 Disk

- path validation,
- append/read roundtrip,
- truncate,
- rename,
- fsync success/failure in SimDisk,
- crash reset behavior.

### 3.2 WAL

- encode/decode roundtrip,
- checksum validation,
- invalid magic,
- unsupported version,
- unknown flags,
- max payload rejection,
- segment ordering.

### 3.3 LSM

- memtable put/get/delete,
- memtable scan order,
- SSTable sorted write/read,
- footer validation,
- manifest replay.

### 3.4 Engine

- put/get/delete,
- scan prefix,
- restart persistence,
- recovery report.

---

## 4. Property tests

Suggested property test library: `proptest`.

### 4.1 WAL durable prefix property

Given a generated sequence of records and a generated crash byte offset:

- write prefix bytes,
- recover,
- recovered records must equal the longest valid prefix,
- decoder must never emit corrupted tail record.

### 4.2 Memtable model property

Generated operations over small keyspace:

- apply to memtable,
- apply to model `BTreeMap`,
- compare `get` and `scan` results.

### 4.3 SSTable roundtrip property

Generated sorted entries:

- write SSTable,
- read all entries,
- compare with input,
- random point lookups match model.

### 4.4 Compaction preservation property

Generated overlapping SSTables:

- compute visible model,
- compact,
- compute visible state from output,
- compare.

---

## 5. Crash tests

### 5.1 WAL crash points

```text
test_wal_crash_before_append
test_wal_crash_during_header
test_wal_crash_during_payload
test_wal_crash_after_append_before_fsync
test_wal_crash_after_fsync
test_wal_recovery_truncates_corrupt_tail
```

### 5.2 Engine crash points

```text
test_engine_crash_after_wal_fsync_before_memtable_apply
test_engine_crash_after_ack
test_engine_crash_during_flush_tmp_write
test_engine_crash_after_sstable_rename_before_manifest
test_engine_crash_after_manifest_fsync
```

### 5.3 Compaction crash points

```text
test_compaction_crash_before_output_publish
test_compaction_crash_after_output_rename_before_manifest
test_compaction_crash_after_manifest_edit
test_compaction_obsolete_inputs_not_required_after_publish
```

---

## 6. Fuzzing targets

Targets:

```text
fuzz_wal_decoder
fuzz_sstable_footer
fuzz_sstable_block
fuzz_manifest_decoder
fuzz_command_frame_decoder
```

Fuzzer requirements:

- malformed input must not panic,
- oversized lengths must be rejected before huge allocation,
- checksum mismatch must produce corruption error,
- no unsafe memory access.

---

## 7. Invariant catalog

### 7.1 Architecture invariants

| ID | Invariant |
|---|---|
| ARCH-001 | A crate does not depend upward in the ownership graph |
| ARCH-002 | Server/CLI cannot bypass engine write path |
| ARCH-003 | Persistent files become live only through owner publication protocol |
| ARCH-004 | Recovery can run without server components |

### 7.2 Disk invariants

| ID | Invariant |
|---|---|
| DSK-001 | Path traversal is impossible |
| DSK-002 | Successful fsync makes file stable in SimDisk |
| DSK-003 | Failed fsync does not imply durability |
| DSK-004 | Crash resets volatile state to stable state |
| DSK-005 | Fault schedule is deterministic |

### 7.3 WAL invariants

| ID | Invariant |
|---|---|
| WAL-001 | LSNs are contiguous and monotonic |
| WAL-002 | Recovery returns a valid prefix |
| WAL-003 | Bad checksums are rejected |
| WAL-004 | Strict ACK implies record recovered after crash |
| WAL-005 | Recovery is idempotent |
| WAL-006 | Tail truncation does not remove valid records |
| WAL-007 | Oversized lengths are rejected before allocation |

### 7.4 Manifest invariants

| ID | Invariant |
|---|---|
| MAN-001 | Manifest replay deterministically reconstructs live table set |
| MAN-002 | File existence alone never makes an SSTable live |
| MAN-003 | Missing manifest-live SSTable fails open |
| MAN-004 | Corrupted non-tail manifest record fails open |
| MAN-005 | Compaction publication is atomic at manifest edit level |

### 7.5 LSM invariants

| ID | Invariant |
|---|---|
| LSM-001 | SSTable entries are sorted by key |
| LSM-002 | SSTable checksum mismatch rejected |
| LSM-003 | Manifest replay determines live files |
| LSM-004 | Flush publication is atomic via manifest |
| LSM-005 | Compaction preserves visible state |
| LSM-006 | Tombstone hides older put |
| LSM-007 | Scan returns sorted unique visible keys |
| LSM-008 | Recovery after flush crash is idempotent |

### 7.6 Engine invariants

| ID | Invariant |
|---|---|
| ENG-001 | Strict successful write survives crash |
| ENG-002 | GET matches reference model |
| ENG-003 | DELETE hides key |
| ENG-004 | SCAN matches sorted model prefix |
| ENG-005 | Recovery is idempotent |
| ENG-006 | Missing required live file fails open |

### 7.7 Future Raft invariants

| ID | Invariant |
|---|---|
| RFT-001 | At most one leader per term |
| RFT-002 | Committed entries are not lost |
| RFT-003 | Log matching property holds |
| RFT-004 | State machine applies entries in log order |
| RFT-005 | Minority partition cannot commit |

---

## 8. Regression policy

Every correctness bug should produce one of:

- unit regression test,
- property test seed,
- simulation seed + trace,
- fuzz corpus input,
- TLA+ spec update.

Bug report template:

```text
KayaDB version:
Command:
Seed:
Trace path:
Expected:
Actual:
Relevant invariant:
```

---

## 9. CI policy

Pull request CI:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
cargo test -p kaya-sim small_seed_suite
```

Optional/manual:

```bash
cargo fuzz run fuzz_wal_decoder
kayadb-sim --seed-range 0..100 --ops 100000
```

Do not run long fuzzing in every PR initially.

---

## 10. Acceptance criteria

Testing system is ready when:

- invariant IDs appear in test names or failure messages,
- WAL property tests exist,
- SimDisk crash tests exist,
- fuzz target skeletons exist,
- CI runs deterministic small simulation,
- failure traces are saved as artifacts locally.
