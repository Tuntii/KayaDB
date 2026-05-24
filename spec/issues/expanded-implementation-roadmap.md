# KayaDB Expanded Implementation Roadmap

**Status:** Draft v0.1  
**Source:** PRD + technical spec expansion  

Bu dosya spec'leri uygulanabilir issue setlerine böler. Issue numaraları GitHub'a taşınırken değişebilir; burada kalıcı ID olarak `KD-*` kullanılır.

---

## Milestone M0 — Project Skeleton

### KD-0001 Create Rust workspace and crate layout

Relevant specs:

- `spec/docs/architecture-spec.md`
- `spec/docs/contributor-workflow-spec.md`

Scope:

- root `Cargo.toml`,
- crates: `kaya-core`, `kaya-io`, `kaya-wal`, `kaya-lsm`, `kaya-engine`, `kaya-sim`, `kayactl`,
- minimal `lib.rs`/`main.rs`,
- root README.

Acceptance criteria:

- `cargo test --workspace` passes,
- each crate compiles,
- README links to `spec/README.md`,
- no storage behavior implemented yet beyond stubs.

### KD-0002 Add CI for fmt, clippy and tests

Acceptance criteria:

- CI runs `cargo fmt --check`,
- CI runs `cargo clippy --workspace --all-targets -- -D warnings`,
- CI runs `cargo test --workspace`,
- status badge can be added to README.

### KD-0003 Add contribution templates and labels

Relevant specs:

- `spec/docs/contributor-workflow-spec.md`

Acceptance criteria:

- issue template exists,
- PR checklist exists,
- labels documented,
- correctness/invariant guidance visible.

---

## Milestone M1 — Disk Layer and WAL Format

### KD-0101 Define core types and error model

Relevant specs:

- `spec/docs/engine-api-spec.md`
- `spec/docs/security-and-safety-spec.md`

Scope:

- `KayaError`,
- `Result<T>`,
- `Lsn`,
- `SequenceNumber`,
- `KeyBytes`,
- `ValueBytes`,
- basic config structs.

Acceptance criteria:

- typed errors cover invalid argument, corruption, IO, fsync failure, unsupported version,
- errors are usable across crates,
- no panic-based corruption handling.

### KD-0102 Define `RelativePath`

Relevant specs:

- `spec/docs/disk-and-io-spec.md`
- `spec/docs/security-and-safety-spec.md`

Acceptance criteria:

- rejects absolute paths,
- rejects `..`,
- normalizes simple relative paths,
- path traversal tests cover Linux and Windows-like examples,
- invariant `DSK-001` referenced in tests.

### KD-0103 Define `Disk` trait

Acceptance criteria:

- supports read, write, append, fsync file, fsync dir, truncate, rename, remove, list, file_len,
- documents short write and failed fsync semantics,
- has shared contract tests prepared for implementations.

### KD-0104 Implement `FileDisk`

Acceptance criteria:

- append/read roundtrip,
- write_at/read_at roundtrip,
- truncate works,
- rename works,
- list_dir works,
- path traversal impossible,
- writes serialized per file in MVP.

### KD-0105 Implement WAL record structs and constants

Relevant specs:

- `spec/docs/wal-spec.md`
- `spec/docs/format-versioning-spec.md`

Acceptance criteria:

- magic/version/header length constants match spec,
- record type enum exists,
- LSN and sequence fields are typed,
- format docs link from code comments.

### KD-0106 Implement WAL encoder

Acceptance criteria:

- encodes PUT, DELETE and NOOP,
- little-endian fields,
- payload CRC32C,
- header CRC32C with checksum field zeroed,
- roundtrip tests for PUT/DELETE.

### KD-0107 Implement WAL decoder

Acceptance criteria:

- decodes valid records,
- rejects bad magic,
- rejects unsupported version,
- rejects unknown flags,
- rejects bad header checksum,
- rejects bad payload checksum,
- rejects oversized payload before allocation,
- malformed data does not panic.

---

## Milestone M2 — WAL Writer, Recovery and SimDisk

### KD-0201 Implement WAL segment writer

Acceptance criteria:

- appends records to active segment,
- returns LSN, sequence, segment id, offset and encoded length,
- strict mode fsyncs before success,
- fsync failure returns error and no ACK,
- `WAL-004` covered.

### KD-0202 Implement WAL recovery reader

Acceptance criteria:

- discovers segment files,
- sorts by segment id,
- reads records sequentially,
- stops on partial header,
- stops on partial payload,
- stops on checksum failure,
- returns valid prefix.

### KD-0203 Implement corrupted tail truncation

Acceptance criteria:

- partial/corrupt tail truncated to last good offset,
- truncation reported in `WalRecoveryReport`,
- valid records before tail remain recoverable,
- idempotence test exists.

### KD-0204 Implement `SimDisk` stable/volatile model

Relevant specs:

- `spec/docs/disk-and-io-spec.md`
- `spec/docs/simulation-spec.md`

Acceptance criteria:

- writes update volatile state,
- successful fsync updates stable state,
- crash resets volatile to stable,
- lost unfsynced write test exists,
- same disk contract tests as FileDisk run where applicable.

### KD-0205 Add deterministic fault schedule to `SimDisk`

Acceptance criteria:

- partial write injection,
- fsync failure injection,
- deterministic seed/rule behavior,
- trace records injected fault,
- replay-oriented event shape exists.

### KD-0206 Add WAL durable-prefix property test

Acceptance criteria:

- random record sequences,
- random truncation/corruption point,
- recovery equals valid prefix,
- no corrupted tail record emitted.

---

## Milestone M3 — Minimal Engine

### KD-0301 Implement memtable

Relevant specs:

- `spec/docs/lsm-storage-format-spec.md`

Acceptance criteria:

- put/get/delete,
- scan prefix sorted,
- tombstone hides put,
- property test against `BTreeMap` model.

### KD-0302 Implement engine PUT/GET/DELETE over WAL + memtable

Acceptance criteria:

- strict PUT writes WAL before ACK,
- GET reads memtable latest visible value,
- DELETE writes tombstone,
- write result includes sequence/LSN/durable,
- engine tests use `SimDisk`.

### KD-0303 Implement engine recovery from WAL

Relevant specs:

- `spec/docs/recovery-spec.md`

Acceptance criteria:

- open data dir,
- recover WAL prefix,
- rebuild memtable,
- data survives restart,
- recovery report exposed.

### KD-0304 Implement `kayactl put/get/delete/scan`

Relevant specs:

- `spec/docs/cli-ux-spec.md`

Acceptance criteria:

- local data directory option,
- clear output,
- exit code 2 for not found,
- write output includes sequence/LSN/durable,
- JSON output for write results.

---

## Milestone M4 — SSTable and Manifest

### KD-0401 Implement SSTable writer/reader

Acceptance criteria:

- sorted entries written,
- footer magic/version/checksum validation,
- index block loaded,
- point lookup works,
- corruption returns typed error.

### KD-0402 Implement manifest record format and replay

Relevant specs:

- `spec/docs/manifest-spec.md`

Acceptance criteria:

- `CURRENT` read/write through temp+rename,
- CREATE_TABLE/DELETE_TABLE edits,
- checksum validation,
- replay reconstructs live table set,
- missing live SSTable fails open.

### KD-0403 Implement memtable flush to SSTable

Acceptance criteria:

- freeze memtable,
- write tmp SSTable,
- fsync file,
- rename to final path,
- fsync directory,
- append/fsync manifest edit,
- crash tests for publication points.

### KD-0404 Implement basic inspect commands

Acceptance criteria:

- `kayactl inspect wal`,
- `kayactl inspect manifest`,
- `kayactl inspect sstable`,
- JSON option for WAL inspector,
- corruption diagnostics include offset/kind.

---

## Milestone M5 — Compaction and Recovery Hardening

### KD-0501 Implement simple L0 compaction

Acceptance criteria:

- selects overlapping L0 tables,
- merges by key and sequence,
- preserves tombstones when uncertain,
- publishes output through one manifest edit,
- visible state property test passes.

### KD-0502 Add recovery idempotence test suite

Acceptance criteria:

- WAL tail truncation idempotent,
- manifest tail truncation idempotent if implemented,
- flush crash recovery idempotent,
- compaction crash recovery idempotent.

### KD-0503 Add fuzz target skeletons

Acceptance criteria:

- `fuzz_wal_decoder`,
- `fuzz_sstable_footer`,
- `fuzz_manifest_decoder`,
- malformed input does not panic.

---

## Milestone M6 — Deterministic Simulator

### KD-0601 Implement simulation runner

Relevant specs:

- `spec/docs/simulation-spec.md`

Acceptance criteria:

- seeded operation generator,
- reference model,
- invariant checker,
- trace recorder,
- crash/restart operation.

### KD-0602 Implement replay mode

Acceptance criteria:

- reads trace JSONL,
- enforces same operations/faults,
- detects divergence,
- reproduces expected invariant violation.

### KD-0603 Add CI small seed suite

Acceptance criteria:

- bounded deterministic seeds,
- runs in reasonable PR time,
- failure artifacts saved locally or as CI artifact,
- no wall-clock sleep based flakiness.

---

## First serious PR recommendation

Recommended first PR bundle:

```text
KD-0101 KD-0102 KD-0103 KD-0104 KD-0105 KD-0106 KD-0107
```

If scope is manageable, add:

```text
KD-0201 KD-0202
```

Keep deterministic fault schedule and full engine work as follow-up PRs if review size grows too large.
