# Benchmarking Spec

**Status:** Draft v0.1  
**Scope:** Benchmark goals, methodology, reporting policy and non-goals  

---

## 1. Purpose

KayaDB is correctness-first, but performance must still be measurable.

Benchmarking exists to:

- prevent severe regressions,
- understand durability trade-offs,
- compare design alternatives,
- document hardware-dependent behavior honestly.

Benchmarks must not create misleading production-readiness claims.

---

## 2. Initial targets

Directional targets from the PRD:

| Mode | Target | Notes |
|---|---:|---|
| relaxed PUT | at least 5,000 ops/sec | commodity Linux hardware |
| hot GET | at least 50,000 ops/sec | memtable/block-cache hot path |
| strict PUT | hardware-dependent | fsync dominates |
| recovery | 1M WAL records in reasonable time | exact target after implementation |

These are not blockers for the earliest internal milestones. Correctness regressions beat benchmark wins.

---

## 3. Benchmark categories

### 3.1 Microbenchmarks

- WAL encode/decode throughput,
- checksum cost,
- memtable get/put/scan,
- SSTable block decode,
- manifest replay.

### 3.2 Storage path benchmarks

- WAL append relaxed,
- WAL append strict fsync per record,
- grouped fsync later,
- recovery from N records,
- SSTable flush throughput,
- compaction throughput.

### 3.3 End-to-end benchmarks

- embedded PUT/GET/DELETE/SCAN,
- CLI overhead for small operations,
- server request throughput later.

---

## 4. Reporting format

Every published benchmark result should include:

```text
KayaDB commit:
Build profile:
OS/kernel:
CPU:
RAM:
Disk model/filesystem:
Durability mode:
Dataset size:
Key/value sizes:
Operation mix:
Command used:
Result:
```

Do not publish bare ops/sec numbers without context. Numbers without context are vibes in a trench coat.

---

## 5. Correctness-first benchmark rules

- Do not disable fsync and call it strict.
- Do not compare relaxed mode to another system's durable mode.
- Do not benchmark with checksums disabled unless clearly labeled.
- Do not use unsafe shortcuts without matching correctness tests.
- Any benchmark-specific feature flag must be clearly non-default.

---

## 6. CI policy

PR CI should not run long benchmarks.

Allowed in PR CI:

- compile benchmark targets,
- tiny smoke benchmarks if fast and non-flaky.

Manual/nightly:

- full benchmark matrix,
- historical comparison,
- regression threshold alerts.

---

## 7. Future benchmark harness

Potential crate/tool:

```text
benches/
  wal_append.rs
  wal_recovery.rs
  memtable.rs
  sstable.rs
  engine_workload.rs
```

Potential workload config:

```toml
[workload]
ops = 1000000
keyspace = 100000
value_bytes = 256
put_weight = 50
get_weight = 45
delete_weight = 5
```

---

## 8. Invariants

| ID | Invariant |
|---|---|
| BENCH-001 | Benchmark mode labels durability truthfully |
| BENCH-002 | Benchmark reports include environment context |
| BENCH-003 | Performance optimization does not remove required correctness checks |
| BENCH-004 | CI benchmarks are non-flaky and bounded |

---

## 9. Acceptance criteria

Benchmarking baseline is ready when:

- WAL append/recovery benchmarks exist,
- engine workload benchmark exists,
- benchmark README documents hardware/reporting format,
- strict vs relaxed results are clearly labeled,
- benchmark code does not bypass production write path unless explicitly testing internals.
