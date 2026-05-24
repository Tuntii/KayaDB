# Observability Spec

**Status:** Draft v0.1  
**Scope:** Logs, metrics, recovery diagnostics, simulation traces and future eBPF tooling  

---

## 1. Purpose

KayaDB should be inspectable when it works and especially when it fails.

Observability must support:

- recovery debugging,
- WAL/SSTable/manifest inspection,
- simulation trace replay,
- performance measurements,
- future Linux/eBPF visibility.

---

## 2. Structured logs

Recommended fields:

```text
timestamp
level
target
component
event
request_id optional
lsn optional
sequence optional
path optional
error optional
```

Important events:

| Component | Events |
|---|---|
| engine | open, recovery_start, recovery_complete, close |
| wal | segment_open, append, fsync, recover_record, truncate_tail |
| lsm | memtable_freeze, flush_start, flush_publish, compaction_start, compaction_publish |
| manifest | current_read, edit_append, edit_fsync, replay_complete |
| disk | fault_injected in SimDisk |
| server | bind, request_start, request_done, shutdown |
| sim | seed, operation, nemesis_decision, invariant_check |

---

## 3. Metrics

Initial engine metrics:

```rust
pub struct EngineStats {
    pub put_count: u64,
    pub get_count: u64,
    pub delete_count: u64,
    pub scan_count: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_count: u64,
    pub memtable_entries: u64,
    pub sstable_count: u64,
    pub last_sequence: u64,
}
```

Future metrics:

- WAL append latency,
- fsync latency,
- recovery duration,
- compaction input/output bytes,
- block cache hits/misses,
- read amplification estimates,
- active immutable memtable count,
- pending compaction bytes.

---

## 4. Recovery diagnostics

Recovery output should be available programmatically and through CLI.

Minimum fields:

```text
manifest_records_replayed
live_sstable_count
wal_records_replayed
wal_truncated_bytes
tmp_files_removed
last_lsn
last_sequence
warnings
```

Warnings must be stable enum values, not just free-form strings.

---

## 5. Inspectors

Inspector commands are observability features and test fixtures.

Required inspectors:

- `kayactl inspect wal <path>`
- `kayactl inspect manifest <path>`
- `kayactl inspect sstable <path>`
- `kayactl recover --dry-run` later

Inspector parser must reuse production decoder logic where possible.

---

## 6. Simulation traces

Simulation traces are JSON Lines for MVP.

Trace must include:

- seed,
- config hash or embedded config,
- operation stream,
- fault decisions,
- disk events,
- invariant checks,
- failure summary if any.

Trace files are correctness artifacts, not performance logs.

---

## 7. Future eBPF scope

v2+ eBPF tooling may include:

- fsync latency probes,
- block I/O latency histograms,
- syscall timeline for KayaDB process,
- flamegraph helper integration,
- trace correlation by PID/TID.

Non-goals for MVP:

- kernel-specific hard dependency,
- requiring root privileges for normal tests,
- production observability claims.

---

## 8. Invariants

| ID | Invariant |
|---|---|
| OBS-001 | Recovery diagnostics expose truncation and warnings |
| OBS-002 | Inspectors reuse production decoders or equivalent test-covered logic |
| OBS-003 | Simulation trace contains enough decisions for replay |
| OBS-004 | Relaxed durability mode is visible in logs/diagnostics |
| OBS-005 | Metrics are not used as source of truth for correctness |

---

## 9. Acceptance criteria

Observability is ready when:

- recovery report is exposed,
- WAL inspector exists,
- simulation failures write trace + summary + config,
- strict vs relaxed durability appears in logs,
- metrics can be retrieved from engine,
- corruption diagnostics include path/offset/kind where safe.
