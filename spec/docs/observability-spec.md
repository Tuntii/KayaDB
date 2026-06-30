# Observability Spec

**Status:** Draft v0.1  
**Scope:** Logs, metrics, recovery diagnostics, simulation traces and Linux eBPF experiments (M12)  

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
    pub wal_fsync_total_us: u64,
    pub wal_fsync_max_us: u64,
    pub memtable_entries: u64,
    pub sstable_count: u64,
    pub last_sequence: u64,
}
```

Userspace WAL fsync latency (`total_us` + `max_us`) was added together with the Linux eBPF experiments. It provides the application-visible cost of strict durability fsyncs and pairs naturally with kernel eBPF histograms.

Track A (2026-06) added:
- `flush_total_us` / `flush_max_us`
- `compaction_total_us` / `compaction_max_us`
(Full wall time of the publish operations in the engine; complements the WAL-only timers and the `syscall-timeline.bt` probe that surfaces the rename/unlink/fsyc points.)

Remaining future metrics (v2+):

- Per-operation append latency (beyond just fsync),
- recovery duration,
- compaction input/output bytes + duration,
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

## 7. Linux eBPF experiments (M12)

**Status (2026-06):** bpftrace scripts + in-process `kaya-ebpf` runtime. Optional `--ebpf` on `kayadb-server`; no hard dependency.

Implemented (scripts + in-process runtime + CLI + userspace metrics) — Track A updates:

- `crates/kaya-ebpf` — attach/detach probe manager, userspace tap + seeded simulated fallback, `trace.jsonl` replay validation, Prometheus `kaya_ebpf_*` histograms.
- `kayadb-server --ebpf [--ebpf-seed N]` — starts probe runtime on Linux; default-off elsewhere.
- `kayactl ebpf status` / `kayactl ebpf trace wal` — read `{data_dir}/ebpf/status.json` and WAL lines from `trace.jsonl`; graceful non-Linux guidance.

- `scripts/ebpf/fsync-latency.bt` — syscall-level fsync/fdatasync latency histograms (µs).
- `scripts/ebpf/block-io-latency.bt` — block layer I/O latency histograms (reads vs writes).
- `scripts/ebpf/syscall-timeline.bt` — write/fsync/fdatasync + rename/unlink + TID correlation + publish timeline for flush/compaction (Track A).
- `kayactl ebpf`:
  - `fsync-latency`, `block-latency`, `syscall-timeline` (with `--pid`, `--run`)
  - `list` + `status` (discover all local kayadb-server PIDs for clusters; show active bpftrace traces)
  - Improved auto-detect and cross-platform graceful messages.
- Userspace latency in `EngineStats` (WAL fsync + new `flush_total_us`/`max`, `compaction_total_us`/`max`) + exposure in `kayactl stats`, server `status` JSON, and human printers. Designed to be compared side-by-side with eBPF histograms.
- Full usage + correlation notes in `scripts/ebpf/README.md`. `kayactl ebpf list` is the recommended way to find PIDs.

v2+ (future) eBPF tooling may still add:

- Per-file / data-dir filters + richer TID/PID attribution.
- Flamegraph helper integration.
- Trace correlation by PID/TID + userspace markers / USDT.
- Optional Rust eBPF crate (Aya or libbpf-rs) behind `ebpf` feature + `cfg(target_os = "linux")`.

Non-goals (unchanged):

- Kernel-specific hard dependency on the core crates.
- Requiring root / eBPF for normal `cargo test` or development workflows.
- Production observability claims or SLOs.

See also:
- ROADMAP.md (M12)
- scripts/ebpf/README.md
- `kayactl ebpf help` output

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
