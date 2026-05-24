# Deterministic Simulation Spec

**Status:** Draft v0.2  
**Scope:** Deterministic storage simulator for MVP; extensible to network/Raft later  

---

## 1. Purpose

The simulator makes failure reproducible.

A failing seed should be a first-class bug artifact:

```bash
kayadb-sim --seed 0xdeadbeef --ops 100000 --nemesis disk,node-crash
kayadb-sim --replay traces/failure-0xdeadbeef.trace.jsonl
```

A failure that cannot be replayed is much less useful.

---

## 2. Principles

1. **Determinism** — same seed + same binary + same config should produce same logical event sequence.
2. **Traceability** — important decisions are written to trace.
3. **Model checking by execution** — a simple reference model runs alongside the real engine.
4. **Failure as normal input** — crashes, partial writes, failed fsyncs and corruption are expected paths.

---

## 3. Components

```text
SimRunner
  ├─ SimRng
  ├─ SimClock
  ├─ SimScheduler
  ├─ SimDisk
  ├─ OperationGenerator
  ├─ ReferenceModel
  ├─ Nemesis
  ├─ InvariantChecker
  └─ TraceRecorder
```

### 3.1 SimRng

Requirements:

- no thread-local random,
- no wall clock for decisions,
- RNG forks are logged or derived deterministically.

### 3.2 SimClock

Fake time:

```rust
pub struct SimClock {
    now: SimTime,
}
```

MVP storage tests may not need timers, but the clock should exist for future Raft election timeouts.

### 3.3 SimScheduler

MVP can be single-threaded:

```text
choose next operation
apply to engine
maybe inject fault
check invariant
```

Future scheduler can interleave async tasks.

---

## 4. Operation generator

Operations:

```text
PUT random_key random_value
GET random_key
DELETE random_key
SCAN random_prefix
FLUSH
COMPACT
CRASH_RESTART
```

Config:

```toml
[sim]
seed = "0xdeadbeef"
ops = 100000
keyspace = 1000
max_value_bytes = 1024
put_weight = 50
delete_weight = 10
get_weight = 30
scan_weight = 5
crash_weight = 3
flush_weight = 1
compact_weight = 1
```

---

## 5. Reference model

The model is a simple in-memory map:

```rust
BTreeMap<Vec<u8>, Vec<u8>>
```

Strict mode model:

- if engine returns success for write, apply to model,
- after crash/restart, engine state must match model,
- ambiguous operations that crash before response require special handling.

MVP can avoid ambiguous client-response crashes by injecting crashes between operations.

---

## 6. Nemesis

MVP nemeses:

```text
DiskPartialWrite
DiskFsyncFailure
DiskCorruptTail
CrashRestart
DiskFull
```

Future nemeses:

```text
PacketLoss
PacketDuplicate
PacketReorder
NetworkPartition
ClockJump
SlowFsync
CompactionPause
NodeKill
```

Interface:

```rust
pub trait Nemesis {
    fn before_op(&mut self, ctx: &mut SimContext, op: &Operation);
    fn after_op(&mut self, ctx: &mut SimContext, op: &Operation, result: &OperationResult);
}
```

Fault decisions must be recorded.

---

## 7. Trace format

MVP trace should be JSON Lines for inspectability.

Example:

```json
{"event_id":1,"kind":"sim_start","seed":"0xdeadbeef","config_hash":"..."}
{"event_id":2,"kind":"op","op_id":1,"command":"put","key":"6b31","value_len":12}
{"event_id":3,"kind":"disk_write","path":"wal/0000000000000001.wal","offset":0,"requested":80,"written":80}
{"event_id":4,"kind":"disk_fsync","path":"wal/0000000000000001.wal","result":"ok"}
{"event_id":5,"kind":"op_result","op_id":1,"result":"ok","lsn":1,"sequence":1}
{"event_id":6,"kind":"crash"}
{"event_id":7,"kind":"restart"}
{"event_id":8,"kind":"invariant_check","id":"ENG-001","result":"ok"}
```

Required event fields:

- `event_id`,
- `kind`,
- enough operation-specific data for replay,
- result/error.

---

## 8. Replay mode

Replay mode reads trace and enforces the same decisions:

```bash
kayadb-sim --replay traces/failure-0xdeadbeef.trace.jsonl
```

Replay must fail if:

- binary cannot understand trace version,
- event sequence diverges,
- operation result differs before expected failure,
- expected invariant violation does not occur.

---

## 9. Failure artifact

On failure, simulator writes:

```text
traces/
  failure-0xdeadbeef.trace.jsonl
  failure-0xdeadbeef.summary.txt
  failure-0xdeadbeef.config.toml
```

Summary example:

```text
Invariant violation: ENG-002
Seed: 0xdeadbeef
Operation: 98211
Expected: key=6b31 value=76616c2d3939
Actual: NOT_FOUND
Replay: kayadb-sim --replay traces/failure-0xdeadbeef.trace.jsonl
```

---

## 10. CI simulation policy

CI should run:

```text
small deterministic seeds: 10 seeds × 1,000 ops
nightly/local manual: 100 seeds × 100,000 ops
```

Avoid flaky timing-based tests. Simulation must not depend on wall-clock sleeps.

---

## 11. Invariants checked by simulator

| ID | Meaning |
|---|---|
| ENG-001 | strict ACK survives crash/recovery |
| ENG-002 | GET matches reference model |
| ENG-003 | DELETE hides key |
| ENG-004 | SCAN matches sorted model prefix |
| WAL-002 | recovery returns prefix |
| WAL-004 | ACK implies recovered prefix |
| LSM-005 | compaction preserves model |

---

## 12. Acceptance criteria

Simulator is ready when:

- same seed produces same generated operation sequence,
- SimDisk fault decisions are recorded,
- crash/restart works,
- model comparison catches incorrect GET/SCAN,
- invariant failure writes replayable trace,
- replay reproduces failure.
