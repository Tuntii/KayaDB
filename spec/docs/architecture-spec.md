# KayaDB Architecture Spec

**Status:** Draft v0.2  
**Scope:** Single-node MVP architecture plus future distributed boundaries  

---

## 1. Architectural principles

1. **Correctness before throughput**  
   Optimize edilmemiş ama doğru çalışan kod, hızlı ama belirsiz koddan iyidir.

2. **Failure is a first-class input**  
   Disk failure, partial write, crash, corruption ve ileride network partition test harness'ın doğal parçalarıdır.

3. **Boundaries must be testable**  
   Disk, WAL, manifest, storage engine ve Raft birbirinden izole test edilebilmelidir.

4. **Real and simulated backends share interfaces**  
   Gerçek disk ve simüle disk aynı trait üzerinden kullanılmalıdır.

5. **Every persistent format is inspectable**  
   WAL/SSTable/Manifest formatları hem dokümante edilir hem CLI ile okunabilir.

---

## 2. System overview

MVP single-node storage engine:

```text
kayactl / embedded caller
        |
        v
Engine API
        |
        +--> WAL manager
        +--> Memtable
        +--> SSTable manager
        +--> Manifest
        +--> Compactor
        v
Disk abstraction
        |
        +--> FileDisk
        +--> SimDisk
        +--> future IoUringDisk
```

Future distributed shape:

```text
Client
  |
  v
Server protocol
  |
  v
Raft group
  |
  v
Replicated log / apply loop
  |
  v
Storage engine
  |
  v
Disk backend
```

---

## 3. Crate layout

```text
crates/
  kaya-core/       shared types, errors, config, bytes utilities
  kaya-io/         Disk trait, FileDisk, SimDisk, future IoUringDisk
  kaya-wal/        WAL records, segments, recovery, inspector
  kaya-lsm/        memtable, SSTable, manifest, compaction
  kaya-engine/     public embedded engine API, write/read orchestration
  kaya-sim/        deterministic simulator, scheduler, traces, nemesis
  kaya-server/     process server, protocol handling
  kayactl/         CLI
  kaya-raft/       future Raft state machine
  kaya-net/        future transport abstraction
```

### 3.1 Dependency direction

Allowed dependency direction for MVP:

```text
kaya-core
  ^
  |
kaya-io        kaya-wal        kaya-lsm
  ^               ^              ^
  |               |              |
  +----------- kaya-engine -------+
                  ^
                  |
       kayactl / kaya-server / kaya-sim
```

Rules:

- `kaya-core` must stay dependency-light.
- `kaya-io` must not depend on `kaya-wal` or `kaya-lsm`.
- `kaya-wal` may depend on `kaya-io` only through traits/types needed for segment I/O.
- `kaya-lsm` may reuse generic framing/checksum helpers, but must not reuse WAL semantics accidentally.
- `kayactl` and `kaya-server` are thin wrappers over engine APIs.

---

## 4. Process lifecycle

### 4.1 Startup

```text
1. Parse config
2. Lock data directory
3. Open/create directory layout
4. Read CURRENT
5. Open manifest
6. Replay manifest
7. Discover WAL segments
8. Recover WAL durable prefix
9. Rebuild memtable or replay unapplied records
10. Open live SSTables
11. Delete or quarantine unreferenced tmp files
12. Start background flush/compaction workers if enabled
13. Accept API calls
```

Startup must fail if:

- data directory is locked by another process,
- manifest is corrupted beyond recoverable prefix,
- SSTable metadata references missing required files,
- incompatible persistent format version is detected,
- checksum mismatch occurs in a required non-tail structure.

Startup may recover automatically if:

- WAL tail is partial/corrupt,
- previous flush created an unreferenced SSTable,
- previous compaction left temporary files,
- `CURRENT` temp file exists but committed `CURRENT` is still valid.

### 4.2 Shutdown

```text
1. Stop accepting new writes
2. Drain in-flight writes
3. Flush WAL according to durability policy
4. Optionally flush memtable
5. Stop background workers
6. Release directory lock
```

Crash shutdown has no steps; recovery must handle it.

---

## 5. Data directory layout

```text
data/
  LOCK
  CURRENT
  MANIFEST-000001
  wal/
    0000000000000001.wal
    0000000000000002.wal
  sst/
    0000000000000001.sst
    0000000000000002.sst
  tmp/
  traces/
```

### 5.1 Persistence ownership

| Path | Owner | Publication rule |
|---|---|---|
| `LOCK` | engine/server | held while DB is open |
| `CURRENT` | manifest manager | temp + rename + directory sync |
| `MANIFEST-*` | manifest manager | append record + fsync |
| `wal/*.wal` | WAL manager | append + optional segment rotation |
| `sst/*.sst` | LSM writer | only live after manifest edit is durable |
| `tmp/*` | LSM/manifest/WAL tools | never live state by itself |
| `traces/*` | simulator | debug artifact only |

---

## 6. Write path

Strict durability write path:

```text
PUT key value
  ↓
Validate command
  ↓
Assign sequence number and LSN
  ↓
Encode WAL record
  ↓
Append WAL record
  ↓
fsync WAL segment
  ↓
Apply to memtable
  ↓
Return ACK
```

Important rule:

> In strict mode, ACK must not happen before WAL record is durable.

### 6.1 Crash points

| Crash point | Strict mode behavior |
|---|---|
| C1 before WAL append | write absent |
| C2 during WAL append | absent or rejected tail; no ACK |
| C3 after WAL append before fsync | may be absent unless fsync completed; no ACK |
| C4 during fsync | depends on fsync result; no ACK if failed |
| C5 after fsync before memtable apply | record recoverable, may appear after restart even if client saw no ACK |
| C6 after memtable apply before ACK | record recoverable, client may not know if ACK lost |
| C7 after ACK | record must recover |

---

## 7. Read path

Point lookup:

```text
GET key
  ↓
Check active memtable
  ↓
Check immutable memtables newest-to-oldest
  ↓
Check L0 SSTables newest-to-oldest
  ↓
Check lower levels according to level ordering
  ↓
Return value / tombstone / not found
```

MVP may initially implement:

```text
active memtable → SSTables sorted by max_sequence descending
```

The public API must not expose this simplification.

---

## 8. Concurrency model

MVP rules:

- Writes may be serialized through one async mutex.
- Reads may run concurrently only if visibility rules stay simple.
- Background flush/compaction can be disabled in deterministic tests.
- No correctness test may rely on wall-clock timing.

Future rules:

- group commit batcher may reorder internal steps but cannot violate ACK durability.
- immutable memtable snapshots define read visibility boundaries.
- compaction publication is manifest-atomic.

---

## 9. Cross-layer invariants

| ID | Invariant |
|---|---|
| ARCH-001 | A crate does not depend upward in the ownership graph |
| ARCH-002 | Server/CLI cannot bypass engine write path |
| ARCH-003 | Persistent files become live only through their owner publication protocol |
| ARCH-004 | Recovery can run without starting network/server components |
| ARCH-005 | Simulated and real disk backends exercise the same storage code |

---

## 10. Acceptance criteria

The architecture spec is satisfied when:

- crates can be created according to this layout,
- `kaya-io`, `kaya-wal`, and `kaya-engine` compile independently,
- WAL tests can run with both `FileDisk` and `SimDisk`,
- engine recovery can be called without starting a server,
- server/CLI are thin wrappers over engine APIs,
- dependency graph violations are caught in CI or documented review checks.
