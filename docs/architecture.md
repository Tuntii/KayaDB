# KayaDB Architecture

This document describes the crate structure, data flow, and key design decisions in KayaDB.

---

## Design principles

1. **Correctness before throughput** — A slow but crash-consistent path is preferred over a fast ambiguous one.
2. **Failure is a normal input** — Partial writes, failed fsyncs, torn pages, and crash/restart must all be testable deterministically.
3. **Every persistent format is inspectable** — WAL, SSTable, and Manifest are readable via `kayactl inspect` without external tools.
4. **Simulation before distribution** — Raft and networking only build on a reliable local storage layer.

---

## Crate map

```text
┌─────────────────────────────────────────────────────────┐
│                    User interfaces                       │
│                                                         │
│   kayactl (CLI)            kayadb-server (TCP server)   │
└────────────┬───────────────────────────┬────────────────┘
             │                           │
             └──────────┬────────────────┘
                        │
              ┌─────────▼──────────┐
              │    kaya-engine     │  put / get / delete / scan
              │  (embedded API)    │
              └──────┬──────┬──────┘
                     │      │
          ┌──────────▼─┐  ┌─▼──────────┐
          │  kaya-wal  │  │  kaya-lsm  │
          │            │  │            │
          │  WAL codec │  │  Memtable  │
          │  Writer    │  │  SSTable   │
          │  Recovery  │  │  Manifest  │
          │  Inspector │  │  Compaction│
          └──────┬─────┘  └─────┬──────┘
                 └──────┬───────┘
                        │
              ┌─────────▼──────────┐
              │     kaya-io        │  Disk trait abstraction
              │                    │
              │  FileDisk          │  → real filesystem (fsync)
              │  SimDisk           │  → deterministic fault injection
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │    kaya-core       │  errors, typed IDs, CRC32C
              └────────────────────┘

Distributed layer (sits above kaya-engine):

  kaya-raft ← kaya-net ← kaya-server
```

---

## Crate responsibilities

### `kaya-core`

- Shared error types (`KayaError`, `KayaResult`)
- Typed IDs (sequence numbers, node IDs)
- Global configuration structs
- CRC32C implementation (no external dependency)

### `kaya-io`

The `Disk` trait is the single I/O boundary for the entire engine:

```rust
pub trait Disk {
    fn write(&mut self, path: &RelativePath, offset: u64, buf: &[u8]) -> KayaResult<()>;
    fn read(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> KayaResult<()>;
    fn sync(&mut self, path: &RelativePath) -> KayaResult<()>;
    fn file_size(&self, path: &RelativePath) -> KayaResult<u64>;
    // ... open, delete, list, rename
}
```

**`FileDisk`** — wraps `std::fs` with real `fsync` calls.  
**`SimDisk`** — in-memory disk with a deterministic `FaultSchedule`. Given the same seed, it injects the same sequence of failures on every run.

### `kaya-wal`

Write-ahead log layer:

- **Codec** — record framing with 4-byte length, 4-byte CRC32C header, and variable-length payload
- **Writer** — append-only writer; calls `sync` on the `Disk` for durability
- **Recovery** — scans a WAL segment and returns the durable prefix (all records where CRC is intact)
- **Inspector** — human-readable record dump used by `kayactl inspect wal`

**Invariant:** After crash, recovery returns only records whose CRC matches. A partial tail record is silently truncated. This is the _durable-prefix property_.

### `kaya-lsm`

LSM-tree storage layer:

- **Memtable** — in-memory sorted map of versioned keys (`InternalKey`: user_key + commit_ts); accumulates writes until flush threshold
- **SSTable** — immutable sorted file: data blocks → index block → footer with bloom/CRC metadata (v4 stores multi-version rows per user_key)
- **Manifest** — append-only log of LSM state transitions (flush events, compaction results)
- **Compaction** — L0 compaction merges all L0 SSTables into a single sorted output atomically via manifest; may drop obsolete versions below the GC watermark

**MVCC (M16):** Logical versions are ordered by commit timestamp (`commit_ts == SequenceNumber` for M16). Default reads remain LWW-latest; snapshot reads use `ReadTimestamp::At(ts)` / `get_at`. Compaction never drops a version with `commit_ts >= watermark`.

### `kaya-engine`

Public embedded API:

- `Engine::open(dir, config, disk)` — opens or creates a database
- `Engine::put(key, value)` — writes to WAL then memtable
- `Engine::get(key)` / `get` with `ReadOptions { read_at }` — memtable first, then SSTables (latest or snapshot)
- `Engine::delete(key)` — writes a tombstone record at a new commit_ts
- `Engine::scan(from, to)` — returns an iterator over a key range

On restart, the engine replays the WAL into a fresh memtable, then applies the manifest to locate live SSTables. Multi-version history is reconstructed from WAL sequences and SST v4 tables.

### `kaya-sim`

Deterministic simulation framework:

- **`SimCluster`** — spawns multiple `Engine` instances sharing a `SimDisk` with a common fault schedule
- **`LinearizabilityChecker`** — validates that the history of operations satisfies sequential consistency
- **`Runner`** — drives workloads, captures traces, and reports violations

All randomness goes through a seeded `Rng` so that a failing scenario is fully reproducible.

### `kaya-raft`

Raft consensus state machine:

- Leader election with randomized election timeouts
- Log replication via `AppendEntries` RPC
- Commit index advancement once a quorum acknowledges an entry
- Partition-tolerant: a partitioned minority stops accepting writes

### `kaya-net`

Wire layer:

- Length-prefixed message codec
- `Transport` abstraction (real TCP or in-memory simulation)
- Node roster management (peer addresses, connection state)

### `kaya-server`

Server process:

- Cluster bootstrap and node lifecycle
- Routes client requests through the Raft leader
- Exposes a TCP interface consumed by `kayactl --server`

### `kayactl`

Command-line interface:

- `put / get / delete / scan` — key-value operations in embedded or server mode
- `inspect wal / sstable / manifest` — human-readable format dumps
- `stats` — storage layer metrics
- `recover --dry-run` — verify consistency without writing

---

## Write path (single node)

```
Client: put("k", "v")
   │
   ▼
kaya-engine::put
   │
   ├─► kaya-wal::Writer::append(record)  →  Disk::write + Disk::sync
   │       (crash here: record absent from WAL on recovery)
   │
   └─► Memtable::insert("k", "v")
           (crash here: WAL replay recovers the entry on restart)

When memtable exceeds threshold:
   │
   ▼
kaya-lsm::flush_memtable_to_sstable
   │
   ├─► write SSTable file  →  Disk::sync
   └─► Manifest::append(FlushEvent)  →  Disk::sync
          (crash before manifest write: SSTable is an orphan, safely ignored on recovery)
```

---

## Data directory layout

```text
<data-dir>/
  wal-000001.wal       WAL segment (append-only)
  sst-000001.sst       SSTable file (immutable after flush)
  sst-000002.sst
  MANIFEST             Manifest log (append-only)
  CURRENT              Points to the active MANIFEST file
```

---

## Durability and recovery model

KayaDB uses a conservative storage model: a write is acknowledged only after the configured durability path has completed.

### Strict durability

In strict mode, a write follows this sequence:

```text
client request
   → encode WAL record
   → append WAL bytes
   → sync WAL segment
   → apply to memtable
   → return success
```

If a crash happens after the WAL sync but before the memtable update, reopening the engine replays the WAL and reconstructs the visible state.

### Relaxed durability

Relaxed mode allows the engine to acknowledge before a durable sync. This can improve throughput during experiments, but a process or machine crash may lose the most recent unsynced writes.

### Durable-prefix recovery

WAL recovery accepts only the longest valid prefix of records. A partial or corrupted tail is treated as a normal crash artifact and is truncated from the recovered view. A corrupted middle record is not silently ignored because that could hide data loss.

### Manifest-defined live state

SSTable file existence alone does not make a table live. The manifest decides which tables belong to the current logical database state. This lets flush and compaction publish new files atomically and ignore orphaned files after crashes.

---

## Cluster request flow

In cluster mode, `kaya-server` hosts both the local storage engine and the Raft state machine.

```text
client PUT/DELETE
   → kaya-net client frame
   → kaya-server client handler
   → Raft proposal on leader
   → replicated log entry
   → committed entry
   → apply command to kaya-engine
   → client response
```

Reads are currently leader-routed. Followers should either redirect clients with a leader hint or reject the request rather than serving potentially stale local state.

Cluster membership uses Raft joint consensus (M11). Nodes start with `--peer` seeds; new members join via `--join-cluster` and are added with the `ADD_MEMBER` client opcode (7) or `kayactl add-node`. The server hot-reloads `NodeRoster` from committed config-change log entries and persists addresses to `data_dir/cluster-roster.json`. Removals use opcode 8 / `kayactl remove-node`. Raft state itself is still in-memory on restart.

---

## Operational design promises

KayaDB's public design is built around a few promises that should stay visible in code review and documentation:

| Promise | Practical meaning |
|---|---|
| Same code under test | `FileDisk` and `SimDisk` sit behind the same `Disk` trait |
| Failures are inputs | Partial writes, failed syncs, and crashes are expected test cases |
| Bytes are inspectable | WAL, SSTable, and manifest files can be inspected with `kayactl` |
| Recovery is repeatable | Running recovery multiple times should not change the logical result |
| Server does not bypass storage | Network writes still flow through `kaya-engine` and the WAL |
| Localhost first | Network defaults should be safe for local development, not public exposure |

---

## Related user docs

- [Getting started](getting-started.md)
- [CLI reference](cli-reference.md)
- [Development guide](development.md)
- [Security guide](security.md)
