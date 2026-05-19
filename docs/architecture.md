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

- **Memtable** — in-memory sorted map; accumulates writes until flush threshold
- **SSTable** — immutable sorted file: data blocks → index block → footer with bloom/CRC metadata
- **Manifest** — append-only log of LSM state transitions (flush events, compaction results)
- **Compaction** — L0 compaction merges all L0 SSTables into a single sorted output atomically via manifest

### `kaya-engine`

Public embedded API:

- `Engine::open(dir, config, disk)` — opens or creates a database
- `Engine::put(key, value)` — writes to WAL then memtable
- `Engine::get(key)` — reads memtable first, then SSTables
- `Engine::delete(key)` — writes a tombstone record
- `Engine::scan(from, to)` — returns an iterator over a key range

On restart, the engine replays the WAL into a fresh memtable, then applies the manifest to locate live SSTables.

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

## Spec references

The technical specifications that govern this implementation live in [`spec/docs/`](../spec/docs/):

- [`architecture-spec.md`](../spec/docs/architecture-spec.md)
- [`disk-and-io-spec.md`](../spec/docs/disk-and-io-spec.md)
- [`wal-spec.md`](../spec/docs/wal-spec.md)
- [`lsm-storage-format-spec.md`](../spec/docs/lsm-storage-format-spec.md)
- [`engine-api-spec.md`](../spec/docs/engine-api-spec.md)
- [`simulation-spec.md`](../spec/docs/simulation-spec.md)
- [`raft-and-distributed-roadmap-spec.md`](../spec/docs/raft-and-distributed-roadmap-spec.md)
