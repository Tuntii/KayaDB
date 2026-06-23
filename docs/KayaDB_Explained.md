# KayaDB Explained: What It Actually Is and How It Works

**KayaDB** is a **correctness-first, embeddable distributed key-value database** written in Rust. Current release: **v0.1.43** (M14). It is a deployable correctness prototype — not a fully hardened multi-tenant production database — whose primary goal is to make storage bugs **reproducible, inspectable, and eventually impossible to introduce silently**.

This document is the single place that tries to explain **everything** about KayaDB — philosophy, architecture, components, unique features, how data flows, how testing works, current status, and where it is headed.

---

## 1. The Core Thesis

Most databases are built by making the happy path work and then hoping the failure cases are rare.

KayaDB flips this completely:

> **"If a storage bug cannot be reproduced, inspected, and turned into an invariant, it is not really fixed."**

The project is built around the idea that **failure is a first-class input**. Crashes, partial writes, torn pages, fsync failures, network partitions, and leader changes must be **deterministically reproducible** and **testable**.

This is not marketing language. It is the actual design center of the entire system.

---

## 2. High-Level Picture

KayaDB can be used in two modes:

- **Embedded** — you link `kaya-engine` directly into your Rust application (like SQLite or RocksDB).
- **Server / Cluster** — you run `kayadb-server` processes that form a small Raft cluster. Clients talk to them over TCP using `kayactl` or the `kaya-client` library.

Everything is deliberately small and modular. The workspace contains ~13 crates with clear boundaries.

### The Crate Map (Simplified)

```
User Interfaces
├── kayactl (CLI + inspectors + stats + dry-run recovery)
└── kaya-client (async Rust client with leader redirection + tracing)

Server Layer
└── kaya-server (TCP server + cluster runtime)

Engine Layer
└── kaya-engine (the public embedded KV API: put/get/delete/scan)

Storage Engine
├── kaya-wal   (Write-Ahead Log: codec, writer, recovery, inspector)
└── kaya-lsm   (LSM-tree: memtable, SSTable, manifest, L0 compaction)

Durability Abstraction
└── kaya-io::Disk
    ├── FileDisk  → real filesystem + fsync
    └── SimDisk   → in-memory, deterministic fault injection

Foundation
├── kaya-core  (errors, typed IDs, CRC32C, config)
├── kaya-raft  (pure Raft state machine)
├── kaya-net   (wire protocol + TCP transport)
└── kaya-sim   (seeded simulator, trace replay, linearizability checker)

Testing & Benchmarking
├── kaya-bench
└── kaya-jepsen-test (Rust-native Jepsen-style workload + nemesis harness)
```

The `Disk` trait is the single most important abstraction in the project. Almost all correctness guarantees flow from it.

---

## 3. The Philosophy & Design Principles

From the architecture document and PRD:

1. **Correctness before throughput**  
   A slow but provably correct path is preferred over a fast ambiguous one.

2. **Failure is a normal input**  
   Partial writes, failed fsyncs, corruption, and crash/restart paths must be testable deterministically.

3. **Every persistent format is inspectable**  
   WAL segments, SSTables, and the Manifest can (and should) be read by humans using `kayactl inspect`.

4. **Simulation before distribution**  
   Raft and real networking were only added after the local storage layer had strong crash/recovery guarantees under simulation.

5. **Design-first + invariant-driven**  
   Formats, recovery semantics, and testing rules are documented before the code that implements them.

The long-term vision (from the PRD):
> A deterministic, crash-tested, io_uring-native storage engine that can be studied, extended, and stress-tested by engineers who care about correctness.

KayaDB does **not** try to compete with TiKV, FoundationDB, or CockroachDB on features or raw performance in its early stages. Its differentiation is **correctness-oriented engineering**.

---

## 4. Core Storage Components

### 4.1 The Disk Abstraction (`kaya-io`)

All I/O in KayaDB goes through the `Disk` trait. This single decision enables the entire correctness story.

- `FileDisk`: Real files on disk. Uses proper `fsync` + directory sync for durability.
- `SimDisk`: The star of the show. An in-memory disk that maintains two states:
  - **Volatile state** (what the process currently sees)
  - **Stable state** (what survives a crash)

A `FaultSchedule` (driven by a seed) can inject:
- Partial writes
- Dropped writes
- Failed fsyncs
- Disk full
- etc.

Because the same engine code runs against both `FileDisk` and `SimDisk`, any bug that appears under fault injection is a real bug that can affect production.

### 4.2 Write-Ahead Log (`kaya-wal`)

- Record format is fully specified (magic, version, flags, LSN, sequence, CRC32C header + payload CRC).
- Strict vs Relaxed durability modes.
- Segment rotation.
- Recovery that returns only the **durable prefix** (records whose CRCs are intact). Partial tail records are truncated.
- Full inspector (`kayactl inspect wal`).

The durable-prefix property is heavily tested with random crash points in simulation.

### 4.3 LSM-Tree Storage (`kaya-lsm`)

Standard but carefully implemented LSM:

- **Memtable**: In-memory ordered structure (currently BTreeMap based for simplicity).
- **SSTable**: Immutable sorted files with data blocks + index block + footer. CRC32C on everything.
- **Manifest**: Tracks live SSTables + sequence numbers. Uses atomic `CURRENT` file + edit log.
- **Flush**: Memtable → SSTable → atomic manifest update.
- **L0 Compaction**: Simple merge of all L0 files (tombstones preserved).

Reads go: memtable → immutable memtables → L0 SSTables (newest first) → lower levels.

All on-disk formats are inspectable with `kayactl inspect sstable` and `kayactl inspect manifest`.

### 4.4 The Engine (`kaya-engine`)

Orchestrates everything:

- `put` / `get` / `delete` / `scan_prefix`
- Durability mode selection per write
- Recovery on open (WAL replay into memtable + loading live SSTables from manifest)
- Exposes `EngineStats` and recovery reports

The engine itself is deliberately "dumb" about distribution — it just applies committed commands. Raft sits on top.

---

## 5. The Killer Feature: Deterministic Everything

This is what truly separates KayaDB from most educational or prototype storage engines.

### SimDisk + Fault Schedules

You can write a test like:

```rust
let schedule = FaultSchedule::new(vec![
    (3, FaultKind::PartialWrite(17)),
    (7, FaultKind::FsyncFailed),
]);
let disk = SimDisk::with_faults(seed, schedule);

let mut engine = Engine::open(config, disk.clone()).await?;
// ... do writes ...
disk.inject_crash();

let recovered = Engine::open(config, disk).await?;
// assert invariants
```

The same seed + same schedule produces **identical** behavior every run. Failures become regression tests.

### The Seeded Simulator (`kaya-sim`)

`kaya-sim` is a full deterministic async runtime + disk + (later) network simulator.

- Operation generator driven by `SimRng` (xorshift).
- Reference model (BTreeMap) for linearizability checking.
- Full JSONL trace recording.
- Replay mode that can reproduce a failure exactly.
- `LinearizabilityChecker` that can verify sequential consistency from history.

This is used both for engine-level invariants and for Raft cluster simulation.

### Fuzzing

Dedicated fuzz targets for:
- WAL decoder
- SSTable footer/block
- Manifest decoder
- Server command frame decoder

All malformed input must produce clean errors, never panics or memory corruption.

### Jepsen-Style Testing

There is a complete Rust-native harness (`kaya-jepsen-test`) with:
- Workloads (Register, Counter, Set, Map)
- Nemeses (Kill node, Partition, etc.)
- History recording + linearizability checking
- Process control scripts (cross-platform .sh/.ps1)

Full external Jepsen (Clojure) is planned but deferred until the Rust-native harness and production hardening mature (snapshots and dynamic membership are now in place).

---

## 6. Distributed Mode (Raft + Cluster)

Once the local engine was solid, a Raft prototype was added.

- `kaya-raft`: Pure state machine (election, log replication, commit index, no-op on leader change, etc.).
- `kaya-net`: Hand-rolled binary protocol + TCP transport + node roster.
- `kaya-server`: Runs the Raft event loop + applies committed commands to the engine + serves client requests with leader redirection (`STATUS_NOT_LEADER` + leader hint).

Client library (`kaya-client`) and `kayactl` automatically follow redirects.

Current limitations (v0.1.43):
- Dynamic membership via joint consensus (operator token required when configured)
- Raft snapshots exist; log compaction policies are evolving in M14
- Reads go through the leader (ReadIndex)
- Full client authZ, data-at-rest encryption, and compliance audit logging are not built-in — see [security §7](security.md#7-accepted-risks-and-future-hardening-m13-exit)

---

## 7. Observability & Tooling

One of the strongest parts of KayaDB is that you can actually **see** what is happening.

`kayactl` can:
- `put/get/delete/scan` (embedded or against a server)
- `inspect wal <file>`
- `inspect sstable <file>`
- `inspect manifest <file>`
- `stats`
- `recover --dry-run` (shows what recovery would do without opening the engine)
- `health`, multi-endpoint failover, timeouts, JSON output

All persistent formats are documented in `spec/docs/` and the inspectors emit human-readable + JSON output.

---

## 8. Current Status (v0.1.43 — M14)

**Shipped and solid:**
- Full LSM engine (WAL + memtable + SSTable + manifest + compaction policies + bloom filters)
- WAL group-commit batching; inspectable on-disk formats
- Deterministic fault injection (`SimDisk`) + seeded simulation + trace replay
- Raft cluster with durable state, snapshots, dynamic membership, leader redirection
- Native TLS (`tls` feature) + operator token for admin ops; mTLS sidecar runbooks
- `kayactl` + `kaya-client`; day-2 runbooks under `docs/runbooks/`
- Jepsen-style harness (T1–T7), chaos-matrix CI, fuzz targets, perf regression gate

**M14 remaining / accepted gaps:**
- Jepsen full suite hardening under partition nemesis (in progress)
- Linux `io_uring` disk backend (planned)
- Full client authZ, data-at-rest encryption, multi-tenant isolation — documented as accepted deployment risks

See [security](security.md), [releases](releases.md), and [ROADMAP](../ROADMAP.md) for the honest envelope.

---

## 9. How to Actually Use It

### Embedded (simplest for learning)

```rust
let disk = Arc::new(FileDisk::new(data_dir));
let mut engine = Engine::open(config, disk).await?;

engine.put(key, value, WriteOptions { durability: Some(DurabilityMode::Strict), .. }).await?;
let val = engine.get(&key, ReadOptions::default()).await?;
```

### Via CLI (great for exploration)

```bash
cargo run -p kayactl -- --data ./mydata put hello world
cargo run -p kayactl -- --data ./mydata get hello
cargo run -p kayactl -- inspect wal ./mydata/wal-000001.wal
```

### As a Cluster

Start 3 nodes (see `scripts/start-cluster.ps1` or `.sh`), then use `--server` with `kayactl` or the Rust client.

The client and CLI will automatically redirect to the current leader.

---

## 10. The Bigger Picture & Why You Might Care

KayaDB is valuable in several roles:

- **Learning vehicle** for people who want to understand real LSM + WAL + Raft internals without reading millions of lines of code.
- **Correctness research platform** — the simulation + fault injection + replay story is unusually strong.
- **Contribution target** for people who like systems programming, formal-ish thinking, and "make the storage layer explain itself."
- **Base for future experiments** (io_uring, better compaction, real Jepsen, eBPF observability, etc.).

It is intentionally opinionated and narrow: it does a few things extremely well (deterministic crash testing, inspectability, clean modular design) and is honest about what it does not do.

---

## 11. Where to Go Next

- Start with the [GitHub Pages documentation](https://tuntii.github.io/KayaDB/)
- Read [docs/getting-started.md](getting-started.md)
- Read [docs/architecture.md](architecture.md)
- Explore the deep specs in `spec/docs/`
- Run the simulator and inject faults yourself
- Look at `crates/kaya-jepsen-test` if you care about distributed correctness

The project is open to contributors who are willing to follow the "design first + test the failure case" culture.

---

**KayaDB summary in one sentence:**

It is a small but serious Rust storage engine whose entire architecture is built around the radical idea that **you should be able to deterministically reproduce, inspect, and regression-test every possible way the system can fail**.

That single idea drives the Disk trait, SimDisk, the simulator, the inspectors, the Raft layering, the Jepsen harness, and the documentation culture.

Everything else (LSM details, Raft, CLI, etc.) exists to support that vision.

---

*This file synthesizes the root README, architecture document, technical specs, roadmap, and crate structure as of v0.1.43 (2026-06-23).*