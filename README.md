# KayaDB

<div align="center">

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](rust-toolchain.toml)
[![Status](https://img.shields.io/badge/status-experimental-yellow.svg)](ROADMAP.md)

**Correctness-first, inspectable, embeddable storage engine — written in Rust.**

</div>

---

KayaDB is an open-source storage engine designed around one core principle: **a bug that cannot be reproduced and inspected is a bug that cannot be fixed.** Every persistent format is human-readable through tooling, every crash path is deterministically testable, and every invariant is documented before it is implemented.

The project is developed spec-first by [Tuntii](https://github.com/Tuntii) and open to contributors who share the same correctness obsession.

---

## 🔬 What sets KayaDB apart

Most storage engines treat crash recovery as a best-effort affair — test it on a real filesystem, hope CI passes, move on. KayaDB takes a fundamentally different approach.

### Deterministic fault injection via `SimDisk`

KayaDB ships a **virtual disk** that runs the *exact same engine code* as production, but injects faults on a fixed schedule:

```
Real engine code  →  Disk trait  →  FileDisk   (production — real fsync, real I/O)
                                →  SimDisk    (test — volatile/stable layers + fault injection)
```

`SimDisk` models a two-layer storage: a **volatile** buffer (lost on crash) and a **stable** buffer (survives fsync). A `FaultSchedule` says things like *"drop the 3rd write, skip the 7th fsync, do a partial write on the 12th append."*

Because every disk operation increments a global counter and all randomness is seeded, **the same `--seed` reproduces the same crash, the same corruption, and the same bug — on any machine, forever.**

```bash
# Find a bug
kayadb-sim --seed 0xdeadbeef --ops 100000 --nemesis disk,node-crash

# Replay it exactly
kayadb-sim --replay traces/failure-0xdeadbeef.trace.jsonl
```

### How this compares

| | KayaDB | RocksDB | sled | Redb | LMDB |
|---|---|---|---|---|---|
| **Seed-based deterministic crashes** | ✅ `SimDisk` | 🟡 `FaultInjectionTestFS` (C++) | ❌ | ❌ | ❌ |
| **Same code in test & production** | ✅ `Disk` trait | ❌ Separate test path | ❌ | ❌ | ❌ |
| **Trace replay** | ✅ `--replay` | ❌ | ❌ | ❌ | ❌ |
| **TLA+ formal model** | ✅ `WalCrash.tla` | ❌ | ❌ | ❌ | ❌ |
| **Linearizability checker** | ✅ Wing-Gong algorithm | ❌ | ❌ | ❌ | ❌ |
| **30+ documented invariants** | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Inspectable on-disk formats** | ✅ `kayactl inspect` | 🟡 `ldb` / `sst_dump` | ❌ | ❌ | ❌ |

---

## Why KayaDB?

| Property | What it means in practice |
|---|---|
| **Correctness-first** | Crash consistency and durable-prefix invariants are verified before performance tuning |
| **Deterministic simulation** | `SimDisk` injects faults on a fixed seed — flaky tests are a bug, not noise |
| **Inspectable formats** | WAL, SSTable, and Manifest files are readable via `kayactl inspect` without external tooling |
| **Spec-driven development** | Every crate boundary, wire format, and invariant is documented in [`spec/`](spec/README.md) before code is written |
| **Embeddable + server modes** | Can be used as a library (`kaya-engine`) or run as a TCP server (`kayadb-server`) |
| **Zero external dependencies** | Core stack uses no external crates — CRC32C, RNG, and codecs are all internal |

---

## Features

- **WAL** — append-only write-ahead log with CRC32C record integrity and crash-safe recovery
- **LSM storage** — memtable → SSTable flush pipeline with L0 compaction and manifest tracking
- **Deterministic fault injection** — `SimDisk` replays the same failure scenario from a seed
- **Raft consensus** — leader election, `AppendEntries`, commit index, partition-tolerant simulation
- **TCP cluster mode** — multi-node `kayadb-server` with Raft transport
- **Fuzz harness** — `fuzz_wal_decoder`, `fuzz_sstable_footer`, `fuzz_manifest_decoder` via `cargo-fuzz`
- **Benchmark suite** — `kaya-bench` with engine workload, SSTable, and WAL append benchmarks
- **Rich CLI** — `kayactl` supports `put/get/delete/scan`, `inspect wal/sstable/manifest`, `stats`, and `recover --dry-run`
- **TLA+ verified** — `WalCrash.tla` formally models the WAL durable-prefix property

---

## Quick start

```bash
# Build everything
cargo build --workspace

# Run all tests
cargo test --workspace

# Start a single-node server
cargo run -p kaya-server -- --dir /tmp/kayadb-data --port 7771

# Write and read data via CLI
cargo run -p kayactl -- --server 127.0.0.1:7771 put hello world
cargo run -p kayactl -- --server 127.0.0.1:7771 get hello

# Check remote cluster node statistics and health
cargo run -p kayactl -- --server 127.0.0.1:7771 status
```

### Programmatic Integration with `kaya-client`

Add `kaya-client` as a dependency in your application's `Cargo.toml` and interact async-natively:

```rust
use std::net::SocketAddr;
use kaya_client::KayaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:7771".parse()?;
    let mut client = KayaClient::connect(addr).await?;
    
    client.put(b"foo", b"bar").await?;
    if let Some(val) = client.get(b"foo").await? {
        println!("Value: {}", String::from_utf8_lossy(&val));
    }
    Ok(())
}
```

### Multi-Node Local Raft Cluster (PowerShell/Cmd Concurrent Mode)

Run three terminals to spin up a local 3-node fault-tolerant cluster:

```powershell
# Terminal 1 (Node 1 - Leader candidate)
cargo run -p kaya-server -- --dir /tmp/kaya-node1 --port 7771 --peers 127.0.0.1:7772,127.0.0.1:7773 --node-id 1

# Terminal 2 (Node 2)
cargo run -p kaya-server -- --dir /tmp/kaya-node2 --port 7772 --peers 127.0.0.1:7771,127.0.0.1:7773 --node-id 2

# Terminal 3 (Node 3)
cargo run -p kaya-server -- --dir /tmp/kaya-node3 --port 7773 --peers 127.0.0.1:7771,127.0.0.1:7772 --node-id 3
```

See [docs/getting-started.md](docs/getting-started.md) for the full walkthrough.

---

## Architecture at a glance

```text
kayactl (CLI)          kayadb-server (TCP)
      \                       /
       ──────── kaya-engine ──────────
                    │
          ┌─────────┴──────────┐
       kaya-wal             kaya-lsm
      (WAL codec,          (Memtable,
      writer, recovery)    SSTable, Manifest)
                    │
                kaya-io
         (Disk trait, FileDisk, SimDisk)
                    │
                kaya-core
         (errors, typed IDs, CRC32C)
```

Distributed layer: `kaya-raft` ← `kaya-net` ← `kaya-server`

See [docs/architecture.md](docs/architecture.md) for the full crate boundary reference.

---

## Workspace layout

```text
crates/
  kaya-core/     shared errors, typed IDs, config, CRC32C checksum helpers
  kaya-io/       Disk trait, RelativePath, FileDisk, SimDisk (fault injection)
  kaya-wal/      WAL record codec, writer, crash-safe recovery, inspector
  kaya-lsm/      memtable, SSTable encoder/decoder, manifest replay
  kaya-engine/   embedded engine API (put/get/delete/scan) over WAL + LSM
  kaya-sim/      deterministic simulation runner, linearizability checker
  kaya-server/   TCP server process, cluster bootstrap, Raft integration
  kaya-raft/     Raft state machine (election, AppendEntries, commit index)
  kaya-net/      wire codec, transport layer, node roster
  kaya-bench/    criterion benchmark suite
  kayactl/       command-line interface

fuzz/            cargo-fuzz targets for WAL, SSTable and Manifest decoders
spec/            full technical specification pack (PRD, architecture, formats)
docs/            user-facing documentation (getting started, CLI reference)
```

---

## Documentation

| Document | Description |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Build, run and first commands |
| [docs/architecture.md](docs/architecture.md) | Crate boundaries, data flow, design decisions |
| [docs/cli-reference.md](docs/cli-reference.md) | Complete `kayactl` command reference |
| [docs/development.md](docs/development.md) | Dev workflow, test strategy, simulation guide |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to open a PR |
| [ROADMAP.md](ROADMAP.md) | Milestone progress |
| [spec/](spec/README.md) | Full technical specification pack |

---

## Development commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Fuzz (requires cargo-fuzz + nightly)
cargo +nightly fuzz run fuzz_wal_decoder

# Benchmarks
cargo bench -p kaya-bench
```

---

## Status

| Milestone | Status |
|---|---|
| M0 — Foundation (workspace, WAL, disk) | ✅ Complete |
| M1 — Crash tests (SimDisk, durable-prefix) | ✅ Complete |
| M2 — LSM (SSTable, manifest, flush, L0 compaction) | ✅ Complete |
| M3 — Fuzz, recovery idempotence, benchmarks | ✅ Complete |
| M4 — Raft, cluster TCP, linearizability | ✅ Complete |
| M5 — Client API, STATS Metrics & kayactl status (M11) | ✅ Complete |
| M6 — Jepsen Prep & Observability Hardening (M12) | ✅ Complete |
| M7 — Linux eBPF, production hardening & Beyond | 🔒 Future |

---

## Honest Limitations & Production Warnings

KayaDB is an experimental, correctness-first research database and is **not yet production-ready**. Before deploying or using it, please note the following architectural constraints:

1. **Static Membership**: Dynamic member addition or removal is not supported in the Raft consensus layer. Changing the cluster roster requires manual configuration updates and coordinated cluster rolling restarts.
2. **Plain TCP Communication**: The replication protocol (`kaya-net`) runs over raw, unencrypted TCP sockets. You **must** restrict cluster and client ports behind a private firewall/VPC or wrap them inside an encrypted tunnel (like IPsec, WireGuard, or ghostunnel).
3. **No Built-in Authentication**: Authentication and role-based access control are not implemented inside the protocol layer. Network-level authorization and private subnet isolation are mandatory.
4. **Single-Core Engine Focus**: The local storage engine is optimized for high-speed, thread-safe sequential LSM write/read execution paths. It is not designed to replace heavy multi-threaded concurrent compaction engines suitable for multi-terabyte cloud scale.
5. **Leader-Routed Reads**: Linearizability in v1.0.0 is guaranteed by routing all client read queries strictly to the active cluster leader. Followers redirect clients to the leader. If a network partition isolates a node, clients querying that partitioned node will receive `NOT_LEADER` errors or request timeouts instead of stale data. (Future versions will implement `ReadIndex` or leader leases for follower local reads).
6. **No Raft Log Compaction**: The Raft replication log currently grows sequentially without automated state machine checkpoint snapshotting or log truncation. In highly active clusters, disk space usage will grow continuously over time. Automated snapshots are planned for v1.1.0.


---

## Contributing

KayaDB is an open-source project — contributions are welcome. The project is spec-first and correctness-obsessed, so the bar for tests is high but the codebase is intentionally small and approachable.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide. In short:

1. Pick an item from [ROADMAP.md](ROADMAP.md) or [`spec/issues/`](spec/issues/).
2. Link the relevant spec document and invariant IDs in your PR description.
3. Write a deterministic test for any crash/corruption path — `SimDisk` makes this easy.
4. Run `cargo fmt`, `cargo clippy`, `cargo test` before opening a PR.

**Good first issues** are tagged in the issue tracker. If you're new to storage engines, start with `kayactl` UX improvements or `kaya-bench` benchmarks — no deep LSM knowledge required.

Correctness beats cleverness.

---

## License

KayaDB is open-source software, licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)

at your option.

---

<div align="center">

**Built with an obsession for correctness by [Tuntii](https://github.com/Tuntii) and contributors.**

⭐ *If this project interests you, consider giving it a star — it helps others discover correctness-first storage research.*

</div>
