# KayaDB Documentation

Welcome to the public KayaDB documentation. This folder is written for users, contributors, operators, and curious systems engineers who want to understand how KayaDB behaves without reading internal design material first.

KayaDB is experimental, but the documentation aims to be practical: build it, run it, inspect it, break it safely, and understand what happened.

---

## Start here

| Document | Use it when you want to... |
|---|---|
| [Getting started](getting-started.md) | Build KayaDB, run a local node, use `kayactl`, and try the Rust client |
| [CLI reference](cli-reference.md) | Look up every `kayactl` mode, flag, output shape, and exit code |
| [Architecture](architecture.md) | Understand the storage stack, Raft layer, disk abstraction, and data flow |
| [Development guide](development.md) | Run tests, simulations, fuzz targets, benchmarks, and contribution checks |
| [Security guide](security.md) | Learn the deployment limits, network warnings, and safe local defaults |

---

## Common paths

### I just want to try it

1. Follow [Getting started](getting-started.md).
2. Run local embedded commands with `kayactl --data ./data`.
3. Start `kayadb-server` and connect with `kayactl --server 127.0.0.1:7379`.
4. Inspect the generated WAL, SSTable, and manifest files.

### I want to embed it in Rust

- Read [Getting started](getting-started.md) and [`crates/kaya-engine`](../crates/kaya-engine/README.md).
- Use `kaya-engine` for in-process storage.
- Use `kaya-client` when connecting to a separate server process.

### I want to understand correctness testing

- Start with [Architecture](architecture.md#durability-and-recovery-model).
- Then read [Development](development.md#test-strategy).
- Run the `kaya-sim` tests and inspect how replayable traces are produced.

### I want to operate a node safely

- Read [Security](security.md) before binding outside localhost.
- Keep Raft and client ports private.
- Use `kayactl status`, `kayactl health`, and `kayactl recover --dry-run` before trusting a data directory.

---

## Project status

KayaDB currently provides:

- an embedded LSM-based key-value engine,
- WAL-backed crash recovery,
- inspectable persistent files,
- deterministic fault injection through `SimDisk`,
- a seeded simulator,
- a Raft-based cluster prototype,
- a TCP server and async Rust client,
- a CLI for local operation, remote operation, inspection, stats, and dry-run recovery.

KayaDB does **not** yet provide production-grade security hardening, built-in authentication, built-in TLS, dynamic cluster membership, or Raft log snapshotting.

---

## Documentation principles

Public docs should be:

- practical before theoretical,
- honest about limitations,
- copy-paste friendly,
- explicit about safety and recovery behavior,
- useful to both first-time users and contributors.

If a command in these docs does not work on a clean checkout, that is a documentation bug worth fixing.
