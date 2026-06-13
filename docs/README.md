# KayaDB Documentation

> **This is the official documentation site for KayaDB**, built with [GitBook](https://www.gitbook.com/).

Welcome to the public KayaDB documentation. This book is written for users, contributors, operators, and curious systems engineers who want to understand how KayaDB behaves.

KayaDB is an **experimental, correctness-first** storage engine. The documentation prioritizes being practical: build it, run it, inspect it, break it safely with deterministic simulation, and understand exactly what happened.

---

## How to use this book

This documentation is published on **GitHub Pages** using Docsify (with GitBook files kept for compatibility).

**Live site:** [https://tuntii.github.io/KayaDB/](https://tuntii.github.io/KayaDB/)

You can:

- Browse the nice rendered version at the link above
- Read the Markdown sources directly on GitHub under `/docs`
- Preview locally (see [Publishing the Documentation](publishing.md))

See the dedicated guide: [Publishing the Documentation](publishing.md) for setup instructions and local preview commands.

---

## Start here

| Document                  | Use it when you want to... |
|---------------------------|------------------------------|
| [Getting started](getting-started.md) | Build KayaDB, run a local node or cluster, use `kayactl`, and try the async Rust client |
| [CLI reference](cli-reference.md)     | Look up every `kayactl` command, flag, JSON output, and exit code |
| [Architecture](architecture.md)       | Understand the storage stack, Raft layer, `SimDisk`, data flow and crate boundaries |
| [Development guide](development.md)   | Run tests, simulations, fuzz targets, benchmarks, and contribution checks |
| [Security guide](security.md)         | Learn the deployment limits, network warnings, and safe local defaults |

---

## Start here

| Document | Use it when you want to... |
|---|---|
| [Getting started](getting-started.md) | Build KayaDB, run a local node, use `kayactl`, and try the Rust client |
| [CLI reference](cli-reference.md) | Look up every `kayactl` mode, flag, output shape, and exit code |
| [Architecture](architecture.md) | Understand the storage stack, Raft layer, disk abstraction, and data flow |
| [Development guide](development.md) | Run tests, simulations, fuzz targets, benchmarks, and contribution checks |
| [Security guide](security.md) | Learn the deployment limits, network warnings, and safe local defaults |
| [Design Specifications](specifications.md) | Read the detailed internal specs (WAL format, recovery rules, invariants, etc.) |

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

---

## Full Documentation Structure

This GitBook organizes KayaDB documentation into the following main sections (see [SUMMARY.md](SUMMARY.md) for the complete navigation):

- **Introduction** — Project goals and current status
- **Getting Started** — Build, run, and first commands
- **Using KayaDB** — `kayactl` reference and client library usage
- **Architecture & Internals** — High-level design and data flows
- **Core Components** — WAL, LSM, Disk simulation, Recovery (with links to detailed specs)
- **Distributed KayaDB** — Raft, cluster mode, client redirection, Jepsen testing
- **Correctness & Testing** — Deterministic simulation, fault injection, fuzzing, linearizability
- **Reference** — Security, configuration, development workflow
- **Design Specifications** — The full internal technical spec pack (under `spec/docs/`)
- **Contributing & Roadmap**

The deep technical specifications (format details, invariants, recovery rules, etc.) live in the `spec/docs/` directory and are linked from the relevant chapters.

## Project Links

- [GitHub Repository](https://github.com/Tuntii/KayaDB)
- [Root README](../README.md)
- [ROADMAP](../ROADMAP.md)
- [Contributing](../CONTRIBUTING.md)
