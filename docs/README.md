# KayaDB Documentation

> **Official documentation** for [KayaDB](https://github.com/Tuntii/KayaDB) — a correctness-first, embeddable **distributed key-value database** built in Rust.

**Live site:** [https://tuntii.github.io/KayaDB/](https://tuntii.github.io/KayaDB/)

**Current release:** [v0.1.46](releases.md) (M15 — remaining tracks ✅)

---

## What is KayaDB?

KayaDB is a small but serious storage system you can use in three ways:

| Mode | How | Comparable to |
|---|---|---|
| **Embedded** | Link `kaya-engine` in your Rust app | RocksDB, SQLite (KV subset) |
| **Server** | Run `kayadb-server` on one host | Redis (persistence + LSM) |
| **Cluster** | Run 3+ nodes with Raft consensus | etcd, TiKV (prototype scale) |

Core properties:

- **LSM-tree engine** with WAL-backed crash recovery
- **Inspectable on-disk formats** — every WAL, SSTable, and manifest is readable via `kayactl inspect`
- **Deterministic fault injection** — `SimDisk` replays crashes and partial writes in tests
- **Raft cluster** with leader redirection, dynamic membership, and day-2 runbooks
- **Correctness culture** — simulation, Jepsen-style harness, fuzz targets, chaos-matrix CI

KayaDB completed **M13–M15** (TLS, tokens, audit, Prometheus, Go client, Docker/K8s examples). It is a deployable correctness prototype — not a fully hardened multi-tenant SaaS database. Read [security](security.md) before any production-like deployment.

---

## Start here

| I want to… | Read |
|---|---|
| Install binaries or crates | [Installation](installation.md) |
| Run my first `put` / `get` | [Getting started](getting-started.md) |
| See practical workflows (TR) | [Kullanım senaryoları](usage.md) |
| Look up every `kayactl` flag | [CLI reference](cli-reference.md) |
| Understand the storage stack | [Architecture](architecture.md) |
| Deploy with Docker/K8s | [Deployment](deployment.md) |
| Operate a cluster safely | [Security](security.md) + [Runbooks](runbooks/rolling-restart.md) |
| CI / GitHub Actions | [CI & Actions](ci-and-actions.md) |
| See version history | [Releases](releases.md) + [CHANGELOG](../CHANGELOG.md) |
| Understand the full picture | [KayaDB Explained](KayaDB_Explained.md) |

---

## Documentation map

### Install & use

- [Installation](installation.md) — crates.io, release binaries, build from source
- [Getting started](getting-started.md) — first server, first commands, cluster quick-start
- [Usage scenarios](usage.md) — embedded, cluster, recovery, inspection, automation
- [CLI reference](cli-reference.md) — `kayactl` commands, flags, JSON output, exit codes
- [Client library](getting-started.md#using-the-kaya-client-library) — async Rust client with leader redirection
- [Client protocol](clients/client-protocol-spec.md) · [Wire format](clients/client-wire-protocol.md) · [Conformance vectors](clients/conformance/vectors.json) · [Go client](clients/go-client.md)
- [Deployment](deployment.md) — Docker Compose + Kubernetes examples

### Architecture & internals

- [Architecture overview](architecture.md) — crate map, write/read paths, recovery model
- [KayaDB Explained](KayaDB_Explained.md) — single comprehensive deep-dive
- [Design specifications](specifications.md) — index into `spec/docs/` (WAL, LSM, Raft, simulation…)

### Distributed operation

- [Jepsen-style testing](jepsen-design.md) — failure injection, linearizability, scenario registry
- [Runbooks](runbooks/rolling-restart.md) — add/remove node, rolling restart, backup/restore, split-brain, mTLS sidecar

### Correctness & development

- [Development guide](development.md) — tests, SimDisk, fuzzing, benchmarks
- [Benchmarks](../BENCHMARKS.md) — methodology and performance envelope

### Reference & project

- [Security & deployment](security.md) — network model, TLS, operator token, accepted risks
- [Releases & versioning](releases.md) — tags, crates.io, GitHub releases
- [Publishing docs](publishing.md) — GitHub Pages, local preview, maintainer publish flow
- [Productization north star](productization.md) — M13 exit gates
- [Roadmap](../ROADMAP.md) · [Contributing](../CONTRIBUTING.md)

---

## Common paths

### Try it in 60 seconds

```bash
# Install CLI (or use cargo run — see installation.md)
cargo install kayactl

kayactl --data ./demo put hello world
kayactl --data ./demo get hello
kayactl --data ./demo inspect wal ./demo/wal-000001.wal
```

### Run a server + cluster

1. [Install](installation.md) `kayadb-server` and `kayactl`
2. Follow [Getting started → single-node server](getting-started.md#run-a-single-node-server)
3. Scale to three nodes: [Getting started → cluster](getting-started.md#multi-node-cluster-quick-setup)
4. Operate safely: [Runbooks](runbooks/rolling-restart.md)

### Embed in Rust

```rust
use kaya_engine::{Engine, ReadOptions, WriteOptions};
// See crates/kaya-engine/examples/embedded.rs
```

Full walkthrough: [Getting started → embedded](getting-started.md#embedded-mode-no-server-needed) and [`kaya-engine` README](../crates/kaya-engine/README.md).

### Contribute or debug correctness

1. [Development guide](development.md)
2. [Architecture → recovery model](architecture.md#recovery-architecture)
3. Run `cargo test -p kaya-sim` and inspect replayable traces

---

## Project links

- [GitHub repository](https://github.com/Tuntii/KayaDB)
- [Issue tracker](https://github.com/Tuntii/KayaDB/issues)
- [crates.io: kaya-engine](https://crates.io/crates/kaya-engine) · [kayactl](https://crates.io/crates/kayactl)
- [Root README](../README.md)

---

## About this site

Published on **GitHub Pages** with [Docsify](https://docsify.js.org/). Navigation lives in [`_sidebar.md`](_sidebar.md); [`SUMMARY.md`](SUMMARY.md) is kept for GitBook compatibility.

To preview locally:

```bash
cd docs && python -m http.server 3000
# open http://localhost:3000
```

See [Publishing](publishing.md) for maintainer details.