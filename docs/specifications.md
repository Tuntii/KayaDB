# Design Specifications

KayaDB was developed **design-first**. Most of the detailed internal specifications live in the `spec/docs/` directory.

These documents define formats, invariants, recovery rules, and boundaries. They are the source of truth for the implementation.

## Core Specifications

| Spec | Description |
|------|-------------|
| [Spec Index](spec/docs/00-spec-index.md) | Overview + terminology + priority map |
| [Disk & I/O](spec/docs/disk-and-io-spec.md) | `Disk` trait, `FileDisk`, `SimDisk`, fault model |
| [WAL](spec/docs/wal-spec.md) | Record format, segments, append protocol, recovery |
| [Recovery](spec/docs/recovery-spec.md) | Idempotent recovery, crash points, manifest + WAL + SSTable interaction |
| [LSM Storage Format](spec/docs/lsm-storage-format-spec.md) | Memtable, SSTable layout, manifest, flush, compaction |
| [Manifest](spec/docs/manifest-spec.md) | Manifest record format and atomic publication rules |
| [Engine API](spec/docs/engine-api-spec.md) | Embedded `Engine` API semantics and error model |
| [Simulation](spec/docs/simulation-spec.md) | Seeded execution, traces, nemesis, replay |
| [Testing & Invariants](spec/docs/testing-and-invariants-spec.md) | Invariant catalog and test strategy |

## Higher Level / Future

- [Server & Protocol](spec/docs/server-and-protocol-spec.md)
- [Raft & Distributed Roadmap](spec/docs/raft-and-distributed-roadmap-spec.md)
- [Security & Safety](spec/docs/security-and-safety-spec.md)
- [Observability](spec/docs/observability-spec.md)
- [Configuration](spec/docs/configuration-spec.md)
- [CLI UX](spec/docs/cli-ux-spec.md)
- [Format Versioning](spec/docs/format-versioning-spec.md)
- [Benchmarking Policy](spec/docs/benchmarking-spec.md)

## Product & Technical Overview

- [KayaDB Explained](KayaDB_Explained.md) — narrative product + architecture tour
- [Expanded Implementation Roadmap](spec/issues/expanded-implementation-roadmap.md) — KD-* issue breakdown

> These specifications are living documents. When persistent formats or public behavior change, the corresponding spec and inspector output must be updated.

For a high-level tour instead of deep specs, start with the [Architecture](architecture.md) chapter.