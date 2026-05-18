# KayaDB

KayaDB is a correctness-first, inspectable storage engine project. The current codebase is an implementation skeleton generated from the specification pack in [`spec/`](spec/README.md).

The first implementation slice follows the technical spec:

```text
FileDisk + SimDisk
    ↓
WAL append/recover
    ↓
strict durability tests
    ↓
kayactl inspect wal
```

## Workspace layout

```text
crates/
  kaya-core/     shared errors, typed IDs, config, checksum helpers
  kaya-io/       Disk trait, RelativePath, FileDisk, SimDisk
  kaya-wal/      WAL record format, encoder/decoder, writer, recovery, inspector
  kaya-lsm/      memtable and value-record primitives
  kaya-engine/   embedded engine API over WAL + memtable
  kaya-sim/      deterministic simulation placeholders
  kaya-server/   future local server process boundary
  kayactl/       command-line tool skeleton, currently WAL inspection
  kaya-raft/     future Raft boundary placeholder
  kaya-net/      future protocol/transport boundary placeholder
```

## Development commands

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Specification links

- [`spec/docs/architecture-spec.md`](spec/docs/architecture-spec.md)
- [`spec/docs/disk-and-io-spec.md`](spec/docs/disk-and-io-spec.md)
- [`spec/docs/wal-spec.md`](spec/docs/wal-spec.md)
- [`spec/docs/engine-api-spec.md`](spec/docs/engine-api-spec.md)
- [`spec/issues/expanded-implementation-roadmap.md`](spec/issues/expanded-implementation-roadmap.md)

This repository intentionally starts small: the skeleton compiles, exposes the crate boundaries, and includes enough WAL/disk/memtable scaffolding for the first serious PRs to land cleanly.
