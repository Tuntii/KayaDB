# kaya-core

Shared errors, typed IDs, checksum helpers, and configuration primitives for the KayaDB workspace.

`kaya-core` is the lowest-level crate in the project. It intentionally stays small and dependency-light so the higher storage, network, and simulation layers can share one consistent vocabulary for errors, sequence numbers, log sequence numbers, durability settings, and size limits.

## What lives in this crate

- `KayaError` and `Result<T>`
- Typed IDs such as `Lsn` and `SequenceNumber`
- Configuration types:
  - `EngineConfig`
  - `DurabilityConfig` / `DurabilityMode`
  - `WalConfig`
  - `MemtableConfig`
  - `SstableConfig`
  - `LimitsConfig`
- Shared byte aliases and key/value types
- `crc32c()` checksum helper used by on-disk formats

## Why it exists

The rest of the workspace depends on `kaya-core` to avoid duplicating foundational concepts in each crate. That keeps error handling uniform and makes tests, CLI output, and wire responses easier to reason about.

## Example

```rust
use kaya_core::{crc32c, DurabilityMode, EngineConfig};

let checksum = crc32c(b"hello, kayadb");
assert_ne!(checksum, 0);

let config = EngineConfig {
    durability: kaya_core::DurabilityConfig {
        mode: DurabilityMode::Strict,
        ..Default::default()
    },
    ..EngineConfig::default()
};

assert_eq!(config.durability.mode, DurabilityMode::Strict);
```

## Intended audience

Most applications will not depend on `kaya-core` directly unless they are:

- building on top of KayaDB internals,
- writing custom tooling around engine/WAL/LSM primitives, or
- integrating multiple KayaDB crates in one binary.

If you are looking for a storage API, start with `kaya-engine`. If you are looking for a TCP client, start with `kaya-client`.

## Related crates

- `../kaya-io` — disk abstraction and deterministic fault injection
- `../kaya-wal` — write-ahead log codec and recovery
- `../kaya-lsm` — memtable, SSTable, and manifest logic
- `../kaya-engine` — embedded key-value engine

## Status

This crate is part of the experimental KayaDB workspace and may evolve with the rest of the project.

See the [workspace README](../../README.md) for the bigger picture.
