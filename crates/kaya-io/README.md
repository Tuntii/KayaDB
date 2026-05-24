# kaya-io

Disk abstraction for KayaDB with two interchangeable backends:

- `FileDisk` for real filesystem I/O and fsync behavior
- `SimDisk` for deterministic crash and fault simulation

This crate is the seam that lets KayaDB run the same higher-level engine code in both production-style and simulation-style environments.

## What it provides

- `Disk` trait used by the storage stack
- `FileDisk` for real on-disk operation
- `SimDisk` for deterministic testing and replayable failure scenarios
- `RelativePath` for safe repository-internal path handling
- Fault injection types:
  - `FaultKind`
  - `FaultRule`
  - `FaultSchedule`
  - `SimSeed`
- Crash and event inspection types:
  - `CrashReport`
  - `SimDiskEvent`

## Design notes

`SimDisk` models storage with two layers:

- **volatile bytes** that may be lost on crash
- **stable bytes** that survive once fsync-like operations complete

That allows KayaDB to test durable-prefix, torn-write, disk-full, and fsync-failure scenarios without maintaining a separate engine code path just for tests. Tiny crate, big chaos energy.

## Example: building a fault schedule

```rust
use kaya_io::{FaultKind, FaultRule, FaultSchedule, SimDisk, SimSeed};

let disk = SimDisk::with_faults(FaultSchedule {
    seed: SimSeed(42),
    rules: vec![FaultRule {
        operation_index: 0,
        kind: FaultKind::DiskFull,
    }],
});

let _ = disk;
```

## Path safety

Use `RelativePath` for internal engine/WAL/LSM paths. It rejects:

- absolute paths,
- Windows drive-prefixed paths,
- `..` path traversal.

That keeps file layout logic deterministic and easy to audit.

## Where it is used

- `kaya-wal` writes and recovers log segments through `Disk`
- `kaya-engine` opens against either `FileDisk` or `SimDisk`
- `kaya-sim` runs full deterministic workloads on `SimDisk`

## Good entry points

- `src/lib.rs` — public API surface
- `src/file.rs` — real filesystem backend
- `src/sim.rs` — deterministic simulation backend
- `src/path.rs` — safe relative path handling

See the [workspace README](../../README.md) for architecture context.
