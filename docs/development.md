# Development Guide

This guide covers the KayaDB development workflow: writing tests, running simulations, fuzzing, benchmarking, and contributing a new feature.

---

## Prerequisites

- Rust 1.85+ (enforced by `rust-toolchain.toml`)
- For fuzzing: Rust nightly + `cargo install cargo-fuzz`
- For benchmarks: stable toolchain is sufficient

---

## Daily workflow

```bash
# Format check (CI gate)
cargo fmt --all -- --check

# Auto-format
cargo fmt --all

# Lint (CI gate — all warnings treated as errors)
cargo clippy --workspace --all-targets -- -D warnings

# Full test suite
cargo test --workspace

# Single crate test
cargo test -p kaya-wal
```

---

## Test strategy

KayaDB has three tiers of tests:

### 1. Unit tests (inline, per crate)

Located in `#[cfg(test)]` modules inside each crate. Test individual functions in isolation — codec round-trips, CRC validation, memtable ordering.

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_wal_round_trip() { ... }
}
```

### 2. Integration tests (crash and recovery)

These are the most important tests in the project. They exercise the full write → crash → recovery path using `SimDisk`:

```rust
// Typical pattern
let mut disk = SimDisk::with_fault_schedule(seed, fault_schedule);
let engine = Engine::open(config, disk.clone()).await?;
engine.put(b"k".to_vec(), b"v".to_vec(), opts).await?;
disk.inject_crash();                          // cuts power mid-write
let recovered = Engine::open(config, disk).await?;
assert_eq!(recovered.get(b"k", ...).await?, Some(b"v".to_vec())); // or None — never corrupted
```

The key invariant: after crash, the engine must never return corrupted data. It is acceptable for the last write to be absent (the durable-prefix property), but never for it to be garbled.

### 3. Simulation / linearizability tests

Located in `kaya-sim`. These run concurrent workloads against a `SimCluster` and verify the history against a sequential linearizability model.

```bash
cargo test -p kaya-sim
```

---

## SimDisk fault injection

`SimDisk` supports the following fault kinds:

| `FaultKind` | Description |
|---|---|
| `DropWrite` | Silently discards a write — simulates a torn page |
| `ReturnError` | Returns an I/O error for the operation |
| `PartialWrite(n)` | Writes only the first `n` bytes of a buffer |
| `SkipSync` | `sync()` call succeeds but does not persist data |

A `FaultSchedule` is a list of `(operation_index, FaultKind)` pairs. Given the same seed, the disk injects the same faults in the same order — making failing scenarios fully reproducible from a seed alone.

```rust
let schedule = FaultSchedule::new(vec![
    (3, FaultKind::DropWrite),   // drop the 3rd write
    (7, FaultKind::SkipSync),    // skip the 7th sync
]);
let disk = SimDisk::new(schedule);
```

---

## Fuzzing

KayaDB ships three fuzz targets under `fuzz/`:

| Target | What it fuzzes |
|---|---|
| `fuzz_wal_decoder` | WAL record frame parser |
| `fuzz_sstable_footer` | SSTable footer parser |
| `fuzz_manifest_decoder` | Manifest entry decoder |

All three targets verify that arbitrary byte sequences never cause a panic or undefined behavior — only `Err(...)` returns.

```bash
# Requires nightly + cargo-fuzz
cargo +nightly fuzz run fuzz_wal_decoder

# Run with a specific corpus
cargo +nightly fuzz run fuzz_wal_decoder fuzz/corpus/wal_decoder/

# Run for a fixed time (seconds)
cargo +nightly fuzz run fuzz_wal_decoder -- -max_total_time=60
```

When `cargo-fuzz` finds a crash, the minimized input is saved to `fuzz/artifacts/<target>/`. Add it as a regression case to prevent future regressions.

---

## Benchmarks

Benchmarks live in `crates/kaya-bench/benches/`:

| File | What it measures |
|---|---|
| `wal_append.rs` | WAL append throughput at different record sizes |
| `sstable.rs` | SSTable encode and decode latency |
| `engine_workload.rs` | End-to-end put/get throughput over `FileDisk` |

```bash
# Run all benchmarks
cargo bench -p kaya-bench

# Run a specific benchmark
cargo bench -p kaya-bench -- wal_append

# Compare against a baseline (requires cargo-criterion)
cargo install cargo-criterion
cargo criterion -p kaya-bench
```

Benchmark results are not committed to the repository. Run them locally for relative comparisons.

---

## Spec-first development

KayaDB is spec-driven. The workflow for a new feature is:

1. **Find or write the spec** — locate the relevant document in `spec/docs/` or create a new one.
2. **Add an invariant** — if your feature has a correctness property (e.g., "after restart, all committed writes are visible"), add it to `spec/docs/testing-and-invariants-spec.md` with an `INV-XXX` identifier.
3. **Open the PR** — link the spec section and the invariant IDs in the PR description.
4. **Write a deterministic test first** — for any crash/recovery path, write the test before the implementation.

---

## Adding a new crate

1. Create the crate directory under `crates/`:
   ```bash
   cargo new --lib crates/kaya-newfeature
   ```
2. Add it to the workspace `members` list in the root `Cargo.toml`.
3. Add shared metadata (`edition`, `license`, `repository`) via `workspace.package` inheritance:
   ```toml
   [package]
   name = "kaya-newfeature"
   version = "0.1.0"
   edition.workspace = true
   license.workspace = true
   repository.workspace = true
   rust-version.workspace = true
   ```
4. Reference it in `workspace.dependencies` if other crates need it.

---

## PR checklist

Before opening a pull request:

- [ ] `cargo fmt --all` passes (no diff)
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] PR description links the relevant spec document or roadmap item
- [ ] New crash/recovery paths have a deterministic regression test
- [ ] New persistent format fields are added to the inspector output

---

## Useful links

- [Architecture overview](architecture.md)
- [Getting started](getting-started.md)
- [spec/docs/testing-and-invariants-spec.md](../spec/docs/testing-and-invariants-spec.md)
- [spec/docs/simulation-spec.md](../spec/docs/simulation-spec.md)
- [ROADMAP.md](../ROADMAP.md)
- [CONTRIBUTING.md](../CONTRIBUTING.md)
