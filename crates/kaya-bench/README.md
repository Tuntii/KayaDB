# kaya-bench

Criterion benchmark suite for KayaDB.

`kaya-bench` contains the workspace’s benchmark harnesses for measuring core storage paths such as WAL append throughput, SSTable encode/decode behavior, and end-to-end engine workloads.

## Benchmarks included

- `wal_append` — append performance for the write-ahead log
- `sstable` — SSTable encode/decode focused benchmarks
- `engine_workload` — broader engine-level workload measurements
- `smoke` — lightweight benchmark sanity coverage

## Running benchmarks

Run the whole suite:

```bash
cargo bench -p kaya-bench
```

Run an individual benchmark target:

```bash
cargo bench -p kaya-bench --bench wal_append
cargo bench -p kaya-bench --bench sstable
cargo bench -p kaya-bench --bench engine_workload
cargo bench -p kaya-bench --bench smoke
```

## Notes

- This crate is marked `publish = false` and is intended for workspace development, not crates.io distribution.
- Benchmarks are powered by `criterion`.
- Results are typically written under the workspace `target/criterion/` directory.

## Why this crate exists

Separating benchmarks from the storage crates keeps production dependencies lean while still making it easy to measure regressions and performance trade-offs during development.

## Related crates

- `../kaya-wal`
- `../kaya-lsm`
- `../kaya-engine`
- `../kaya-io`

See [BENCHMARKS.md](../../BENCHMARKS.md) and the [workspace README](../../README.md) for broader performance context.
