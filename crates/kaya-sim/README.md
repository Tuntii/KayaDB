# kaya-sim

Deterministic simulation runner, fault injection harness, and linearizability tooling for KayaDB.

`kaya-sim` is where KayaDB’s correctness-first story becomes executable. It runs real engine and Raft code against deterministic schedules so crashes, partitions, and ordering bugs can be reproduced from a seed.

## What this crate includes

- `SimRunner` for deterministic workload execution
- `SimulationConfig` and `SimulationReport`
- trace replay with `replay_trace(...)`
- cluster simulation helpers such as `ClusterSim`
- a linearizability checker and history types
- re-exports of `SimDisk` fault scheduling types from `kaya-io`
- `NodeController` utilities used for process-level control in tests

## Example

```rust
use kaya_sim::{replay_trace, SimRunner, SimulationConfig, SimSeed};

let report = SimRunner::new(SimulationConfig {
    seed: SimSeed(0xdead_beef),
    max_operations: 200,
    ..SimulationConfig::default()
})
.run();

assert!(report.invariant_failures.is_empty());
replay_trace(&report.trace).expect("trace should replay without divergence");
```

## Why it matters

The simulator exercises the same engine and consensus code paths used elsewhere in the workspace. Combined with deterministic seeds and `SimDisk`, that enables:

- reproducible crash scenarios,
- replayable traces,
- invariant-focused regression tests,
- linearizability checking for distributed behavior.

## Configuration knobs

`SimulationConfig` lets you control:

- random seed
- total operations
- keyspace size
- maximum generated value size
- weights for `put`, `get`, `delete`, `scan`, `flush`, `compact`, and crash events

## Typical workflow

1. Run a seeded simulation.
2. Capture the returned JSONL trace.
3. Replay the trace to verify deterministic reproduction.
4. Reduce the failing seed or trace into a regression test.

## Related crates

- `../kaya-io` — provides `SimDisk` and fault schedules
- `../kaya-engine` — real embedded engine exercised by the simulator
- `../kaya-raft` — real consensus logic exercised by cluster simulation
- `../kaya-server` — process-level lifecycle helpers are tested here as well

See the [workspace README](../../README.md) and [development docs](../../docs/development.md) for more background.
