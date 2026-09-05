# kaya-sim

Deterministic simulation runner, fault injection harness, and linearizability tooling for KayaDB.

`kaya-sim` is where KayaDB’s correctness-first story becomes executable. It runs real engine and Raft code against deterministic schedules so crashes, partitions, and ordering bugs can be reproduced from a seed.

## What this crate includes

- `SimRunner` for deterministic workload execution
- `SimulationConfig` and `SimulationReport`
- trace replay with `replay_trace(...)`
- cluster simulation helpers such as `ClusterSim`
- a linearizability checker and history types (`check_concurrent`, greedy `minimal_counterexample`, MUS enumeration)
- `kaya-wgl` CLI explorer: JSONL history in, greedy counterexample (and optional MUSs) out
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

## WGL explorer (`kaya-wgl`)

Offline diagnosis of a recorded KV history. Input is JSONL (file or stdin); output is the greedy minimal counterexample, and optionally every inclusion-minimal unsatisfiable subset (MUS) under the WGL op cap (14).

```bash
# greedy report (exit 1 if not linearizable)
cargo run -p kaya-sim --bin kaya-wgl -- history.jsonl

# enumerate MUSs + machine JSON
cargo run -p kaya-sim --bin kaya-wgl -- --mus --json history.jsonl

# stdin
cat history.jsonl | cargo run -p kaya-sim --bin kaya-wgl -- --mus -
```

JSONL: one object per op. Unknown fields are errors. Byte fields are UTF-8, or hex if prefixed `0x`.

```json
{"client":0,"start":1,"end":2,"op":"put","key":"k","value":"v","result":"ok"}
{"client":1,"start":1,"end":3,"op":"get","key":"k","result":"v"}
{"op":"get","key":"k","result":null}
{"op":"delete","key":"k","result":"ok"}
{"op":"scan","prefix":"a","result":[["a1","v"]]}
```

`start`/`end` are a half-open tick interval; omit both to auto-assign sequential ticks. `put`/`delete` result is `"ok"` or `{"error":"..."}`. Flags: `--mus`, `--mus-cap N` (default 14), `--json`, `--help`.

Library API: `LinearizabilityChecker::minimal_counterexample` (greedy) and `minimal_unsatisfiable_subsets(cap)` (all MUSs). See [jepsen-design.md](../../docs/jepsen-design.md#wgl-explorer-kaya-wgl).

## Related crates

- `../kaya-io` — provides `SimDisk` and fault schedules
- `../kaya-engine` — real embedded engine exercised by the simulator
- `../kaya-raft` — real consensus logic exercised by cluster simulation
- `../kaya-server` — process-level lifecycle helpers are tested here as well

See the [workspace README](../../README.md) and [development docs](../../docs/development.md) for more background.
