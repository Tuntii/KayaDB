# kaya-raft

Raft consensus state machine for KayaDB.

`kaya-raft` implements the core in-memory protocol logic for leader election, log replication, commit tracking, and application ordering. It is deliberately separated from networking and storage so it can be driven deterministically in tests and simulations.

## Design philosophy

This crate keeps I/O out of the Raft state machine.

Callers are expected to:

- advance logical time with `tick()`,
- deliver inbound messages with `handle(...)`,
- send the returned outbound envelopes over their own transport,
- execute applied commands in the host state machine.

That split makes the implementation easier to test and pair naturally with the simulator.

## Public API highlights

- `RaftConfig`
- `RaftNode`
- `RaftStatus`
- `Role`
- `Envelope` and message types
- `MemLog`
- typed IDs such as `NodeId`, `Term`, and `LogIndex`

## Example shape

```rust
use kaya_raft::{NodeId, RaftConfig, RaftNode};

let mut node = RaftNode::new(RaftConfig {
    id: NodeId(1),
    peers: vec![NodeId(2), NodeId(3)],
    election_timeout_ticks: 15,
    heartbeat_interval_ticks: 3,
});

let outbound = node.tick();
let _ = outbound;
```

## Core operations

- `tick()` — advances timers and may start elections or send heartbeats
- `handle(env)` — processes a single inbound message and returns outbound messages
- `propose(command)` — leader-only append of a new command
- `broadcast()` — immediately emit `AppendEntries` after a proposal
- `status()` — inspect the node’s current role, term, and commit progress
- `drain_applied()` — retrieve newly applied commands for the host state machine

## Where it is used

- `kaya-server` hosts `RaftNode` inside a live TCP server
- `kaya-net` transports Raft `Envelope`s between peers
- `kaya-sim` stress-tests elections, partitions, crashes, and invariants deterministically

## Scope and limitations

This crate focuses on the current KayaDB prototype’s consensus loop. Features such as membership changes and snapshot/log compaction are outside the present scope.

See the [workspace README](../../README.md) and [architecture docs](../../docs/architecture.md) for the larger system context.
