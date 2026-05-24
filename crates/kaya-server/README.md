# kaya-server

TCP server process for KayaDB with Raft consensus, cluster bootstrap, and client routing.

This crate exposes KayaDB as a standalone networked service. It wires together:

- `kaya-engine` for local storage,
- `kaya-raft` for replicated write coordination,
- `kaya-net` for TCP transport and client framing.

The package includes both a library API and the `kayadb-server` binary.

## What the server does

- opens a local engine in the configured data directory,
- listens for Raft peer traffic,
- listens for client requests,
- routes `PUT`/`DELETE` through Raft replication,
- serves `GET`/`SCAN` from the leader,
- returns leader hints so clients can retry against the correct node,
- exposes health and status endpoints over the client protocol.

## Binary

The binary target is named `kayadb-server`.

### Single-node example

```bash
cargo run -p kaya-server --bin kayadb-server
```

### Three-node local cluster example

```bash
# Node 1
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data ./data1

# Node 2
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 2 \
  --raft-addr 127.0.0.1:7482 \
  --client-addr 127.0.0.1:7380 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data ./data2

# Node 3
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 3 \
  --raft-addr 127.0.0.1:7483 \
  --client-addr 127.0.0.1:7381 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --data ./data3
```

## Library surface

The library re-exports the main cluster types:

- `ClusterConfig`
- `ClusterNode`

Use these if you want to host the server runtime from another Rust binary instead of using the provided CLI entrypoint.

## Request model

Client opcodes currently support:

- `PUT`
- `GET`
- `DELETE`
- `SCAN`
- `HEALTH`
- `STATS`

Writes are proposed through Raft and acknowledged after commit+apply. Reads are leader-routed for linearizable behavior in the current design.

## Related crates

- `../kaya-client` — async Rust client
- `../kayactl` — CLI client and inspection tool
- `../kaya-net` — transport and codec layer
- `../kaya-raft` — replicated state machine logic

See the [workspace README](../../README.md) and [getting started guide](../../docs/getting-started.md) for full setup instructions.
