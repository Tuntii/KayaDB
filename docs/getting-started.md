# Getting Started with KayaDB

This guide walks you through building KayaDB, running the server, and using `kayactl` to interact with it.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Rust toolchain | 1.85 or later (see `rust-toolchain.toml`) |
| cargo | Ships with Rust |
| Linux / macOS / Windows | All platforms supported for development |

Install Rust via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

---

## Clone and build

```bash
git clone https://github.com/Tuntii/KayaDB.git
cd KayaDB

# Check formatting and lints (CI gates on these)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Build all crates
cargo build --workspace

# Run the full test suite
cargo test --workspace
```

---

## Run a single-node server

```bash
# Create a data directory
mkdir -p /tmp/kayadb-data

# Start the server on the default port (7777)
cargo run -p kaya-server -- --dir /tmp/kayadb-data

# Or specify a custom port
cargo run -p kaya-server -- --dir /tmp/kayadb-data --port 7777
```

The server writes its WAL, SSTables, and manifest under `--dir`.

---

## First commands with kayactl

`kayactl` speaks to the server over TCP when `--server` is given, or operates directly on a data directory in embedded mode.

### Embedded mode (no server needed)

```bash
DATA=/tmp/kayadb-data

# Write a key
cargo run -p kayactl -- --dir $DATA put hello world

# Read it back
cargo run -p kayactl -- --dir $DATA get hello

# Delete it
cargo run -p kayactl -- --dir $DATA delete hello

# Scan a range
cargo run -p kayactl -- --dir $DATA scan --from a --to z
```

### Server mode

```bash
# Connect to a running server
cargo run -p kayactl -- --server 127.0.0.1:7777 put hello world
cargo run -p kayactl -- --server 127.0.0.1:7777 get hello
```

---

## Inspect storage files

KayaDB formats are designed to be inspectable without external tooling.

```bash
DATA=/tmp/kayadb-data

# Inspect the WAL segment
cargo run -p kayactl -- inspect wal $DATA/wal-000001.wal

# Inspect an SSTable
cargo run -p kayactl -- inspect sstable $DATA/sst-000001.sst

# Inspect the manifest
cargo run -p kayactl -- inspect manifest $DATA/MANIFEST
```

Each command prints records in human-readable format including offsets, CRC status, and entry payloads.

---

## Database stats

```bash
cargo run -p kayactl -- --dir $DATA stats
```

Reports memtable size, WAL segment count, SSTable count per level, and compaction state.

---

## Dry-run recovery

Run the recovery path without writing anything to disk:

```bash
cargo run -p kayactl -- --dir $DATA recover --dry-run
```

Useful for verifying a data directory is consistent before starting a node.

---

## Multi-node cluster (quick setup)

```bash
# Node 1
cargo run -p kaya-server -- --dir /tmp/kaya-node1 --port 7771 \
  --peers 127.0.0.1:7772,127.0.0.1:7773 --node-id 1

# Node 2
cargo run -p kaya-server -- --dir /tmp/kaya-node2 --port 7772 \
  --peers 127.0.0.1:7771,127.0.0.1:7773 --node-id 2

# Node 3
cargo run -p kaya-server -- --dir /tmp/kaya-node3 --port 7773 \
  --peers 127.0.0.1:7771,127.0.0.1:7772 --node-id 3
```

The cluster runs Raft consensus. Reads and writes are routed through the current leader.

---

## Next steps

- [Architecture overview](architecture.md) — understand crate boundaries and data flow
- [CLI reference](cli-reference.md) — full `kayactl` command reference
- [Development guide](development.md) — writing tests, running simulations, fuzz testing
- [spec/](../spec/README.md) — full technical specification
