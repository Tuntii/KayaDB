# Getting Started with KayaDB

This guide gets you from zero to a running KayaDB node (or embedded data directory) and your first `put` / `get`.

> **Already have binaries?** Skip to [First commands](#first-commands-with-kayactl).  
> **Need install options?** See [Installation](installation.md) (crates.io, GitHub releases, or build from source).

---

## Prerequisites

| Requirement | Version |
|---|---|
| Rust toolchain | 1.85 or later (see `rust-toolchain.toml`) — only if building from source |
| `kayactl` + `kayadb-server` | [v0.1.46](releases.md) or latest from crates.io / `main` |
| Platform | Linux, macOS, or Windows |

Install Rust via [rustup](https://rustup.rs/) if you plan to build from source:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Quick install without cloning:

```bash
cargo install kayactl
cargo install kaya-server --bin kayadb-server
```

---

## Clone and build (from source)

```bash
git clone https://github.com/Tuntii/KayaDB.git
cd KayaDB

# Check formatting and lints (CI gates on these)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Build all crates
cargo build --workspace

# Run the full test suite (CI excludes kaya-jepsen-test on PR path)
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1
```

On Windows, if tests flake with port errors: `cargo test --workspace -j 1 -- --test-threads=1`. See [CI & Actions](ci-and-actions.md).
```

---

## Run a single-node server

```bash
# Create a data directory
mkdir -p /tmp/kayadb-data

# Start the server on the default localhost addresses
cargo run -p kaya-server --bin kayadb-server -- --data /tmp/kayadb-data

# Or specify explicit Raft and client addresses
cargo run -p kaya-server --bin kayadb-server -- \
  --data /tmp/kayadb-data \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379
```

The server writes its WAL, SSTables, and manifest under `--data`.

Default addresses:

| Address | Default | Used for |
|---|---|---|
| `--raft-addr` | `127.0.0.1:7481` | Raft peer traffic between nodes |
| `--client-addr` | `127.0.0.1:7379` | Client traffic from `kayactl` or `kaya-client` |

The default bind address is localhost. Keep it that way unless you have read the [security guide](security.md) and placed the node behind private networking.

---

## First commands with kayactl

`kayactl` speaks to the server over TCP when `--server` is given, or operates directly on a data directory in embedded mode.

### Embedded mode (no server needed)

```bash
DATA=/tmp/kayadb-data

# Write a key
cargo run -p kayactl -- --data $DATA put hello world

# Read it back
cargo run -p kayactl -- --data $DATA get hello

# Delete it
cargo run -p kayactl -- --data $DATA delete hello

# Scan a range
cargo run -p kayactl -- --data $DATA scan he
```

### Server mode

```bash
# Connect to a running server
cargo run -p kayactl -- --server 127.0.0.1:7379 put hello world
cargo run -p kayactl -- --server 127.0.0.1:7379 get hello
```

---

## Inspect storage files

KayaDB formats are designed to be inspectable without external tooling.

```bash
DATA=/tmp/kayadb-data

# Inspect the WAL segment
cargo run -p kayactl -- inspect wal $DATA/wal/0000000000000001.wal

# Inspect an SSTable
cargo run -p kayactl -- inspect sstable $DATA/sst/0000000000000001.sst

# Inspect the manifest
cargo run -p kayactl -- inspect manifest $DATA/MANIFEST-000001
```

Each command prints records in human-readable format including offsets, CRC status, and entry payloads. Add `--json` to emit structured JSON for automation.

### JSON output examples

**WAL inspection** (`inspect wal --json`):

```json
{
  "segment": "0000000000000001.wal",
  "records": [
    {
      "offset": 0,
      "lsn": 1,
      "sequence": 1,
      "type": "PUT",
      "key_len": 6,
      "value_len": 5
    },
    {
      "offset": 59,
      "lsn": 2,
      "sequence": 2,
      "type": "PUT",
      "key_len": 6,
      "value_len": 3
    }
  ],
  "warnings": []
}
```

Fields: `offset` (byte position), `lsn` (log sequence number), `sequence` (write order), `type` (PUT/DEL), `key_len` / `value_len` (or `null` for DEL).

**SSTable inspection** (`inspect sstable --json`):

```json
{
  "path": "./data/sst/0000000000000001.sst",
  "version": 4,
  "mvcc": true,
  "entry_count": 3,
  "min_seq": 1,
  "max_seq": 3,
  "entries": [
    {
      "seq": 1,
      "commit_ts": 1,
      "type": "put",
      "user_key": "user:1",
      "key": "user:1",
      "value": "alice"
    },
    {
      "seq": 2,
      "commit_ts": 2,
      "type": "put",
      "user_key": "user:2",
      "key": "user:2",
      "value": "bob"
    },
    {
      "seq": 3,
      "commit_ts": 3,
      "type": "del",
      "user_key": "user:3",
      "key": "user:3"
    }
  ],
  "warnings": []
}
```

Fields: `version` (SSTable format), `mvcc` (multi-version), `entry_count`, `seq` (sequence at write), `commit_ts` (MVCC timestamp, present if `mvcc=true`), `type` (put/del), `value` (present only for PUT).

**Manifest inspection** (`inspect manifest --json`):

```json
{
  "path": "./data/MANIFEST-000001",
  "last_sequence": 3,
  "last_edit_seq": 2,
  "live_tables": [
    {
      "table_id": 1,
      "level": 0,
      "path": "sst/0000000000000001.sst",
      "entries": 3,
      "min_seq": 1,
      "max_seq": 3,
      "smallest": "user:1",
      "largest": "user:3"
    }
  ],
  "warnings": []
}
```

Fields: `last_sequence` (highest written key), `last_edit_seq` (manifest edit count), `live_tables` (active SSTables per level), `table_id`, `level` (LSM tree level).

---

## Database stats

```bash
cargo run -p kayactl -- --data $DATA stats
```

Reports memtable size, WAL segment count, SSTable count per level, and compaction state.

---

## Dry-run recovery

Run the recovery path without writing anything to disk:

```bash
cargo run -p kayactl -- --data $DATA recover --dry-run
```

Useful for verifying a data directory is consistent before starting a node.

---

## Multi-node cluster (quick setup)

```bash
# Node 1
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data /tmp/kaya-node1

# Node 2
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 2 \
  --raft-addr 127.0.0.1:7482 \
  --client-addr 127.0.0.1:7380 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data /tmp/kaya-node2

# Node 3
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 3 \
  --raft-addr 127.0.0.1:7483 \
  --client-addr 127.0.0.1:7381 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --data /tmp/kaya-node3
```

The cluster runs Raft consensus. Writes are proposed through the leader, and followers can return leader hints so clients can retry on the correct node.

---

## Checking Cluster Status & Metrics

You can remotely inspect the health, role, term, and engine statistics of any node in the cluster:

```bash
# Get human-readable status from Node 1
cargo run -p kayactl -- --server 127.0.0.1:7379 status

# Get JSON-formatted status for automation
cargo run -p kayactl -- --server 127.0.0.1:7379 status --json
```

**Example output:**
```text
role:          leader
term:          3
commit_index:  45
applied_index: 45
peer_count:    2
engine.put_count:          100
engine.get_count:          50
engine.delete_count:       5
engine.scan_count:         10
engine.wal_bytes_written:  2048
engine.wal_fsync_count:    105
engine.memtable_entries:   0
engine.sstable_count:      1
engine.last_sequence:      105
```

---

## Using the `kaya-client` Library

For application developers, KayaDB provides an ergonomic, async-native Rust client library (`kaya-client`) featuring automatic leader redirection:

```rust
use std::net::SocketAddr;
use kaya_client::KayaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:7379".parse()?;
    
    // Connects to the target node
    let mut client = KayaClient::connect(addr).await?;
    
    // Write value (transparently retries and redirects if Node 1 is a follower)
    client.put(b"hello", b"world").await?;
    
    // Read value
    if let Some(val) = client.get(b"hello").await? {
        println!("Value: {}", String::from_utf8_lossy(&val));
    }
    
    // Query statistics
    let stats = client.stats().await?;
    println!("Stats: {}", stats);
    
    Ok(())
}
```

---

## What files should I expect?

After local writes, the data directory may contain files like:

```text
<data-dir>/
  wal-000001.wal       append-only write-ahead log segment
  sst-000001.sst       immutable sorted table created after flush
  MANIFEST             append-only live-table metadata log
  CURRENT              pointer to the active manifest
```

The exact set depends on whether the memtable has flushed yet. WAL files usually appear first; SSTables and manifest entries appear after flush/compaction paths run.

Use `kayactl inspect ...` and `kayactl recover --dry-run` to understand a data directory before deleting or reusing it.

---

## Cleanup

For local experiments, stop all running `kayadb-server` processes and remove the data directories you created:

```bash
rm -rf /tmp/kayadb-data /tmp/kaya-node1 /tmp/kaya-node2 /tmp/kaya-node3
```

On Windows PowerShell:

```powershell
Remove-Item -Recurse -Force $env:TEMP\kayadb-data,$env:TEMP\kaya-node1,$env:TEMP\kaya-node2,$env:TEMP\kaya-node3
```

---

## Troubleshooting

| Symptom | What to check |
|---|---|
| `address already in use` | Another node is still running on the same `--raft-addr` or `--client-addr`. Stop it or choose different ports. |
| `not leader` | Send the command to the hinted leader, or let `kayactl`/`kaya-client` retry when a leader address is returned. |
| `NOT_FOUND` | The key does not exist in the current logical view, or you are reading a different data directory/cluster. |
| Recovery warning | Run `kayactl --data <dir> recover --dry-run --json` and inspect WAL/manifest files before continuing. |
| Public network exposure | Do not expose KayaDB ports directly; read [Security](security.md). |

---

## Next steps

- [Documentation index](README.md) — choose the next guide based on your goal
- [Installation](installation.md) — crates.io, release binaries, TLS feature
- [Usage scenarios](usage.md) — practical workflows (cluster, recovery, automation)
- [Architecture overview](architecture.md) — crate boundaries and data flow
- [CLI reference](cli-reference.md) — full `kayactl` command reference
- [Runbooks](runbooks/rolling-restart.md) — operate a cluster safely
- [Development guide](development.md) — tests, simulations, fuzzing
