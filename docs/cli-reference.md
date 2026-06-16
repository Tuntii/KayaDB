# kayactl CLI Reference

`kayactl` is the command-line tool for KayaDB. It can operate in two modes:

- **Embedded mode** — opens the data directory directly (no server required)
- **Server mode** — connects to a running `kayadb-server` over TCP (`--server`)

Use embedded mode when inspecting or repairing a local data directory. Use server mode when interacting with a running node or cluster.

---

## Global flags

| Flag | Default | Description |
|---|---|---|
| `--data <path>` | `./data` | Path to the KayaDB data directory (embedded mode) |
| `--server <host:port>` | — | Connect to a server; all KV commands go over TCP |
| `--durability <mode>` | `strict` | Durability mode: `strict` (fsync on every write) or `relaxed` |
| `--json` | off | Emit machine-readable JSON output instead of human-readable text |

---

## Common workflows

### Local smoke test

```bash
kayactl --data ./data put hello world
kayactl --data ./data get hello
kayactl --data ./data scan he
kayactl --data ./data stats
```

### Check a data directory after a crash

```bash
kayactl --data ./data recover --dry-run
kayactl inspect wal ./data/wal-000001.wal
kayactl inspect manifest ./data/MANIFEST
```

### Talk to a running node

```bash
kayactl --server 127.0.0.1:7379 health
kayactl --server 127.0.0.1:7379 put user:1 ada
kayactl --server 127.0.0.1:7379 get user:1
kayactl --server 127.0.0.1:7379 status --json
```

### Automation-friendly output

Use `--json` when a command feeds scripts, dashboards, or CI checks:

```bash
kayactl --json --data ./data stats
kayactl --json --server 127.0.0.1:7379 status
```

---

## Key-value commands

### `put <key> <value>`

Write a key-value pair.

```bash
kayactl put hello world
kayactl --data /tmp/db put hello world
kayactl --server 127.0.0.1:7379 put hello world
```

**Output (default):**
```
OK sequence=1 lsn=1 durable=true
```

**Output (`--json`):**
```json
{"ok": true, "sequence": 1, "lsn": 1, "durable": true}
```

---

### `get <key>`

Read a value by key. Exits with a non-zero code if the key is not found.

```bash
kayactl get hello
kayactl --server 127.0.0.1:7379 get hello
```

**Output (default):**
```
world
```

**Output when key is absent:**
```
NOT_FOUND
```

**Output (`--json`):**
```json
{"found": true, "value": "world"}
{"found": false}
```

---

### `delete <key>`

Write a tombstone for the key.

```bash
kayactl delete hello
kayactl --server 127.0.0.1:7379 delete hello
```

**Output:**
```
OK sequence=2 lsn=2 durable=true
```

---

### `scan <prefix>`

Scan all keys that start with the given prefix, ordered lexicographically.

```bash
kayactl scan user:
kayactl --server 127.0.0.1:7379 scan user:
```

**Output (default):**
```
user:alice  alice-data
user:bob    bob-data
```

**Output (`--json`):**
```json
{"items": [{"key": "user:alice", "value": "alice-data"}, {"key": "user:bob", "value": "bob-data"}]}
```

---

## Inspect commands

Inspect commands operate directly on files. They do not require a running server.

### `inspect wal <path>`

Dump all records in a WAL segment file. Shows sequence number, CRC status, record type, and payload bytes.

```bash
kayactl inspect wal /tmp/db/wal-000001.wal
kayactl inspect wal /tmp/db/wal-000001.wal --json
```

**Output example:**
```
WAL /tmp/db/wal-000001.wal  records=3  truncated_tail=false
  [0] offset=0     lsn=1  type=PUT   crc=OK  key="hello"  value="world"
  [1] offset=32    lsn=2  type=PUT   crc=OK  key="foo"    value="bar"
  [2] offset=64    lsn=3  type=DEL   crc=OK  key="hello"
```

A `truncated_tail=true` entry indicates the WAL contains a partial record at the end — this is expected after a crash and is handled transparently during recovery.

---

### `inspect sstable <path>`

Dump the footer, index block, and data blocks of an SSTable file. Shows bloom filter status, CRC validation, and key-value pairs per block.

```bash
kayactl inspect sstable /tmp/db/sst-000001.sst
kayactl inspect sstable /tmp/db/sst-000001.sst --json
```

**Output example:**
```
SSTable /tmp/db/sst-000001.sst
  footer:  entry_count=2  index_offset=128  bloom_offset=192  crc=OK
  block[0] offset=0  entries=2  crc=OK
    "foo"  →  "bar"
    "hello"  →  "world"
```

---

### `inspect manifest <path>`

Replay the manifest log and print each state transition event.

```bash
kayactl inspect manifest /tmp/db/MANIFEST
kayactl inspect manifest /tmp/db/MANIFEST --json
```

**Output example:**
```
Manifest /tmp/db/MANIFEST  events=2
  [0] FLUSH   seq=1  sstable="sst-000001.sst"  entry_count=2
  [1] COMPACT seq=2  inputs=["sst-000001.sst","sst-000002.sst"]  output="sst-000003.sst"
```

---

## Diagnostic commands

### `stats`

Print storage engine metrics for the data directory.

```bash
kayactl stats
kayactl --data /tmp/db stats
kayactl --data /tmp/db stats --json
kayactl --data /tmp/db stats --latency   # focused Track A durability + flush/compaction timers
```

**Output example (normal):**
```
put_count:         42
...
flush_count:       3
flush_total_us:    12345
flush_avg_us:      4115 (total/count)
...
```

Use `--latency` for a clean section with WAL fsync, flush, and compaction numbers plus eBPF correlation tips.

### `flush`

(Local/embedded mode only) Force a memtable flush to SSTable. This exercises the full LSM publish path (SST build, manifest edits, fsyncs, CURRENT update) so you can immediately observe the new latency metrics.

```bash
kayactl --data /tmp/db flush
kayactl --data /tmp/db --json flush
kayactl --data /tmp/db flush && kayactl --data /tmp/db stats --latency
```

**Output (human):**
```
OK flushed 1234 memtable entries. Live SSTables: 5
Flush latency metrics updated. Key values:
  flush_count:    4
  flush_total_us: 56789
  flush_avg_us:   14197
...
For full view + eBPF correlation tips: kayactl [--data ...] stats --latency
```

Great for Track A development: drive writes, flush, inspect `stats --latency`, attach `ebpf syscall-timeline` or `fsync-latency` in another terminal.

---

### `recover --dry-run`

Run the full recovery path — WAL replay, manifest replay, orphan detection — without writing anything to disk. Useful for verifying that a data directory is consistent.

```bash
kayactl recover --dry-run
kayactl --data /tmp/db recover --dry-run
kayactl --data /tmp/db recover --dry-run --json
```

**Output example:**
```
Recovery report:
  wal_records_replayed:  3
  wal_truncated_tail:    false
  manifest_events:       2
  orphaned_sstables:     0
  status:                OK
```

A non-zero exit code is emitted if recovery would fail.

---

## Linux eBPF observability (Track A / M12)

`kayactl ebpf` provides Linux-only observability helpers using bpftrace. It does **not** require eBPF at build time or for normal operation.

```bash
# Show available subcommands and guidance
kayactl ebpf
kayactl ebpf help

# Discover PIDs (great for 3-node local clusters) + active traces
kayactl ebpf list
kayactl ebpf status

# Trace fsync/fdatasync latency (in microseconds) for a running server
kayactl ebpf fsync-latency
kayactl ebpf fsync-latency --pid 12345

# Trace block-layer I/O latency (device + scheduler time)
kayactl ebpf block-latency --pid 12345

# New (Track A): broader syscall timeline with write/fsync correlation by TID + rename/unlink for flush/compaction
kayactl ebpf syscall-timeline [--pid N] [--run]
```

On non-Linux the command prints a helpful message and points to `scripts/ebpf/`.

**Prerequisites on the target Linux machine:**
- `bpftrace` package installed
- `sudo` (or appropriate BPF capabilities)
- A running `kayadb-server` (use `kayactl ebpf list` for auto-discovery of all local nodes)

See:
- `scripts/ebpf/README.md` (detailed usage + one-liners + correlation guide)
- `spec/docs/observability-spec.md` §7
- `ROADMAP.md` (Track A)

The probes are read-only diagnostics. They help answer "why are my strict fsyncs sometimes slow?" and "what is the cost of flush / compaction publish?" by showing kernel-side histograms + publish events that pure userspace timers (now also exposed as `flush_total_us` etc. in stats) cannot reveal. Use together with `kayactl stats --latency` / `flush` (to force publish paths) / `--server ... status`.

---

## Server health and status

These commands require `--server <addr>`.

### `health`

Check whether a node is reachable and what role it currently reports.

```bash
kayactl --server 127.0.0.1:7379 health
kayactl --server 127.0.0.1:7379 health --json
```

Typical human output:

```text
OK role=leader
```

### `status`

Print Raft and storage metrics for a node.

```bash
kayactl --server 127.0.0.1:7379 status
kayactl --server 127.0.0.1:7379 status --json
```

The status payload includes:

| Field | Meaning |
|---|---|
| `role` | Current Raft role: leader, follower, or candidate |
| `term` | Current Raft term |
| `commit_index` | Highest committed Raft log index known to the node |
| `applied_index` | Highest committed entry applied to the local engine |
| `peer_count` | Number of configured peers excluding the local node |
| `engine.*` | Storage counters such as puts, gets, WAL bytes, fsync count, SSTable count |

Followers may return `NOT_LEADER` for some client operations. When a leader hint is available, `kayactl` retries a limited number of times against the hinted address.

### `add-node` / `remove-node`

Propose a joint-consensus membership change on the current leader (requires `--server`).

```bash
# Add node 4 after it has started with --join-cluster and seed peers
kayactl --server 127.0.0.1:7379 add-node 4 127.0.0.1:7484 127.0.0.1:7383

# Remove node 4 from the voter set (cannot shrink below 2 voters)
kayactl --server 127.0.0.1:7379 remove-node 4
```

These map to client opcodes `ADD_MEMBER` (7) and `REMOVE_MEMBER` (8). The change commits asynchronously; poll `status` on all nodes until `peer_count` reflects the new roster.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Key not found (`get`) |
| `2` | Invalid argument |
| `3` | I/O error |
| `4` | Corruption detected |
| `5` | Not the Raft leader (server mode) |
