# kayactl CLI Reference

`kayactl` is the command-line tool for KayaDB. It can operate in two modes:

- **Embedded mode** — opens the data directory directly (no server required)
- **Server mode** — connects to a running `kayadb-server` over TCP (`--server`)

---

## Global flags

| Flag | Default | Description |
|---|---|---|
| `--data <path>` | `./data` | Path to the KayaDB data directory (embedded mode) |
| `--server <host:port>` | — | Connect to a server; all KV commands go over TCP |
| `--durability <mode>` | `strict` | Durability mode: `strict` (fsync on every write) or `relaxed` |
| `--json` | off | Emit machine-readable JSON output instead of human-readable text |

---

## Key-value commands

### `put <key> <value>`

Write a key-value pair.

```bash
kayactl put hello world
kayactl --data /tmp/db put hello world
kayactl --server 127.0.0.1:7777 put hello world
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
kayactl --server 127.0.0.1:7777 get hello
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
kayactl --server 127.0.0.1:7777 delete hello
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
kayactl --server 127.0.0.1:7777 scan user:
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
```

**Output example:**
```
memtable_entries: 3
memtable_bytes:   96
wal_segments:     1
wal_bytes:        128
sstable_count:    2
sstable_bytes:    4096
last_lsn:         5
last_flush_lsn:   3
```

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

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Key not found (`get`) |
| `2` | Invalid argument |
| `3` | I/O error |
| `4` | Corruption detected |
| `5` | Not the Raft leader (server mode) |
