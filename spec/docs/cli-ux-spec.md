# CLI UX Spec

**Status:** Draft v0.1  
**Scope:** `kayactl` command structure, output contracts, inspect commands and exit codes  

---

## 1. Purpose

`kayactl` is both a user-facing tool and a debugging lens into KayaDB internals.

It must support:

- simple local key-value operations,
- data directory selection,
- WAL/SSTable/manifest inspection,
- simulation run/replay commands,
- machine-readable output for tests.

---

## 2. Global flags

Recommended global flags:

```text
--data <path>          data directory, default ./data
--config <path>        config file path
--durability <mode>    strict | relaxed, overrides config for write commands
--json                 machine-readable output
--quiet                suppress non-result logs
--verbose              include diagnostics
```

Rules:

- `--json` output must be stable enough for tests.
- Human output may improve over time but should remain understandable.
- Errors should go to stderr.

---

## 3. KV commands

### 3.1 `put`

```bash
kayactl --data ./data put user:1 '{"name":"Ada"}'
```

Human output:

```text
OK sequence=42 lsn=42 durable=true
```

JSON output:

```json
{"ok":true,"sequence":42,"lsn":42,"durable":true}
```

### 3.2 `get`

```bash
kayactl --data ./data get user:1
```

Found:

```text
{"name":"Ada"}
```

Not found:

```text
NOT_FOUND
```

Exit codes:

| Case | Code |
|---|---:|
| found | 0 |
| not found | 2 |
| error | 1 |

### 3.3 `delete`

```bash
kayactl --data ./data delete user:1
```

Human output:

```text
OK sequence=43 lsn=43 durable=true
```

### 3.4 `scan`

```bash
kayactl --data ./data scan user:
```

Human output:

```text
user:1 {"name":"Ada"}
user:2 {"name":"Linus"}
```

JSON output:

```json
{"items":[{"key":"user:1","value":"{\"name\":\"Ada\"}"}]}
```

MVP may treat CLI keys and values as UTF-8 strings. Engine stores bytes.

---

## 4. Inspect commands

### 4.1 WAL

```bash
kayactl inspect wal ./data/wal/0000000000000001.wal
```

Output:

```text
segment: 0000000000000001.wal
records: 3

offset=0   lsn=1 seq=1 type=PUT    key_len=6 value_len=12 checksum=ok
offset=58  lsn=2 seq=2 type=DELETE key_len=6              checksum=ok
```

On corruption:

```text
CORRUPTION offset=108 kind=BadPayloadChecksum tail=true recoverable=true
```

### 4.2 Manifest

```bash
kayactl inspect manifest ./data/MANIFEST-000001
```

Output should include:

- record count,
- live table count,
- last sequence,
- each edit type,
- warnings.

### 4.3 SSTable

```bash
kayactl inspect sstable ./data/sst/0000000000000001.sst
```

Output should include:

- table id if available,
- entry count,
- sequence range,
- key range as hex and best-effort UTF-8,
- block count,
- checksum status.

---

## 5. Recovery diagnostics

```bash
kayactl recover --data ./data --dry-run
```

MVP may not implement a standalone recover command immediately, but engine open should expose diagnostics used by this command later.

Human output example:

```text
recovery: ok
manifest_records=4 live_sstables=2 wal_records=12 wal_truncated_bytes=0 warnings=0
```

---

## 6. Simulation commands

```bash
kayactl sim run --seed 0xdeadbeef --ops 10000 --nemesis disk-partial-write,node-crash
kayactl sim replay traces/failure-0xdeadbeef.trace.jsonl
```

`kayadb-sim` may exist as a dedicated binary; `kayactl sim` can be a wrapper or alias.

---

## 7. Exit code policy

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | generic error |
| 2 | not found |
| 3 | corruption detected |
| 4 | invalid argument |
| 5 | invariant violation |
| 6 | lock conflict |

Exit code stability matters for scripts and tests.

---

## 8. Acceptance criteria

CLI UX is ready when:

- `put/get/delete/scan` call engine APIs,
- output includes sequence/LSN/durable for writes,
- not-found uses exit code 2,
- `inspect wal` prints valid records and corruption diagnostics,
- `--json` exists for at least inspect WAL and write results,
- CLI never parses persistent files with a separate incompatible parser.
