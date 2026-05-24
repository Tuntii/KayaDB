# Manifest Spec

**Status:** Draft v0.1  
**Scope:** Manifest record format, CURRENT handling, live table publication and recovery  

---

## 1. Purpose

The manifest is the source of truth for metadata that cannot be derived safely from file existence alone.

It tracks:

- live SSTables,
- table levels,
- key ranges,
- sequence ranges,
- WAL flush/checkpoint boundaries,
- compaction publication,
- last durable metadata state.

Rule:

> An SSTable is live only if a durable manifest edit says it is live.

---

## 2. Files

```text
data/
  CURRENT
  MANIFEST-000001
  MANIFEST-000002
```

`CURRENT` contains the active manifest filename:

```text
MANIFEST-000001
```

`CURRENT` update protocol:

```text
1. write CURRENT.tmp
2. fsync CURRENT.tmp
3. rename CURRENT.tmp -> CURRENT
4. fsync data directory
```

MVP may best-effort directory fsync on platforms where it is limited, but SimDisk must model the boundary.

---

## 3. Manifest record framing

MVP may reuse a generic checksummed frame similar to WAL without WAL-specific LSN semantics.

Recommended frame:

```text
+----------------+--------+-----------------------------------+
| magic          | u32    | 0x4b4d414e, "KMAN"                |
| version        | u16    | manifest format version           |
| header_len     | u16    | bytes in header                   |
| record_type    | u16    | manifest edit type                |
| flags          | u16    | reserved                          |
| edit_seq       | u64    | monotonically increasing edit seq |
| payload_len    | u32    | payload bytes                     |
| header_crc32c  | u32    | CRC with checksum field zero      |
| payload_crc32c | u32    | CRC of payload                    |
| payload        | var    | encoded edit                      |
+----------------+--------+-----------------------------------+
```

Initial version: `1`.

---

## 4. Edit types

| Type | Meaning | Required for MVP? |
|---|---|---:|
| `CREATE_TABLE` | Add newly flushed/compacted SSTable | yes |
| `DELETE_TABLE` | Mark SSTable obsolete | yes for compaction |
| `SET_CURRENT_WAL` | Advance WAL checkpoint/segment boundary | later |
| `SET_LAST_SEQUENCE` | Persist latest sequence | yes |
| `COMPACTION_START` | Optional debug marker | no |
| `COMPACTION_FINISH` | Optional debug marker | no |
| `MANIFEST_ROLLOVER` | Link old/new manifest | later |

---

## 5. Table metadata

```rust
pub struct TableMetadata {
    pub table_id: TableId,
    pub level: u32,
    pub path: RelativePath,
    pub smallest_key: Bytes,
    pub largest_key: Bytes,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub entry_count: u64,
    pub file_size: u64,
    pub footer_checksum: u32,
}
```

`smallest_key` and `largest_key` are raw bytes. Human/JSON inspectors should encode them as hex by default.

---

## 6. Replay algorithm

```text
1. Read CURRENT
2. Open referenced manifest
3. offset = 0
4. Read next manifest frame
5. Validate magic/version/header/payload checksum
6. Decode edit payload
7. Apply edit to in-memory metadata builder
8. Repeat until EOF or allowed corrupted tail
9. Verify referenced live SSTables exist
10. Verify required live SSTable footers
11. Return ManifestState
```

Manifest corruption policy:

| Position | Default behavior |
|---|---|
| clean EOF | success |
| partial tail frame | recover prefix, truncate if configured |
| bad checksum at tail | recover prefix, truncate if configured |
| bad checksum in middle | fail open |
| unsupported version | fail open |
| missing live file | fail open |

---

## 7. Publication rules

### 7.1 Flush publication

```text
SSTable file fsync + rename + dir fsync
    ↓
append CREATE_TABLE edit
    ↓
fsync manifest
    ↓
table is live
```

### 7.2 Compaction publication

```text
output SSTable fsync + rename + dir fsync
    ↓
append CREATE_TABLE output + DELETE_TABLE inputs in one logical batch
    ↓
fsync manifest
    ↓
output live, inputs obsolete
```

If batch framing is not available in MVP, a compaction edit must be encoded as one payload containing both additions and deletions.

---

## 8. Manifest rollover

Not required for MVP, but the format should allow future rollover:

```text
1. Write snapshot of current ManifestState to MANIFEST-N
2. fsync MANIFEST-N
3. update CURRENT through temp+rename
4. old manifest becomes obsolete after CURRENT durable
```

Rollover must not be introduced until basic manifest replay has crash tests.

---

## 9. Inspector output

Command:

```bash
kayactl inspect manifest ./data/MANIFEST-000001
```

Human output example:

```text
manifest: MANIFEST-000001
records: 4
live_tables: 2
last_sequence: 120

edit=1 type=CREATE_TABLE table=1 level=0 seq=1..60 entries=60 path=sst/0000000000000001.sst
edit=2 type=CREATE_TABLE table=2 level=0 seq=61..120 entries=60 path=sst/0000000000000002.sst
```

JSON output should be stable enough for tests.

---

## 10. Invariants

| ID | Invariant |
|---|---|
| MAN-001 | Manifest replay deterministically reconstructs live table set |
| MAN-002 | File existence alone never makes an SSTable live |
| MAN-003 | Missing manifest-live SSTable fails open |
| MAN-004 | Corrupted non-tail manifest record fails open |
| MAN-005 | Compaction publication is atomic at manifest edit level |
| MAN-006 | `CURRENT` update never points to a partial manifest as committed state |

---

## 11. Acceptance criteria

Manifest is ready when:

- `CURRENT` can be created/read/updated through temp+rename,
- manifest edit encoding has checksum validation,
- replay reconstructs live tables,
- partial/corrupt tail behavior is tested,
- missing live SSTable fails open,
- flush publication crash tests pass,
- compaction publication crash tests pass.
