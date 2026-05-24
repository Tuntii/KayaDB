# Write-Ahead Log Spec

**Status:** Draft v0.2  
**Scope:** WAL file format, append semantics, recovery, durability  

---

## 1. Purpose

The WAL is KayaDB's primary durability mechanism.

In strict durability mode:

> A write may be acknowledged only after its WAL record is durable.

The WAL must survive process crashes and tolerate partial or corrupted tail records without panicking or silently resurrecting invalid data.

---

## 2. Design goals

- Append-only write path.
- Sequential recovery.
- Detect corrupted records.
- Stop at first invalid tail record.
- Truncate invalid tail when safe.
- Segment rotation.
- Inspectable binary format.
- Compatible with deterministic disk simulation.
- Decoder rejects oversized lengths before allocation.

---

## 3. Directory layout

```text
data/wal/
  0000000000000001.wal
  0000000000000002.wal
  0000000000000003.wal
```

Segment filename format:

```text
{segment_id:016x}.wal
```

MVP segment id may simply be monotonically increasing. Later it may become first LSN.

---

## 4. Record types

Initial logical payload types:

| Value | Type | Meaning |
|---:|---|---|
| 0 | NOOP | reserved/test record |
| 1 | PUT | set key to value |
| 2 | DELETE | tombstone key |

Future payload types:

| Type | Scope |
|---|---|
| RAFT_ENTRY | replicated log payload |
| SNAPSHOT_MARKER | snapshot/install boundary |
| CONFIG_CHANGE | cluster membership/change metadata |

The WAL framing validates record framing and checksums. It should not need to understand every future payload type.

---

## 5. Binary record format

All integer fields are little-endian.

```text
+----------------+--------+-----------------------------------+
| field          | size   | description                       |
+----------------+--------+-----------------------------------+
| magic          | u32    | constant 0x4b415941, "KAYA"       |
| version        | u16    | record format version             |
| header_len     | u16    | bytes in header                   |
| flags          | u16    | compression/encryption/etc.       |
| record_type    | u16    | logical payload type              |
| lsn            | u64    | monotonically increasing LSN      |
| sequence       | u64    | engine sequence number            |
| payload_len    | u32    | payload bytes                     |
| header_crc32c  | u32    | CRC of header with this field zero|
| payload_crc32c | u32    | CRC of payload                    |
| payload        | var    | encoded command                   |
+----------------+--------+-----------------------------------+
```

Header size v1:

```text
4 + 2 + 2 + 2 + 2 + 8 + 8 + 4 + 4 + 4 = 40 bytes
```

Constants:

```text
magic = 0x4b415941
version = 1
header_len = 40
flags = 0 for MVP
```

Unknown flags must be rejected in MVP.

---

## 6. LSN and sequence rules

- first LSN is `1`,
- LSN increases by `1` per WAL record,
- recovery rejects non-monotonic LSN unless an explicit gap policy is introduced,
- LSN is global across segments,
- MVP may set `sequence == lsn`,
- both fields remain because distributed log order and local visibility order may diverge later.

---

## 7. Payload encoding

### 7.1 PUT payload

```text
+-------------+--------+
| key_len     | u32    |
| value_len   | u32    |
| key         | bytes  |
| value       | bytes  |
+-------------+--------+
```

### 7.2 DELETE payload

```text
+-------------+--------+
| key_len     | u32    |
| key         | bytes  |
+-------------+--------+
```

### 7.3 Limits

MVP defaults:

```text
max_key_len = 4096 bytes
max_value_len = 16 MiB
max_payload_len = 32 MiB
```

Decoder must reject oversized lengths before allocation. This is both correctness and safety requirement.

---

## 8. Append protocol

Strict append:

```text
1. Allocate LSN and sequence
2. Encode payload
3. Build header
4. Compute payload_crc32c
5. Compute header_crc32c with checksum field zeroed
6. Append bytes to active segment
7. fsync active segment
8. Return AppendResult
```

`AppendResult`:

```rust
pub struct AppendResult {
    pub lsn: Lsn,
    pub sequence: SequenceNumber,
    pub segment_id: SegmentId,
    pub offset: u64,
    pub encoded_len: u32,
    pub durable: bool,
}
```

In strict mode, successful append returns only if `durable == true`.

---

## 9. Segment rotation

Config:

```rust
pub struct WalConfig {
    pub segment_max_bytes: u64,
    pub max_record_bytes: u32,
}
```

Rotation condition:

```text
if current_segment_len + encoded_record_len > segment_max_bytes:
    close current segment
    fsync current segment
    create next segment
    fsync wal directory after file creation
```

MVP may rotate only before appending a record. Records must never be split across segments.

---

## 10. Recovery protocol

Input:

```text
data/wal/*.wal
```

Algorithm:

```text
1. List segment files
2. Sort by segment id
3. For each segment:
   a. offset = 0
   b. read header-sized prefix
   c. if EOF at offset 0: segment empty; continue or stop according to policy
   d. if partial header: mark invalid tail; stop
   e. validate magic/version/header_len/flags
   f. validate header_crc32c
   g. validate payload_len <= max_payload_len
   h. read payload
   i. if partial payload: mark invalid tail; stop
   j. validate payload_crc32c
   k. validate monotonic LSN
   l. emit recovered record
   m. offset += header_len + payload_len
4. If invalid tail found, truncate segment to last_good_offset when allowed
5. Ignore later segments after invalid tail unless policy changes
```

### 10.1 Stop at first invalid record

Recovery must stop at the first invalid record. It must not scan forward searching for another magic number. Searching forward risks resurrecting bytes that were never durably committed as a record sequence.

### 10.2 Tail truncation

Tail truncation is allowed when:

- invalid record is at the tail of the active or last considered segment,
- all previous records are valid,
- no later segment is considered valid.

Tail truncation must be reported.

---

## 11. Recovery outcomes

```rust
pub struct WalRecoveryReport {
    pub records: Vec<RecoveredRecord>,
    pub last_lsn: Option<Lsn>,
    pub valid_bytes: u64,
    pub truncated_bytes: u64,
    pub warnings: Vec<WalWarning>,
}
```

Warnings:

```text
PartialHeader
PartialPayload
BadMagic
BadHeaderChecksum
BadPayloadChecksum
UnsupportedVersion
UnknownFlags
NonMonotonicLsn
TrailingSegmentsIgnored
TailTruncated
```

---

## 12. Corruption policy

| Position | Behavior |
|---|---|
| Tail record | recover prefix, truncate tail if allowed |
| Middle of active segment | recover prefix, stop; may require manual repair if later data exists |
| Earlier closed segment | fail normal open unless explicit salvage mode is enabled |
| Unknown segment filename | ignore or fail depending on strictness config |

MVP default:

- tolerate corrupted tail,
- fail on corruption in earlier closed segment,
- do not salvage automatically beyond valid prefix.

---

## 13. Durability modes

### 13.1 Strict

```text
write returns success only after WAL fsync success
```

### 13.2 Relaxed

```text
write may return success after append without fsync
```

Relaxed mode must be visibly marked in logs and config. Tests claiming durability must use strict mode.

---

## 14. Inspector output

Command:

```bash
kayactl inspect wal data/wal/0000000000000001.wal
```

Human output example:

```text
segment: 0000000000000001.wal
records: 3

offset=0   lsn=1 seq=1 type=PUT    key_len=6 value_len=12 checksum=ok
offset=58  lsn=2 seq=2 type=DELETE key_len=6              checksum=ok
offset=108 lsn=3 seq=3 type=PUT    key_len=6 value_len=14 checksum=ok
```

JSON output should be available:

```bash
kayactl inspect wal --json data/wal/0000000000000001.wal
```

---

## 15. Test requirements

Required tests:

1. encode/decode roundtrip,
2. reject bad magic,
3. reject unsupported version,
4. reject unknown flags,
5. reject bad header checksum,
6. reject bad payload checksum,
7. reject oversized payload before allocation,
8. recover multiple records,
9. recover across multiple segments,
10. truncate partial header tail,
11. truncate partial payload tail,
12. reject non-monotonic LSN,
13. strict append does not ACK on fsync failure,
14. SimDisk crash after ACK preserves record.

---

## 16. Invariants

| ID | Invariant |
|---|---|
| WAL-001 | Valid records form a contiguous LSN sequence |
| WAL-002 | Recovery returns a prefix of appended valid records |
| WAL-003 | A bad checksum record is never emitted as recovered |
| WAL-004 | Strict ACK implies record is in recovered prefix after crash |
| WAL-005 | Recovery is idempotent |
| WAL-006 | Tail truncation never removes a previously recovered valid record |
| WAL-007 | Decoder rejects oversized lengths before allocation |

---

## 17. Acceptance criteria

WAL implementation is ready when:

- record format is implemented and documented,
- `FileDisk` WAL append/recover tests pass,
- `SimDisk` partial write tests pass,
- corrupted tail is truncated,
- strict fsync failure does not ACK,
- `kayactl inspect wal` can print records,
- TLA+ WAL durable-prefix model exists,
- property tests cover random append/crash/recover sequences.
