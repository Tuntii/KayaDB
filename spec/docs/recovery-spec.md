# Recovery Spec

**Status:** Draft v0.1  
**Scope:** Cross-layer database open/recovery behavior across manifest, SSTables and WAL  

---

## 1. Purpose

Recovery is the process that turns files on disk into a valid in-memory database state after clean shutdown, process crash, partial write, interrupted flush or interrupted compaction.

The key requirement:

> Recovery must be idempotent and must not silently lose acknowledged strict-mode writes.

Formally for stable data directory state `D`:

```text
recover(recover(D)) == recover(D)
```

The equality means the recovered logical state and persistent cleanup result are the same, not necessarily that byte-for-byte metadata timestamps are identical.

---

## 2. Recovery inputs

Recovery reads:

| Input | Required? | Owner |
|---|---:|---|
| `LOCK` | no; created/acquired at open | engine |
| `CURRENT` | yes after manifest exists | manifest manager |
| `MANIFEST-*` | yes if referenced by CURRENT | manifest manager |
| `wal/*.wal` | yes for unflushed writes | WAL manager |
| `sst/*.sst` | yes when live in manifest | LSM |
| `tmp/*` | no; cleanup/quarantine only | various |
| `traces/*` | no | simulator |

---

## 3. High-level algorithm

```text
1. Validate and lock data directory
2. Create missing required subdirectories for empty DB
3. Read CURRENT if present
4. Open and replay manifest if present
5. Validate live SSTable metadata and required files
6. Discover WAL segments
7. Recover WAL durable prefix
8. Determine WAL replay start boundary
9. Rebuild active memtable from unreplayed WAL records
10. Clean or quarantine unreferenced temp files
11. Produce RecoveryReport
12. Open engine for reads/writes
```

Empty database special case:

- no `CURRENT`, no manifest and no WAL is valid,
- open creates required directories,
- first write creates WAL segment,
- first flush creates manifest/CURRENT.

---

## 4. Manifest-first ordering

Recovery must replay manifest before applying WAL to the memtable because manifest determines:

- live SSTables,
- flushed sequence boundaries,
- obsolete SSTables,
- last known compacted state,
- last published table metadata.

WAL replay must not assume that all WAL records are unflushed. Later, WAL deletion/checkpointing depends on manifest flush boundaries.

---

## 5. Recovery report

```rust
pub struct RecoveryReport {
    pub manifest_records_replayed: u64,
    pub live_sstable_count: u64,
    pub wal_records_replayed: u64,
    pub wal_truncated_bytes: u64,
    pub tmp_files_removed: u64,
    pub tmp_files_quarantined: u64,
    pub last_lsn: Option<Lsn>,
    pub last_sequence: Option<SequenceNumber>,
    pub warnings: Vec<RecoveryWarning>,
}
```

Warnings:

```text
WalTailTruncated
WalTrailingSegmentsIgnored
ManifestTailTruncated
UnreferencedSstableFound
TmpFileRemoved
TmpFileQuarantined
RelaxedDurabilityDataLostPossible
DirectoryFsyncBestEffort
```

---

## 6. Crash scenarios

### 6.1 Crash during WAL append

Expected:

- recovery returns valid prefix,
- partial/corrupt tail is not emitted,
- tail is truncated if allowed,
- no strict ACK was returned for failed fsync path.

### 6.2 Crash after WAL fsync before memtable apply

Expected:

- record is recovered,
- memtable contains the record after restart,
- client may have seen no ACK; this is allowed ambiguity.

### 6.3 Crash during SSTable temp write

Expected:

- temp file ignored or deleted,
- manifest does not reference it,
- WAL still contains records needed to rebuild state.

### 6.4 Crash after SSTable rename before manifest edit

Expected:

- SSTable exists but is not live,
- recovery may delete, quarantine or keep for inspection,
- logical state comes from WAL/live manifest state.

### 6.5 Crash after manifest edit fsync

Expected:

- SSTable is live,
- WAL records covered by flush boundary may eventually be garbage-collected,
- recovery fails if the manifest references a missing required SSTable.

### 6.6 Crash during compaction

Expected:

- compacted output is not live until manifest edit is durable,
- obsolete inputs are still required until manifest delete edit is durable,
- recovery chooses exactly the live table set from manifest replay.

---

## 7. Cleanup policy

`tmp/` cleanup must be conservative.

| File kind | Default action | Rationale |
|---|---|---|
| incomplete tmp SSTable | delete | never live |
| fsynced tmp SSTable not renamed | delete or quarantine | not manifest-live |
| renamed SSTable not in manifest | keep or quarantine in debug mode; delete in normal mode later | useful for inspection |
| obsolete SSTable after durable manifest delete | eligible for deletion | no longer live |
| unknown file in `wal/` | warn or fail according to strictness | avoid silent ambiguity |

MVP can keep unreferenced files and warn; deletion policy can be tightened after inspectors exist.

---

## 8. Error policy

Recovery should fail open only for recoverable tail corruption. It should fail fast for:

- unsupported format version,
- required live SSTable missing,
- required live SSTable footer corruption,
- manifest corruption in non-tail position,
- WAL corruption in closed earlier segment,
- directory path traversal or invalid filenames.

Explicit salvage mode can be added later, but must not be default.

---

## 9. Idempotence tests

Required pattern:

```text
1. Generate disk state with a crash point
2. Run recovery
3. Capture logical state and recovery report
4. Run recovery again on same data dir
5. Assert logical state unchanged
6. Assert second report has no additional destructive cleanup beyond allowed stable cleanup
```

Important invariants:

| ID | Invariant |
|---|---|
| REC-001 | Recovery is idempotent |
| REC-002 | Strict ACKed writes are present after recovery |
| REC-003 | Non-live files do not affect logical state |
| REC-004 | Required live file corruption fails open |
| REC-005 | Manifest replay defines live SSTable set |
| REC-006 | WAL recovery emits only valid prefix |

---

## 10. Acceptance criteria

Recovery is ready when:

- empty DB open works,
- WAL tail truncation is reported,
- manifest replay is idempotent,
- unreferenced temp files do not become live,
- crash during flush keeps acknowledged writes,
- crash during compaction preserves visible state,
- missing required live SSTable fails open,
- recovery report is exposed to tests and CLI diagnostics.
