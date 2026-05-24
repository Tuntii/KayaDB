# LSM Storage Format Spec

**Status:** Draft v0.2  
**Scope:** Memtable, SSTable, manifest integration, flush, compaction  

---

## 1. Purpose

The LSM layer turns WAL records into an efficient persistent key-value store.

MVP goals:

- support `PUT`, `GET`, `DELETE`, `SCAN prefix`,
- flush memtable to immutable SSTable,
- recover from WAL + manifest,
- detect corrupted SSTables,
- keep compaction correctness simple.

---

## 2. Data model

Logical entry:

```rust
pub enum ValueRecord {
    Put { value: Bytes, sequence: SequenceNumber },
    Delete { sequence: SequenceNumber },
}
```

Visibility rule:

> For a key, the visible state is the record with the highest sequence number among all live sources.

If the highest record is `Delete`, key is not found.

---

## 3. Key ordering

Keys are raw byte strings ordered lexicographically by unsigned byte value.

Ordering must be identical across:

- memtable,
- SSTable writer,
- SSTable reader,
- compaction,
- scan.

CLI may accept UTF-8 strings, but engine stores bytes.

---

## 4. Memtable

MVP implementation:

```rust
BTreeMap<KeyBytes, ValueRecord>
```

Requirements:

- ordered by key,
- stores latest record per key,
- supports point lookup,
- supports prefix/range iteration,
- supports snapshot for flush,
- tracks approximate memory usage.

Operations:

```rust
pub trait Memtable {
    fn put(&mut self, key: Bytes, value: Bytes, seq: SequenceNumber);
    fn delete(&mut self, key: Bytes, seq: SequenceNumber);
    fn get(&self, key: &[u8]) -> Option<ValueRecordRef<'_>>;
    fn scan_prefix(&self, prefix: &[u8]) -> MemtableIterator<'_>;
    fn freeze(self) -> ImmutableMemtable;
}
```

---

## 5. SSTable overview

MVP SSTable layout:

```text
+--------------------+
| data block 0       |
+--------------------+
| data block 1       |
+--------------------+
| ...                |
+--------------------+
| index block        |
+--------------------+
| footer             |
+--------------------+
```

Future optional blocks:

```text
filter block
compression dictionary
properties block
```

---

## 6. Data block format

```text
+----------------+--------+
| entry_count    | u32    |
| restart_count  | u32    |
| entries        | var    |
| restarts       | var    |
| block_crc32c   | u32    |
+----------------+--------+
```

Simple MVP entry format:

```text
+----------------+--------+
| key_len        | u32    |
| value_len      | u32    | 0 for delete |
| sequence       | u64    |
| kind           | u8     | 1=put, 2=delete |
| key            | bytes  |
| value          | bytes  |
+----------------+--------+
```

MVP can omit prefix compression. Later, add restart points and prefix compression without changing visibility semantics.

---

## 7. Index block format

Index entry:

```text
+----------------+--------+
| separator_len  | u32    |
| block_offset   | u64    |
| block_len      | u32    |
| first_seq      | u64    |
| last_seq       | u64    |
| separator_key  | bytes  |
+----------------+--------+
```

Index block is sorted by separator key. MVP point lookup may binary search index block then scan inside block.

---

## 8. Footer format

```text
+----------------------+--------+
| index_block_offset   | u64    |
| index_block_len      | u32    |
| table_min_seq        | u64    |
| table_max_seq        | u64    |
| entry_count          | u64    |
| format_version       | u16    |
| footer_len           | u16    |
| footer_crc32c        | u32    |
| magic                | u32    | 0x4b535354, "KSST" |
+----------------------+--------+
```

Reader opens SSTable by reading last fixed footer size, checking magic and CRC.

---

## 9. SSTable writer

Requirements:

- input iterator must be sorted,
- duplicate keys in a flush should be collapsed to latest sequence,
- emits checksummed blocks,
- writes footer last,
- writes to temp path first,
- fsyncs file,
- renames into `sst/`,
- fsyncs directory,
- publishes through manifest, not file existence.

---

## 10. SSTable reader

Requirements:

- validate footer,
- validate index block checksum,
- validate data block checksum when read,
- support point lookup,
- support prefix scan,
- return structured corruption error.

MVP may keep index block in memory.

---

## 11. Flush protocol

```text
1. Freeze current memtable
2. Create new active memtable
3. Write immutable memtable to tmp SSTable
4. fsync tmp SSTable
5. Rename tmp SSTable to final path
6. fsync sst directory
7. Append manifest CREATE_TABLE edit
8. fsync manifest
9. Mark immutable memtable flushed
10. WAL segments older than flush boundary become eligible for deletion later
```

Crash behavior:

| Crash point | Recovery behavior |
|---|---|
| before tmp write complete | ignore/delete tmp |
| after tmp fsync before rename | ignore/delete tmp |
| after rename before manifest | SSTable exists but unreferenced; ignore/delete/quarantine |
| after manifest fsync | SSTable live |

---

## 12. Compaction protocol

Initial compaction: L0 to L1 only.

```text
1. Select input SSTables
2. Merge entries by key and sequence
3. Drop overwritten older entries
4. Preserve tombstones if they may hide older data
5. Write output SSTable to tmp
6. fsync output
7. Rename output
8. fsync directory
9. Append manifest edit: add output, delete inputs
10. fsync manifest
11. Delete obsolete input files eventually
```

Important rule:

> A compacted output is not live until the manifest edit is durable.

---

## 13. Tombstone policy

MVP simple policy:

- tombstones are retained during L0 compaction,
- tombstones can be dropped only if there are no older tables that may contain the deleted key,
- if uncertain, keep tombstone.

Correctness beats space reclamation.

---

## 14. Read semantics across sources

Source priority:

1. active memtable,
2. immutable memtables newest to oldest,
3. L0 SSTables newest to oldest,
4. lower levels newest sequence where overlapping possible.

For MVP with no levels:

- sort SSTables by max sequence descending for point lookup,
- first matching key wins,
- scans merge sources and choose highest sequence per key.

---

## 15. Scan semantics

`SCAN prefix` returns visible keys with the prefix, sorted by key.

Must not return:

- deleted keys,
- older overwritten values,
- duplicate keys.

MVP can implement scan by merging iterators naively. Performance is secondary.

---

## 16. Invariants

| ID | Invariant |
|---|---|
| LSM-001 | SSTable entries are sorted by key |
| LSM-002 | SSTable block checksum mismatch is never ignored |
| LSM-003 | Manifest replay determines the live table set |
| LSM-004 | Flush publication is atomic through manifest |
| LSM-005 | Compaction preserves visible state |
| LSM-006 | Tombstone hides older put records |
| LSM-007 | Scan returns keys in lexicographic order |
| LSM-008 | Recovery after flush crash is idempotent |
| LSM-009 | SSTable file existence alone does not make it live |

---

## 17. Acceptance criteria

LSM layer is ready when:

- memtable supports put/get/delete/scan,
- SSTable writer/reader works on sorted entries,
- SSTable corruption is detected,
- manifest replay reconstructs live tables,
- flush crash tests pass,
- simple compaction preserves state,
- scan does not return duplicates or deleted keys.
