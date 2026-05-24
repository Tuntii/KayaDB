# kaya-lsm

LSM-tree storage primitives for KayaDB: memtable management, SSTable encoding/decoding, manifest replay, and level-0 compaction support.

This crate provides the persistent sorted-storage layer that sits under `kaya-engine`. It is intentionally split from the engine so the on-disk structures can be tested, fuzzed, inspected, and evolved in isolation.

## What is inside

- `Memtable` and `ImmutableMemtable`
- `SstableBuilder` and `SstableReader`
- `SstEntry`, `SstFooter`, and footer decoding helpers
- Manifest encoding/decoding and replay functions
- Table metadata and live-set tracking
- Inspection helpers for SSTable and manifest files

## Example

```rust
use kaya_core::SequenceNumber;
use kaya_lsm::Memtable;

let mut memtable = Memtable::new();
memtable.put(b"user:1".to_vec(), b"alice".to_vec(), SequenceNumber::new(1));
memtable.put(b"user:2".to_vec(), b"bob".to_vec(), SequenceNumber::new(2));

let items = memtable.scan_prefix(b"user:");
assert_eq!(items.len(), 2);
```

## Responsibilities

### Memtable

The in-memory table stores the newest version of each key, including tombstones, and supports prefix scans in sorted key order.

### SSTable

The SSTable path handles immutable sorted files, block decoding, footer validation, and inspection. These code paths are also covered by fuzz targets in the workspace.

### Manifest

The manifest tracks live tables and sequence/edit metadata so engine recovery can reconstruct the current LSM state after restart.

## Why it is separate from the engine

Keeping LSM logic in its own crate makes it easier to:

- test persistent formats independently,
- fuzz decoders without dragging in the whole engine,
- build inspection and recovery tooling, and
- reason about storage invariants at a lower level.

## Useful exports

- `replay_manifest`
- `encode_manifest_edit` / `decode_manifest_edit`
- `inspect_manifest_path`
- `inspect_sstable_path`
- `CURRENT_FILE_NAME`, `CURRENT_TMP_FILE_NAME`, `MANIFEST_FILE_NAME`

## Related crates

- `../kaya-wal` — ordered write log feeding sequence history
- `../kaya-engine` — orchestrates memtable flush and compaction
- `../../fuzz` — decoder fuzz targets for SSTable and manifest formats

See the [workspace README](../../README.md) for project-wide context.
