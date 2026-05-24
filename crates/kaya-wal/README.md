# kaya-wal

Write-ahead log support for KayaDB: record codec, append-only writer, inspection, and crash-safe recovery.

`kaya-wal` is responsible for turning logical write intents into durable log records that can be replayed after restart. It is designed around correctness and inspection rather than opaque magic.

## Features

- CRC32C-protected WAL record encoding and decoding
- Append-only `WalWriter`
- Segment-based WAL layout
- Recovery with truncation of corrupted or partial tails
- Human-inspectable WAL tooling via `inspect_wal_path`
- Warnings for damaged or suspicious records instead of silent failure

## Public API highlights

- `WalWriter`
- `WalPayload`
- `WalRecord` / `WalRecordType`
- `recover_wal`
- `WalRecoveryReport`
- `inspect_wal_path`
- `WalInspection`

## Example

```rust
use std::sync::Arc;

use kaya_core::{DurabilityMode, Result, WalConfig};
use kaya_io::SimDisk;
use kaya_wal::{recover_wal, WalPayload, WalWriter};

async fn demo() -> Result<()> {
    let disk = Arc::new(SimDisk::new());
    let config = WalConfig::default();

    let mut writer = WalWriter::open(config.clone(), disk.clone()).await?;
    writer
        .append(
            WalPayload::Put {
                key: b"user:1".to_vec(),
                value: b"alice".to_vec(),
            },
            DurabilityMode::Strict,
        )
        .await?;

    let report = recover_wal(config, disk).await?;
    assert_eq!(report.records.len(), 1);
    Ok(())
}
```

## Recovery model

Recovery is built to preserve the durable prefix:

- strictly persisted records should survive crash/restart,
- non-fsynced trailing writes may be lost,
- malformed tails are truncated rather than replayed blindly.

That behavior is exercised heavily in unit tests and higher-level simulation.

## When to use this crate directly

Use `kaya-wal` directly if you are:

- experimenting with WAL format behavior,
- writing tooling that inspects raw log segments,
- testing durability logic independently from the full engine.

If you want an embedded key-value API, prefer `kaya-engine`.

## Related crates

- `../kaya-io` — abstract disk backends used during append/recovery
- `../kaya-core` — shared config and error types
- `../kaya-engine` — consumes WAL during normal operation and recovery

See the [workspace README](../../README.md) for the full architecture.
