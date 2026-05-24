# kaya-engine

Embedded storage engine API for KayaDB.

`kaya-engine` is the crate to reach for when you want to use KayaDB as a library inside your own Rust process. It composes the WAL, LSM layer, and disk backend behind a small async API for `put`, `get`, `delete`, and `scan` operations.

## What this crate does

- opens an engine over any `kaya_io::Disk` implementation,
- writes through the WAL before updating the memtable,
- flushes immutable SSTables and tracks them in the manifest,
- supports recovery on restart,
- exposes operational stats and recovery reports,
- supports explicit flush and compaction calls.

## Public API highlights

- `Engine<D>`
- `WriteOptions`, `ReadOptions`, `ScanOptions`
- `WriteResult`, `FlushResult`, `CompactionResult`
- `EngineStats`
- `RecoveryReport`
- free function `recover(...)`

## Example

Adapted from [`examples/embedded.rs`](examples/embedded.rs):

```rust
use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig, Result};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::FileDisk;

async fn demo() -> Result<()> {
    let data_dir = std::env::temp_dir().join("kayadb_embedded_example");
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(data_dir));

    let mut engine = Engine::open(config, disk).await?;

    engine
        .put(
            b"key1".to_vec(),
            b"value1".to_vec(),
            WriteOptions {
                durability: Some(DurabilityMode::Strict),
                idempotency_key: None,
            },
        )
        .await?;

    let value = engine.get(b"key1", ReadOptions::default()).await?;
    assert_eq!(value.as_deref(), Some(&b"value1"[..]));

    Ok(())
}
```

## Key operations

- `open` — open the engine and perform recovery
- `put` / `delete` — write through WAL and update in-memory state
- `get` / `scan_prefix` — read current logical values
- `flush` — move memtable contents into SSTables
- `compact` — compact level-0 tables
- `stats` — expose counters and storage stats
- `last_recovery` — inspect how startup recovery behaved

## Backends

The engine is generic over `Disk`, which means you can run it with:

- `FileDisk` for normal filesystem-backed use
- `SimDisk` for deterministic fault injection and crash testing

That same-code-path design is one of the central ideas in KayaDB.

## Examples and next steps

- Embedded example: [`examples/embedded.rs`](examples/embedded.rs)
- TCP server mode: `../kaya-server`
- Async client library: `../kaya-client`

See the [workspace README](../../README.md) and [architecture docs](../../docs/architecture.md) for more detail.
