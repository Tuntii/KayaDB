# Engine API Spec

**Status:** Draft v0.2  
**Scope:** Embedded storage engine API, command semantics, CLI/server mapping  

---

## 1. Purpose

The engine API is the stable boundary between storage internals and external interfaces such as CLI, server, simulation and future Raft apply loop.

The API should make it easy to:

- run embedded tests,
- start a local server,
- drive deterministic simulations,
- later apply committed Raft log entries.

---

## 2. Logical operations

Supported MVP operations:

```text
PUT key value
GET key
DELETE key
SCAN prefix
```

Keys and values are bytes. CLI string handling is presentation only.

---

## 3. Rust API shape

```rust
pub struct Engine<D: Disk> {
    // internal fields
}

impl<D: Disk> Engine<D> {
    pub async fn open(config: EngineConfig, disk: Arc<D>) -> Result<Self>;
    pub async fn close(&self) -> Result<()>;

    pub async fn put(&self, key: Bytes, value: Bytes, opts: WriteOptions) -> Result<WriteResult>;
    pub async fn delete(&self, key: Bytes, opts: WriteOptions) -> Result<WriteResult>;
    pub async fn get(&self, key: &[u8], opts: ReadOptions) -> Result<Option<Bytes>>;
    pub async fn scan_prefix(&self, prefix: &[u8], opts: ScanOptions) -> Result<Vec<KeyValue>>;

    pub async fn flush(&self) -> Result<FlushResult>;
    pub async fn compact(&self) -> Result<CompactionResult>;
    pub fn stats(&self) -> EngineStats;
}
```

MVP may use `&mut self` internally if concurrency is not ready. Public shape may be adjusted to avoid premature locking complexity.

---

## 4. Options

```rust
pub struct WriteOptions {
    pub durability: Option<DurabilityMode>,
    pub idempotency_key: Option<Bytes>, // future
}

pub struct ReadOptions {
    pub read_at: ReadTimestamp,
}

pub enum ReadTimestamp {
    Latest,
}

pub struct ScanOptions {
    pub limit: Option<usize>,
}
```

MVP supports only latest reads.

---

## 5. Results

```rust
pub struct WriteResult {
    pub sequence: SequenceNumber,
    pub lsn: Lsn,
    pub durable: bool,
}

pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}
```

For strict writes, `durable` must be true on success.

---

## 6. Command semantics

### 6.1 PUT

Requirements:

- rejects empty key unless config permits it,
- rejects key/value larger than configured limit,
- overwrites previous value for same key,
- writes WAL before visible ACK in strict mode,
- after success, `GET(k)` returns `v` unless later operation changes it.

### 6.2 GET

Requirements:

- returns latest visible value,
- returns not-found if key was deleted,
- must not mutate engine state except metrics/cache.

### 6.3 DELETE

Requirements:

- appends tombstone,
- idempotent from logical user perspective,
- after success, `GET(k)` returns not-found unless later `PUT` happens.

### 6.4 SCAN

Requirements:

- returns visible keys with prefix,
- sorted lexicographically,
- no duplicates,
- tombstoned keys omitted,
- respects optional limit.

---

## 7. Recovery API

`Engine::open` performs recovery by default.

For tests and tools:

```rust
pub async fn recover<D: Disk>(config: EngineConfig, disk: Arc<D>) -> Result<RecoveryReport>;
```

Recovery report fields are defined in `recovery-spec.md`.

---

## 8. Error model

```rust
pub enum KayaError {
    InvalidArgument { message: String },
    NotFound,
    Corruption { message: String },
    Io { message: String },
    DiskFull,
    FsyncFailed,
    UnsupportedVersion { found: u16 },
    LockConflict,
    InvariantViolation { id: String, message: String },
    Internal { message: String },
}
```

Rules:

- corrupted input must not panic,
- oversized key/value must return `InvalidArgument`,
- not-found is not an exceptional internal error,
- invariant violation in simulation should fail loudly.

---

## 9. Concurrency policy

MVP policy:

- writes may be serialized through a mutex,
- reads may run concurrently if implementation is simple,
- background flush can be disabled for deterministic tests,
- no public API promise about lock-free reads yet.

Future policy:

- group commit batcher,
- immutable memtable snapshots,
- low-lock read path,
- epoch-based resource cleanup.

Correctness tests must not depend on timing.

---

## 10. Metrics

```rust
pub struct EngineStats {
    pub put_count: u64,
    pub get_count: u64,
    pub delete_count: u64,
    pub scan_count: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_count: u64,
    pub memtable_entries: u64,
    pub sstable_count: u64,
    pub last_sequence: u64,
}
```

Metrics are best-effort observability, not correctness state.

---

## 11. Engine invariants

| ID | Invariant |
|---|---|
| ENG-001 | Strict successful write has durable WAL record |
| ENG-002 | GET returns highest-sequence visible record |
| ENG-003 | DELETE hides older PUT |
| ENG-004 | SCAN returns sorted unique visible keys |
| ENG-005 | Recovery is idempotent |
| ENG-006 | Engine open never silently ignores required live files |
| ENG-007 | CLI/server cannot bypass engine validation |

---

## 12. Acceptance criteria

Engine API is ready when:

- embedded tests can open an engine over `SimDisk`,
- put/get/delete/scan work,
- strict writes report durable true,
- recovery report is available,
- CLI can call engine APIs,
- simulation can drive engine without server process.
