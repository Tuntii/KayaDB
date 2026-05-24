# Disk and I/O Spec

**Status:** Draft v0.2  
**Scope:** Disk abstraction, file-backed disk, simulated disk, future io_uring boundary  

---

## 1. Purpose

The disk layer isolates storage logic from physical I/O behavior.

KayaDB must run the same WAL and storage engine code against:

1. a real filesystem-backed disk,
2. a deterministic simulated disk,
3. a future `io_uring` backend.

Storage correctness cannot be validated only on happy-path filesystem behavior. Short writes, failed fsyncs, partial directory persistence and corrupted reads are normal inputs for tests.

---

## 2. Disk trait

Initial async trait shape:

```rust
pub trait Disk: Send + Sync + 'static {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64>;
    async fn fsync_file(&self, path: &RelativePath) -> Result<()>;
    async fn fsync_dir(&self, path: &RelativePath) -> Result<()>;
    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()>;
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<()>;
    async fn remove_file(&self, path: &RelativePath) -> Result<()>;
    async fn list_dir(&self, path: &RelativePath) -> Result<Vec<DirEntry>>;
    async fn file_len(&self, path: &RelativePath) -> Result<u64>;
}
```

Path lives inside methods because the database has many files: WAL segments, manifests, SSTables and temp files. The abstraction is filesystem-level, not single-file-level.

---

## 3. RelativePath

`RelativePath` prevents escaping the database root.

Rules:

- no absolute paths,
- no `..`,
- no empty path unless explicitly representing root,
- normalized `/` separators internally,
- UTF-8 paths are acceptable for MVP,
- raw OS paths may be introduced later.

Examples:

| Input | Result |
|---|---|
| `wal/0001.wal` | accepted |
| `./wal/0001.wal` | normalized to `wal/0001.wal` |
| `wal//0001.wal` | normalized or rejected consistently |
| `../secret` | rejected |
| `C:\\data\\x` | rejected as absolute on Windows |
| `/var/db/x` | rejected |

---

## 4. Required semantics

### 4.1 `write_at`

Allowed outcomes:

- full write success,
- short write with returned byte count,
- error before any write,
- error after partial write.

Caller must not assume a failed write wrote zero bytes unless the disk implementation explicitly guarantees it.

### 4.2 `append`

Appends bytes to end of file and returns starting offset.

For `FileDisk`, append must be serialized per file in MVP to avoid races. For `SimDisk`, append must be deterministic and event-logged.

### 4.3 `fsync_file`

Requests durable persistence of file contents and metadata required to read the file after crash.

A failed fsync does not mean no data reached stable storage; it means the caller cannot rely on durability. Strict write paths must not ACK after failed fsync.

### 4.4 `fsync_dir`

Used after file creation, rename and deletion where directory entry durability matters.

MVP may implement directory sync best-effort on platforms with limitations, but the call boundary must remain visible and testable in `SimDisk`.

### 4.5 `rename`

Used for atomic publication of `CURRENT`, manifests and SSTable temp files.

Assumption: local filesystem rename is atomic within a directory. SimDisk must model rename as an event whose directory-entry durability depends on directory fsync.

---

## 5. FileDisk

Requirements:

- stores data under a configured root directory,
- rejects path traversal,
- supports append, read, write, truncate, rename, remove and list,
- uses blocking I/O behind a simple async wrapper if necessary,
- serializes writes per file for correctness in MVP,
- exposes clear `Io` errors,
- preserves platform-specific errors without leaking absolute paths unnecessarily.

Possible shape:

```rust
pub struct FileDisk {
    root: PathBuf,
    file_locks: FileLockTable,
}
```

MVP does not need maximal concurrency. Correct serialization beats cleverness here; the dragon can nap.

---

## 6. SimDisk

### 6.1 Goals

`SimDisk` must make these cases easy to test:

- partial write,
- torn write,
- failed fsync,
- disk full,
- corrupted read,
- lost unfsynced write after crash,
- persisted write after crash,
- rename crash window,
- temp file cleanup,
- directory entry persistence bugs.

### 6.2 Model

SimDisk keeps two states:

```text
volatile_state: writes that process can currently read
stable_state: writes guaranteed after crash
```

Before crash:

```text
reads see volatile_state
```

After crash:

```text
volatile_state := stable_state plus implementation-defined persisted subset
```

MVP exact model:

1. `write_at` updates volatile bytes.
2. `append` updates volatile bytes.
3. successful `fsync_file` copies file volatile bytes to stable bytes.
4. `crash` resets volatile bytes to stable bytes.
5. failed fsync does not update stable bytes.

This model is stricter and simpler than many real filesystems; it is suitable for initial durability tests.

### 6.3 File state

```rust
struct SimFile {
    volatile: Vec<u8>,
    stable: Vec<u8>,
    metadata_stable: bool,
}
```

Directory entries should eventually have volatile/stable distinction:

```rust
struct SimDir {
    volatile_entries: BTreeMap<String, Node>,
    stable_entries: BTreeMap<String, Node>,
}
```

MVP can simplify directory durability but must model file content durability correctly for WAL.

---

## 7. Fault schedule

Faults are selected deterministically:

```rust
pub struct SimSeed(pub u64);

pub struct FaultSchedule {
    pub seed: SimSeed,
    pub rules: Vec<FaultRule>,
}
```

Example rules:

```text
on operation #17, make next write partial at 31 bytes
on operation #42, fail fsync
with probability p under seed, corrupt one byte on read
```

Trace output must record the actual decision, not just the rule.

---

## 8. Events

Each disk operation emits an event:

```json
{
  "event_id": 123,
  "kind": "write_at",
  "path": "wal/0000000000000001.wal",
  "offset": 4096,
  "requested_len": 128,
  "actual_len": 97,
  "result": "partial_success"
}
```

Replay must be able to enforce the same operation result.

---

## 9. Error model

```rust
pub enum DiskError {
    NotFound,
    AlreadyExists,
    PermissionDenied,
    DiskFull,
    ShortWrite { requested: usize, written: usize },
    CorruptedRead,
    FsyncFailed,
    InvalidPath,
    Io(std::io::ErrorKind),
}
```

Short writes and fsync failures are normal correctness cases, not impossible branches.

---

## 10. Invariants

| ID | Invariant |
|---|---|
| DSK-001 | Path traversal is impossible |
| DSK-002 | Successful fsync makes file bytes stable in SimDisk |
| DSK-003 | Failed fsync does not imply durability |
| DSK-004 | Crash resets volatile state to stable state |
| DSK-005 | Fault schedule is deterministic |
| DSK-006 | SimDisk trace contains every injected fault |
| DSK-007 | FileDisk and SimDisk pass the same logical disk contract tests |

---

## 11. Acceptance criteria

Disk layer is ready when:

- `RelativePath` rejects traversal and absolute paths,
- `FileDisk` can create, append, read, truncate, rename and list files,
- `SimDisk` can run the same WAL tests as `FileDisk`,
- partial write injection is deterministic,
- fsync failure injection is deterministic,
- crash resets volatile state correctly,
- every simulated fault appears in trace output,
- path traversal is rejected in unit and property tests.
