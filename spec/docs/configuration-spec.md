# Configuration Spec

**Status:** Draft v0.1  
**Scope:** Config files, defaults, validation, CLI overrides  

---

## 1. Purpose

Configuration must make durability and failure-testing behavior explicit.

Defaults should be safe for local experimentation:

- strict durability by default,
- localhost server bind by default,
- bounded key/value sizes,
- deterministic simulation seed visible in trace.

---

## 2. Config file format

Recommended format: TOML.

Example:

```toml
[data]
dir = "./data"

[durability]
mode = "strict" # strict | relaxed
fsync_every_n_records = 1

[memtable]
max_bytes = 67108864

[wal]
segment_max_bytes = 67108864
max_record_bytes = 33554432

[sstable]
block_target_bytes = 32768

[server]
host = "127.0.0.1"
port = 7379

[simulation]
seed = "0xdeadbeef"
```

---

## 3. Rust shape

```rust
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub durability: DurabilityConfig,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
    pub sstable: SstableConfig,
    pub limits: LimitsConfig,
}

pub struct DurabilityConfig {
    pub mode: DurabilityMode,
    pub fsync_every_n_records: NonZeroU64,
}

pub enum DurabilityMode {
    Strict,
    Relaxed,
}
```

---

## 4. Defaults

| Field | Default | Rationale |
|---|---:|---|
| `data.dir` | `./data` | local dev convenience |
| `durability.mode` | `strict` | correctness-first |
| `durability.fsync_every_n_records` | `1` | strict ACK clarity |
| `wal.segment_max_bytes` | `64 MiB` | simple rotation |
| `wal.max_record_bytes` | `32 MiB` | allocation safety |
| `memtable.max_bytes` | `64 MiB` | bounded memory |
| `sstable.block_target_bytes` | `32 KiB` | simple blocks |
| `server.host` | `127.0.0.1` | safe default |
| `server.port` | `7379` | memorable local port |
| `limits.max_key_len` | `4096` | parser safety |
| `limits.max_value_len` | `16 MiB` | MVP bound |

---

## 5. Validation rules

Config loader must reject:

- unknown durability mode,
- zero `fsync_every_n_records`,
- `wal.max_record_bytes` greater than hard safety maximum,
- `wal.segment_max_bytes` smaller than max record overhead,
- absolute/invalid data dir only when forbidden by calling context,
- server port outside valid range,
- negative or zero size values.

Warnings:

- relaxed durability enabled,
- server host not localhost,
- very large max value size,
- directory fsync unsupported or best-effort on platform.

---

## 6. CLI override precedence

```text
hard-coded defaults
  ↓
config file
  ↓
environment variables, if introduced
  ↓
CLI flags
```

CLI flags must be visible in diagnostics when they override file config.

---

## 7. Environment variables

Environment variables are optional for MVP.

If introduced, recommended prefix:

```text
KAYADB_DATA_DIR
KAYADB_DURABILITY_MODE
KAYADB_LOG
```

No secrets are required for MVP. If future auth is added, secret handling must not print sensitive values.

---

## 8. Configuration invariants

| ID | Invariant |
|---|---|
| CFG-001 | Default durability is strict |
| CFG-002 | Unsafe/relaxed settings are visible in logs/diagnostics |
| CFG-003 | Config validation rejects impossible size/limit combinations |
| CFG-004 | CLI overrides are deterministic and documented |
| CFG-005 | Server default bind is localhost |

---

## 9. Acceptance criteria

Configuration is ready when:

- default config can be constructed without file,
- TOML config can be loaded,
- invalid settings return typed errors,
- CLI override precedence is tested,
- relaxed durability warning is emitted,
- config appears in simulation trace or config artifact.
