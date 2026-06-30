# kaya-ebpf

Optional in-process observability for KayaDB: probe attach/detach, deterministic
`trace.jsonl` capture, replay validation, and Prometheus histogram aggregates.

## Features

| Feature | Description |
|---------|-------------|
| *(default)* | Userspace tap + trace pipeline; no libbpf/aya linkage |
| `kernel-probes` | Linux-only aya kprobe/kretprobe loader + `bpf/fsync_latency.bpf.c` (requires clang) |

Downstream crates use optional `ebpf` feature:

```bash
cargo build -p kaya-server --features ebpf --bin kayadb-server
cargo build -p kayactl --features ebpf
```

## Enable on the server

```bash
kayadb-server --ebpf --ebpf-seed 42 --metrics-addr 127.0.0.1:9090
```

With `--ebpf`, the server switches engine durability to **strict** so WAL fsync
samples are meaningful. A background pump polls engine stats every 200ms and
writes `trace.jsonl` incrementally.

Artifacts under `{data_dir}/ebpf/`:

- `status.json` — attachment/streaming state
- `trace.jsonl` — seeded ordered durability events

## CLI

```bash
kayactl ebpf status --data ./data
kayactl ebpf trace wal --data ./data
```

## Prometheus

Distinct from userspace counters:

- `kaya_ebpf_fsync_latency_us_*`
- `kaya_ebpf_fdatasync_latency_us_*`

Engine counters remain `kaya_wal_fsync_*`.

## Kernel probes (Linux)

```bash
# On Linux with clang/llvm:
cargo build -p kaya-ebpf --features kernel-probes
```

Loads `fsync_enter`/`fsync_exit` and `fdatasync_*` kprobes via aya, streaming
from a BPF ring buffer. Falls back to userspace tap when attach fails.

## Tests

```bash
cargo test -p kaya-ebpf
cargo test -p kaya-server --features ebpf
```

Non-Linux production config (`ProbeConfig::for_data_dir`) is a true no-op until
tap events arrive; tests use `ProbeConfig::for_tests` for seeded simulation.