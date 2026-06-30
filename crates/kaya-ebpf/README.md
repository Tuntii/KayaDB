# kaya-ebpf

Optional in-process observability for KayaDB: probe attach/detach, deterministic
`trace.jsonl` capture, replay validation, and Prometheus histogram aggregates.

## Event sources (priority order on Linux)

| Backend | When active | Sample source |
|---------|-------------|---------------|
| **Kernel ringbuf** | `kernel-probes` + BPF object built + CAP_BPF attach OK | Per-op kprobe/kretprobe on `__x64_sys_fsync` / `__x64_sys_fdatasync` |
| **Userspace tap** | Always (default) | Engine `wal_fsync_*` stats delta or explicit `report_fsync` |
| **Simulated** | `ProbeConfig::for_tests` only | Seeded deterministic events for CI |

When kernel probes are streaming, `sync_from_engine_stats` drains the ring buffer
only — it does **not** inject synthetic tap events for the same window.

## Features

| Feature | Description |
|---------|-------------|
| *(default)* | Userspace tap + trace pipeline; no libbpf/aya linkage |
| `kernel-probes` | Linux aya loader + `bpf/fsync_latency.bpf.c` (clang + `bpf/include/` headers) |

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
samples are meaningful. A background pump (200ms) drains kernel ringbuf when
attached, otherwise syncs engine stats into the userspace tap, and writes
`trace.jsonl` incrementally.

Artifacts under `{data_dir}/ebpf/`:

- `status.json` — attachment/streaming state (`backend` field shows active path)
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

### Build prerequisites

1. `clang` / `llvm` with BPF target support
2. `cargo build -p kaya-ebpf --features kernel-probes` on Linux x86_64 or aarch64
3. Optional: `bpftool` + `/sys/kernel/btf/vmlinux` for accurate `vmlinux.h` generation
4. Runtime: `CAP_BPF` (or root) for kprobe attach

Bundled fallback headers live under `bpf/include/` (minimal `vmlinux.h` for x86_64).

### BPF program

`bpf/fsync_latency.bpf.c` — fsync/fdatasync kprobe + kretprobe, ring buffer `events` map.

### Live attach test (optional)

```bash
KAYA_EBPF_LIVE_KERNEL=1 cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored
```

## Tests

```bash
cargo test -p kaya-ebpf
cargo test -p kaya-ebpf --test kernel_ringbuf
cargo test -p kaya-server --features ebpf
```

Non-Linux production config (`ProbeConfig::for_data_dir`) is a true no-op until
tap events arrive; tests use `ProbeConfig::for_tests` for seeded simulation.