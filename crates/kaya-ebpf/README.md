# kaya-ebpf

Optional in-process observability for KayaDB: probe attach/detach, deterministic
`trace.jsonl` capture, replay validation, and Prometheus histogram aggregates.

## Backend slots (explicit — no silent mixing)

| Slot | When | `kaya_ebpf_*` source |
|------|------|----------------------|
| **kernel-live** | Linux + `kernel-probes` + CAP_BPF attach OK | BPF ringbuf kprobes |
| **kernel-simulated** | Server `--ebpf` fallback / Windows CI | Ringbuf-shaped deterministic events |
| **userspace-tap** | `ProbeConfig::for_tap` only | Explicit tap (not used by server) |
| **simulated** | `ProbeConfig::for_tests` | Seeded test events |

`kayadb-server --ebpf` selects **kernel-live** or **kernel-simulated** via
`ProbeConfig::for_server`. Engine counters (`kaya_wal_fsync_*`) remain separate.

## Features

| Feature | Description |
|---------|-------------|
| *(default)* | Kernel-simulated or test backends; no aya linkage |
| `kernel-probes` | Linux aya loader + `bpf/fsync_latency.bpf.c` |

```bash
cargo build -p kaya-server --features ebpf --bin kayadb-server
cargo build -p kayactl --features ebpf
```

## Server

```bash
kayadb-server --ebpf --ebpf-seed 42 --metrics-addr 127.0.0.1:9090
```

Strict durability + 200ms pump drains kernel slot (live ringbuf or simulated WAL activity).

## Tests

```bash
cargo test -p kaya-ebpf
cargo test -p kaya-ebpf --test kernel_pipeline_integration   # required kernel-slot gate
cargo test -p kaya-server --features ebpf
```

Linux optional:

```bash
cargo build -p kaya-ebpf --features kernel-probes
KAYA_EBPF_LIVE_KERNEL=1 cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored
```