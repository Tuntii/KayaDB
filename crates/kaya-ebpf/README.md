# kaya-ebpf

Optional in-process observability for KayaDB. Provides probe attach/detach lifecycle,
deterministic `trace.jsonl` capture, replay validation, and Prometheus histogram
aggregates for WAL fsync/fdatasync latency.

## Enable on the server

```bash
kayadb-server --ebpf --ebpf-seed 42 --metrics-addr 127.0.0.1:9090
```

Artifacts are written under `{data_dir}/ebpf/`:

- `status.json` — attachment/streaming state for `kayactl ebpf status`
- `trace.jsonl` — seeded, ordered durability events for replay validation

## CLI

```bash
kayactl ebpf status --data ./data
kayactl ebpf trace wal --data ./data
```

On non-Linux hosts both commands print guidance and exit successfully.

## Prometheus

When `--ebpf` is enabled, `/metrics` adds eBPF-derived series distinct from userspace counters:

- `kaya_ebpf_fsync_latency_us_*`
- `kaya_ebpf_fdatasync_latency_us_*`

Userspace engine counters remain `kaya_wal_fsync_*`.

## Backends

| Platform | Backend | Notes |
|----------|---------|-------|
| Linux | `linux-userspace-tap` | Ingests real WAL fsync timing via engine stats sync |
| Linux (CI) | `linux-tap+simulated` | Seeded simulated events when `CAP_BPF` is unavailable |
| Non-Linux | `noop-stub` | No kernel attach; workspace tests use simulated events |

External bpftrace scripts remain under `scripts/ebpf/` for kernel-level experiments.

## Tests

```bash
cargo test -p kaya-ebpf
```

Includes replay validation and a bounded chaos-style trace producer.

## Build notes (future kernel probes)

Real kprobe attachment will live behind an optional feature and require clang/llvm
and `CAP_BPF`. The default build does not link libbpf or aya.