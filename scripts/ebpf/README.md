# Linux eBPF Observability for KayaDB (M12 experiments)

**Status:** Experimental / Linux-only  
**Scope (per observability-spec.md):** fsync latency probes, block I/O latency tracking.  
**Non-goals:** Hard kernel dependency, root required for normal development/tests, production SLA claims.

## Prerequisites

- Linux kernel with eBPF + BTF support (5.5+ recommended, recent distros are fine).
- `bpftrace` installed (preferred for these scripts; easy one-liners too).
- Root / `sudo` or `CAP_BPF` + `CAP_PERFMON` capabilities for attaching probes.
- Optional: `bcc` tools or `perf` for additional views.

On Debian/Ubuntu:
```bash
sudo apt install bpftrace
```

On Fedora/RHEL:
```bash
sudo dnf install bpftrace
```

## Quick start

From `scripts/ebpf/` (with `kayadb-server` running on Linux):

```bash
make help          # print targets
make list          # discover kayadb-server PIDs
make fsync         # fsync/fdatasync latency histogram (first PID)
make block         # block I/O read/write latency histograms
make timeline      # write/fsync/rename/unlink syscall timeline
make verify        # Linux kernel gate (bpf compile + kaya-ebpf tests)
```

Override the target PID or pair with a timed workload:

```bash
make fsync PID=12345
make timeline PID=$(pgrep -f kayadb-server | head -1) DURATION=30
# In another terminal: kayactl --data ./db put k v && kayactl --data ./db flush
```

`make verify` runs `scripts/linux_verify_ebpf_kernel.sh` from the repo root (skips on non-Linux hosts).

## Provided Scripts

All scripts are designed to be attached to a running `kayadb-server` (or any KayaDB process).

### 1. fsync-latency.bt

Traces `fsync` / `fdatasync` syscalls (and common vfs entry) and produces a latency histogram in **microseconds**.

Usage (target by PID):
```bash
sudo bpftrace -p <PID> scripts/ebpf/fsync-latency.bt
```

Or filter inside script by comm / pid (edit the script).

Example output:
```
@fsync_latency_us: 
[0, 1)         0 |                                                    |
[1, 2)         0 |                                                    |
...
[256, 512)     7 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@            | 
[512, 1K)     12 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@| 
...
```

Useful for:
- Seeing whether WAL `fsync` is the dominant latency contributor under strict durability.
- Spotting occasional long fsync tails (queueing, device, CoW filesystems).

### 2. block-io-latency.bt

Traces block layer request issue → complete (via tracepoints) for a target process and builds I/O latency histograms (separate read/write).

Usage:
```bash
sudo bpftrace -p <PID> scripts/ebpf/block-io-latency.bt
```

Gives visibility into actual storage device / elevator / scheduler latency that the userspace `fsync` time includes.

### 3. syscall-timeline.bt (Track A — added 2026-06)

Traces the broader durability + LSM publish syscalls: `write*`, `fsync*`/`fdatasync`, `rename` (SST tmp→live + CURRENT), `unlink` (tmp cleanup), and directory fsync points.

- Per-TID correlation between recent write and following fsync (helps answer "which write caused this long fsync?")
- Counts + light printf timeline for rename/unlink (correlates with `flush()` / `compact()` publish steps in engine)
- Interval summaries + final histograms

Usage (single node):
```bash
sudo bpftrace -p $(pgrep -f kayadb-server | head -1) scripts/ebpf/syscall-timeline.bt
```

For a 3-node local cluster use `kayactl ebpf list` (or `status`) to discover all PIDs, then attach to specific ones in separate terminals (or run multiple `bpftrace` instances).

The script is a bpftrace prototype illustrating Track A goals (per-file/dir filtering and richer correlation are easy to extend inside the script or via a future Rust eBPF crate behind an optional feature).

## Correlation with KayaDB

- Use `kayactl --server <addr> status` (or local `kayactl stats --latency`) to see:
  - `wal_fsync_*` (WAL durability cost)
  - Track A fields: `flush_*` / `compaction_*` (count + total + max + avg)
- `kayactl [--data DIR] flush` is the easiest way to drive the publish path on a local data dir and immediately see the numbers move (great for pairing with bpftrace).
- Userspace numbers give the full op wall time (Rust + all fsyncs inside flush/compact). Compare against kernel eBPF histograms from `fsync-latency.bt` or the richer `syscall-timeline.bt`.
- Cross reference examples:
  - WAL: `avg_us = wal_fsync_total_us / wal_fsync_count` vs `@fsync_latency_us`
  - Flush/compact cost: `flush_total_us` (includes multiple fsync_dir + manifest publish) vs rename/unlink events + fsync hists from `syscall-timeline.bt`
- `kayactl ebpf list` / `status` help discover PIDs across a local multi-node cluster.
- Run eBPF script(s) + a write/flush workload (e.g. `kayactl --data ./db put k v ; kayactl --data ./db flush`) at the same time.
- Process name filter defaults to `kayadb-server` and `kayactl` (easy to edit the .bt files).

## One-liners (no script file)

```bash
# fsync latency histogram (us) for PID
sudo bpftrace -e '
  kprobe:sys_fsync, kprobe:sys_fdatasync { @start[tid]=nsecs; }
  kretprobe:sys_fsync, kretprobe:sys_fdatasync /@start[tid]/ {
    $us = (nsecs - @start[tid])/1000;
    @fsync_us = hist($us);
    delete(@start[tid]);
  }' -p <PID>

# Block IO latency (us) — reads vs writes
sudo bpftrace -e '
  tracepoint:block:block_rq_issue { @start[args->sector] = nsecs; }
  tracepoint:block:block_rq_complete /@start[args->sector]/ {
    $us=(nsecs-@start[args->sector])/1000;
    if (args->rwbs ~ "*W*") { @bio_write_us = hist($us); } else { @bio_read_us = hist($us); }
    delete(@start[args->sector]);
  }' -p <PID>
```

## Limitations & Safety

- These are **read-only observation tools**. They do not modify KayaDB behavior.
- Attaching may have small overhead (acceptable for experiments / diagnostics).
- Scripts may need small adjustments across kernel versions (tracepoint args, kprobe names).
- `fsync_dir` in current `FileDisk` is best-effort (no-op on some platforms); the probes will mostly surface file `fsync` activity from WAL + manifest + SSTable publication.
- Future: userspace + kernel flame graphs, uprobes on hot engine paths, `io_uring` completion tracing once an io_uring `Disk` backend lands.

## Future Expansions (v2+)

See `spec/docs/observability-spec.md` §7.

- Syscall timeline (open, write, fsync, rename, unlink) for the KayaDB PID.
- Flamegraph integration (`bpftrace -f flamegraph ... | flamegraph.pl`).
- Custom USDT probes inside KayaDB (if we add them later).
- Integration with `kayactl observe` that can auto-discover PIDs of local nodes.

## How to contribute probes

1. Add `.bt` file under `scripts/ebpf/`.
2. Document in this README + link from `spec/docs/observability-spec.md`.
3. If it becomes Rust code (Aya / libbpf-rs), put behind `cfg(target_os = "linux")` + optional feature `ebpf`.
4. Never make eBPF required for `cargo test --workspace` or basic development.

## References

- bpftrace reference: https://github.com/bpftrace/bpftrace
- Linux tracing: https://www.brendangregg.com/linuxperf.html
- KayaDB observability spec: `spec/docs/observability-spec.md`
- Roadmap: `ROADMAP.md` (M12)
