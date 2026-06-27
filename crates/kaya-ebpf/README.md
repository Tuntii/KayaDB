# kaya-ebpf

Optional Linux eBPF scaffolding for KayaDB. This crate is a **stub** — it does not attach kernel probes and is not a hard dependency of the workspace test suite.

On Linux, see `scripts/ebpf/` for bpftrace helpers (`fsync-latency.bt`, `durability-syscalls.bt`, etc.).