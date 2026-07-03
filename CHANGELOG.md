# Changelog

All notable changes to KayaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

Track A Phase 2A: kayactl ebpf CLI hardening, bpftrace wrappers, userspace–kernel correlation.

### Added
- `kayactl ebpf list` — discover local `kayadb-server` PIDs (with cmdline), active bpftrace processes, and catalog script names
- `kayactl ebpf correlate` — userspace WAL fsync vs `{data_dir}/ebpf/trace.jsonl` kernel summary with rough delta hints and flush pairing notes
- `kayactl ebpf fsync-latency|block-latency|syscall-timeline [--run] [--duration <sec>]` — bpftrace wrappers (spawn with streamed output + timeout, or print manual `sudo bpftrace` command)
- `kayactl ebpf trace wal` — WAL-relevant lines from in-process `{data_dir}/ebpf/trace.jsonl`
- `scripts/ebpf/Makefile` — `make list|fsync|block|timeline|verify` helpers
- `scripts/docker_verify_ebpf_kernel.{sh,ps1}` — Docker-based kernel eBPF verification harness

### Changed
- `kayactl ebpf status` reads `{data_dir}/ebpf/status.json` when present; falls back to PID discovery hints
- `kayactl stats --latency` cross-references `kayactl ebpf correlate`
- Track A short-term ROADMAP items marked done/partial for Phase 2A
- `docs/cli-reference.md`, `spec/docs/observability-spec.md` §7 synced with implemented CLI

---

## [0.1.46] — 2026-06-30

M15 remaining tracks: client auth, audit logging, conformance, Go client, Prometheus, deployment, HELLO handshake, kayactl watch.

### Added
- Client token auth for data-path ops (`CLIENT\x00` framing): opcodes 1–4 and 6 require matching token when `--client-token` / `KAYA_CLIENT_TOKEN` configured; HEALTH (op 5) stays open
- Structured JSONL audit log at `{data_dir}/audit.jsonl` with `--audit-log` / `--no-audit-log` (default on when any token configured)
- Protocol conformance vectors (`docs/clients/conformance/vectors.json`) and Rust runner (`crates/kaya-net/tests/conformance_vectors.rs`)
- Go client (`clients/kaya-go/`): Put/Get/Delete/Scan/Health/Stats with leader redirect and client token support
- Prometheus `/metrics` HTTP endpoint via `--metrics-addr` (default `127.0.0.1:9090`)
- `kaya-ebpf` workspace crate (Linux-gated stub, `probe_catalog()` / `available_scripts()`)
- Docker 3-node cluster (`deploy/docker/`) and Kubernetes StatefulSet manifests (`deploy/k8s/`)
- HELLO protocol version handshake (opcode 0, `PROTO_VERSION = 1`)
- `kayactl watch [--interval <secs>] status` for polling remote STATS
- EngineStats v2 fields: `block_cache_hits`, `block_cache_misses`, `recovery_duration_us`

### Changed
- `docs/security.md` §7: client token auth and local audit logging marked implemented; remaining gaps documented (data-at-rest, multi-tenant, SIEM export)
- ROADMAP M15 section and parallel tracks (D, E, A, G) status updated
- Documentation site: GitHub Pages bundle via `scripts/prepare_docs_site.*` (ROADMAP, CHANGELOG, specs, deploy READMEs); fixed `../` links that caused 404 on Pages; added `404.md` and release notes template

### Security
- Data-path authZ available via `--client-token`; compliance SIEM export and data-at-rest encryption remain accepted deployment risks — see `docs/security.md` §7

---

## [0.1.45] — 2026-06-27

Post-M14 storage + correctness tracks: ZSTD/prefix compression, block cache stats, eBPF stub, rich Jepsen nemesis, manifest TLA+.

### Added
- SSTable v3 with optional LZ4 data-block compression (`SstableConfig.compression_lz4`, `SstableBuilder::with_options`)
- Per-reader decoded block LRU cache (`SstableConfig.block_cache_capacity`, `SstableReader::open_with_cache`)
- ZSTD compression (`SstableConfig.compression_zstd`, `COMPRESSION_CODEC_ZSTD`) and prefix compression with restart points (`SstableConfig.prefix_compression`)
- Public `SstableReader::block_cache_stats()` and `SstableBuildOptions` builder config
- `kaya-ebpf` stub crate (Linux-gated module, non-hard workspace dep) + `scripts/ebpf/durability-syscalls.bt`
- Jepsen nemesis types `ClockSkew` and `DiskLatency` with runner actions; `rich_nemesis_scenario` in registry
- TLA+ model `spec/specs/manifest/ManifestCompaction.tla` (+ `.cfg`)

### Changed
- Removed no-op Clojure Jepsen `workflow_dispatch` stub from `.github/workflows/jepsen.yml` (Rust-native harness is the sole CI gate)
- Updated stale docs: `KayaDB_Explained` (EN/TR), `kaya-raft` README, `productization.md`, `publishing.md`, `SUMMARY.md`, `jepsen-design.md`, `ROADMAP.md`
- New SSTables without compression remain format v2; LZ4/ZSTD tables use v3 footer (`SST_FOOTER_LEN_V3`)

---

## [0.1.44] — 2026-06-25

M14 closure: Jepsen full suite hardening, honest partition observability, and Linux `io_uring` Disk prototype.

### Added
- `PartitionTracker` in `kaya-jepsen-test` with partition attempted/applied/failed stats on `TestResult`
- `tests/scenario_registry.rs` — registry integrity checks for smoke + T1–T7 (shared `register` key, design client counts)
- `tests/partition_nemesis.rs` — linux-only `partition_nemesis_applies` proof (`applied>0` via iptables)
- `IoUringDisk` in `kaya-io` behind `io_uring` feature flag (Linux-only, `io-uring` crate)
- Shared Disk contract helpers (`contract` module) and `tests/disk_contract.rs` for FileDisk/SimDisk/IoUringDisk
- `KAYA_JEPSEN_FAST=1` env for shortened local full-gate verification
- `ClusterConfig.network_partitioned` inbound drop flag (set only when OS partition rules succeed)
- `apply_os_partition` / `heal_os_partition` in `cluster_controller` (iptables / Windows firewall)
- TLS raft listener partition flag parity (`start_raft_listener_tls` + unit test)

### Changed
- WGL full gate: shared-key multi-client register workload with leader-confirmation intervals
- Full gate: `partition_attempted>0` for T2/T5 always; `partition_applied>0` asserted on linux only
- Partition `applied` counter records OS rule success only (no in-process flag inflation)
- ROADMAP M14 marked complete; README status updated

---

## [0.1.43] — 2026-06-23

M14 correctness + algorithms milestone prep: storage algorithm upgrades, module splits, and expanded CI correctness gates.

### Added
- `CompactionPolicy` trait in `kaya-lsm` with L0 merge, leveled, and size-tiered strategies; wired through `EngineConfig.compaction`
- SSTable v2 bloom filter (configurable `bloom_bits_per_key`) with read-path negative lookup pruning
- WAL group-commit batching via `WalBatchWriter` and `WalBatchConfig` (record count, byte limit, time flush)
- Chaos matrix CI (`.github/workflows/chaos-matrix.yml`): DiskFull, NetworkPartition, ClockSkew axes
- Jepsen CI (`.github/workflows/jepsen.yml`): PR smoke scenario + nightly/tag full T1–T7 WGL gate
- Security audit CI (`audit.yml`: `cargo audit` + `cargo deny`) and `deny.toml`
- crates.io badges for `kaya-engine` and `kayactl` in README

### Changed
- Split `kaya-engine` god-file into `memtable`, `flush`, `snapshot`, and `stats` modules
- Split `kaya-server` cluster god-file into `client_ops`, `election`, `replication`, `snapshot`, and `stats`
- Split `kayactl` god-file into `cli`, `local`, `server`, `inspect`, `stats_cmd`, and `ebpf` modules
- ROADMAP and README status badge updated to M14 correctness+algorithm (in progress)

---

## [M13] — 2026-06-21

M13 productization milestone: operators can run KayaDB with documented security controls and day-2 procedures. Experimental status label removed.

### Added
- Native TLS transport (`tls` feature): rustls listeners for Raft + client (`kaya-net`, `kaya-server`, `kaya-client`, `kayactl`)
- Operator token auth for `ADD_MEMBER` / `REMOVE_MEMBER` (opcodes 7/8) via `--operator-token` / `KAYA_OPERATOR_TOKEN`
- Day-2 runbooks: `docs/runbooks/` (add-remove-node, rolling-restart, backup-restore, detecting-split-brain, mtls-sidecar)
- Durable Raft hard state + log persistence across `kayadb-server` restart
- Security enforcement table with code cross-references in `docs/security.md`
- Performance regression gate in CI (`kaya-bench/tests/perf_gate.rs`)

### Changed
- Experimental badge removed; README and ROADMAP reflect M13 exit
- `docs/security.md` §7: post-M13 gaps documented as accepted deployment risks with mitigations
- Runbooks aligned to current CLI (`--node-id`, `--data`, `--join-cluster` + `--peer`)

### Security
- Accepted risks (not correctness bugs): full client authZ, data-at-rest encryption, multi-tenant isolation, compliance audit logging — see `docs/security.md` §7

---

## Historical

See [ROADMAP.md](ROADMAP.md) for the detailed development history (M0–M13 productization tracks).

Major milestones completed before formal changelog adoption:
- Complete LSM + WAL engine with SimDisk deterministic fault injection
- Raft consensus + dynamic membership (joint consensus)
- TCP cluster + async client with leader redirection
- `kayactl` operator tooling + inspect for all on-disk formats
- Simulation testing, Jepsen-style harness, fuzz targets
- Multi-platform release binaries via GitHub Actions

For older notes see git history and the archived sections of ROADMAP.md.
