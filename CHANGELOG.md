# Changelog

All notable changes to KayaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added (M16 — MVCC)
- Versioned storage: typed InternalKey, multi-version memtable, SSTable v4
- Snapshot reads via ReadTimestamp::At / get_at
- Compaction GC watermark
- Versioned sim RefModel + MVCC crash property

### Added (Roadmap)
- ROADMAP: new **M16–M25 "Distributed Transactional KV" arc** — txn-first sequencing (M16 MVCC → M17 single-group ACID txn → M18 secondary indexes → M19 CDC), then distribution (M20 multi-raft + HLC → M21 range metadata/splits → M22 rebalancing/placement → M23 cross-shard 2PC), then hardening + scale proof (M24 encryption-at-rest/ACL/observability, M25 grand-matrix Jepsen + ecosystem close-out → v0.2.0). API stays KV + txn + index (no SQL this arc). Every v0.1.47 deferred item is now mapped to a concrete milestone. Approved design spec: `docs/superpowers/specs/2026-07-12-m16-m25-roadmap-design.md`

## [0.1.47] — 2026-07-11

Roadmap parallel-track close-out: durable directory-entry semantics (Track B), richer deterministic chaos faults (Track C), Rust client retry/pooling/observability + a new Python client (Track D), latency histograms + error guidance (Tracks F/G), SLO envelope, incremental backup, and SIEM syslog export (Track E).

### Added (Track B — directory durability)
- Real Unix directory fsync in `FileDisk` and `IoUringDisk` (`#[cfg(unix)]` opens the directory and `sync_all`s its descriptor); previously `fsync_dir` was a no-op on every platform, so an acknowledged rename/publish could be lost on crash even after the file's own data was fsync'd. Windows remains a documented best-effort no-op
- WAL segment directory-entry durability: a rotated (or first) segment is now `fsync_dir`'d on the `wal` directory right after its first append creates the file, instead of prematurely in `rotate()` before the file existed (`active_dir_synced` tracking in `kaya-wal`)
- `SimDisk::with_strict_namespace()` models directory-entry durability: file creation, rename and removal are volatile until the containing directory is `fsync_dir`'d, and `crash()` reverts namespace mutations that were never made durable — so a missing `fsync_dir` after an atomic publish is now detectable (6 new tests; default disk keeps the content-only crash model)

### Added (Track D — Python client)
- New `clients/kaya-py/`: a pure-standard-library (zero-dependency) synchronous Python client — `put`/`get`/`delete`/`scan`/`health`/`stats`/`hello`, connection reuse with reconnect, leader redirect on `NOT_LEADER`, optional client token (`CLIENT\x00` framing), per-request timeout. Byte-compatible with the Rust/Go clients; tested with codec byte-layout checks and an in-process mock-server loopback (including redirect)

### Fixed (docs)
- `docs/clients/client-wire-protocol.md` §5: corrected the PUT example frame length (19 = 1 opcode + 18 payload, not 17) and showed the bytes in actual little-endian order; the previous example understated the payload size and used big-endian display

### Added (Track E — operations)
- `docs/slo-envelope.md`: explicit operating envelope — enforced hard input limits (grounded in `kaya-core` constants), durability/consistency SLOs, latency/throughput guidance tied to the new histograms/Prometheus metrics, and a conservative error-budget posture
- `kayactl backup --data <src> --out <dest> [--incremental]`: filesystem backup of a node's durable state; incremental mode skips immutable files (SSTables, sealed WAL segments) already present with the same size, copying only new/changed files. Atomic per-file (temp + rename); `--json` summary. Documented in `docs/runbooks/backup-restore.md`

### Added (Track E — SIEM audit export)
- Optional remote audit forwarding: `--audit-syslog <host:port>` / `KAYA_AUDIT_SYSLOG` streams each audit record to a SIEM collector as an RFC 5424 syslog datagram over UDP (best-effort, never blocks the data path). `docs/security.md` §7 SIEM row updated from accepted-risk to implemented

### Added (Track F — benchmarks) & Track A/G roadmap close-out
- `kaya-bench/benches/mixed_workload.rs`: large-value (64 KiB), high-key-count (5 000 keys, flush + cold SSTable reads), and interleaved put/get/delete/scan benchmarks (Track F workload shapes)
- eBPF Makefile targets `make datadir` (per-file/data-dir filtered trace via the existing `durability-syscalls.bt`) and `make parallel` (fsync + block + timeline probes concurrently, DURATION-bounded, separate logs)
- `CONTRIBUTING.md` "Good first issues" now points at eBPF-script and language-client-porting areas
- ROADMAP marked all short/medium-term parallel-track items done; remaining items are explicitly `⬜ deferred` (TS/JS & Zig clients, TLA+ expansion, web dashboard, cross-node production tracing, io_uring completion tracing, privileged-CI stap/perf, Track H research) — each needs its own spec

### Added (Track F — latency observability)
- `LatencyHistogram` in `kaya-core`: dependency-free fixed-bucket (Prometheus-compatible `le` bounds) histogram with `observe`, `percentile_us` (p50/p99), `mean_us`, `merge`, and cumulative-bucket export
- Read-path latency is now measured: `EngineStats.get_total_us/get_max_us` and `scan_total_us/scan_max_us` (`get()`/`scan_prefix()` were previously unmeasured)
- `Engine::histograms()` exposes per-op p50/p99 distributions for get, scan, WAL fsync, flush, and compaction
- Prometheus exporter expanded beyond WAL fsync: `kaya_flush_*`, `kaya_compaction_*`, `kaya_get_*`, `kaya_scan_*` latency metrics and `kaya_engine_ops_total{op=…}` counters

### Added (Track G — error DX)
- `KayaError::guidance()` returns actionable operator advice for recoverable errors (corruption → `kayactl recover --dry-run`, lock conflict, disk full, fsync failure, version mismatch); `kayactl` prints it as a `HINT:` line after the error
- The data-directory lock failure now returns the structural `KayaError::LockConflict` (exit code 6) instead of a stringly-typed `Internal`, so the guidance is uniform

### Added (Track D — Rust client high-level features)
- `RetryPolicy` (`kaya-client`): configurable `max_attempts`, exponential backoff with optional full jitter, `max_backoff` cap, and a per-attempt `request_timeout` — replacing the previous fixed 60 ms sleep and unbounded read. Retry budget is now separate from the leader-redirect budget (`RetryPolicy::none()` restores single-shot behavior). Set via `KayaClient::set_retry_policy`
- Connection reuse (keep-alive) for the plain-TCP path via `kaya_net::request_on_stream`: the client holds one connection and reconnects only on error or leader redirect, instead of a fresh `TcpStream::connect` per operation (TLS still connects per op)
- `ClientObserver` hook + `OpObservation` / `OpOutcome`: per-operation metrics/tracing callback (opcode, attempts, redirects, outcome, end-to-end latency) with a blanket impl for any `Fn(&OpObservation)`; installed via `KayaClient::set_observer`. No dependency on any specific metrics framework

### Added (Track C — deterministic chaos)
- `SimNetworkConfig.latency_ticks`: fixed per-message delivery delay in logical ticks (tick-accurate hold-back via `SimNetwork::advance_tick`); `0` preserves historical same-tick delivery
- `SimNetworkConfig.reorder_percent`: deterministic out-of-order delivery within a drained batch (seeded, reproducible); `0` preserves per-destination FIFO
- Asymmetric partition helpers `SimNetwork::isolate_outgoing` / `isolate_incoming` for one-way link failures (split-brain triggers)
- New election-safety tests under network latency, reorder+latency+drop+dup, and asymmetric partition

Technical-debt hardening pass: scan caps, graceful shutdown, connection limits, format fixtures, decoder tests, disk-append contract.

### Added (hardening)
- Server-side scan caps: `LimitsConfig.max_scan_results` (default 100 000) and `max_scan_bytes` (default 64 MiB) bound every `scan_prefix` — merge memory included; oversized scan prefixes (> `max_key_len`) are rejected with `InvalidArgument` (surfaced as `STATUS_INVALID_ARGUMENT` on the wire)
- Graceful shutdown: `kayadb-server` handles Ctrl-C (all platforms) and SIGTERM (Unix); the run loop falls through to its cleanup path (eBPF detach/flush, OTel span shutdown) instead of dying mid-flight
- Client connection cap: `--max-client-connections` / `KAYA_MAX_CLIENT_CONNECTIONS` (default 1024) — accept-loop semaphore applies TCP-backlog backpressure; integration test `test_max_client_connections_backpressure`
- Persistent format golden fixtures (M1 debt, format-versioning-spec §6): committed WAL v1, SSTable v2/v3, and manifest v1 fixtures with byte-exact golden tests + typed corruption rejections (27 tests in `kaya-wal`/`kaya-lsm` `tests/format_fixtures.rs`)
- WAL decoder edge-case suite (M1 debt): 16 targeted tests in `kaya-wal/tests/decoder_edge_cases.rs` covering `UnknownFlags`, `OversizedPayload`, `BadHeaderChecksum`, `BadPayloadChecksum`, `UnknownRecordType`, `MalformedPayload`, partial header/payload, decoder statelessness, and recovery-truncation semantics
- `Disk::append` concurrency contract documented; `FileDisk` now serializes appends internally (shared lock across clones) with a concurrent-append contract test for `FileDisk`/`SimDisk`/`IoUringDisk`

### Fixed (hardening)
- `IoUringDisk::append` offset race: file-length probe now happens under the ring mutex, so concurrent appends cannot clobber each other
- Flaky `wal_fsync_marker_emits_balanced_exit_on_fsync_failure`: the global probe-marker callback now filters by test thread, immune to parallel sibling tests
- `kaya-ebpf` clippy violations (`new_without_default`, `len_zero`) that broke `cargo clippy --workspace -D warnings`

Track A Phase 2C: flamegraph helper, optional OpenTelemetry durability spans, external USDT operator docs.

### Added (Phase 2C)
- `scripts/ebpf/durability-flamegraph.bt` and `make flamegraph` — bpftrace `-f flamegraph` stack-collapse for `flamegraph.pl`
- `kayactl ebpf flamegraph [--pid] [--run] [--duration]` — resolves script, prints manual Linux command on non-Linux
- Optional `kayadb-server --features otel --otel` — OTLP-exportable spans for `wal_fsync` and `flush` at existing durability hooks
- `kaya_core::set_probe_span_callback` parallel to USDT markers; `ProbeMarkerSite::as_str()` shared taxonomy
- External stap/perf USDT operator guide in `scripts/ebpf/README.md` and `observability-spec.md` §7

Track A Phase 2B+: USDT-shaped userspace markers, extended `ProbeEvent` schema, publish-phase trace correlation.

### Added
- `ProbeEvent::UsdtMarker` and `ProbeEvent::PublishSyscall` in `trace.jsonl` with replay validation and mixed-kind fixtures
- Global `kaya_core::emit_probe_marker` hooks at WAL strict fsync and `Engine::flush` entry/exit (no-op when ebpf off)
- `kaya_ebpf::install_usdt_marker_sink` wires markers into `kayadb-server --ebpf` trace artifacts
- `kayactl ebpf correlate` / `trace wal` surfaces USDT marker counts and publish syscall kinds
- Kernel-simulated `sync_flush_activity` emits publish-shaped events from flush stats deltas

Track A Phase 2A: kayactl ebpf CLI hardening, bpftrace wrappers, userspace–kernel correlation.

### Added (Phase 2A)
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
