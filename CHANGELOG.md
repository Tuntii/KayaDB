# Changelog

All notable changes to KayaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added (M16–M25 arc — production path complete)
- **Arc close-out (2026-07-17):** M16–M25 documented production path closed. ROADMAP north-star re-eval: **v0.2.0 candidate** with residual risks listed honestly (no live range migrate, sequential 2PC + fail-closed recovery, no HLC uncertainty clamp, Jepsen grand matrix / minimal counterexample / scheduled profiling CI / Zig client / key rotation / Dashboard v2 still open). Not an unqualified production-SLA claim.

### Added (M25 — Scale proof & ecosystem production path)
- **Go client TXN + retries:** opcodes 9–12, `BeginTxn` / `Transaction` Get/Put/Delete/Commit/Rollback, `RetryPolicy` (exp backoff + full jitter + request timeout)
- **TypeScript client:** `clients/kaya-ts/` zero-dep Node TCP (put/get/delete/health/hello, NOT_LEADER redirect, optional client token)
- **Conformance v3:** MERGE_RANGE (17) + SPLIT_RANGE + txn edge vectors; Rust runner coverage
- **Perf envelope v2:** `kaya-bench` smoke helpers `run_smoke_txn_multi_key` + `run_smoke_multi_range_2pc`; CI `perf_gate` loose budgets; `BENCHMARKS.md` + `spec/docs/benchmarking-spec.md` tables
- **Deployment guide v2:** `docs/deployment-guide-v2.md` — M22–M24 flags (`--drain`, `--dashboard-addr`, `--encryption-key-file`, `--acl-file`), range ops, staging profile; linked from deployment/SLO/docs nav
- SLO notes: multi-key / 2PC guidance in `docs/slo-envelope.md`
- ROADMAP: M25 production path closed; M16–M25 arc complete as v0.2.0 candidate. *Out of path:* scheduled profiling CI, full Jepsen grand matrix, linearizability minimal counterexample, Zig client

### Added (M24 — Production hardening path)
- **Encryption-at-rest:** `EncryptedDisk` AES-256-GCM Disk wrapper (`KAYAENC1 | plain_len | nonce | ciphertext+tag`); server `--encryption-key-file` / `KAYA_ENCRYPTION_KEY_FILE` (32 raw bytes; v1 single key as KEK=DEK); contract + unit tests
- **Per-prefix ACL:** `PrefixAcl` + `--acl-file` / `KAYA_ACL_FILE` (JSON `prefix → token`, UTF-8 or `0x`/`hex:`); longest-prefix authorize on PUT/GET/DELETE/SCAN/TXN_*; empty map denies all; IT `per_prefix_acl_two_tokens`
- **Security docs:** `docs/security.md` enforcement table + §7 re-justified for M24 exit (encryption + ACL closed; key rotation and full multi-tenancy remain accepted risks)
- ROADMAP: M24 production path closed. *Out of path:* online KEK/DEK rotation, full kernel+userspace eBPF attribution, io_uring completion tracing, stap/perf privileged CI, Dashboard v2

### Added (M23 — Cross-shard transactions production path)
- **2PC engine records (shared-engine):** `\x00txn/rec/{txn_id}` + `\x00txn/intent/{txn_id}/{key}`; `Engine::apply_txn_prepare` / `apply_txn_commit_2pc` / `apply_txn_abort_2pc`; commit materializes intents via `apply_mutations` (index+CDC fire)
- **RaftCommand types 5/6/7:** `TxnPrepare` / `TxnCommit2pc` / `TxnAbort2pc` (types 1–4 layouts unchanged)
- **Coordinator:** `txn_coord::commit_cross_group` on multi-group `TXN_COMMIT` (lex-smallest-key coordinator group); **sequential** prepare then commit/abort proposes; single-group stays type-4 `TxnCommit`; client opcodes unchanged (range-transparent)
- **Recovery (minimal):** startup scan aborts local `Preparing`/`Prepared` records (fail-closed; no durable global decision log)
- **TLA+ sketch:** `spec/specs/txn/TwoPhaseCommit.tla` + `.cfg` (prepare/decide/recover; TXN-2PC invariants)
- **IT:** `test_cross_range_txn_commit`; multi-range bank `test_multi_range_bank_sum_invariant` (SI transfers across ranges, sum holds)
- Spec: `spec/docs/transactions-spec.md` §17
- ROADMAP: M23 production path closed. *Out of path:* parallel prepare/commit stretch, HLC uncertainty-interval clamp, multi-range Jepsen bank under full chaos matrix

### Added (M22 — Rebalancing, merges & placement production path)
- **Range merge (shared-engine):** `StaticRangeTable::merge_with_next` + wire `MERGE_RANGE` (17) + `kayactl range merge`; IT `test_range_merge_recombines`. Routing-only merge (no physical key move); orphan Raft group after merge stays hosted (reclaim follow-on)
- **Leadership transfer:** admin `TRANSFER_LEADER` (18) — leader steps down for free election among voters (no TimeoutNow / forced target win); operator-token path; rolling-restart runbook note
- **Learner replicas:** `ClusterMember.is_learner` (forward-compatible encode); learners receive log but do not vote or campaign; admin `PROMOTE_LEARNER` (19); learner remove allowed without voter-floor violation
- **Advisory balancer:** `plan_range_count` + admin `REBALANCE_PLAN` (20) + `kayactl range rebalance-plan` — range-count heuristic only; **does not** move data, transfer leases, or change the meta table (no live migrate)
- **Drain / decommission:** `kayadb-server --drain` / `KAYA_DRAIN=1`; STATS JSON `"drain": true|false`; draining node rejects `SPLIT_RANGE`; runbook `docs/runbooks/decommission-node.md` (transfer leaders → remove member → wipe `data_dir`)
- **Dashboard v1:** optional `--dashboard-addr` read-only HTTP — `GET /health`, `/v1/ranges`, `/v1/raft`
- Spec: `spec/docs/range-routing-spec.md` (merge algorithm, REBALANCE_PLAN advisory, exit table)
- ROADMAP: M22 production path closed. *Out of path:* live range migrate, locality tags, auto size-threshold split

### Added (M21 — Range metadata, routing & splits)
- Epoch’d range descriptors + `StaticRangeTable::split_at` / meta_epoch (shared-engine routing split)
- Runtime Raft group hosting on split (`ensure_group_hosted`)
- Wire: `LIST_RANGES` (15), `SPLIT_RANGE` (16), `STATUS_RANGE_MOVED` (11)
- Rust client: `list_ranges` / `split_range` + `RangeCache`
- `kayactl --server … range list|split`
- IT: `test_range_split_no_lost_writes`
- Spec: `spec/docs/range-routing-spec.md`

### Added (M18/M19 polish)
- **Secondary indexes polish:** field extractors (`WholeValue` / `Prefix` / `Field`), meta v2, online backfill pause/resume/step, `verify_index` divergence gate, chaos churn test, `kayactl index create|list|drop|scan|verify|backfill`
- **CDC polish:** `cdc_compact` (rewrite log below min consumer seq), backup watermark file + `kayactl backup --cdc-consumer`, crash/reopen failover continuity test
- **CDC wire:** opcodes 13 (`CDC_POLL`) / 14 (`CDC_CHECKPOINT`); Rust `KayaClient` + Go `CdcPoll`/`CdcCheckpoint`; conformance vectors v2
- Specs updated: `spec/docs/secondary-index-spec.md`, `spec/docs/cdc-spec.md`

### Added (M16–M20 production path close-out)
- **Atomic multi-key SI commit:** `RaftCommand::TxnCommit` (type 4) — single Raft log entry; `txn_take_commit` + `Engine::apply_mutations`; no sequential N Put/Delete proposes
- **HLC commit timestamps:** `EngineConfig.use_hlc` + `WalWriter::ensure_min_sequence`; multi-group ClusterNode auto-enables HLC
- **Multi-raft ClusterNode:** always hosts `MultiRaftHost` (≥ group 0); `StaticRangeTable` client routing; per-group persist/apply-index; IT `test_multi_raft_static_ranges_put_get`
- Index + CDC fire on Raft apply (shared put/delete path with `apply_mutations` / `TxnCommit`)
- ROADMAP: M16–M20 marked production path closed; M21+ still open; no full v0.2.0 / north-star claim yet

### Added (M20 — Multi-raft foundation)
- Hybrid logical clock: `kaya_core::Hlc` with update / tick / to_u64 / from_u64 packing for commit_ts
- Raft `Envelope.group_id` (default 0) + codec field after `to_id` for transport multiplexing
- Per-group storage paths: `raft_group_dir` / `DiskRaftStorage::open_group` (`groups/{id}/` for id≠0; group 0 keeps legacy root)
- `MultiRaftHost` + `GroupId` + `StaticRangeTable` (key → group lookup; coalesced `tick_all`; per-group propose/handle)
- Spec: `spec/docs/multi-raft-spec.md`

### Added (M19 — CDC / changefeeds foundation)
- Engine CDC: change events on successful user put/delete after WAL (seq, key, value, op)
- File sink `cdc/log.jsonl` + per-consumer cursors `cdc/cursors/{id}` (`EngineConfig.enable_cdc`, default on)
- API: `cdc_subscribe` / `cdc_poll` / `cdc_checkpoint` (at-least-once; per-key order by seq)
- Spec: `spec/docs/cdc-spec.md`

### Added (M18 — Secondary indexes foundation)
- Engine secondary indexes: `create_index` / `list_indexes` / `drop_index` / `scan_by_index`
- System keys under `\x00idx/meta|data/`; value-as-secondary for keys under `primary_prefix`
- Automatic index maintenance on put/delete (covers txn_commit materialization)
- Spec: `spec/docs/secondary-index-spec.md`

### Added (M17 — Single-group ACID transactions)
- Snapshot Isolation write intents + single-node engine txn API (`begin`/`put`/`get`/`delete`/`commit`/`rollback`)
- Wire `TXN_*` opcodes 9–12 with server SI transaction dispatch
- Rust client transaction API
- TLA+ model for single-group commit (`spec/specs/txn/`)
- Jepsen bank workload helpers and transfer-sum invariant tests

### Added (M16 — MVCC)
- Versioned storage: typed InternalKey, multi-version memtable, SSTable v4
- Snapshot reads via ReadTimestamp::At / get_at
- Compaction GC watermark
- Versioned sim RefModel + MVCC crash property

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
