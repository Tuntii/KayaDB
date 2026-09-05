# KayaDB SLO & Limit Envelope

**Status:** Living document
**Applies to:** v0.1.x (correctness prototype)

This document states the **operating envelope** KayaDB is designed and tested for: the hard input limits it enforces, the service-level objectives (SLOs) it targets, and the error-budget posture operators should adopt. It exists so deployments run with eyes open — the roadmap's "no production-readiness claim before proof" principle requires an explicit envelope, not implicit assumptions.

> KayaDB remains an experimental, correctness-first system. The numbers below are **design targets and enforced limits**, not a contractual SLA. Validate them against your own workload before relying on them.

---

## 1. Hard input limits (enforced)

These are enforced by the engine and protocol; requests exceeding them are rejected with `InvalidArgument` (wire `STATUS_INVALID_ARGUMENT`) or bounded, never silently accepted. Defaults come from `kaya-core` constants and are configurable where noted.

| Limit | Default | Constant / config | Behavior on breach |
|---|---|---|---|
| Max key length | 4 KiB | `DEFAULT_MAX_KEY_LEN` / `LimitsConfig.max_key_len` | Reject `InvalidArgument` |
| Max value length | 16 MiB | `DEFAULT_MAX_VALUE_LEN` / `LimitsConfig.max_value_len` | Reject `InvalidArgument` |
| Max scan results | 100 000 | `DEFAULT_MAX_SCAN_RESULTS` / `LimitsConfig.max_scan_results` | Truncated (bounded merge) |
| Max scan bytes | 64 MiB | `DEFAULT_MAX_SCAN_BYTES` / `LimitsConfig.max_scan_bytes` | Truncated after first entry |
| Max scan-prefix length | = max key length | `LimitsConfig.max_key_len` | Reject `InvalidArgument` |
| Max protocol payload | 32 MiB | `DEFAULT_MAX_PAYLOAD_LEN` | Reject / connection closed |
| Max wire frame | 64 MiB | `kaya-net` frame limit | Connection closed |
| Max client connections | 1 024 | `DEFAULT_MAX_CLIENT_CONNECTIONS` / `--max-client-connections` | TCP backpressure (accept paused) |
| WAL segment size | 64 MiB | `DEFAULT_SEGMENT_MAX_BYTES` | Rotates to a new segment |
| Memtable flush threshold | 64 MiB | `DEFAULT_MEMTABLE_MAX_BYTES` | Auto-flush to SSTable |

**Implication:** the largest single logical record is bounded by the value limit (16 MiB); the largest single response is bounded by the scan byte cap (64 MiB). Size client buffers accordingly.

---

## 2. Durability & consistency SLOs

These are the correctness guarantees the test suite (crash/restart property tests, Jepsen-style T1–T7, deterministic sim) is built to defend. They take precedence over any latency/throughput target.

| Objective | Target |
|---|---|
| Acknowledged Strict write durability | 100% — an `OK`'d PUT/DELETE is committed via Raft and fsync'd (WAL + directory entry) before the ack |
| Read linearizability | Leader-served reads are linearizable via ReadIndex; followers reply `NOT_LEADER` |
| Crash recovery | Recovers to the last durable prefix; a corrupt/torn tail is truncated, never surfaced |
| Election safety | ≤ 1 leader per term under drop/dup/latency/reorder/partition (RAFT-INV-001) |
| Directory-entry durability | Publishes (create/rename) survive crash on Unix (real `fsync_dir`); Windows is best-effort (documented) |

Relaxed-durability writes trade the first row for throughput and are explicitly **not** covered by the durability SLO.

---

## 3. Latency & throughput targets (design guidance)

KayaDB does **not** yet publish a certified latency SLA. Use these as design guidance and measure on your hardware. Latency is observable per-op via `Engine::histograms()` (p50/p99) and Prometheus (`kaya_get_*`, `kaya_scan_*`, `kaya_wal_fsync_*`, `kaya_flush_*`, `kaya_compaction_*`), and validated against a regression budget in CI (`BENCHMARKS.md`, `kaya-bench/tests/perf_gate.rs`).

| Signal | Guidance |
|---|---|
| Strict write latency | Dominated by WAL fsync + Raft quorum RTT; watch `kaya_wal_fsync_max_us` and `kaya_raft_*` |
| Read latency (leader) | Memtable/SSTable lookup + ReadIndex quorum; watch `kaya_get_max_us` |
| Throughput | Group-commit batching (`WalBatchConfig`) amortizes fsync; tune batch size/interval to your durability/latency trade-off |
| Multi-key SI / cross-range 2PC | Engine path gated loosely in CI (envelope v2); distributed latency dominated by per-group Raft propose RTT × sequential prepare/commit phases |

**CI performance envelope v2 (M25):** `cargo test -p kaya-bench --test perf_gate --release` asserts put/get, multi-key SI, and multi-range 2PC smoke budgets (see `BENCHMARKS.md`). These are regression fences, not customer SLOs.

**Recommended alerting signals:** `kaya_wal_fsync_max_us` spikes, `kaya_raft_is_leader` flapping, growing `kaya_engine_live_sstables` (compaction falling behind), rising `kaya_get_max_us`/`kaya_scan_max_us`.

---

## 4. Error budget posture

Because this is a correctness prototype, adopt a **conservative** error budget:

- **Durability/consistency violations: zero budget.** Any acknowledged-write loss or split-brain is a stop-ship correctness bug, not a budget spend — file it, do not absorb it.
- **Availability: workload-defined.** A minority partition or a node restart is expected to cause transient `NOT_LEADER` / ret; the client retry policy (`RetryPolicy`: bounded attempts, exponential backoff + jitter, per-attempt timeout) absorbs these. Budget your availability SLO around re-election time and your retry ceiling.
- **Latency: budget against p99, not mean.** The mean hides tail fsync/compaction stalls; alert on the p99 histograms above.

---

## 5. What is explicitly out of envelope

- Values > 16 MiB, keys > 4 KiB, scans intended to return > 100 000 keys or > 64 MiB in one call.
- More than `--max-client-connections` concurrent clients (excess is backpressured, not served).
- Relaxed-durability writes where the durability SLO is expected to hold.
- Delivery-guaranteed audit export, plus tenant quotas / RBAC / billing — see `docs/security.md` §7. Named-tenant isolation (`--tenant-file`, exclusive prefixes, audit `tenant` field) and per-prefix ACL (`--acl-file`) are available; they do not include quotas or a control plane.
- Live range migrate, parallel-commit 2PC, and contractual p99 SLAs — see [deployment-guide-v2.md](deployment-guide-v2.md) non-goals.

---

## 6. Changing the envelope

Limits in §1 are configurable via `LimitsConfig` / server flags. Raising them widens the memory and latency envelope — re-run the benchmark gate (`BENCHMARKS.md`) and the crash/Jepsen suites before relying on new values. Never raise a limit to accept input you have not tested recovery for.
