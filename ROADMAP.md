# KayaDB Development Roadmap

**Status:** Living roadmap  
**Last updated:** 2026-07-17 (M23 cross-shard 2PC production path closed; M24+ open)

> **"Geniş ve yaşayan yol haritası"** — Bu belge hem tarihi başarıları arşivler, hem şu anki odak noktalarını gösterir, hem de uzun vadeli vizyonu (birden fazla paralel track ile) detaylandırır. Tasarım-öncelikli ve correctness-öncelikli felsefe korunur.

**Not (2026-06-30):** M0–M11'in çok detaylı listeleri ve eski "Current snapshot" bölümü bu belgenin okunabilirliğini bozduğu için büyük ölçüde arşivlenmiştir (yukarıdaki "Completed Work — Historical Archive" bölümüne bakın). M15 tamamlandı (2026-06-30); roadmap artık **geniş gelecek vizyonu** + paralel track'lere odaklanıyor.

KayaDB is developed design-first and correctness-first. The roadmap intentionally prioritizes crash consistency, deterministic failure testing and inspectable storage formats before performance or distributed features.

This document is the public, human-readable project roadmap. Detailed team implementation notes are maintained separately.

---

## North star: prototype → product

**Intent (do not lose sight of this):** KayaDB is experimental today, but the project is deliberately being evolved into a **trustworthy, deployable distributed database** — not a forever-demo. Correctness-first work (M0–M12) is the foundation; productization is the planned next arc.

| Today (honest) | Target (product) |
|---|---|
| Strong LSM engine, sim harness, Raft prototype + initial durable state (hard state + log) | Same core, proven under chaos and restart |
| TCP cluster, snapshots, dynamic membership | Survives real restarts and operator workflows |
| Open admin RPCs on localhost | TLS + auth on client and membership/admin paths |
| “Experimental” badge | Documented deployment guide with explicit SLO/limit envelopes |

**We do not claim production-ready until the exit gates below are met.** Until then, treat every release as a correctness prototype.

### M13 — Productization ✅

Goal: cross the line from “serious prototype” to “operators can run this with eyes open.”

1. **Durable Raft state** ✅ — Implemented on `feat/validation-first-consensus`: `raft-hard-state` (term + voted_for + snapshot boundary with CRC) and `raft-log` (framed entries) persist via `DiskRaftStorage`; `RaftNode::recover` + server startup reload on restart. Hard state flushed every loop; log rewritten on propose, follower append, and compaction. SimDisk crash/restart property tests cover suffix-only append tracking. Cluster survives process restart without losing term, vote, or log history.
2. **Authenticated transport** ✅ — Native TLS (rustls) scaffolding complete: kaya-net `tls` feature, TlsConfig, wrapped listeners for raft+client, kaya-client/kayactl support, ClusterConfig flags + env, integration test with TLS cluster. Operator token + sidecar docs also present.
3. **Chaos proof** ✅ — PR chaos-smoke now green (0 violations). Debugged root cause of 65 violations: clients=2 + sequential checker (overlapping completion order) + Error-recorded PUTs (response lost on kill, even if committed) leading to GET seeing unrecorded value. Fixed: 1 client, retry-until-success recording only confirmed ops, reconnect heuristic. T7 + harness solid.
4. **Operations** ✅ — Day-2 runbooks under `docs/runbooks/` (add/remove, rolling-restart, backup-restore, split-brain detection, mtls-sidecar). `kayactl` + scripts updated with token/TLS awareness.
5. **Security audit pass** ✅ — Enforcement table in `docs/security.md` cross-referenced to code; Section 7 documents accepted deployment risks.
6. **Performance envelope** ✅ — published benchmark methodology + regression budget in CI (`BENCHMARKS.md` gates + `kaya-bench/tests/perf_gate.rs` release-mode assertion on smoke path). CI step added to main rust job.

**M13 exit (2026-06-21):** experimental label dropped; remaining deployment hardening documented as accepted risks in `docs/security.md` §7.

### M14 — Correctness + algorithms ✅

Goal: deepen LSM algorithm choices and distributed correctness proof while keeping formats inspectable and modules maintainable. Completed in v0.1.44 (2026-06-24).

1. **Compaction policy** ✅ — `CompactionPolicy` trait in `kaya-lsm` (`L0MergePolicy`, `LevelStrategy`, `TierStrategy`); `EngineConfig.compaction.policy` selects strategy at engine open.
2. **Bloom filter** ✅ — SSTable v2 footer with blocked double-hashing bloom; `SstableConfig.bloom_bits_per_key` (default on); read path skips blocks on negative lookup.
3. **WAL group-commit batching** ✅ — `WalBatchWriter` in `kaya-wal` with `WalBatchConfig` (`batch_max_records`, `batch_max_bytes`, `batch_flush_interval_us`); strict durability preserved via single group fsync per batch.
4. **God-file splits** ✅ — `kaya-engine` → `memtable`, `flush`, `snapshot`, `stats`; `kaya-server/cluster` → `client_ops`, `election`, `replication`, `snapshot`, `stats`; `kayactl` → `cli`, `local`, `server`, `inspect`, `stats_cmd`, `ebpf`.
5. **Chaos matrix CI** ✅ — `.github/workflows/chaos-matrix.yml`: PR smoke + nightly `DiskFull`, `NetworkPartition`, `ClockSkew` matrix cells.
6. **Jepsen CI** ✅ — `.github/workflows/jepsen.yml`: PR `smoke_scenario` gate; nightly/tag `full_gate` T1–T7 with WGL concurrent verify.
7. **Publish CI** ✅ — GitHub Pages docs deploy (`docs.yml`), multi-platform release binaries (`release.yml`), crates.io badges + `scripts/smart_publish.ps1` publish helper; `audit.yml` (`cargo audit` + `cargo deny`).
8. **Jepsen full suite** ✅ — Partition nemesis observability (`PartitionTracker`), scenario registry tests (`scenario_registry.rs`), full gate partition assertions for T2/T5; `KAYA_JEPSEN_FAST=1` for shortened local verification.
9. **io_uring backend** ✅ — `IoUringDisk` in `kaya-io` behind `io_uring` feature flag (Linux-only); shared `contract` helpers + `tests/disk_contract.rs` for FileDisk/SimDisk/IoUringDisk parity.

**M14 exit (2026-06-24):** Jepsen full gate T1–T7 pass with WGL concurrent verify; `io_uring` prototype compiles and satisfies Disk contract tests on Linux with `--features io_uring`. Rust-native `kaya-jepsen-test` is the sole CI correctness gate; external Clojure Jepsen is out-of-band only (no in-repo harness).

### M15 — Remaining tracks ✅

Goal: close post-M14 parallel-track gaps across security, client ecosystem, observability, and deployment. Completed in v0.1.46 (2026-06-30).

1. **Client token auth** ✅ — `CLIENT\x00` framing for data-path ops (opcodes 1–4, 6); `--client-token` / `KAYA_CLIENT_TOKEN` on server, `kayactl`, and `kaya-client`; integration test `data_ops_require_client_token`.
2. **Structured audit logging** ✅ — Append-only JSONL at `{data_dir}/audit.jsonl`; `--audit-log` / `--no-audit-log` (default on when any token configured); hooks in `client_ops::dispatch`.
3. **Protocol conformance suite** ✅ — `docs/clients/conformance/vectors.json` (20+ cases) + `crates/kaya-net/tests/conformance_vectors.rs` runner.
4. **Go client** ✅ — `clients/kaya-go/` with Put/Get/Delete/Scan/Health/Stats, leader redirect, client token support.
5. **Prometheus metrics** ✅ — `--metrics-addr` (default `127.0.0.1:9090`) HTTP `/metrics` exposition (`kaya_wal_fsync_*`, `kaya_engine_live_sstables`, `kaya_raft_*`).
6. **kaya-ebpf crate** ✅ — Linux-gated stub with `probe_catalog()` / `available_scripts()`; non-hard workspace dependency.
7. **Docker deployment** ✅ — `deploy/docker/` multi-stage Dockerfile + 3-node `docker-compose.yml`.
8. **Kubernetes manifests** ✅ — `deploy/k8s/` StatefulSet (3 replicas), headless service, ConfigMap roster template.
9. **Protocol version handshake** ✅ — HELLO opcode 0 (`PROTO_VERSION = 1`); optional client negotiation; backward compatible when skipped.
10. **kayactl watch + EngineStats v2** ✅ — `kayactl watch [--interval 2] status`; `block_cache_hits/misses` and `recovery_duration_us` in stats JSON.

**M15 exit (2026-06-30):** Client authZ, local audit logging, conformance vectors, Go client, Prometheus exporter, deployment manifests, HELLO handshake, and watch mode land. Remaining accepted risks: data-at-rest encryption, multi-tenant isolation, SIEM audit export — see `docs/security.md` §7.

---

## Next arc: M16–M25 — Distributed Transactional KV

**Status (2026-07-17):** **M16–M23 closed** (txn path through cross-shard 2PC). M24–M25 remain open. We still do **not** claim full v0.2.0 / north-star production readiness until M24–M25 exit gates.

**Hedef kimlik (M25):** range sharding + multi-raft + **cross-shard transaction** (Raft üzerine 2PC) — CockroachDB/TiKV çekirdeği sınıfında, adım adım kanıtlanarak. API yüzeyi programatik kalır: **KV + txn + secondary index** (SQL yok; post-M25 v2 adayı). Sıralama: **önce transaction, sonra sharding** — cross-shard txn zaten MVCC + timestamp altyapısı ister; önce tek grupta Jepsen'le kanıtla, sonra dağıt.

Her milestone değişmez disiplini korur: **spec → sim → gerçek implementasyon → chaos/Jepsen exit gate → docs/CHANGELOG sync**. v0.1.47'deki deferred kalemlerin tamamı bu arc'ta somut bir eve yerleşti (aşağıda ⬅ işaretli).

### Faz 1 — Transaction çekirdeği (tek Raft grubu)

1. **M16 — MVCC storage foundation ✅** — Complete (2026-07-12): multi-version memtable (typed `InternalKey`), SSTable v4, engine `get_at` / `ReadTimestamp::At`, compaction GC watermark, versioned sim `RefModel` + MVCC crash properties, `kayactl inspect` v4. *Exit met:* snapshot-read + GC safety in sim; workspace tests green.
2. **M17 — Single-group ACID transactions ✅ production path** — Complete (2026-07-12): SI write intents, TXN opcodes 9–12, Rust client txn API, TLA+ commit model (`spec/specs/txn/`), bank workload. **Production close-out:** atomic `RaftCommand::TxnCommit` (type 4) — single log entry for multi-key commit (no sequential N Put/Delete). Spec: `spec/docs/transactions-spec.md`. *Exit met:* SI + wire/client + TLA+ + bank + Raft atomic commit path green.
3. **M18 — Secondary indexes ✅ production path + polish** — Complete (2026-07-12 path; polish 2026-07-16): engine-local secondary indexes; maintenance on put/delete **and** Raft apply. **Polish:** field extractors (`WholeValue` / `Prefix` / `Field`), online backfill pause/resume/step, `verify_index` chaos divergence gate, `kayactl index create|list|drop|scan|verify|backfill`, meta v2. Spec: `spec/docs/secondary-index-spec.md`.
4. **M19 — CDC / changefeeds ✅ production path + polish** — Complete (2026-07-12 path; polish 2026-07-16): file CDC on put/delete; fires on Raft apply. **Polish:** TCP opcodes 13/14 + Rust/Go subscribe, crash/reopen failover gate, `cdc_compact`, `kayactl backup --cdc-consumer` watermark, conformance v2 CDC vectors. Spec: `spec/docs/cdc-spec.md`.

### Faz 2 — Dağıtım (multi-raft + sharding)

5. **M20 — Multi-raft foundation ✅ production path** — Complete (2026-07-12): Envelope.group_id; per-group storage; MultiRaftHost + StaticRangeTable; HLC; **ClusterNode always hosts MultiRaftHost (≥ group 0)** with static range routing; HLC commit_ts via `EngineConfig.use_hlc` / multi-group auto-enable. Spec: `spec/docs/multi-raft-spec.md`. *IT:* `test_multi_raft_static_ranges_put_get`. *Follow-on (M21+):* dynamic splits / RANGE_MOVED, cross-group 2PC (M23), per-range Jepsen, full OTel trace-context, live clock-skew nemesis.
6. **M21 — Range metadata, routing & splits ✅** — Complete (2026-07-16): epoch’d meta range table (`StaticRangeTable` / `RangeTable`), `split_at` + runtime group host, wire `LIST_RANGES` (15) / `SPLIT_RANGE` (16), `STATUS_RANGE_MOVED` (11), client `list_ranges`/`split_range` cache, `kayactl range list|split`, IT `test_range_split_no_lost_writes`. Spec: `spec/docs/range-routing-spec.md`. *Shared-engine routing split* (no physical key move).
7. **M22 — Rebalancing, merges & placement ✅ production path** — Complete (2026-07-17): shared-engine `merge_with_next` + wire `MERGE_RANGE` (17) + `kayactl range merge` (no physical key move; orphan group reclaim follow-on); admin `TRANSFER_LEADER` (18) (step-down; no TimeoutNow); learner membership flag + `PROMOTE_LEARNER` (19) (learners receive log, do not vote/campaign); **advisory** `REBALANCE_PLAN` (20) range-count heuristic (**no live migrate / MOVE_RANGE**); drain mode (`--drain` / `KAYA_DRAIN`) + decommission runbook; Dashboard v1 read-only HTTP (`--dashboard-addr`: `/health`, `/v1/ranges`, `/v1/raft`). Spec: `spec/docs/range-routing-spec.md`. *Not in this path:* live range migrate, locality tags, auto size-threshold split, TimeoutNow preferred-candidate election, full chaos add/drain/decommission gate (operator workflow documented; chaos matrix remains M25).
8. **M23 — Cross-shard transactions ✅ production path** — Complete (2026-07-17): shared-engine 2PC records (`\x00txn/rec|intent/…`, `apply_txn_{prepare,commit_2pc,abort_2pc}`); RaftCommand types 5/6/7 (`TxnPrepare` / `TxnCommit2pc` / `TxnAbort2pc`); server `txn_coord::commit_cross_group` when `TXN_COMMIT` spans >1 group via `StaticRangeTable` (client-transparent); **sequential** prepare then commit/abort proposes (not parallel-commit stretch); conservative startup recovery (local `Preparing`/`Prepared` → abort); TLA+ sketch `spec/specs/txn/TwoPhaseCommit.tla`; IT `test_cross_range_txn_commit` + multi-range bank `test_multi_range_bank_sum_invariant`. Spec: `spec/docs/transactions-spec.md` §17. *Not in this path:* parallel prepare/commit stretch, HLC uncertainty-interval wait/clamp, durable global decision log, multi-range Jepsen bank under split+merge+rebalance+kill+partition chaos (→ M25).

### Faz 3 — Hardening + kanıt

9. **M24 — Production hardening ⬜** — Encryption-at-rest (pluggable Disk wrapper, AES-GCM, KEK/DEK + rotation; §7 riski kapanır), per-prefix ACL. ⬅ *Kernel+userspace birleşik attribution*, ⬅ *io_uring completion tracing*, ⬅ *stap/perf privileged CI*, ⬅ *Dashboard v2* (trace timeline + eBPF + range health). *Exit:* security.md §7 tablosu boşalır/yeniden gerekçelenir; dashboard day-2 ops'ta kullanılır.
10. **M25 — Scale proof & ecosystem close-out ⬜** — Performance envelope v2 (txn+sharded regression gate'ler; ⬅ *scheduled profiling CI*), Jepsen grand matrix, ⬅ *linearizability minimal counterexample*, ⬅ *TS/JS + Zig client'lar* + Go txn/retry paritesi + conformance v3, deployment guide v2 + SLO v2. *Exit:* north-star yeniden değerlendirilir → **v0.2.0**.

**Kapsam dışı (bilinçli):** SQL katmanı (post-M25 v2 adayı), tam multi-tenancy (yalnızca per-prefix ACL), geo-replication/follower reads, kanıtsız production SLA iddiası.

---

## Roadmap principles

1. **Correctness before throughput**  
   A slow but correct storage path is preferred over a fast ambiguous one.

2. **Failure is a normal input**  
   Partial writes, failed fsyncs, corruption and crash/restart paths must be testable.

3. **Every persistent format is inspectable**  
   WAL, manifest, SSTable and traces should be readable through tooling.

4. **Simulation before distribution**  
   Raft and networking should start only after local storage recovery is reliable.

5. **No production-readiness claim before proof**  
   KayaDB remains experimental until crash, recovery, fuzzing and distributed validation are mature.

---

## Legend

| Mark | Meaning |
|---|---|
| ✅ | Implemented or mostly complete for current scope |
| 🟡 | Partially implemented / actively next |
| ⬜ | Planned |
| 🔒 | Future scope, intentionally blocked by earlier milestones |

---

## Completed Work — Historical Archive (M0–M11)

**Tüm M0–M11 başarıyla tamamlandı.** (Son büyük güncelleme ~2026-06-14)

KayaDB şu anda güçlü bir **correctness prototype** seviyesindedir:
- Tam fonksiyonel LSM engine (WAL, memtable, SSTable, manifest, L0 compaction, recovery).
- Deterministic simulation + SimDisk + crash/restart property testleri.
- Tam Raft (election, log replication, snapshots, joint-consensus dynamic membership).
- Gerçek TCP cluster + resilient client (`kaya-client` + `kayactl --server`).
- Linearizability (sequential + concurrent), Jepsen-style harness, client tracing.
- Kapsamlı tooling: `kayactl` (inspect, stats, recover, membership), fuzzing, benchmarks.
- 150+ test + güvenlik sınırları.

**Arşiv:** Detaylı "Implemented" listeleri, KD-XXXX backlog'lar ve M0–M11 bölümleri bu belgenin hacmini çok büyüttüğü için buradan kaldırıldı. 

İlgili tarihsel içerik için:
- Git'teki 2026-06-14 ROADMAP snapshot'ı
- `spec/docs/` (özellikle simulation-spec, raft-and-distributed-roadmap-spec)
- `memory/` klasöründeki session notları
- `docs/KayaDB_Explained.md` ve eski milestone PR'ları

Artık roadmap **ileriye ve genişe** odaklanıyor.

---

## M0 — Project foundation ✅ (Arşiv)

Goal: establish the repository as a compilable Rust workspace with design-driven contribution flow.

(Detaylar arşive taşındı — yukarıya bakın.)

- Rust workspace and crate layout.
- Initial `README.md` and `CONTRIBUTING.md`.
- CI workflow for formatting, clippy and tests.
- PR template with roadmap/invariant sections.
- Initial internal design notes.

Primary backlog IDs:

- `KD-0001` Create Rust workspace and crate layout.
- `KD-0002` Add CI for fmt, clippy and tests.
- `KD-0003` Add contribution templates and labels.

Exit criteria:

- ✅ `cargo test --workspace` passes.
- ✅ Crate boundaries compile.
- ✅ Public documentation explains crate boundaries and development flow.

---

## M1 — Disk layer and WAL format ✅

Goal: make the durability foundation explicit and testable.

Implemented:

- Core types and error model.
- `RelativePath` validation.
- `Disk` trait.
- `FileDisk` implementation.
- WAL record constants/types.
- WAL encoder and decoder.
- Basic malformed WAL rejection tests.
- `FileDisk` append serialization (internal lock, documented `Disk::append` contract) + shared concurrent-append contract test (2026-07-08).
- Decoder edge-case suite: unknown flags, oversized lengths, header/payload CRC corruption, unknown record type, malformed payloads (`kaya-wal/tests/decoder_edge_cases.rs`, 2026-07-08).
- Persistent format golden fixtures for WAL v1, SSTable v2/v3, manifest v1 (`tests/fixtures/` + byte-exact golden tests, 2026-07-08).

Primary backlog IDs:

- `KD-0101` Define core types and error model.
- `KD-0102` Define `RelativePath`.
- `KD-0103` Define `Disk` trait.
- `KD-0104` Implement `FileDisk`.
- `KD-0105` Implement WAL record structs and constants.
- `KD-0106` Implement WAL encoder.
- `KD-0107` Implement WAL decoder.

Related public docs:

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/development.md`](docs/development.md)

---

## M2 — WAL writer, recovery and SimDisk ✅

Goal: prove that strict ACKed WAL records survive crash/recovery.

Implemented:

- WAL segment writer.
- Strict and relaxed append modes.
- WAL recovery reader.
- Corrupted/partial tail truncation path.
- Basic `SimDisk` volatile/stable model.
- SimDisk event recording.
- Deterministic fault schedule (`FaultKind`: `FsyncFailed`, `IoError`, `DiskFull`, `PartialWrite`) with `SimDisk::with_faults`.
- WAL durable-prefix property test (KD-0206).
- WAL crash/recovery idempotence tests.
- Multi-segment recovery test.

Still needed:

- TLA+ model execution instructions/tooling.

Primary backlog IDs:

- `KD-0201` Implement WAL segment writer. ✅
- `KD-0202` Implement WAL recovery reader. ✅
- `KD-0203` Implement corrupted tail truncation. ✅
- `KD-0204` Implement `SimDisk` stable/volatile model. ✅
- `KD-0205` Add deterministic fault schedule to `SimDisk`. ✅
- `KD-0206` Add WAL durable-prefix property test. ✅

Related public docs:

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/development.md`](docs/development.md)

Recommended next PR focus:

```text
KD-0401 KD-0402
```

---

## M3 — Minimal single-node engine ✅

Goal: provide durable local key-value operations over WAL + memtable.

Implemented:

- Memtable `put/get/delete/scan_prefix`.
- Engine `put/get/delete/scan_prefix`.
- Engine recovery from WAL into memtable.
- `kayactl put/get/delete/scan`.
- `kayactl inspect wal`.
- Engine crash/restart tests over `SimDisk`.
- Delete-then-restart correctness test.
- Scan-prefix correctness after restart.

Since closed (previously "still needed"):

- Stronger validation around limits and scan behavior — ✅ key/value limits + scan hard caps (`max_scan_results` / `max_scan_bytes`) and scan-prefix validation (2026-07-08).
- Recovery diagnostics exposed through CLI — ✅ `kayactl recover --dry-run` (M9).
- Clear data directory locking behavior — ✅ `KAYA_LOCK` exclusive lock in `Engine::open` (share-mode 0 on Windows, `flock` on Unix).

Still needed:

- More CLI JSON output and snapshot-style CLI tests. (Minor; folded into ongoing Track G DX work — not milestone-blocking.)

Primary backlog IDs:

- `KD-0301` Implement memtable. ✅
- `KD-0302` Implement engine PUT/GET/DELETE over WAL + memtable. ✅
- `KD-0303` Implement engine recovery from WAL. ✅
- `KD-0304` Implement `kayactl put/get/delete/scan`. ✅

Related public docs:

- [`docs/getting-started.md`](docs/getting-started.md)
- [`docs/cli-reference.md`](docs/cli-reference.md)
- [`docs/development.md`](docs/development.md)

---

## M4 — SSTable and manifest ✅

Goal: move beyond WAL-only persistence into LSM storage with manifest-defined live state.

Implemented:

- SSTable writer (`SstableBuilder`) and reader (`SstableReader`).
- Three-level on-disk layout: data blocks → index block → footer.
- CRC32C validation on data blocks, index block and footer.
- Footer magic and version check.
- Manifest frame format: 32-byte header with CRC + variable payload.
- Manifest edit types: `CreateTable`, `DeleteTable`, `SetLastSequence`.
- `replay_manifest` reconstructing `ManifestState` from any suffix of valid edits.
- `CURRENT` file atomic update (tmp-write → fsync → rename → fsync dir).
- Engine `flush()`: SSTable build → atomic disk publish → manifest append → CURRENT update.
- Engine `get()` falls back through live SSTables newest-first.
- Engine `scan_prefix()` merges SSTable and memtable results (tombstones respected).
- Engine `open()` loads manifest and live SSTables from disk.
- `kayactl inspect sstable <path>` and `kayactl inspect manifest <path>`.
- Flush-and-reopen roundtrip tests, delete-after-flush test, scan merge test.

Primary backlog IDs:

- `KD-0401` Implement SSTable writer/reader. ✅
- `KD-0402` Implement manifest record format and replay. ✅
- `KD-0403` Implement memtable flush to SSTable. ✅
- `KD-0404` Implement basic inspect commands. ✅

Related public docs:

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/cli-reference.md`](docs/cli-reference.md)

Exit criteria:

- ✅ Recovered database can read from live SSTables.
- ✅ Manifest replay reconstructs live table set.
- ✅ Crash during flush does not lose acknowledged writes.

---

## M5 — Compaction and recovery hardening ✅

Goal: preserve visible state while reducing read amplification and hardening crash recovery.

Implemented:

- Simple L0 compaction: merges all L0 SSTables by key and sequence, tombstones preserved.
- Atomic compaction publication: `CreateTable(output)` → `DeleteTable(inputs)` → fsync, single manifest batch.
- `Engine::compact()` crash-safe: output is live before inputs are removed; partial manifest state is valid.
- Recovery idempotence: flush and compaction crash recovery verified with double-crash tests.
- Manifest tail corruption test: appended garbage survives a simulated fsync, recovery truncates it gracefully.
- Fuzz target skeletons: `fuzz/fuzz_targets/fuzz_wal_decoder.rs`, `fuzz_sstable_footer.rs`, `fuzz_manifest_decoder.rs` using `libfuzzer-sys`.
- Malformed-input unit tests for all three decoders: no panic on arbitrary byte sequences.

Primary backlog IDs:

- `KD-0501` Implement simple L0 compaction. ✅
- `KD-0502` Add recovery idempotence test suite. ✅
- `KD-0503` Add fuzz target skeletons. ✅

Related public docs:

- [`docs/development.md`](docs/development.md)
- [`docs/security.md`](docs/security.md)

Exit criteria:

- ✅ Compaction preserves visible key-value state.
- ✅ Flush and compaction crash recovery are idempotent.
- ✅ Malformed persistent files do not panic expected paths.

---

## M6 — Deterministic simulator ✅

Goal: make failures reproducible by seed and replayable trace.

Delivered work:

- `crates/kaya-sim/src/rng.rs` — xorshift64 `SimRng`, fully deterministic.
- `crates/kaya-sim/src/model.rs` — `RefModel` BTreeMap reference model.
- `crates/kaya-sim/src/trace.rs` — hand-rolled JSONL `TraceWriter` + `parse_trace`.
- `crates/kaya-sim/src/runner.rs` — `run_async` (seeded op gen, invariant checks, crash/restart) and `replay_async`.
- `crates/kaya-sim/src/lib.rs` — `SimRunner`, `replay_trace`, full public API + tests.
- Invariants checked: ENG-001 (durability after crash), ENG-002 (GET matches model),
  ENG-003 (DELETE hides key), ENG-004 (SCAN matches model prefix).

Primary backlog IDs:

- `KD-0601` Implement simulation runner. ✅
- `KD-0602` Implement replay mode. ✅
- `KD-0603` Add CI small seed suite (10 seeds × 1 000 ops). ✅

Related public docs:

- [`docs/development.md`](docs/development.md)
- [`docs/architecture.md`](docs/architecture.md)

Exit criteria:

- Same seed produces same operation sequence. ✅
- Invariant failure writes JSONL trace with full event history. ✅
- Replay verifies GET/SCAN results match original run. ✅

---

## M7 — Raft prototype in simulation ✅

Goal: introduce replicated log semantics only after local storage correctness is stable.

Implemented:

- `Term`, `LogIndex`, `NodeId` types and `RaftApplyCommand` in `kaya-raft`.
- `MemLog`: in-memory Raft log with 1-based indexing, truncation and conflict resolution.
- `VoteRequest`, `VoteResponse`, `AppendRequest`, `AppendResponse`, `Message`, `Envelope` message types.
- `RaftNode`: tick-driven pure state machine with full Raft §5 semantics:
  - Leader election: election timeout, RequestVote, vote counting, majority quorum.
  - Log replication: AppendEntries, prevLog consistency check, conflict truncation, commit index advancement.
  - No-op entry on new leader to establish commit barrier for previous-term entries (§5.4.2).
  - Leader heartbeats every `heartbeat_interval_ticks` ticks.
  - `propose()` for submitting commands when leader.
  - `applied_entries` visible to simulator for invariant verification.
- `SimNetwork`: deterministic per-seed message drop and duplication, unidirectional partitions, `isolate` / `reconnect` helpers.
- `ClusterSim`: multi-node Raft driver in `kaya-sim`:
  - Staggered election timeouts for deterministic first-election convergence.
  - Two-round message delivery per tick (request → response in same tick).
  - RAFT-INV-001 invariant check: ≤1 leader per term.
  - `propose`, `current_leader`, `statuses`, `applied_entries`, `network_mut` API.
- 14 simulation tests pass (including multi-seed CI suite and partition/rejoin).

Primary backlog IDs:

- `KD-0701` Implement Raft state machine (`kaya-raft`). ✅
- `KD-0702` Implement deterministic simulated network (`kaya-sim`). ✅
- `KD-0703` Implement `ClusterSim` with election-safety invariant. ✅
- `KD-0704` Add partition/rejoin and multi-seed CI tests. ✅

Related public docs:

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/development.md`](docs/development.md)

Exit criteria:

- ✅ Same seed produces same operation sequence.
- ✅ At most one leader per term across all tested scenarios.
- ✅ Committed entries survive partition and rejoin.
- ✅ `cargo test --workspace` passes (59 tests).

---

## M8 — Real cluster mode ✅

Goal: run a small KayaDB cluster over real TCP after simulated Raft is reliable.

Completed work:

- **KD-0801** — TCP Raft transport in `kaya-net` (`codec.rs`, `roster.rs`, `transport.rs`).
  - Hand-rolled wire format: `frame_len(u32 LE) | from_id | to_id | msg_type | payload`.
  - Client frame helpers: PUT/GET/DELETE/SCAN/HEALTH encode + decode.
  - `send_envelopes`, `start_raft_listener`, `roundtrip` transport primitives.
  - `NodeRoster` — static cluster membership table.
- **KD-0802** — `ClusterNode` in `kaya-server` with Raft event loop and client handler.
  - `RaftCommand` (Put / Delete) with hand-rolled codec.
  - `raft_event_loop`: `tokio::select!` over tick / raft-rx / propose-rx.
  - `drain_and_apply` path writes committed entries into the LSM engine.
  - Pending client oneshot map for linearizable PUT/DELETE replies.
- **KD-0803** — `kayadb-server` cluster CLI (`--node-id`, `--raft-addr`, `--client-addr`, `--peer`, `--data`).
- **KD-0804** — `kayactl --server <addr>` cluster-aware commands (put/get/delete/scan/health).

Related public docs:

- [`docs/getting-started.md`](docs/getting-started.md)
- [`docs/architecture.md`](docs/architecture.md)
- [`docs/security.md`](docs/security.md)

Verification:

- ✅ `cargo build --workspace` succeeds.
- ✅ `cargo test --workspace` passes (72 tests, +13 from M8: kaya-net × 9, kaya-server × 4).

---

## M9 — Benchmarking, observability and linearizability ✅

Goal: validate distributed behavior and improve Linux-native observability/performance.

Completed work:

- **KD-0901** — Benchmark harness (`crates/kaya-bench`): Criterion-based microbenchmarks
  for WAL append (relaxed/strict/recovery), SSTable build+scan+get, and engine PUT/GET workloads.
- **KD-0902** — Observability commands in `kayactl`:
  - `kayactl [--data <dir>] [--json] stats` — live `EngineStats` + recovery summary.
  - `kayactl [--data <dir>] [--json] recover --dry-run` — WAL replay report without
    opening the engine; shows `records_replayed`, `truncated_bytes`, `last_lsn`, warnings.
- **KD-0903** — `LinearizabilityChecker` in `kaya-sim` (`src/linear.rs`):
  - Records `(Op, OpResult, start_tick, end_tick)` history via `record` / `record_next`.
  - `check_sequential()` replays against in-memory `RefModel`; verifies every GET/SCAN
    returns the value that was last committed. Errors are non-constraining.
  - 6 tests: consistent reads, delete-then-get, stale-read violation, error non-constraining,
    scan consistent, scan missing-entry violation.
  - Public API exported from `kaya-sim` for future Jepsen-style test drivers.

Related public docs:

- [`docs/cli-reference.md`](docs/cli-reference.md)
- [`docs/development.md`](docs/development.md)
- [`BENCHMARKS.md`](BENCHMARKS.md)

Future (v2+):

- Additional eBPF probes (syscall timeline, USDT, flamegraphs); the basic fsync + block I/O probes landed in M12 via bpftrace + `kayactl ebpf`.
- Jepsen workloads (requires Clojure + Linux cluster).
- Concurrent linearizability via WGL algorithm (needs concurrent op history).
- `io_uring` backend (Linux-only).

Verification:

- ✅ `cargo build --workspace` succeeds.
- ✅ `cargo test --workspace` passes (78 tests, +6 from M9: linear × 6).

---

## M10 — Cluster correctness hardening and client API ✅

Goal: harden the protocol surface, add malformed-input tests, improve client/operator UX and enable trace-based cluster validation.

Completed work:

- **P0-1** — Linearizable read policy: GET/SCAN go through ReadIndex; followers return `STATUS_NOT_LEADER`. ✅ (already in M8)
- **P0-2** — Leader discovery and retry hints: `NOT_LEADER` response includes leader `client_addr`; `kaya-client` and `kayactl` auto-redirect. ✅ (already in M8)
- **P0-3** — Real cluster integration tests: 3-node spawn, leader election, PUT/GET/DELETE/SCAN, follower `NOT_LEADER`, leader crash, re-election, linearizability history check. ✅ (already in M8)
- **P0-4** — Protocol alignment and malformed frame tests (KD-1001):
  - 26 malformed-input unit tests for all client payload decoders and Raft envelope decoder in `kaya-net`.
  - 12 TCP loopback tests for `read_client_frame`, `write_client_response`, `encode_client_frame` and `roundtrip`.
  - `STATUS_INVALID_ARGUMENT` (1) now returned by server on malformed client payloads (was `STATUS_ERROR`).
  - `STATUS_INVALID_ARGUMENT` re-exported from `kaya-net` and handled by `kaya-client` and `kayactl`.
- **P1-6** — `kayactl --server` improvements (KD-1002):
  - `--timeout <ms>` flag for request timeouts.
  - Multi-endpoint mode: `--server a --server b --server c` with automatic failover.
  - `health` command now uses `roundtrip_with_retry` for endpoint failover.
- **P1-9** — Recovery diagnostics: already complete in M9 (`RecoveryReport` includes manifest, SSTable, WAL, temp files, warnings). ✅
- **P1-11** — Client trace recording (KD-1003):
  - `KayaClient::enable_tracing()` / `disable_tracing()` / `take_trace(seed)` / `check_trace()`.
  - Records every PUT/GET/DELETE/SCAN into `LinearizabilityChecker` for sequential linearizability verification.
  - `take_trace()` exports JSONL trace compatible with simulation replayer.
- **P2-13** — Protocol fuzz tests: `fuzz_command_frame_decoder` already covers all `kaya-net` decoders. ✅

Verification:

- ✅ `cargo fmt --all -- --check` passes.
- ✅ `cargo clippy --workspace --all-targets -- -D warnings` passes.
- ✅ `cargo test --workspace` passes (128 tests, +50 from M10: kaya-net codec × 26, kaya-net transport × 12, kaya-raft × 3, kaya-server × 4, kaya-sim × 5).

---

## Immediate next actions

M10 completed the client/protocol hardening pass. The project now has:

- Linearizable reads via ReadIndex, leader hints, and auto-redirect in both `kaya-client` and `kayactl`.
- 38 malformed-input and TCP loopback tests in `kaya-net`.
- `STATUS_INVALID_ARGUMENT` for malformed client payloads.
- Multi-endpoint and timeout support in `kayactl --server`.
- Client-side trace recording compatible with `LinearizabilityChecker`.
- 157+ passing tests across the workspace.

### M11 — Demo/readiness pass ✅

1. **Benchmark reporting discipline.** ✅ `BenchmarkReport`, `scripts/bench-report.{sh,ps1}`, CI smoke step, `BENCHMARKS.md` metadata table.
2. **Concurrent linearizability checker.** ✅ `LinearizabilityChecker::check_concurrent` (WGL); `kaya-jepsen-test` `History::check_concurrent` + `record_timed`.
3. **Raft log snapshotting/compaction.** ✅ InstallSnapshot wire codec (MSG 5/6), sim + server integration, `test_install_snapshot_over_tcp`.
4. **Dynamic cluster membership.** ✅ Joint-consensus with `ClusterMember` addresses, `Arc<RwLock<NodeRoster>>` hot reload, `ADD_MEMBER`/`REMOVE_MEMBER` opcodes (7/8), `kayactl add-node`/`remove-node`, `--join-cluster`, persisted `cluster-roster.json`.
5. **Production security boundaries.** ✅ `--allow-public-bind` guard, bind validation, 64 MiB frame limits, `docs/security.md` enforcement table.

### M12 — Jepsen prep + Linux observability experiments ✅

6. **Jepsen preparation.** ✅
   - Workload and nemesis definitions documented in `docs/jepsen-design.md`.
   - Process-control scripts: `scripts/start-cluster.{sh,ps1}`, `stop-cluster.{sh,ps1}`, `kill-node.{sh,ps1}`, `restart-node.{sh,ps1}`.
   - Rust-native test harness in `crates/kaya-jepsen-test/`:
     - `workload` module: concurrent client generators (Register, Counter, Set, Map workloads).
     - `nemesis` module: failure injectors (kill node, partition — partition requires `iptables`/`tc`).
     - `history` module: thread-safe operation recorder with `LinearizabilityChecker` integration.
     - `runner` module: test orchestrator with workload + nemesis + verification pipeline.
   - Full Jepsen (Clojure) deferred until snapshot and dynamic membership are stable.
7. **Linux eBPF observability.** ✅ (initial experiments + userspace complement delivered)
   - ✅ `scripts/ebpf/fsync-latency.bt` and `block-io-latency.bt` (bpftrace)
   - ✅ `kayactl ebpf fsync-latency|block-latency [--pid N]`
   - ✅ `scripts/ebpf/README.md` + docs updates
   - ✅ Userspace WAL fsync latency metrics in EngineStats (`wal_fsync_total_us` / `max_us`) + printing in stats/status (pairs with eBPF kernel histograms)
   - `io_uring` backend remains separate future item (see disk-and-io-spec).
   - Next possible: deeper probes (syscall timeline etc.), optional aya-based Rust eBPF crate scaffold, more userspace latencies (flush, compaction).

### P3 — GUI/dashboard scope, deliberately deferred

8. **Do not build a general GUI client yet.**
9. **Later dashboard candidate: trace and cluster viewer.**
   - Read JSONL traces and show operation timeline, leader changes, partitions and invariant failures.
   - Show node status/metrics from `kayactl status`/server `STATS`.

Suggested next milestone shape:

```text
M11 — Benchmark discipline, concurrent linearizability, Raft snapshots, dynamic membership ✅
M12 — Jepsen prep + Linux observability experiments ✅
M13 — Productization ✅
M14 — Correctness + algorithms (compaction, bloom, WAL batching, CI gates, Jepsen full, io_uring) ✅
M15 — Remaining tracks (client auth, audit, conformance, Go client, Prometheus, deploy, HELLO, watch) ✅
M16–M25 — Distributed Transactional KV arc (MVCC → txn → index → CDC → multi-raft → sharding → 2PC → hardening → scale proof) ⬜
```

---

## Validation commands

Run before roadmap-affecting PRs:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For deeper correctness work, add the relevant property, crash, fuzz or simulation command once those tools exist.

---

## Long-term Expanded Roadmap — Parallel Tracks (Baya Geniş Versiyon)

Aşağıdaki track'ler **paralel** ilerleyebilir. Her biri kendi içinde öncelikli, orta ve uzun vadeli adımlar içerir. Özellikle **Linux eBPF / Observability** track'i detaylı yazıldı çünkü şu anda aktif olarak çalışıyoruz.

**Durum özeti (v0.1.47, 2026-07-11):** Milestone omurgası (M0–M15) ve tüm paralel-track'lerin **kısa + orta vadeli** kalemleri tamamlandı. Geriye kalan işaretli kalemler bilinçli olarak **⬜ deferred**: her biri kendi spec'ini gerektiren büyük/greenfield veya araştırma işleri — TS/JS & Zig client'lar, TLA+ model genişletme, web/GUI dashboard, cluster-genelinde production-grade tracing + birleşik kernel/userspace attribution, io_uring completion tracing, privileged-CI gerektiren stap/perf, ve Track H araştırma deneyleri. Bunlar bir kod oturumunda dürüstçe "bitirilecek" işler değildir; roadmap'in kendi *"kanıtlanmadan production iddiası yok"* ilkesiyle uyumlu olarak açıkça ertelenmiştir.

**Güncelleme (2026-07-12):** Deferred kalemlerin tamamı artık **M16–M25 arc'ında** somut milestone'lara yerleşti (yukarıdaki "Next arc" bölümü + spec'teki placement map): TLA+ → M17/M23, cluster tracing → M20/M24, clock-skew nemesis → M20, dashboard → M22/M24, kernel attribution + io_uring tracing + stap CI → M24, scheduled profiling + minimal counterexample + TS/JS & Zig client'lar → M25. Track H araştırma track'i olarak kalır.

### Track A: Observability, Diagnostics & Linux/eBPF Tooling (Aktif — Yüksek Öncelik)

**Mevcut durum (Track A Phase 2A — 2026-07-03):** 
- bpftrace script'leri: fsync-latency, block-io-latency, **syscall-timeline.bt** (write/fsync/rename/unlink + TID correlation + publish timeline)
- `kayactl ebpf`: **list**, **status**, **trace wal**, **correlate**, bpftrace wrappers (**fsync-latency**, **block-latency**, **syscall-timeline** with **--run --duration**), multi-PID discovery, script resolution via `KAYA_EBPF_SCRIPT_DIR` / repo walk-up
- Userspace metrics: WAL fsync + **flush_total/max/count + compaction_total/max/count** in EngineStats, full exposure in `kayactl stats` (incl. **--latency**), server `status`, JSON + human printers
- `kayactl ebpf correlate` — userspace WAL vs `{data_dir}/ebpf/trace.jsonl` kernel summary with delta hints
- `scripts/ebpf/Makefile` (`make list|fsync|block|timeline|verify`) + Docker kernel verify harness (`scripts/docker_verify_ebpf_kernel.{sh,ps1}`)
- In-process runtime (`kaya-ebpf`, `kayadb-server --ebpf`) + Linux CI gate (`scripts/linux_verify_ebpf_kernel.sh`)
- docs synced: cli-ref, ebpf/README, observability-spec §7, CHANGELOG Unreleased
- Cross-platform (graceful on Windows); Linux eBPF remains optional / no hard dep

**Kısa vadeli (Track A Phase 2A — tamamlandı / kısmi):**
- ✅ `syscall-timeline.bt` (write, fsync, fdatasync, rename, unlink)
- ✅ Basit write + fsync korelasyonu (aynı TID'de, syscall-timeline.bt)
- ✅ `kayactl ebpf list` ve `kayactl ebpf status` (çalışan trace'leri + status.json)
- ✅ Auto-detect tüm local `kayadb-server` process'leri (cluster node'lar için)
- ✅ `--run` bpftrace wrapper (output capture, `--duration` timeout + SIGTERM)
- ✅ Userspace latency: `flush_*` / `compaction_*` + `kayactl stats --latency`
- ✅ eBPF + userspace korelasyon: `kayactl ebpf correlate`
- ✅ Per-file veya data-dir filtreli bpftrace (`durability-syscalls.bt` path-substring filter + `make datadir DATADIR=...`)
- ✅ Multiple script paralel `--run` (`make parallel` — fsync+block+timeline eşzamanlı, DURATION-bounded, ayrı loglar)

**Orta vadeli (Track A Phase 2B+ — tamamlandı / kısmi, 2026-07-03):**
- ✅ `crates/kaya-ebpf` in-process runtime + Linux `kernel-probes` tier-B CI gate
- ✅ USDT-shaped userspace markers: `kaya_core::emit_probe_marker` at WAL fsync + flush boundaries → `{data_dir}/ebpf/trace.jsonl` when `--ebpf`
- ✅ Extended `ProbeEvent`: `usdt_marker`, `publish_syscall` + replay/schema-drift tests
- ✅ `kayactl ebpf correlate` marker + publish summaries; `trace wal` prints publish/USDT lines
- ✅ `scripts/ebpf/Makefile` (Phase 2A)
- ✅ Flamegraph + stack collapse entegrasyonu (Phase 2C): `durability-flamegraph.bt`, `kayactl ebpf flamegraph`, `make flamegraph`
- ✅ OpenTelemetry spans (Phase 2C; `kayadb-server --features otel --otel`; Prometheus `/metrics` ✅ M15)
- 🟡 External stap/perf USDT attachment (in-process markers + operator guide in `scripts/ebpf/README.md`; ⬜ stap-in-CI **deferred → M24** — needs a privileged Linux CI runner)

**Uzun vadeli / İleri seviye** — ⬜ **deferred, artık M16–M25 arc'ında planlı** (building blocks — histograms, USDT markers, flamegraphs, correlate, per-file trace — hazır):
- Kernel + userspace birleşik attribution (hangi fsync'in ne kadarını kernel'da geçirdiğini net raporla) → **M24**
- Production-grade tracing (trace correlation across cluster nodes + client) → **M20 (v1) / M24 (full)**
- GUI / web dashboard — ✅ M22 v1 (read-only cluster/range/raft JSON via `--dashboard-addr`); full trace timeline + eBPF histogram → **M24 (v2)**
- io_uring completion tracing (Track B ile birleşik) → **M24**

**Non-goals (değişmez):**
- Root gerektiren testler default olarak
- Kernel bağımlılığını core crate'lere sızdırmak
- Production SLA claim'leri (henüz)

### Track B: I/O & Low-level Storage

- Linux `io_uring` Disk implementasyonu (yeni async backend) — ✅ M14 (`IoUringDisk`, `io_uring` feature)
- Gelişmiş compaction stratejileri (leveled + tiered hibrit) — ✅ `CompactionPolicy` wired (M14)
- Block cache, bloom filter, compression seçenekleri (SSTable v2/v3) — ✅ bloom (M14); block cache + LZ4 compression (v0.1.45 track)
- WAL group-commit batching — ✅ `WalBatchWriter` (M14)
- Daha iyi fsync_dir semantiği + directory sync optimizasyonları — ✅ v0.1.47 (real Unix `fsync_dir` in `FileDisk`/`IoUringDisk`, WAL segment dir-sync timing fix, `SimDisk::with_strict_namespace()` crash-model)

### Track C: Distributed Correctness & Chaos

- Tam Clojure Jepsen suite (gerçek cluster + dynamic membership + snapshots altında) — out-of-band only; Rust-native T1–T7 full gate ✅ M14
- Rust-native Jepsen CI (smoke + nightly T1–T7) — ✅ M14
- Chaos matrix CI (DiskFull, NetworkPartition, ClockSkew) — ✅ M14
- Daha zengin nemesis seti + clock skew, disk latency injection — 🟡 v0.1.47 (deterministic sim: `SimNetworkConfig.latency_ticks` + `reorder_percent`, asymmetric `isolate_outgoing`/`isolate_incoming`; live-cluster wall-clock skew/disk-latency still needs OS tooling)
- Linearizability checker'in production'a yakın versiyonu (WGL) — ✅ M11 (`LinearizabilityChecker::check_concurrent`, WGL algorithm, `Vec<String>` violation report; used in Jepsen full gate). Daha zengin raporlama (minimal counterexample) ⬜ → **M25**
- TLA+ modellerinin genişletilmesi (manifest + compaction + Raft bir arada) — 🟡 M17 single-group commit ✅ + M23 2PC sketch ✅ (`spec/specs/txn/`); full multi-module (manifest+compaction+Raft) model still open

### Track D: Client & Language Ecosystem

- Go client gerçek implementasyon + conformance — ✅ M15 (`clients/kaya-go/`)
- Protocol conformance vectors + Rust runner — ✅ M15 (`docs/clients/conformance/vectors.json`)
- HELLO protocol version handshake (opcode 0) — ✅ M15
- Python, TypeScript/JavaScript, Zig native client'lar — 🟡 Python ✅ v0.1.47 (`clients/kaya-py/`, zero-dep); TS/JS, Zig ⬜ **deferred → M25** (each a standalone client sub-project; Python + Go + wire-spec give the reference to port from)
- Yüksek seviye özellikler: retry policy'leri, observability hook'lar, connection pooling — 🟡 Rust ✅ v0.1.47 (`RetryPolicy` backoff+jitter+timeout, keep-alive connection reuse, `ClientObserver` hook); porting the same to Go ⬜ → **M25**

### Track E: Operations, Security & Production

- Backup/restore (snapshot + incremental) — ✅ v0.1.47 (`kayactl backup --data --out [--incremental]`, atomic per-file, skips unchanged immutable files) + runbook
- TLS + auth her yerde (Raft, client, admin RPC) — ✅ M13 TLS + M13 operator token + M15 client token
- Day-2 operasyon dokümanları + kayactl komutları — ✅ M13 runbooks + M15 `kayactl watch`
- Deployment (systemd, Docker, Kubernetes örnekleri) — ✅ M15 `deploy/docker/` + `deploy/k8s/`
- Monitoring stack (Prometheus + eBPF + custom exporter) — ✅ M15 Prometheus `/metrics`; 🟡 eBPF stub (`kaya-ebpf`)
- Structured audit logging — ✅ M15 local JSONL; ✅ v0.1.47 optional SIEM export via `--audit-syslog` (RFC 5424 UDP)
- Graceful shutdown (SIGTERM/Ctrl-C → clean cleanup path) + client connection cap (`--max-client-connections`, default 1024) — ✅ 2026-07-08
- Scan hard caps (`max_scan_results` / `max_scan_bytes`) against unbounded-scan memory abuse — ✅ 2026-07-08
- SLO / error budget / limit envelope tanımları — ✅ v0.1.47 (`docs/slo-envelope.md`: enforced limits, durability/consistency SLOs, latency guidance, error-budget posture)

### Track F: Performance & Benchmarking

- Latency histogram'ları her yerde (WAL, read path, compaction) — ✅ v0.1.47 (`kaya_core::LatencyHistogram` p50/p99; `Engine::histograms()` for get/scan/fsync/flush/compaction; read-path now measured; Prometheus latency metrics expanded)
- CI regression gate'leri + BENCHMARKS.md otomasyonu — ✅ (`kaya-bench/tests/perf_gate.rs` release-mode assertion, `scripts/bench-report.{sh,ps1}`, CI step in `.github/workflows/ci.yml`, `BENCHMARKS.md`)
- Linux perf + eBPF ile düzenli profiling — 🟡 bpftrace/eBPF tooling ✅ (Track A); scheduled/automated profiling runs ⬜ → **M25** (needs a Linux perf CI runner)
- Large value, high concurrency, mixed workload benchmark'ları — ✅ v0.1.47 (`kaya-bench/benches/mixed_workload.rs`: 64 KiB large-value, 5 000-key flush+cold-read, interleaved put/get/delete/scan)

### Track G: DX, Tooling & Documentation

- `kayactl` interactive / watch modları — ✅ M15 `kayactl watch status`
- Trace + cluster görselleştirme (dashboard) — 🟡 M22 v1 ✅ (`GET /health|/v1/ranges|/v1/raft`); full timeline + eBPF ⬜ → **M24 (v2)**
- Daha iyi hata mesajları ve recovery rehberliği — ✅ v0.1.47 (`KayaError::guidance()` actionable hints; structural `LockConflict`; `kayactl` prints `HINT:` lines)
- Katkı deneyimini iyileştirme (eBPF "good first issue" etiketleri vs.) — ✅ `CONTRIBUTING.md` "Good first issues" now lists eBPF-script and language-client-porting areas

### Track H: Research & Future Experiments

⬜ **Deferred by design** — this track is exploratory; items are open research directions, not committed deliverables. Each needs its own investigation/spec before implementation.

- Learned indexes / filtreler
- Yeni durability modları (group commit, relaxed + periodic fsync)
- eBPF + io_uring ultra low-latency path denemeleri
- Daha fazla formal yöntem + property test

---

Bu yapı ile roadmap **hem tarihsel olarak temiz, hem çok geniş (8 paralel track), hem de eBPF/observability için somut actionable adımlar** içeriyor.

Devam etmek istersen bir track'i derinleştirelim (örneğin Track A'daki eBPF script'leri veya Track B'deki block cache + compression).
