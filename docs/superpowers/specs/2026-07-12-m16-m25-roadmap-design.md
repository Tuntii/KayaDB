# M16–M25 Roadmap Design — Distributed Transactional KV Arc

**Date:** 2026-07-12
**Status:** Approved (brainstorming session, 2026-07-12)
**Scope:** Roadmap design for milestones M16–M25 — the arc that takes KayaDB from a
single-Raft-group correctness-proven KV store to a **sharded, cross-shard
transactional distributed KV database**.

---

## 1. Context

M0–M15 are complete (v0.1.47). All short/medium-term parallel-track items are done;
the remaining track items are explicitly deferred, each needing its own spec. This
document defines the next arc and gives every deferred item a concrete home.

Current foundation this arc builds on:

- LSM engine (WAL, memtable, SSTable v2/v3, manifest, compaction policies, bloom,
  block cache, LZ4), deterministic sim harness (`SimDisk`, `SimNetwork`, `ClusterSim`),
  crash/restart property tests, golden format fixtures.
- Single Raft group over real TCP: durable Raft state, snapshots, joint-consensus
  membership, linearizable reads (ReadIndex), TLS + operator/client tokens, audit log.
- Rust-native Jepsen harness (T1–T7 full gate, WGL concurrent checker), chaos matrix CI.
- Observability: Prometheus, latency histograms, eBPF/bpftrace tooling, OTel spans
  (single node), flamegraphs.
- Clients: Rust (retry/pooling/observer), Go, Python; conformance vectors; HELLO
  handshake.

## 2. Decision log

Decisions made with the project owner (2026-07-12):

1. **Axis:** Mixed — new heavy core structures form the backbone; deferred items are
   embedded into the milestones where they pay off most.
2. **M25 target identity:** Fully sharded distributed transactional KV —
   range sharding + multi-raft + **cross-shard transactions** (2PC layered on Raft).
   CockroachDB/TiKV-core class of system, built stepwise and proven at each step.
3. **API surface:** Programmatic KV + transactions + secondary indexes. **No SQL**
   in this arc (noted as a post-M25 v2 candidate; a SQL layer is its own multi-
   milestone project and would dilute the correctness focus).
4. **Sequencing:** Transactions first, then sharding. M16–M19 build and prove the
   transaction stack on the existing single Raft group; M20–M23 distribute that
   proven stack across multiple groups; M24–M25 harden and prove at scale.
   Rationale: cross-shard transactions require MVCC + timestamp infrastructure
   anyway (Percolator/Cockroach lesson); proving the txn layer under Jepsen on one
   group before distributing it is the natural extension of the project's
   correctness-first philosophy. Retrofitting MVCC under already-sharded data was
   rejected as the riskiest ordering.

## 3. Non-negotiable discipline (applies to every milestone)

Each milestone follows the established pattern:

1. **Spec first** — a design doc in `spec/docs/` (or `docs/superpowers/specs/`)
   before implementation.
2. **Sim first** — new protocols proven in the deterministic simulator before the
   real TCP implementation.
3. **Inspectable formats** — every new persistent format gets `kayactl inspect`
   support and golden byte-exact fixtures.
4. **Chaos/Jepsen exit gate** — a milestone is not done until its invariants hold
   under the relevant nemesis set in CI.
5. **Docs + CHANGELOG + ROADMAP sync** on every landing PR.

## 4. Milestone design

### Phase 1 — Transaction core on a single Raft group (M16–M19)

#### M16 — MVCC storage foundation

Goal: versioned storage everything above builds on.

- Versioned key encoding: `user_key + commit_timestamp` suffix; SSTable v4 +
  golden fixtures; versioned tombstones.
- Snapshot reads: consistent read at any retained timestamp.
- MVCC garbage collection integrated with compaction: GC watermark; versions above
  the watermark are never removed while visible.
- `RefModel` becomes versioned; crash/restart property tests re-proven over MVCC.
- `kayactl inspect sstable` understands v4; version-aware stats.

Exit gate: snapshot-read correctness at any retained ts + GC safety proven in sim
(property tests); all existing tests stay green.

#### M17 — Single-group ACID transactions

Goal: interactive transactions with a proven commit protocol.

- `spec/docs/transactions-spec.md`: isolation = snapshot isolation with
  write-write conflict detection; serializable mode is a stretch goal.
- Write intents / lock records in storage; atomic commit record through Raft.
- Protocol opcodes: `TXN_BEGIN` / `TXN_OP` / `TXN_COMMIT` / `TXN_ROLLBACK`;
  Rust client transaction API (read-your-writes buffer).
- **Deferred item lands here — TLA+ expansion (part 1):** TLA+ model of the
  single-group commit protocol (this is where formal methods pay off most).
- Jepsen: new **bank workload** (transfer invariant) + append workload.

Exit gate: Jepsen bank workload green under kill + partition nemeses on a single
group.

#### M18 — Secondary indexes

Goal: indexes that can never diverge from primary data.

- Transactionally-maintained secondary indexes (index entries written in the same
  transaction as the primary row).
- Online backfill (build an index on existing data) with pause/resume.
- Index-driven scans; `kayactl index create/list/verify`.
- Conformance vectors v2 (txn + index opcodes).

Exit gate: automated index↔primary divergence checker stays clean under
crash/chaos.

#### M19 — CDC / changefeeds

Goal: a correct, resumable change stream.

- Raft-log-based changefeed: per-consumer resumable cursors (checkpoints),
  per-key ordering guarantee, at-least-once delivery contract.
- Sinks: TCP stream + file sink.
- `kayactl backup --incremental` re-based on CDC checkpoints.
- Subscribe API in Rust + Go clients.

Exit gate: deterministic chaos test — no lost events and no beyond-contract
duplicates across leader failover.

### Phase 2 — Distribution: multi-raft + sharding (M20–M23)

#### M20 — Multi-raft foundation

Goal: many Raft groups per process, statically partitioned key space.

- Transport multiplexing (`group_id` in envelope), per-group storage layout,
  tick/heartbeat coalescing and batching.
- **HLC (hybrid logical clocks)** — the timestamp foundation for cross-shard
  consistency later.
- Range abstraction: key space split into static ranges; each range = one Raft
  group (no dynamic splits yet).
- **Deferred items land here:** production-grade tracing v1 (OTel trace-context
  propagation node↔node↔client — multi-raft is not debuggable without it);
  live-cluster clock-skew nemesis (pairs with HLC).

Exit gate: 3 nodes × N ranges; all existing Jepsen tests pass per-range.

#### M21 — Range metadata, routing & dynamic splits

Goal: the cluster knows where data lives and can split under load.

- Meta range: range-descriptor table with epochs; bootstrapped meta group.
- Client-side range cache; retry on stale descriptor (`RANGE_MOVED` status);
  batch operations split per range.
- Dynamic split by size threshold; split protocol proven in sim first
  (split = descriptor txn + new group bootstrap).
- `kayactl range list/describe/split`.

Exit gate: zero lost writes during splits under load (per-key linearizability).

#### M22 — Rebalancing, merges & placement

Goal: operators can grow, shrink, and balance the cluster.

- Replica movement: learner → promote (reuses per-group joint consensus);
  leadership/lease transfer.
- Balancer: store-level heuristics (range count/size) + locality labels for
  placement rules.
- Range merges for cold adjacent ranges.
- Node decommission runbook.
- **Deferred item lands here — dashboard v1:** read-only web cluster/range viewer
  (operational visibility becomes mandatory at this point).

Exit gate: node add/drain/decommission with continuous availability under chaos.

#### M23 — Cross-shard distributed transactions

Goal: atomic commit across ranges — the arc's summit.

- 2PC layered on Raft: transaction record + intents, coordinator crash recovery.
- HLC commit timestamps + uncertainty intervals; parallel-commit optimization as
  a stretch goal.
- **Deferred item lands here — TLA+ expansion (part 2):** TLA+ model of
  2PC + recovery.
- Jepsen: multi-range bank workload under the combined nemesis set
  (split + merge + rebalance + kill + partition).

Exit gate: bank invariant holds across the combined nemesis set; coordinator
crash recovery proven.

### Phase 3 — Hardening + proof (M24–M25)

#### M24 — Production hardening: security & observability

- Encryption-at-rest: pluggable `Disk` wrapper (AES-GCM), KEK/DEK hierarchy,
  key rotation — closes the `docs/security.md` §7 accepted risk.
- Per-prefix ACLs (pragmatic first step toward multi-tenant isolation).
- **Deferred items land here:** unified kernel+userspace latency attribution;
  io_uring completion tracing; stap/perf USDT attachment in privileged CI.
- Dashboard v2: trace timeline + eBPF histograms + range/cluster health.

Exit gate: security.md §7 accepted-risk table emptied or explicitly re-justified;
dashboard usable for day-2 operations.

#### M25 — Scale proof & ecosystem close-out

- Performance envelope v2: regression gates over transactional + sharded paths;
  **deferred item:** scheduled profiling runs on a Linux perf CI runner.
- Jepsen "grand matrix": all nemeses combined, full gate.
- **Deferred item:** linearizability checker minimal-counterexample reporting.
- **Deferred items:** TS/JS client + Zig client; Go client txn/retry parity;
  conformance vectors v3.
- Deployment guide v2 + SLO envelope v2; `docs/KayaDB_Explained.md` update.

Exit gate: north-star table updated; production-readiness claim re-evaluated
against the original gates → **v0.2.0** release.

## 5. Deferred-item placement map

| Deferred item (v0.1.47) | Home |
|---|---|
| TLA+ model expansion | M17 (commit protocol) + M23 (2PC + recovery) |
| Production-grade cross-node tracing | M20 (v1), M24 (full) |
| Web/GUI dashboard | M22 (v1 read-only), M24 (v2 trace+eBPF) |
| Live-cluster clock-skew / disk-latency nemesis | M20 (with HLC) |
| Kernel+userspace unified attribution | M24 |
| io_uring completion tracing | M24 |
| stap/perf privileged CI | M24 |
| Scheduled profiling (Linux perf runner) | M25 |
| Linearizability minimal counterexample | M25 |
| TS/JS + Zig clients, Go parity | M25 |
| Track H research (learned indexes, durability modes) | stays a track; feeds off SSTable v4 / new structures |

## 6. Non-goals (deliberate)

- **SQL layer** — post-M25 v2 candidate only.
- **Full multi-tenancy** — only per-prefix ACLs in M24.
- **Geo-replication / follower reads / CRDTs** — out of scope for this arc.
- Production SLA claims ahead of the M25 re-evaluation.

## 7. Risks

- **MVCC format migration (M16)** is the arc's highest-leverage risk: every later
  milestone depends on it. Mitigation: SSTable v4 behind a version gate, golden
  fixtures, dual-read path during migration, sim-first proof.
- **Multi-raft resource usage (M20):** many groups → tick/heartbeat storms.
  Mitigation: coalescing designed in from the start, benchmarked in M20.
- **2PC liveness (M23):** coordinator failure must not leave permanent intents.
  Mitigation: TLA+ model + explicit recovery protocol + Jepsen combined nemesis.
- **Scope creep:** each milestone has a single-sentence goal; anything not
  serving it moves to the deferred list.

## 8. Validation

Unchanged project gate for every landing PR:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

plus the milestone's own sim/property/Jepsen gates as they land.
