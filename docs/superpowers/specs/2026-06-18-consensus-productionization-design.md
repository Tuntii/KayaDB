# KayaDB Consensus & Productionization — Validation-First Design

**Status:** Approved (brainstorming)  
**Date:** 2026-06-18  
**Scope:** Phase 0 Raft persistence + Rust-native chaos proof (M13-3) as prerequisite arc before TLS/ops

---

## 1. Summary

KayaDB is evolving from a correctness prototype toward a deployable distributed database. This design prioritizes **validation before productization**: prove linearizability and durability under chaos (kill, partition, membership change, snapshot) before investing in TLS, ops runbooks, and release hardening.

### Decisions (locked in brainstorming)

| Topic | Decision |
|-------|----------|
| Priority | Validation-first (chaos + linearizability proof) |
| Harness | Rust-native `kaya-jepsen-test` only — no Clojure Jepsen |
| Scope | Full M13 chaos gate: T1–T5 + T6 (membership) + T7 (snapshot under nemesis) |
| Prerequisite | **Hard gate:** `DiskRaftStorage` + `RaftNode::recover` before any chaos CI |
| Linearizability | Dual: sequential on PR smoke; WGL (`check_concurrent`) on nightly full gate |
| Cluster lifecycle | Hybrid: Rust `ClusterController` for CI; existing scripts for manual demo |

### Recommended approach

**Hybrid (Approach 3):** CI tests use programmatic `ClusterController` (`ClusterNode` spawn); partition uses port-aware `iptables` from Rust on Linux; `scripts/` and `jepsen_demo` remain for operator/manual use.

---

## 2. Phasing

```
Phase 0 (BLOCKER)     Phase 1              Phase 2           Phase 3
────────────────      ───────              ───────           ───────
DiskRaftStorage       Harness extension    PR smoke gate     Full M13 gate
+ recover             + scenario library     (sequential)      (WGL, nightly)
+ SimDisk tests       + membership nemesis
                      + snapshot hooks
```

| Phase | M13 item | Exit |
|-------|----------|------|
| Phase 0 | M13-1 Durable Raft state | Restart preserves term/vote/log; SimDisk property tests pass |
| Phase 1 | — (harness) | `ClusterController` + T1–T7 scenario library compiles and runs locally |
| Phase 2 | M13-3 partial | PR `chaos-smoke` green |
| Phase 3 | M13-3 complete | Nightly `chaos-full` green; satisfies M13 exit gate #3 |

**After M13-3:** M13-2 (TLS/auth), M13-4 (ops), M13-5 (security audit), M13-6 (benchmark envelope).

---

## 3. Harness architecture (`kaya-jepsen-test`)

### 3.1 Crate layout

```
crates/kaya-jepsen-test/
├── src/
│   ├── lib.rs
│   ├── cluster_controller.rs   # NEW — Rust-native lifecycle
│   ├── nemesis.rs              # EXTEND — membership + composite
│   ├── workload.rs
│   ├── history.rs
│   ├── runner.rs               # REFACTOR — Scenario-driven
│   └── scenarios/
│       ├── mod.rs
│       ├── smoke_t1.rs
│       ├── t1..t5.rs
│       ├── t6_membership.rs
│       └── t7_snapshot.rs
├── tests/
│   ├── smoke.rs                # PR gate (sequential)
│   └── full_gate.rs            # Nightly (WGL) — #[ignore] by default
└── examples/
    └── jepsen_demo.rs          # Preserved (script-based demo)
```

**New dev-dependency:** `kaya-server` (for `ClusterNode` + `ClusterConfig` spawn). Extract shared helpers from `integration_tests.rs`.

### 3.2 `ClusterController`

```rust
pub struct ClusterController {
    base_dir: PathBuf,
    nodes: Vec<ManagedNode>,
}

impl ClusterController {
    pub async fn spawn_three_node(base_dir: PathBuf) -> Result<Self>;
    pub async fn spawn_join_node(&mut self, id: u64, seeds: &[...]) -> Result<()>;
    pub async fn wait_for_leader(&self, timeout: Duration) -> LeaderInfo;
    pub async fn kill_node(&mut self, id: u64);
    pub async fn restart_node(&mut self, id: u64);
    pub async fn partition_node(&self, id: u64);   // port-aware iptables (Linux)
    pub async fn heal_partition(&self, id: u64);
    pub async fn add_member(&self, leader: SocketAddr, spec: MemberSpec);
    pub async fn remove_member(&self, leader: SocketAddr, id: u64);
    pub fn client_endpoints(&self) -> Vec<SocketAddr>;
    pub async fn shutdown_all(&mut self);
}
```

| Operation | Mechanism | Rationale |
|-----------|-----------|-----------|
| Spawn / kill / restart | `tokio::spawn` + `abort()` | CI deterministic; no script cwd issues |
| Partition | `iptables` with dynamic ports | Real kernel isolation; not fixed-port scripts |
| Membership | `roundtrip(leader, 7/8, payload)` | Existing wire protocol |
| Cleanup | `remove_dir_all(base_dir)` | Isolated temp dir per test |

**Port strategy:** CI tests use **dynamic ports** (`get_free_port`). Partition nemesis passes node-specific ports to `iptables` — does not rely on `partition-node.sh` fixed ports (7379–7483). Scripts remain for manual demo with `start-cluster.sh`.

### 3.3 Nemesis API

```rust
pub enum NemesisType {
    KillNode, KillNodeById(usize),
    Partition, PartitionById(usize),
    AddMember(MemberSpec),
    RemoveMember(u64),
    Composite(Vec<NemesisType>),
    None,
}
```

Snapshot forcing is a **workload hook**, not a nemesis:

```rust
pub enum WorkloadHook {
    BurstWrites { count: u128, key_prefix: &'static str },
}
```

Pattern from `test_install_snapshot_over_tcp`: 128+ PUTs → `applied_index >= 64` → follower kill → InstallSnapshot catch-up.

### 3.4 `Scenario` and verification

```rust
pub enum VerifyMode {
    Sequential,   // PR smoke
    Concurrent,   // WGL — nightly full gate
}

pub struct Scenario {
    pub id: &'static str,
    pub workload: WorkloadConfig,
    pub hooks: Vec<WorkloadHook>,
    pub nemesis: Option<NemesisConfig>,
    pub duration: Duration,
    pub verify: VerifyMode,
    pub topology: Topology,  // ThreeNode | FourNodeJoin
}
```

`TestRunner::run_scenario(scenario, cluster)`:

1. Wait for leader
2. Start workload + nemesis concurrently
3. Fire timed hooks (T7 burst writes)
4. Stop after duration
5. `check_sequential()` or `check_concurrent()` per `verify`
6. On failure: export JSONL trace

### 3.5 Scenario catalog

| ID | Source | Workload | Nemesis | Topology | Verify |
|----|--------|----------|---------|----------|--------|
| **smoke** | T1 short | Register, 2 clients, 30s | Kill random | 3-node | Sequential |
| **T1** | jepsen-design | Register, 5 clients, 120s | Kill+restart | 3-node | WGL |
| **T2** | jepsen-design | Set, 5 clients | Majority partition | 3-node | WGL |
| **T3** | jepsen-design | Register, 10 clients | Kill leader | 3-node | WGL |
| **T4** | jepsen-design | Set, 5 clients | Rolling restart | 3-node | WGL |
| **T5** | jepsen-design | Register+Set, 20 clients | Composite kill+partition | 3-node | WGL |
| **T6** | M13 extension | Register, 5 clients | AddMember + kill during joint | 4-node join | WGL |
| **T7** | M13 extension | BurstWrites hook | Kill follower mid-compaction | 3-node | WGL + durability assert |

**T7 extra assertion:** After restart, `GET snap-127` succeeds on all nodes (snapshot catch-up).

### 3.6 Failure artifacts

| Condition | Behavior |
|-----------|----------|
| Linearizability violation | Fail; upload JSONL trace artifact |
| Leader election timeout | Fail (not flaky) |
| iptables denied | Fail nightly; smoke has no partition |
| Nemesis error | Fail + nemesis log artifact |

Trace path: `{base_dir}/traces/{scenario}-{run_id}.jsonl`. CI retention: 14 days.

---

## 4. CI integration

### 4.1 Workflows

```
.github/workflows/
├── ci.yml              # existing + chaos-smoke job (Phase 2+)
└── chaos-nightly.yml   # NEW — full M13 gate (Phase 3)
```

Chaos jobs disabled until Phase 0 completes (`vars.CHAOS_CI_ENABLED != 'true'`).

### 4.2 Job: `chaos-smoke` (every PR)

```yaml
chaos-smoke:
  runs-on: ubuntu-latest
  needs: rust
  if: vars.CHAOS_CI_ENABLED == 'true'
  timeout-minutes: 10
  steps:
    - cargo build -p kaya-server --bin kayadb-server
    - cargo test -p kaya-jepsen-test --test smoke -- --nocapture
```

| Parameter | Value |
|-----------|-------|
| Scenario | `smoke` |
| Duration | ~30s test + ~15s boot |
| Nemesis | Kill only (no partition) |
| Verify | Sequential |
| Budget | ≤ 3 minutes |

`cargo test --workspace` does **not** run smoke automatically — separate job keeps unit tests fast.

### 4.3 Job: `chaos-full` (nightly + release)

```yaml
# chaos-nightly.yml
on:
  schedule: [{ cron: '0 3 * * *' }]
  workflow_dispatch:
  push:
    tags: ['v*']

chaos-full:
  runs-on: ubuntu-latest
  timeout-minutes: 45
  steps:
    - cargo build -p kaya-server --release --bin kayadb-server
    - cargo test -p kaya-jepsen-test --test full_gate -- --ignored --nocapture --test-threads=1
```

Runs T1–T7 sequentially. Budget: ~25–35 minutes (T5 = 300s).

### 4.4 Activation timeline

| Stage | Trigger | What runs |
|-------|---------|-----------|
| Phase 0 | — | Chaos jobs off |
| Phase 2 | `CHAOS_CI_ENABLED=true` | PR smoke |
| Phase 3 | `chaos-nightly.yml` merged | Nightly + tag release gate |

### 4.5 M13 exit gate #3 mapping

ROADMAP: *"external Jepsen (or equivalent) run against a multi-node cluster with membership changes and snapshots under nemesis"*

| Requirement | This design |
|-------------|-------------|
| Jepsen or equivalent | `kaya-jepsen-test` = equivalent ✅ |
| Multi-node cluster | 3-node + 4-node (T6) ✅ |
| Membership under nemesis | T6 ✅ |
| Snapshots under nemesis | T7 ✅ |
| CI proof | Nightly green + release tag gate ✅ |

**M13-3 exit:** 7 consecutive nightly `chaos-full` greens + one documented `workflow_dispatch` run.

---

## 5. Phase 0 — Raft persistence (`DiskRaftStorage`)

### 5.1 Principle

Raft log stays **separate from engine WAL** (`raft-and-distributed-roadmap-spec`). Shared: `kaya_core::crc32c` + manifest-style framed headers.

### 5.2 On-disk layout

```
data_dir/
├── raft-hard-state          # atomic, small, frequent fsync
├── raft-log                 # full log rewrite (prototype)
├── raft-snapshot-meta       # boundary metadata (payload in engine)
├── raft-apply-index.jsonl   # existing — engine correlation
└── cluster-roster.json      # existing — membership
```

### 5.3 `raft-hard-state` (64 bytes fixed)

```
offset  field                    size
0       magic (0x484B5352)       u32  # "HSKR" LE — raft-hard-state
4       version                  u32  (=1)
8       current_term             u64
16      voted_for                u64  (0 = None)
24      last_included_index      u64
32      last_included_term       u64
40      reserved                 20 bytes
60      crc32c (bytes 0..60)     u32
```

Write path: `tmp` → `fsync` → `rename` → `fsync(dir)`.

### 5.4 `raft-log` (framed full rewrite)

On each persist event, rewrite entire log file:

```
[frame_header 32B][entry_payload]
  magic, version, logical_index, term, cmd_len, crc32c(header+payload)
  command bytes (opaque)
```

Snapshot boundary in hard-state; compacted entries omitted from file.

### 5.5 `RaftStorage` trait

```rust
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}

pub trait RaftStorage: Send {
    fn load(&self) -> Result<PersistedRaftState, RaftStorageError>;
    fn save_hard_state(&mut self, hs: &HardState) -> Result<()>;
    fn save_log(&mut self, log: &MemLog) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}
```

| Implementation | Used by |
|----------------|---------|
| `MemRaftStorage` | `kaya-sim` ClusterSim |
| `DiskRaftStorage` | `kaya-server` |
| `SimDiskRaftStorage` | crash property tests |

**Feature flag** on `kaya-raft`:

```toml
[features]
disk-storage = ["kaya-io"]
```

`kaya-server` enables `disk-storage`; `kaya-sim` does not.

### 5.6 `RaftNode::recover`

```rust
impl RaftNode {
    pub fn recover(config: RaftConfig, state: PersistedRaftState) -> Self;
}
```

- Load term, voted_for, log from disk
- Reset volatile state: `role = Follower`, election timers, leader maps
- `last_applied` = max index from `RaftApplyIndex::load_all()`
- `commit_index` = `last_applied` initially (advances via replication)

**Persist triggers** (server loop):

| Event | Persist |
|-------|---------|
| Term increase | hard-state |
| Vote grant | hard-state |
| propose / follower append | log + sync |
| truncate_from (conflict) | log |
| install_snapshot | hard-state + log + snapshot-meta |
| End of raft loop iteration | hard-state |

**Not persisted:** role, leader_id, election_ticks, next_index, match_index, pending_reads.

### 5.7 Server startup

```
1. Engine::open(data_dir)
2. RaftApplyIndex::open(data_dir)
3. DiskRaftStorage::open(data_dir)
4. RaftNode::recover() or RaftNode::new()
5. Adjust last_applied/commit_index from apply_index
6. Raft event loop + persister.maybe_flush() after mutations
```

### 5.8 SimDisk property tests

| Test | Scenario |
|------|----------|
| `raft_persist_roundtrip` | save → load → equal state |
| `raft_crash_mid_save` | FsyncFailed during save → recover valid prefix |
| `raft_crash_before_rename` | stale tmp → recover previous hard-state |
| `raft_log_truncation_survives_restart` | conflict truncate → restart |
| `raft_snapshot_boundary_survives_restart` | install_snapshot → restart |

### 5.9 Phase 0 exit criteria

| # | Criterion | Verification |
|---|-----------|--------------|
| 1 | `current_term` survives restart | unit + integration |
| 2 | Log entries survive restart | unit |
| 3 | `voted_for` survives restart | unit |
| 4 | Snapshot boundary survives restart | integration |
| 5 | SimDisk crash properties | ≥ 5 seeds in CI |
| 6 | 3-node kill → restart → rejoin + PUT/GET | integration extension |
| 7 | `cargo test --workspace` green | CI |
| 8 | Enable `CHAOS_CI_ENABLED` | manual repo variable |

**Deferred (not Phase 0):** incremental log append, `kayactl inspect raft-log`, cross-node snapshot payload format changes.

---

## 6. Documentation updates (implementation phase)

- `docs/jepsen-design.md` — Phase 3 CI, T6/T7, dynamic-port partition note
- `ROADMAP.md` — M13-1/M13-3 status when phases complete
- `docs/architecture.md` — Raft persistence boundary (brief)

---

## 7. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Full log rewrite latency | Acceptable for prototype; benchmark later (M13-6) |
| iptables flaky on GHA | `--test-threads=1`; smoke avoids partition |
| WGL slow on large histories | Cap T5 duration; nightly only |
| Restart without persistence breaks chaos | Hard Phase 0 gate blocks chaos CI |
| Dual lifecycle (Rust + scripts) | Scripts frozen for demo; CI uses Rust only |

---

## 8. Success criteria (overall)

1. Phase 0 exit criteria met
2. PR `chaos-smoke` green for 2 weeks
3. Nightly `chaos-full` green for 7 consecutive days
4. M13-3 documented as satisfied in ROADMAP
5. No known linearizability gaps in chaos scenarios

---

## References

- `ROADMAP.md` — M13 productization
- `docs/jepsen-design.md` — workloads, nemeses, T1–T5
- `crates/kaya-jepsen-test/` — existing harness
- `crates/kaya-server/src/integration_tests.rs` — cluster patterns
- `spec/docs/raft-and-distributed-roadmap-spec.md` — Raft/WAL separation