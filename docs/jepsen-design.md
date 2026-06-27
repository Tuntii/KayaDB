# KayaDB Jepsen-Style Test Design

**Status:** Draft  
**Last updated:** 2026-06-18

This document defines the workloads, nemeses, and test scenarios for Jepsen-style correctness testing of KayaDB clusters.

---

## Goals

Validate that KayaDB maintains **linearizability** and **availability** under:
- Node failures (crash, kill, restart)
- Network partitions (isolation, delay, packet loss)
- Clock skew (future: requires NTP manipulation)
- Concurrent client workloads

Success criteria:
- **No linearizability violations** detected by `LinearizabilityChecker`
- **No data loss** for acknowledged writes
- **Eventual availability** after partition heals or node restarts

---

## Workloads

### W1: Register (single-key read/write)

**Description:** Multiple clients perform concurrent PUT/GET on a single shared key.

**Operations:**
- `PUT key=register value=<random_8_bytes>`
- `GET key=register`

**Invariant:** Every GET returns the value from the most recent PUT (linearizability).

**Use case:** Validates basic read-after-write consistency under contention.

---

### W2: Counter (increment/read)

**Description:** Multiple clients increment a counter and read it back.

**Operations:**
- `PUT key=counter value=<current_value + 1>` (read-modify-write)
- `GET key=counter`

**Invariant:** Final counter value equals total number of successful PUTs.

**Use case:** Detects lost updates under concurrent writes.

**Note:** KayaDB does not support atomic increment, so this workload uses optimistic read-modify-write. Lost updates are expected without idempotency keys or transactions.

---

### W3: Set (append/read)

**Description:** Clients append unique values to a set and read the full set.

**Operations:**
- `PUT key=set:<unique_id> value=<unique_id>`
- `SCAN prefix=set:`

**Invariant:** SCAN returns all successfully written keys (no lost writes).

**Use case:** Validates write durability and scan consistency.

---

### W4: Map (multi-key transactional)

**Description:** Clients write to multiple keys and verify cross-key consistency.

**Operations:**
- `PUT key=map:a value=<v>`
- `PUT key=map:b value=<v>`
- `GET key=map:a`
- `GET key=map:b`

**Invariant:** If both PUTs succeed, both GETs return the same value.

**Use case:** Detects partial failures in multi-key operations (future: transactions).

**Note:** KayaDB does not support multi-key transactions yet. This workload documents expected behavior.

---

## Nemeses

### N1: Kill Node

**Description:** Randomly kill a node process (SIGKILL or task abort).

**Parameters:**
- `target`: node ID (1, 2, or 3) or "random"
- `duration`: how long the node stays down (seconds)

**Expected behavior:**
- Cluster elects new leader within 10 seconds
- Clients redirect to new leader (via `STATUS_NOT_LEADER` hint)
- No data loss for acknowledged writes
- Node rejoins and catches up after restart

---

### N2: Partition Network

**Description:** Isolate one or more nodes from the cluster.

**Parameters:**
- `targets`: list of node IDs to isolate
- `duration`: partition duration (seconds)
- `type`: "full" (no traffic) or "partial" (packet loss/delay)

**Expected behavior:**
- Majority partition continues serving requests
- Minority partition rejects writes (`STATUS_NOT_LEADER` or timeout)
- After partition heals, minority nodes catch up
- No split-brain (at most one leader per term)

---

### N3: Clock Skew (future)

**Description:** Inject clock drift on one or more nodes.

**Parameters:**
- `target`: node ID
- `offset`: clock offset in milliseconds (positive or negative)

**Expected behavior:**
- Raft uses logical ticks, not wall-clock time, so clock skew should not affect correctness
- Metrics and logs may show inconsistent timestamps

**Note:** Requires NTP manipulation or `clock_gettime` interception. Not implemented yet.

---

### N4: Disk Full

**Description:** Fill the disk to trigger write failures.

**Parameters:**
- `target`: node ID
- `threshold`: disk usage percentage to trigger (e.g., 95%)

**Expected behavior:**
- Node rejects writes with `STATUS_ERROR` (disk full)
- Cluster continues with remaining nodes
- After disk space is freed, node recovers

**Note:** Requires filesystem-level control (e.g., `fallocate` or disk quota).

---

### N5: Slow Network

**Description:** Inject latency or packet loss on network links.

**Parameters:**
- `targets`: list of node IDs
- `latency_ms`: added latency (e.g., 100ms)
- `loss_pct`: packet loss percentage (e.g., 10%)

**Expected behavior:**
- Cluster remains available but slower
- Leader election may take longer
- Clients experience higher latency

**Note:** Requires `tc` (traffic control) or similar network emulation tool.

---

## Test Scenarios

### T1: Single Node Kill + Recovery

**Workload:** W1 (register)  
**Nemesis:** N1 (kill random node, wait 30s, restart)  
**Duration:** 120 seconds  
**Clients:** 5 concurrent

**Steps:**
1. Start 3-node cluster
2. Start 5 concurrent clients running W1
3. After 10s, kill a random node
4. After 30s, restart the killed node
5. After 120s, stop clients
6. Verify linearizability with `LinearizabilityChecker`

**Pass criteria:**
- No linearizability violations
- All acknowledged writes are durable
- Node rejoins and catches up

---

### T2: Majority Partition

**Workload:** W3 (set)  
**Nemesis:** N2 (partition 1 node from the other 2)  
**Duration:** 120 seconds  
**Clients:** 5 concurrent

**Steps:**
1. Start 3-node cluster
2. Start 5 concurrent clients running W3
3. After 10s, partition node 3 from nodes 1 and 2
4. After 60s, heal partition
5. After 120s, stop clients
6. Verify all writes are visible on all nodes

**Pass criteria:**
- Majority partition (nodes 1, 2) continues serving writes
- Minority partition (node 3) rejects writes
- After heal, node 3 catches up
- No lost writes

---

### T3: Leader Kill + Re-election

**Workload:** W1 (register)  
**Nemesis:** N1 (kill current leader, wait 20s, restart)  
**Duration:** 90 seconds  
**Clients:** 10 concurrent

**Steps:**
1. Start 3-node cluster
2. Start 10 concurrent clients running W1
3. Identify current leader
4. After 10s, kill the leader
5. After 20s, restart the killed node
6. After 90s, stop clients
7. Verify linearizability

**Pass criteria:**
- New leader elected within 10s
- No linearizability violations
- Clients successfully redirect to new leader

---

### T4: Rolling Restart

**Workload:** W3 (set)  
**Nemesis:** N1 (restart each node sequentially, 10s apart)  
**Duration:** 120 seconds  
**Clients:** 5 concurrent

**Steps:**
1. Start 3-node cluster
2. Start 5 concurrent clients running W3
3. After 10s, restart node 1 (kill, wait 5s, start)
4. After 20s, restart node 2
5. After 30s, restart node 3
6. After 120s, stop clients
7. Verify all writes are durable

**Pass criteria:**
- Cluster remains available throughout (at least 2 nodes up)
- No lost writes
- All nodes catch up after restart

---

### T5: Stress Test (high concurrency + failures)

**Workload:** W1 (register) + W3 (set)  
**Nemesis:** N1 (random kills) + N2 (random partitions)  
**Duration:** 300 seconds  
**Clients:** 20 concurrent

**Steps:**
1. Start 3-node cluster
2. Start 20 concurrent clients (10 running W1, 10 running W3)
3. Inject random nemeses every 30s:
   - 50% chance: kill random node (restart after 20s)
   - 50% chance: partition random node (heal after 20s)
4. After 300s, stop clients and nemeses
5. Verify linearizability and durability

**Pass criteria:**
- No linearizability violations
- No data loss
- Cluster remains available (at least 2 nodes up at all times)

---

### T6: Membership Change Under Nemesis (M13 extension)

**Workload:** W1 (register)  
**Nemesis:** `ADD_MEMBER` (node 4) + N1 (kill random node during joint consensus)  
**Duration:** 120 seconds  
**Clients:** 5 concurrent  
**Topology:** 4-node join (3-node cluster + pre-spawned join node 4)

**Steps:**
1. Start 3-node cluster via `ClusterController`
2. Spawn join-cluster node 4 (not yet in roster)
3. Start 5 concurrent clients running W1 against nodes 1–3
4. Nemesis alternates every 20s: `ADD_MEMBER` for node 4, then kill a random node (restart after 10s)
5. After 120s, stop clients and nemeses
6. Verify concurrent linearizability with WGL (`check_concurrent`)

**Pass criteria:**
- No concurrent linearizability violations
- `ADD_MEMBER` completes under joint consensus while kills are in flight
- Cluster remains writable on the majority partition

---

### T7: Snapshot Catch-up Under Nemesis (M13 extension)

**Workload:** W1 (register) + burst writes (`snap-0` … `snap-127`)  
**Nemesis:** Kill follower before compaction burst, restart after 15s  
**Duration:** 120 seconds  
**Clients:** 5 concurrent  
**Topology:** 3-node

**Steps:**
1. Start 3-node cluster via `ClusterController`
2. Identify a follower and kill it before the burst-write hook
3. Leader writes 128 keys (`snap-{i}`) to force log growth / snapshot install
4. Restart the killed follower; continue Register workload
5. After 120s, stop clients and nemeses
6. Verify WGL linearizability
7. **Durability assert:** `GET snap-127` succeeds on every node endpoint (snapshot catch-up)

**Pass criteria:**
- No concurrent linearizability violations
- Killed follower catches up via InstallSnapshot after restart
- `snap-127` readable on all nodes

---

## Implementation Plan

### Phase 1: Process-Control Scripts (current)

Create shell/PowerShell scripts to manage cluster lifecycle:
- `scripts/start-cluster.sh` - Start 3-node cluster
- `scripts/stop-cluster.sh` - Stop all nodes
- `scripts/kill-node.sh` - Kill a specific node (nemesis)
- `scripts/restart-node.sh` - Restart a specific node
- `scripts/partition-network.sh` - Create network partition (requires `iptables` or `tc`)

### Phase 2: Test Harness (done)

Build a Rust-based test harness in `crates/kaya-jepsen-test/`:
- **Workload generators** - Concurrent clients running W1-W4 (Register/Counter/Set/Map)
- **Nemesis injectors** - Kill (via scripts or `ClusterController`), **Partition** (cross-platform: `partition-node.ps1` + `heal-partition.ps1` using Windows Firewall `New-NetFirewallRule`, Linux `iptables` with comments; falls back gracefully). Restart also completed for Windows symmetry (`restart-node.ps1`).
- **History recorder** - Thread-safe `History` + `Operation` recording with `kaya_sim` Op/OpResult
- **Linearizability checker** - Sequential (`check_sequential`) for PR smoke; WGL concurrent (`check_concurrent`) for nightly full gate
- **Test runner** - `TestRunner` + `TestConfig` + `TestResult` (duration-based orchestration, nemesis + workload, post-run verification + trace export on failure)
- **Scenario registry** - Declarative `smoke` + T1–T7 scenarios in `scenario.rs`; `run_scenario()` drives workloads, hooks, and nemeses against an in-process cluster
- **`ClusterController`** - Programmatic cluster lifecycle for CI: spawns in-process `ClusterNode` instances on **dynamic ports** (`127.0.0.1:0`), kill/restart, `ADD_MEMBER` / `REMOVE_MEMBER`, and port-aware partition (see below). Existing `scripts/` remain for manual operator demos.

A ready-to-run `examples/jepsen_demo.rs` was added for end-to-end "tam deneme" (cargo run -p kaya-jepsen-test --example jepsen_demo). It exercises Partition nemesis + real clients against a live cluster started via the scripts.

#### Dynamic-port partition via `ClusterController`

Script-based partition (`partition-node.sh`) assumes fixed ports from `start-cluster.sh`. CI clusters bind ephemeral ports, so partition nemeses in scenario tests use `ClusterController::partition_node` / `heal_partition` instead:

- On **Linux**, inserts `iptables` OUTPUT DROP rules targeting each node's **actual** `client_addr` and `raft_addr` ports (comment tag `kaya-jepsen-n{id}`)
- Requires `sudo` on the runner host; nightly T2/T5 partition scenarios fail hard if rules cannot be installed
- PR **chaos-smoke** uses kill-only nemesis (no partition) to stay fast and avoid iptables dependency

### Phase 3: CI Integration (done)

Chaos gates live in `.github/workflows/jepsen.yml`. `cargo test --workspace` in `ci.yml` still excludes `kaya-jepsen-test` integration tests so unit tests stay fast; Jepsen smoke runs in the dedicated workflow.

| Job | Workflow | Trigger | What runs | Budget |
|-----|----------|---------|-----------|--------|
| **smoke** | `jepsen.yml` | Every PR + push to `main` | `cargo test -p kaya-jepsen-test` lib + `cluster_controller_smoke` + `--test smoke` — 30s Register + kill-node, **sequential** verify | ≤ 5 min |
| **full-suite** | `jepsen.yml` | Nightly cron (`0 3 * * *`), `workflow_dispatch` (suite=full), release tags `v*` | `cargo test -p kaya-jepsen-test --test full_gate` + `partition_nemesis` — T1–T7, **WGL concurrent** verify | ≤ 45 min |

**Manual dispatch:** use **Actions → Jepsen → Run workflow** with `suite=smoke` or `suite=full`.

**External Clojure Jepsen:** not vendored in this repo. The Rust-native `kaya-jepsen-test` harness is the sole CI correctness gate. Operators who want upstream Jepsen can run it out-of-band against a provisioned cluster.

**Failure artifacts:** On `chaos-full` failure, JSONL traces under `{tmpdir}/traces/{scenario}-*.jsonl` are uploaded as the `chaos-traces` artifact (14-day retention).

**M13 exit gate #3 mapping:** Nightly green on T1–T7 (including T6 membership + T7 snapshot under nemesis) satisfies the "Jepsen or equivalent" chaos-proof requirement in `ROADMAP.md`.

---

## Related Work

- [Jepsen](https://jepsen.io/) - Distributed systems testing framework (Clojure)
- [CockroachDB Jepsen tests](https://github.com/cockroachdb/jepsen)
- [TiDB Jepsen tests](https://github.com/pingcap/jepsen)

KayaDB's approach: build a lightweight Rust-native test harness instead of using Clojure/Jepsen directly. This keeps the toolchain consistent and avoids JVM dependencies.

---

## References

- `crates/kaya-sim/src/linear.rs` - Sequential linearizability checker
- `crates/kaya-server/src/integration_tests.rs` - Existing cluster tests
- `crates/kaya-jepsen-test/examples/jepsen_demo.rs` - Runnable full demo (Partition + runner)
- `crates/kaya-jepsen-test/src/cluster_controller.rs` - In-process cluster spawn, dynamic ports, port-aware partition
- `crates/kaya-jepsen-test/tests/smoke.rs` - PR `chaos-smoke` gate
- `crates/kaya-jepsen-test/tests/full_gate.rs` - Nightly T1–T7 WGL gate (`#[ignore]` locally)
- `.github/workflows/jepsen.yml` - PR smoke + nightly full-suite
- `.github/workflows/ci.yml` - workspace unit tests (excludes `kaya-jepsen-test` integration)
- `ROADMAP.md` - M12 harness complete; M13 chaos gates (T6/T7) + durable Raft on `feat/validation-first-consensus`
