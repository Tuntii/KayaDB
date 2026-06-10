# KayaDB Jepsen-Style Test Design

**Status:** Draft  
**Last updated:** 2026-06-10

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

## Implementation Plan

### Phase 1: Process-Control Scripts (current)

Create shell/PowerShell scripts to manage cluster lifecycle:
- `scripts/start-cluster.sh` - Start 3-node cluster
- `scripts/stop-cluster.sh` - Stop all nodes
- `scripts/kill-node.sh` - Kill a specific node (nemesis)
- `scripts/restart-node.sh` - Restart a specific node
- `scripts/partition-network.sh` - Create network partition (requires `iptables` or `tc`)

### Phase 2: Test Harness (next)

Build a Rust-based test harness in `crates/kaya-jepsen-test/`:
- **Workload generators** - Concurrent clients running W1-W4
- **Nemesis injectors** - Kill, partition, delay
- **History recorder** - Record all operations with timestamps
- **Linearizability checker** - Use existing `kaya_sim::LinearizabilityChecker`
- **Test runner** - Orchestrate workloads, nemeses, and verification

### Phase 3: CI Integration (future)

- Run T1-T4 in CI on every PR
- Run T5 (stress test) nightly
- Publish results to dashboard

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
- `ROADMAP.md` - M12 milestone
