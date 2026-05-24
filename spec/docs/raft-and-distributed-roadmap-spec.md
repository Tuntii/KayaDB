# Raft and Distributed Roadmap Spec

**Status:** Draft v0.1  
**Scope:** Future Raft replication, deterministic network simulation, real cluster milestones  

---

## 1. Purpose

Raft is deliberately future scope.

KayaDB must first make single-node storage recovery reliable. Replication cannot fix a local engine that loses acknowledged writes.

This spec records future distributed boundaries so local storage design does not block Raft later.

---

## 2. Design rule

The future distributed write path should be:

```text
client command
  ↓
server protocol
  ↓
Raft propose
  ↓
Raft log commit
  ↓
apply command to storage engine
  ↓
respond
```

Storage engine must not know whether a command came from:

- local embedded caller,
- standalone server,
- Raft apply loop,
- simulation.

---

## 3. Future crates

```text
kaya-raft/       Raft state machine, log abstraction, membership
kaya-net/        transport abstraction and simulated network
kaya-server/     node process and request routing
kaya-sim/        deterministic cluster simulator
```

Dependency rule:

- `kaya-engine` must not depend on `kaya-raft`.
- `kaya-raft` may depend on stable command/log types from `kaya-core`.
- `kaya-server` wires Raft and engine together.

---

## 4. Raft MVP scope

Raft prototype in simulation should include:

- terms,
- leader election,
- RequestVote,
- AppendEntries,
- commit index,
- apply index,
- persistent Raft log abstraction,
- deterministic network drops/delays/duplicates,
- static membership.

Out of initial Raft prototype:

- dynamic membership,
- snapshots,
- leadership transfer,
- joint consensus,
- real TLS/auth,
- WAN tuning.

---

## 5. Raft persistence boundary

Open question:

> Should Raft log reuse KayaDB WAL framing or use a dedicated Raft log format?

Initial recommendation:

- do not force reuse prematurely,
- share generic checksummed frame helpers if useful,
- keep Raft log semantics separate from storage WAL semantics,
- document any shared format versioning carefully.

---

## 6. Deterministic network simulator

Future simulator components:

```text
SimNetwork
  ├─ packet drop
  ├─ packet duplicate
  ├─ packet reorder
  ├─ delay queue
  ├─ partitions
  └─ healing events
```

Network nemeses:

```text
PacketLoss
PacketDuplicate
PacketReorder
NetworkPartition
SlowNode
NodeCrashRestart
ClockSkewLater
```

All decisions must be seed-derived and trace-recorded.

---

## 7. Distributed invariants

| ID | Invariant |
|---|---|
| RFT-001 | At most one leader per term |
| RFT-002 | Committed entries are not lost |
| RFT-003 | Log matching property holds |
| RFT-004 | State machine applies entries in log order |
| RFT-005 | Minority partition cannot commit |
| RFT-006 | Restarted node does not forget durable Raft state |
| RFT-007 | Applied engine sequence follows committed Raft log order |

---

## 8. Jepsen roadmap

Jepsen is not MVP. It becomes relevant after real cluster mode.

Initial workloads:

- single-key linearizable register,
- multi-key independent registers,
- append-only list model if supported,
- process kill/restart,
- network partitions,
- clock skew only where relevant.

Jepsen results must not replace deterministic simulation; they complement it.

---

## 9. Milestone plan

| Milestone | Goal |
|---|---|
| M7 | Raft state machine in deterministic simulation |
| M8 | Real TCP cluster mode |
| M9 | Jepsen and eBPF-assisted observability |

Do not start M7 until storage WAL/recovery and engine semantics are stable enough to avoid debugging two dragons at once.

---

## 10. Acceptance criteria

Distributed roadmap enters implementation when:

- single-node strict durability tests pass,
- WAL/manifest recovery is idempotent,
- deterministic simulator can crash/restart a storage node,
- engine apply path can accept externally ordered commands,
- Raft invariants are listed in test plan.
