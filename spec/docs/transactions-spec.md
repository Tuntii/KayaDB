# Transactions Spec

**Status:** Draft v0.1  
**Scope:** Single-group ACID transactions with Snapshot Isolation (M17)  
**Primary crate:** `kaya-engine` (intents + local SI), `kaya-server` / `kaya-raft` (distributed commit path)  
**Depends on:** `mvcc-spec.md`, `engine-api-spec.md`, `wal-spec.md`, `server-and-protocol-spec.md`  

---

## 1. Purpose

M17 adds interactive multi-key transactions on top of M16 MVCC. A transaction
observes a consistent snapshot, buffers provisional writes as **intents**, and
either commits all intents at a single logical commit point or rolls them back.

This spec defines:

- isolation model (Snapshot Isolation with write–write conflict detection),
- transaction lifecycle (`BEGIN` → ops → `COMMIT` | `ROLLBACK`),
- snapshot assignment and read visibility,
- write-intent representation and conflict rules,
- commit / rollback / crash recovery contracts,
- reserved protocol opcodes and `TXN_CONFLICT` status,
- single-node engine path (unit tests) vs Raft atomic commit path (distributed).

Serializable isolation is a **stretch / non-goal** for M17.

---

## 2. Terminology

| Term | Meaning |
|---|---|
| txn_id | Engine-local transaction identifier (`u64`) |
| read_ts / snapshot_ts | Timestamp bound assigned at `BEGIN` (or first read); reads use `ReadTimestamp::At(read_ts)` |
| write intent | Provisional write keyed by user_key, held by a live txn and not visible to other txns |
| commit_ts | Timestamp of a successful commit; for M17 equals the sequence assigned to durable intent materialization |
| write–write conflict | Another txn holds an intent on the key, or a committed version exists with `commit_ts > read_ts` |
| RYW | Read-your-writes: a txn sees its own intents before snapshot storage |
| commit record | Atomic marker that converts intents → committed versions (Raft log entry on distributed path) |

---

## 3. Isolation model

### 3.1 Snapshot Isolation (SI) — default for M17

- Each transaction `T` is assigned a `read_ts` at `BEGIN` (see §5).
- **Reads** observe only versions with `commit_ts ≤ read_ts`, plus `T`'s own intents (RYW).
- **Writes** are provisional intents until commit.
- **Commit** succeeds only if no write–write conflict is detected on any key `T` wrote.
- Concurrent transactions may produce write skew (classic SI anomaly). That is accepted for M17.

### 3.2 Non-goals / stretch

| Item | M17 status |
|---|---|
| Serializable / SSI (anti-write-skew) | Stretch / non-goal |
| Predicate locks / range locks | Non-goal |
| Cross-shard / multi-group 2PC | M23 (see §17) |
| HLC commit timestamps | Deferred to M20 (seq remains `commit_ts` for M17) |

---

## 4. Lifecycle

```text
BEGIN
  → OP (read / write / delete)*
  → COMMIT | ROLLBACK
```

| Phase | Behavior |
|---|---|
| `BEGIN` | Allocate `txn_id`; assign `read_ts` (see §5). |
| `OP` read | RYW buffer (own intents) then `get_at(read_ts)` / scan at `read_ts`. |
| `OP` write | Conflict-check key; store/replace intent for this `txn_id`. |
| `COMMIT` | Re-check conflicts as needed; assign `commit_ts`; materialize intents as committed versions; clear intents. |
| `ROLLBACK` | Discard all intents for `txn_id`; free txn state. |

After `COMMIT` or `ROLLBACK`, the `txn_id` is invalid for further ops.

Idempotent client retries of `COMMIT`/`ROLLBACK` after success may return success or `invalid argument` (unknown txn); they must not double-apply intents.

---

## 5. Snapshot assignment

**Decision (M17 default):** `read_ts` is assigned at `BEGIN` as the engine's current
`last_sequence` (highest committed sequence observed by the engine).

- Reads use `ReadTimestamp::At(read_ts)` against memtable + SSTables (MVCC rules from `mvcc-spec.md`).
- Versions committed by other transactions after `read_ts` are invisible to this txn's reads.
- Alternative (allowed later): defer `read_ts` until first read; once set, it is fixed for the txn lifetime.

Implications:

- Non-transactional `put`/`delete` that advance `last_sequence` after `BEGIN` are invisible to the txn's snapshot reads and cause write–write conflict if the txn later writes the same key (when the committed version has `seq > read_ts`).
- Open snapshots constrain GC watermark advancement (`mvcc-spec.md` §7); M17 should eventually publish the min open `read_ts` as the watermark horizon (phase-1 engine may keep watermark manual/0).

---

## 6. Write intents

### 6.1 Logical record

```text
Intent {
  txn_id: u64,
  value: Option<Bytes>,   // Some(v) = put intent; None = delete intent
}
```

### 6.2 Storage (phase 1 — single-node engine)

Minimal durable approach for unit tests and embedded use:

- `Engine` holds an in-memory map `user_key → Intent`.
- Reverse index `txn_id → set of keys` for cleanup on commit/rollback.
- Intents are **not** visible to other transactions' reads or to default LWW `get`.
- On `Engine::open` / recovery, the in-memory intent map is **empty**. Lost intents after process crash mean those transactions did not commit (no half-committed user-visible state). The distributed Raft commit path (later M17 tasks) will re-establish durable intent/commit-record recovery.

Future durable options (not required for phase 1):

- system-prefix keys (e.g. `\x00intent/` + user_key), or
- WAL/Raft special records for intent write and clear.

### 6.3 Visibility

| Observer | Sees intent? |
|---|---|
| Owning txn (`txn_get`) | Yes (RYW) |
| Other txn | No |
| Non-txn `get` / `scan` | No |
| After successful commit | Materialized as normal Put/Delete versions at `commit_ts` |

---

## 7. Conflict detection (write–write)

On `txn_put` / `txn_delete` (and again at commit if required by implementation):

1. **Intent conflict:** another live txn holds an intent on the same user_key → `TXN_CONFLICT`.
2. **Committed SI conflict:** a committed version exists for the key with `commit_ts > read_ts` → `TXN_CONFLICT`.

Same-txn overwrite of an existing own intent is allowed (last intent wins).

First writer to place an intent wins; the loser fails with `TXN_CONFLICT` (fail on write is acceptable; fail on commit is also acceptable if detection is deferred — M17 engine phase 1 detects on write).

---

## 8. Commit

### 8.1 Steps (logical)

```text
1. Validate txn_id is open
2. For each intent key: ensure still no foreign intent / SI conflict
3. Assign commit_ts (see §8.2)
4. Convert each intent into a durable Put or Delete at commit sequence(s)
5. Clear intents and txn metadata
6. Return commit_ts to client
```

### 8.2 commit_ts assignment

- **Distributed path:** propose a single Raft commit record; on apply, materialize all intents at the log-assigned sequence / `commit_ts` atomically w.r.t. other Raft applies.
- **Single-node engine path (unit tests):** `txn_take_commit` → `apply_mutations` (each put/delete gets its own WAL sequence). Record `commit_ts` as the **last** sequence. SI remains correct for single-key conflicts; multi-key WAL atomicity mid-batch is best-effort on this path only (see §10).
- **Distributed server path (production):** one Raft log entry `RaftCommand::TxnCommit` (type byte 4) carries all mutations. Apply is all-or-nothing w.r.t. other Raft entries; recovery cannot observe a partial multi-key commit.

Empty commit (no intents): succeeds; `commit_ts` may equal current `last_sequence` without writing.

### 8.3 Durability

Committed materialization uses the engine durability mode (default **strict**: WAL fsync before ACK). After a successful commit returns, non-txn `get` observes the new versions (subject to normal recovery rules).

---

## 9. Rollback

```text
1. Validate txn_id is open (or treat unknown as no-op success — implementation choice; prefer error on unknown)
2. Remove all intents for txn_id
3. Drop txn metadata (read_ts, key set)
```

No durable user-visible change. Other txns waiting on conflicts may proceed after intents clear.

---

## 10. Crash recovery

| Path | Contract |
|---|---|
| Phase-1 in-memory intents | Lost on crash; no committed partial user state from uncommitted txns |
| Single-node sequential materialize | Mid-commit crash may leave a **prefix** of intents durable (each put/delete is individually WAL-protected). Unit-test path only. |
| Distributed `TxnCommit` (type 4) | Single Raft entry; all-or-nothing apply on recovery. Production path for multi-key SI commits. |
| Raft single-group commit record | Intents (or intent effects) plus commit record are applied so recovery does **not** leave a half-committed multi-key transaction once the commit record is durable and applied. This is the production contract for M17 exit. |

Recovery must not invent commits. Uncommitted intents never become visible as Latest versions without a commit.

---

## 11. Protocol surface (reserved)

Opcodes reserved for M17 wire protocol (implementation in server/client tasks):

| Opcode | Name | Role |
|---:|---|---|
| 9 | `TXN_BEGIN` | Start transaction; response carries `txn_id` (+ optional `read_ts`) |
| 10 | `TXN_OP` | Read/write/delete within a transaction |
| 11 | `TXN_COMMIT` | Commit; response carries `commit_ts` |
| 12 | `TXN_ROLLBACK` | Abort and clear intents |

### 11.1 Error status

| Status | Code | Meaning |
|---|---:|---|
| `TXN_CONFLICT` | **3** (suggested) | Write–write or intent conflict; client should retry with a new transaction |

Wire integration (codec, server dispatch, client mapping) lands with RaftCommand / opcode work. If code `3` collides with an existing meaning in a given layer, the transactions layer still names the error `TXN_CONFLICT` and documents the numeric mapping at integration time.

Engine / Rust API surfaces the same condition as `KayaError::TxnConflict`.

---

## 12. Engine API (single-node)

```rust
pub type TxnId = u64;

impl<D: Disk> Engine<D> {
    pub fn begin_txn(&mut self) -> (TxnId, u64 /* snapshot_ts */);
    pub fn txn_get(&mut self, txn_id: TxnId, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn txn_put(&mut self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()>;
    pub fn txn_delete(&mut self, txn_id: TxnId, key: Bytes) -> Result<()>;
    pub async fn txn_commit(&mut self, txn_id: TxnId) -> Result<SequenceNumber>;
    pub fn txn_rollback(&mut self, txn_id: TxnId) -> Result<()>;
    pub fn txn_prepare_commit(&mut self, txn_id: TxnId) -> Result<Vec<(Bytes, Option<Bytes>)>>;
    pub fn txn_finish(&mut self, txn_id: TxnId) -> Result<()>;
}
```

Notes:

- `txn_put` / `txn_delete` may be synchronous if they only touch the intent map; they may be `async` for API uniformity with other engine methods.
- `txn_commit` is `async` because it issues durable WAL-backed puts/deletes.
- Default non-txn API remains LWW Latest (`ReadTimestamp::Latest`).

---

## 13. Invariants checklist

| ID | Invariant |
|---|---|
| TXN-1 | Snapshot reads never observe versions with `commit_ts > read_ts` |
| TXN-2 | Intents of txn T are invisible to all observers except T (RYW) |
| TXN-3 | Two live txns never both hold an intent on the same user_key |
| TXN-4 | Successful commit of a write fails if any written key has committed `commit_ts > read_ts` |
| TXN-5 | After commit ACK (strict), all materialized keys are recoverable from WAL |
| TXN-6 | Rollback leaves no intents and no new committed versions for that txn |
| TXN-7 | Raft commit-record path never yields half-committed multi-key state after recovery |
| TXN-8 | Unknown / finished `txn_id` does not silently apply new intents |

---

## 14. Decision records (TXN)

| ID | Decision | Rationale |
|---|---|---|
| TXN-D-001 | Isolation = SI with write–write detection | Matches M17 goal; SSI is stretch |
| TXN-D-002 | `read_ts` at BEGIN = `last_sequence` | Simple, matches MVCC `ReadTimestamp::At` |
| TXN-D-003 | Phase-1 intents in-memory on Engine | Fast unit tests; Raft path adds durability |
| TXN-D-004 | Fail on write for conflicts | Fail-fast; simpler client loops |
| TXN-D-005 | Single-node commit applies puts sequentially | Reuses WAL path; full atomic multi-key via Raft record |
| TXN-D-006 | Opcodes 9–12 reserved for TXN_* | Avoid collision with membership 7–8 |
| TXN-D-007 | `TXN_CONFLICT` suggested status 3 | Distinct client retry signal |

---

## 15. Test expectations (engine)

| Case | Expected |
|---|---|
| Two txns put same key | Second put (or its commit) returns `TxnConflict` |
| RYW | `txn_put` then `txn_get` returns own value |
| Snapshot isolation | T1 begins; T2 commits put; T1 `txn_get` still sees pre-T2 value |
| Rollback | After rollback, intents gone; other txn may write key |
| Commit durability | After commit + reopen, keys visible via normal `get` |

---

## 16. Out of scope (later M17 tasks)

- RaftCommand intent/commit records and server session map
- Client `Transaction` type and conformance vectors
- TLA+ `TxnCommit` model
- Jepsen bank workload exit gate

---

## 17. Cross-shard 2PC (M23)

When a multi-key transaction spans more than one Raft group (range shard), the
server uses a coordinator-driven two-phase commit over per-group Raft logs.
Single-group commits continue to use `RaftCommand::TxnCommit` (type 4).

### 17.1 System keys

Durable participant state lives under the reserved prefix `\x00txn/` (not
writable via public put/delete):

| Key | Value |
|---|---|
| `\x00txn/rec/{txn_id_be8}` | 1-byte state: `1=Preparing`, `2=Prepared`, `3=Committed`, `4=Aborted`, `5=Committing` |
| `\x00txn/intent/{txn_id_be8}/{user_key}` | Intent payload: `0` = delete tombstone; `1 \|\| value` = put |
| `\x00txn/dec/{txn_id_be8}` | **Global decision log** (§17.4): 1 byte, `1` = commit, `0` = abort |

`txn_id` in keys is encoded as **8-byte big-endian** for ordered scans.

The `rec` and `intent` keys are *participant* state, written by the group that
owns the key range. The `dec` key is the *coordinator's* decision, replicated on
the **meta group (group 0)**. A missing `dec` key means "not decided *here*
yet" — which is not the same as "aborted", because this node's group-0 log may
simply be behind (see §17.4).

### 17.2 RaftCommand variants

| Type byte | Variant | Payload |
|---:|---|---|
| 5 | `TxnPrepare { txn_id, coordinator_group, mutations }` | Persist intents + mark `Prepared` |
| 6 | `TxnCommit2pc { txn_id }` | Mark `Committing`, materialize intents via `apply_mutations`, clear intents, mark `Committed` |
| 7 | `TxnAbort2pc { txn_id }` | Delete intents only, mark `Aborted` (rejects `Committing`/`Committed`) |
| 9 | `TxnDecision { txn_id, commit }` | Write the global decision record (meta group only) |

Types 1–4 retain their existing wire layouts (Put / Delete / ConfigChange /
single-group `TxnCommit`); type 8 is `RangeMeta`.

`TxnDecision` is `9 \| txn_id(u64 LE) \| commit(u8)`. Apply is idempotent, and a
second decision that *contradicts* a durable one is rejected: a 2PC decision is
final.

### 17.3 Engine apply API

```rust
impl Engine {
    pub async fn apply_txn_prepare(
        &mut self,
        txn_id: u64,
        mutations: &[(Bytes, Option<Bytes>)],
    ) -> Result<()>;
    pub async fn apply_txn_commit_2pc(&mut self, txn_id: u64) -> Result<()>;
    pub async fn apply_txn_abort_2pc(&mut self, txn_id: u64) -> Result<()>;
    pub async fn apply_txn_decision(&mut self, txn_id: u64, commit: bool) -> Result<()>;
    pub fn read_txn_decision(&mut self, txn_id: u64) -> Result<Option<bool>>;
}
```

- **Prepare:** write record `Preparing` → write each intent → write `Prepared`.
- **Commit:** write durable `Committing` **before** any user-key write; load
  intents for `txn_id`, `apply_mutations` to user keys (index + CDC fire),
  delete intent keys, set record `Committed`. Idempotent if already
  `Committed`; resumes from `Committing` if interrupted; rejects if `Aborted`.
- **Abort:** delete intent keys, set record `Aborted`. Does not touch user keys.
  Idempotent if already `Aborted`; rejects if `Committed` or `Committing`.
- **Decision:** write `\x00txn/dec/{txn_id}`. Idempotent; rejects a flip.

Prepared intents are **not** visible to ordinary user `get`/`scan` (they live
only under `\x00txn/intent/…`).

### 17.4 Coordinator role, decision log, and recovery

#### Coordinator

The **coordinator** for a cross-group transaction is the node that holds the
transaction's staged intents, i.e. the leader of the **meta group (group 0)** —
`TXN_BEGIN` / `TXN_OP` / `TXN_COMMIT` are all served there. Participants are the
Raft groups owning the ranges the staged mutations map to.

`coordinator_group` (recorded in each `TxnPrepare`) is the group of the
lexicographically smallest key. It is a recovery/diagnostic hint only; it does
not elect the coordinator.

#### Algorithm

1. Partition mutations by range → group; deterministic participant order by
   group id.
2. **Prepare — parallel.** `TxnPrepare` is fanned out to every participant at
   once, under a per-phase timeout (`DEFAULT_PHASE_TIMEOUT`, 5s). A timeout is a
   failure.
3. **Decide.** All prepared → propose `TxnDecision { commit: true }` on group 0
   and wait for it to be committed *and applied*. This is the transaction's
   commit point.
4. **Commit — parallel.** `TxnCommit2pc` is fanned out to every participant.
5. **Abort path.** Prepare or the decision failed → propose
   `TxnDecision { commit: false }` (so a restarted participant resolves without
   guessing), then fan `TxnAbort2pc` out to every group that reached `Prepared`.

Nothing user-visible changes before step 3, and step 4 never starts before step
3 finishes — that is the ordering the recovery rules below rely on.

#### Multi-leader participants (forwarding)

The coordinator does **not** have to lead the participant groups. When it is not
the leader of a participant group, it forwards the 2PC command to that group's
leader over the existing client RPC:

| Opcode | Body | Reply |
|---:|---|---|
| 22 `TXN_FORWARD` | `group_id(u64 LE) \| raft_command_bytes` | `STATUS_OK` once committed **and applied** on that group; `STATUS_NOT_LEADER` otherwise |

The leader is taken from the local Raft status of that group
(`status_of(group).leader_id`) and mapped to a client address through the node
roster. `TXN_FORWARD` carries a raw replicated command, so it is treated as an
**admin** opcode: it requires the operator token when one is configured, and is
refused outright when a client token or prefix ACL is configured without an
operator token (it would otherwise bypass the data-path ACL).

*Limitations.* Forwarding uses the plaintext client RPC, so on a TLS-enabled
cluster the forward fails and the transaction aborts — no worse than the pre-#26
`NOT_LEADER`, but TLS-aware forwarding is a follow-on. Leader lookup is a single
attempt: a stale leader answers `NOT_LEADER`, the transaction aborts, and the
client retries.

#### Recovery

On every `Engine::open` (and again at server startup for logging; the second
pass is idempotent) each incomplete participant record is resolved against the
decision log:

| Local record | Decision log | Action |
|---|---|---|
| any in-flight state | `commit` | Finish commit (`txn2pc_finished_commits`) |
| any in-flight state | `abort` | Abort, drop intents (`txn2pc_aborted`) |
| `Preparing` | **absent** | Abort, drop intents (`txn2pc_aborted`) |
| `Prepared` | **absent** | **Leave in doubt** — intents held (`txn2pc_pending`) |
| `Committing` | any | Finish commit — never abort |
| `Committed` / `Aborted` | any | Leave untouched |

The `Preparing` / `Prepared` split is the safety rule (**TXN-2PC-8**):

- `Preparing` means the prepare was never acked to the coordinator, so the
  coordinator cannot have counted this participant and cannot have decided
  commit. Aborting locally is safe.
- `Prepared` means the prepare **was** acked. The coordinator may have decided
  commit and other participants may already have committed, while *this* node's
  group-0 log lagged. Aborting it here would produce a partial user-visible
  commit, so a participant must never do it. The record stays `Prepared` with
  its intents held — invisible to readers either way — until a decision arrives.

**Resolving an in-doubt record.** Applying a `TxnDecision` drives any local
`Preparing` / `Prepared` record for that `txn_id` to the decided outcome, so a
lagging participant is unblocked the moment its group-0 log catches up. The
participant group's own log also still carries `TxnCommit2pc`, which resolves it
independently.

**Liveness — coordinator recovery.** Because participants no longer self-abort,
the coordinator has to close out orphans. When a node **gains** meta-group
leadership (including its first election after a restart) it scans for `Prepared`
records with no decision and proposes `TxnDecision { commit: false }` for each.
At that instant any earlier coordinator has lost its term, so an undecided
transaction is orphaned and can only abort. This is safe even against a commit
decision still in flight: decisions are ordered by the group-0 log, that entry
precedes the sweep's, and a durable decision is never flipped — the commit wins
and the abort is discarded.

**Coordinator death.** The decision record survives the coordinator, because it
lives in group 0's Raft log rather than in coordinator memory:

- died *before* the decision → participants hold `Prepared`; the next meta-group
  leader records an abort decision and every participant releases. No partial
  commit.
- died *after* the decision, before/while committing → participants that already
  committed stay committed; the rest read `commit` from the decision log and
  finish. No partial commit.

*Residuals:* a transaction stays in doubt (intents held, but never user-visible)
from the coordinator's death until a new meta-group leader is elected. The sweep
runs on leadership gain, so a transaction that reaches `Prepared` in the
microseconds between that election and the scan can be aborted spuriously — a
client-visible failed commit, not a correctness problem. Recovery is node-local
(it writes to the engine, not through Raft), so replicas can differ transiently
until the group log delivers the same outcome — unchanged from the `Committing`
recovery already shipped. Decision records are never garbage-collected (same as
`rec` keys).

### 17.5 Invariants (2PC)

| ID | Invariant |
|---|---|
| TXN-2PC-1 | User keys never change until a durable `Committing` decision is written |
| TXN-2PC-2 | After `Committed` ACK, all participant intents are cleared and user keys are recoverable |
| TXN-2PC-3 | After `Aborted`, no user-key mutation from that txn remains |
| TXN-2PC-4 | Types 1–4 decode/encode unchanged after adding types 5–7 |
| TXN-2PC-5 | `Committing` always finishes to `Committed` on recovery; never aborted |
| TXN-2PC-6 | The global decision record is durable **before** any participant is asked to commit |
| TXN-2PC-7 | A durable decision is final: it is never flipped, and recovery follows it |
| TXN-2PC-8 | A participant never aborts a `Prepared` record on its own; only the decision log (or an un-acked `Preparing`) may release it |

### 17.6 Client transparency

The client `TXN_BEGIN` / `TXN_OP` / `TXN_COMMIT` / `TXN_ROLLBACK` opcodes are
unchanged. When staged mutations map to more than one Raft group, the server
coordinator runs 2PC; single-group commits still use type-4 `TxnCommit`. No
client API or wire break for cross-range transactions. `TXN_FORWARD` (22) is an
internal node-to-node opcode; clients never send it.

### 17.7 HLC commit timestamps and uncertainty (#27)

Multi-group ClusterNode auto-enables `EngineConfig.use_hlc`. Commit sequences
are HLC-packed as `(physical_ms << 16) | logical` (see `multi-raft-spec.md` §8
and `kaya_core::Hlc`).

**Uncertainty bound.** `EngineConfig.max_clock_offset_micros`
(`ClusterConfig.max_clock_offset_micros`, CLI `--max-clock-offset-micros`, env
`KAYA_MAX_CLOCK_OFFSET_MICROS`) bounds how far a remote HLC observation's
physical component may lead this node's own wall clock. Default 500ms,
matching CockroachDB's `--max-offset`.

**Reject on ingest.** `Engine::sync_clock` (the entry point for merging a
remote HLC sample) uses `Hlc::checked_update` instead of the plain
unconditional `update`: a remote physical time more than the bound ahead of
local wall-clock time is rejected outright (`KayaError::ClockSkew`) and does
**not** mutate the local clock. This is the "reject when skew exceeds bound"
half of the design — a single skewed or misbehaving peer cannot drag this
node's clock arbitrarily far into the future.

**Wait-before-serve on the write path.** After `checked_update` accepts a
remote sample within the bound, the local HLC's physical component can
legitimately lead real wall-clock time by up to the bound (e.g. a peer whose
clock genuinely runs a bit fast). `prepare_hlc_write_sequence` — the tick
path every `put`/`delete` goes through when `use_hlc` is set — detects this
lead (`Hlc::lead_over_wall_ms`) and sleeps it out, capped at the bound,
*before* the WAL append/memtable insert that would make the write (and its
commit_ts) durable and visible. A commit_ts is therefore never exposed to a
reader before the wall clock has actually caught up to it. This is a no-op
(zero wait) in the common case of a single node or already-synced clocks.

Net effect: the uncertainty interval is enforced both ways — a sample too far
ahead is rejected at the source, and a sample within bound only delays local
exposure rather than being trusted immediately.

**Out of scope for #27:** propagating HLC samples over the wire between
peers (no `sync_clock` caller exists yet outside tests — see
`multi-raft-spec.md` §8/§10) and a read-side uncertainty retry (CockroachDB's
"restart the read at a higher timestamp" for a version observed inside the
uncertainty window of a snapshot read). The write-path wait above covers the
snapshot-anomaly risk from the write side; the read-side symmetric case is a
follow-on once cross-node HLC gossip exists.

See `docs/runbooks/hlc-clock-skew.md` for operator guidance when skew
exceeds the bound.

Formal sketch: `spec/specs/txn/TwoPhaseCommit.tla` (TLC-checkable small model).
