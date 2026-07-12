# CDC / Changefeed Spec (M19 foundation)

**Status:** Draft v0.1  
**Scope:** Engine-local change data capture for user put/delete  
**Milestone:** M19 (foundation; not full production changefeed suite)

---

## 1. Purpose

Provide an **at-least-once**, **per-key ordered** stream of change events so
consumers can:

- replicate or project data to external systems
- resume after restarts via durable per-consumer cursors
- (later) drive incremental backup watermarks

This foundation is **engine-local** and file-backed. It is **not** yet
Raft-log based, has no TCP subscribe path, and does not prove leader-failover
contracts under chaos.

---

## 2. Event model

Each successful **user** `put` / `delete` (after WAL append) emits:

| Field | Type | Notes |
|---|---|---|
| `seq` | `u64` | Engine sequence number of the primary write |
| `key` | bytes | User key |
| `value` | `Option<bytes>` | `Some` for put, `None` for delete |
| `op` | `put` \| `delete` | Operation kind |

### Ordering and delivery

- Events are totally ordered by `seq` (global sequence).
- Per-key order follows global order (monotone `seq` per key).
- Delivery is **at-least-once**: after a crash without checkpoint, poll may
  redeliver events already seen.
- System keys (e.g. secondary index maintenance under `\x00idx/`) are **not**
  emitted. Transaction commits that materialize via public `put`/`delete` **are**
  emitted (one event per materialised write).

---

## 3. On-disk layout

```text
{data_dir}/cdc/log.jsonl           # append-only event log
{data_dir}/cdc/cursors/{consumer}  # last delivered seq (decimal text)
```

### 3.1 Log line format (JSONL, no nested objects)

```text
{"v":1,"seq":7,"op":"put","key":"<hex>","value":"<hex>"}
{"v":1,"seq":8,"op":"delete","key":"<hex>"}
```

- `v` — log format version (`1`)
- `key` / `value` — lowercase hex of raw bytes
- One JSON object per line; blank lines ignored on load

### 3.2 Cursor file

Plain decimal `u64` (optionally trailing newline). Meaning: highest `seq`
already delivered to this consumer.

---

## 4. Engine API

```rust
pub fn cdc_subscribe(&self, consumer_id: &str, from_seq: Option<u64>) -> Result<CdcCursor>
pub fn cdc_poll(&mut self, cursor: &mut CdcCursor, limit: usize) -> Result<Vec<CdcEvent>>
pub async fn cdc_checkpoint(&mut self, consumer_id: &str) -> Result<()>
```

| Call | Behavior |
|---|---|
| `cdc_subscribe` | Build a cursor. `from_seq: Some(s)` starts after `s`. `None` uses last polled/checkpointed seq for that consumer (or `0`). |
| `cdc_poll` | Return events with `seq > cursor.last_seq`, up to `limit`. Advances cursor + in-memory consumer position. |
| `cdc_checkpoint` | Persist in-memory consumer position to `cdc/cursors/{id}`. |

Config: `EngineConfig.enable_cdc` (default `true`). When `false`, no log is
written and API calls return invalid-argument.

---

## 5. Backup interaction

`kayactl backup --incremental` remains a **filesystem tree** copy of immutable
files (SSTables, sealed WAL segments) plus changed mutable files.

**Later:** incremental backup can treat a CDC consumer checkpoint as a watermark
for logical incremental export. This foundation only implements
`cdc_checkpoint(consumer_id)`; it does **not** re-base `backup --incremental`
on CDC yet.

---

## 6. Guarantees (foundation)

| Guarantee | Status |
|---|---|
| Events after successful user put/delete | Yes |
| Resume via cursor without loss (at-least-once) | Yes |
| Reopen engine continues from log file | Yes |
| Per-key order by seq | Yes |
| Exactly-once | No |
| Raft-log source / multi-node | No |
| TCP / Go subscribe API | No |
| Chaos: no lost events across leader failover | No |

---

## 7. Out of scope → later M19

- Raft-log-based changefeed (true cluster CDC)
- TCP + file multi-sink fanout
- Rust + Go network subscribe API
- `backup --incremental` driven by CDC checkpoints
- Leader failover chaos / Jepsen gate
- Compaction / truncation of old CDC log segments
- Filtering (prefix / table) and projection
