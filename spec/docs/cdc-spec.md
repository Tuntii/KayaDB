# CDC / Changefeed Spec (M19 + polish)

**Status:** Draft v0.2  
**Scope:** Change data capture for user put/delete (engine file sink + TCP subscribe)  
**Milestone:** M19 (production path + polish)

---

## 1. Purpose

Provide an **at-least-once**, **per-key ordered** stream of change events so
consumers can:

- replicate or project data to external systems
- resume after restarts via durable per-consumer cursors
- drive incremental backup watermarks

Events fire on the shared put/delete path — including **Raft apply** of
`Put` / `Delete` / `TxnCommit` materialization. The durable sink is still
engine-local JSONL; TCP opcodes expose poll/checkpoint on the leader.

---

## 2. Event model

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
{data_dir}/cdc/log.jsonl           # append-only event log (rewritten on compact)
{data_dir}/cdc/cursors/{consumer}  # last delivered seq (decimal text)
{data_dir}/cdc/backup_watermark    # optional; set by kayactl backup --cdc-consumer
```

### 3.1 Log line format (JSONL)

```text
{"v":1,"seq":7,"op":"put","key":"<hex>","value":"<hex>"}
{"v":1,"seq":8,"op":"delete","key":"<hex>"}
```

### 3.2 Cursor file

Plain decimal `u64`. Meaning: highest `seq` already delivered to this consumer.

---

## 4. Engine API

```rust
cdc_subscribe(consumer_id, from_seq: Option<u64>) -> CdcCursor
cdc_poll(cursor, limit) -> Vec<CdcEvent>
cdc_checkpoint(consumer_id)
cdc_compact(retain_below: Option<u64>) -> removed_count
cdc_consumer_seq(consumer_id) -> u64
cdc_write_backup_watermark(seq) / cdc_read_backup_watermark()
```

| Call | Behavior |
|---|---|
| `cdc_subscribe` | Build a cursor. `from_seq: Some(s)` starts after `s`. `None` uses checkpoint. |
| `cdc_poll` | Events with `seq > cursor.last_seq`, up to `limit`. Advances cursor. |
| `cdc_checkpoint` | Persist consumer position to `cdc/cursors/{id}`. |
| `cdc_compact` | Drop events with `seq <= cutoff` (default: min consumer checkpoint); rewrite log. |

Config: `EngineConfig.enable_cdc` (default `true`).

---

## 5. Wire protocol (TCP)

| Opcode | Name | Role |
|---|---|---|
| 13 | `CDC_POLL` | Leader-local poll (client-token path) |
| 14 | `CDC_CHECKPOINT` | Persist consumer cursor |

### 5.1 CDC_POLL request

```text
consumer_len(u16 LE) | consumer_utf8 | from_seq(u64 LE) | limit(u32 LE)
```

### 5.2 CDC_POLL response

```text
count(u32 LE) | repeated:
  seq(u64 LE) | op(u8: 1=put, 2=delete)
  | key_len(u32 LE) | key
  | [value_len(u32 LE) | value  if put]
```

### 5.3 CDC_CHECKPOINT request

```text
consumer_len(u16 LE) | consumer_utf8
```

Clients: Rust `KayaClient::cdc_poll` / `cdc_checkpoint`; Go `CdcPoll` / `CdcCheckpoint`.

---

## 6. Backup interaction

```text
kayactl backup --data <src> --out <dest> [--incremental] [--cdc-consumer <id>]
```

After the filesystem tree copy, if `--cdc-consumer` is set, the tool reads that
consumer's durable cursor from the source engine and writes
`dest/cdc/backup_watermark` with that sequence. JSON output includes
`cdc_watermark`.

---

## 7. Guarantees

| Guarantee | Status |
|---|---|
| Events after successful user put/delete (incl. Raft apply) | Yes |
| Resume via cursor without loss (at-least-once) | Yes |
| Reopen engine continues from log file | Yes |
| Crash/reopen: no loss of post-checkpoint events | Yes (sim failover gate) |
| Log compaction below min consumer seq | Yes |
| Exactly-once | No |
| Multi-node shared CDC log | No (per-node apply + file) |
| TCP + Go subscribe | Yes (opcodes 13/14) |

---

## 8. Limitations

- CDC log is per engine data dir (each Raft apply path writes locally).
- Compaction cannot resurrect dropped prefixes.
- Filtering / projection and multi-sink fanout remain future work.
