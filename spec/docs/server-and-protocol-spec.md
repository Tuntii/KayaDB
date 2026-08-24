# Server and Protocol Spec

**Status:** Draft v0.1  
**Scope:** Local server process, request framing, command mapping, future distributed boundary  

---

## 1. Purpose

Server mode is a wrapper around the engine, not a separate storage implementation.

The server should allow local clients to issue KayaDB commands while preserving the same durability, validation and recovery semantics as the embedded API.

MVP may skip server mode until embedded engine and CLI are stable. This spec defines the boundary so future implementation does not leak storage concerns into networking.

---

## 2. Process model

```text
kayadb-server
  ├─ config loader
  ├─ data dir lock
  ├─ engine open/recovery
  ├─ listener
  ├─ connection handlers
  └─ graceful shutdown coordinator
```

Startup:

```text
1. Parse config/flags
2. Bind only after config validation, or bind after recovery? TBD
3. Lock data directory
4. Open engine and run recovery
5. Start listener
6. Serve requests
```

Preferred MVP behavior:

> Do not accept client connections until recovery has completed successfully.

---

## 3. Binding defaults

Security-conscious defaults:

```text
host = "127.0.0.1"
port = 7379
```

Rules:

- no production-readiness claim,
- no unauthenticated remote deployment claim,
- if binding to non-localhost, logs must warn clearly,
- authentication is out of MVP scope unless explicitly added.

---

## 4. Protocol framing

Long-term base should be a simple length-prefixed binary protocol.

Frame:

```text
+-------------+--------+
| frame_len   | u32    |
| opcode      | u8     |
| payload     | bytes  |
+-------------+--------+
```

All integers little-endian.

Limits:

```text
max_frame_len = 64 MiB default
max_key_len = engine config max_key_len
max_value_len = engine config max_value_len
```

Decoder must reject oversized frames before allocation.

---

## 5. Opcodes

| Opcode | Command | Payload |
|---:|---|---|
| 1 | PUT | key_len u32, value_len u32, key, value |
| 2 | GET | key_len u32, key |
| 3 | DELETE | key_len u32, key |
| 4 | SCAN | prefix_len u32, limit u32, prefix |
| 5 | HEALTH | empty |
| 6 | STATS | empty |

Opcodes 7–21 (membership, TXN, CDC, range routing) are specified where they are
implemented: `spec/docs/range-routing-spec.md` §4 covers 15–21
(`LIST_RANGES` … `MOVE_RANGE`).

Future:

| Opcode | Command |
|---:|---|
| 30 | AUTH |

---

## 6. Response framing

Response frame:

```text
+-------------+-------------+--------+
| frame_len   | status_code | payload|
| u32         | u16         | bytes  |
+-------------+-------------+--------+
```

Status codes:

| Code | Meaning |
|---:|---|
| 0 | OK |
| 1 | INVALID_ARGUMENT |
| 2 | NOT_FOUND |
| 3 | CORRUPTION |
| 4 | IO |
| 5 | DISK_FULL |
| 6 | FSYNC_FAILED |
| 7 | UNSUPPORTED_VERSION |
| 8 | LOCK_CONFLICT |
| 9 | INTERNAL |

---

## 7. Text protocol for debugging

A line-based text protocol may be introduced for local debugging only:

```text
PUT user:1 {"name":"Ada"}
GET user:1
DELETE user:1
SCAN user:
```

If implemented, it must map to the same engine API and must not become the only tested protocol.

---

## 8. Request handling semantics

- Each write request returns only after engine write result is known.
- Strict durability semantics are determined by engine config or request options.
- Server must not ACK before engine returns success.
- Connection drop after durable write but before response creates normal client ambiguity.
- Server must not retry non-idempotent writes internally unless idempotency keys exist.

---

## 9. Shutdown behavior

Graceful shutdown:

```text
1. Stop accepting new connections
2. Let in-flight requests finish or time out
3. Close engine
4. Release data dir lock
5. Exit 0
```

Crash/kill has no shutdown steps; recovery must handle it.

---

## 10. Invariants

| ID | Invariant |
|---|---|
| SRV-001 | Server never bypasses engine write path |
| SRV-002 | Strict ACK is not returned before engine durable success |
| SRV-003 | Decoder rejects oversized frames before allocation |
| SRV-004 | Server does not accept requests before successful recovery |
| SRV-005 | Localhost is default bind address |

---

## 11. Acceptance criteria

Server boundary is ready when:

- embedded engine can run without server,
- server opens engine and exposes PUT/GET/DELETE/SCAN,
- request decoder has malformed/oversized tests,
- strict durability tests pass through server path,
- default bind is localhost,
- shutdown does not corrupt acknowledged writes.
