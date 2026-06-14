# KayaDB Client Wire Protocol (Reference)

**Status:** Authoritative  
**Date:** 2026-06-14  
**For:** Anyone implementing a KayaDB client in Go, Python, TypeScript, Zig, etc.  
**Reference implementation:** `crates/kaya-net` + `crates/kaya-server`

This document defines the **exact on-the-wire format** for the TCP client protocol. The high-level semantics (leader redirection, retries, linearizability expectations) are described in [client-protocol-spec.md](client-protocol-spec.md).

---

## 1. Connection & Framing

- Clients connect over **TCP** (default client port: 7379).
- All frames use **little-endian** encoding.
- No TLS in v1 (see security roadmap).

### Client Request Frame (sent by client)

```
[frame_len: u32 LE]   // total bytes after this field = 1 + len(payload)
[opcode: u8]
[payload: bytes]
```

- `frame_len` = 1 (opcode) + payload length.
- Maximum frame: 64 MiB (server enforces; clients should be conservative).

### Server Response Frame

```
[frame_len: u32 LE]   // total after this field = 2 + len(payload)
[status: u16 LE]
[payload: bytes]
```

- `frame_len` = 2 + payload length.

After sending a request, the client **must** read exactly one response frame before sending the next request on the same connection (no pipelining in v1).

---

## 2. Opcodes

| Opcode | Name    | Description                  | Payload (request)          | Typical response status |
|--------|---------|------------------------------|----------------------------|-------------------------|
| 1      | PUT     | Store key → value            | put_payload                | 0 (OK) or 10 (NOT_LEADER) |
| 2      | GET     | Read value for key           | key_payload                | 0 (OK + value), 2 (NOT_FOUND), 10 |
| 3      | DELETE  | Remove key                   | key_payload                | 0 (OK) or 10            |
| 4      | SCAN    | Prefix scan                  | key_payload (as prefix)    | 0 (OK + scan_response) or 10 |
| 5      | HEALTH  | Liveness + role probe        | (empty)                    | 0 (OK + "leader" or "follower") |
| 6      | STATS   | Server metrics (JSON)        | (empty)                    | 0 (OK + JSON bytes)     |
| 7      | ADD_MEMBER | Propose joint-consensus add (leader only) | member_payload | 0 (OK + message) or 10 |
| 8      | REMOVE_MEMBER | Propose joint-consensus remove (leader only) | remove_member_payload | 0 or 10 |

Unknown opcode → server replies with `STATUS_ERROR`.

`ADD_MEMBER` and `REMOVE_MEMBER` are **unauthenticated admin RPCs** in the current prototype. Restrict client port access to trusted operators (see [security.md](../security.md)).

---

## 3. Payload Formats (Request)

All integers are **little-endian u32** unless noted.

### PUT (opcode 1)

```
key_len   : u32 LE
value_len : u32 LE
key       : [key_len bytes]
value     : [value_len bytes]
```

**Reference:** `encode_put_payload` / `decode_put_payload`

### GET / DELETE / SCAN (opcodes 2, 3, 4)

```
key_len : u32 LE
key     : [key_len bytes]
```

For SCAN the "key" bytes are interpreted as a **prefix**.

**Reference:** `encode_key_payload`, `encode_scan_payload`

### HEALTH / STATS (opcodes 5, 6)

Empty payload (frame_len = 1, just the opcode byte).

### ADD_MEMBER (opcode 7)

```
node_id     : u64 LE
raft_len    : u32 LE
raft_addr   : UTF-8 host:port (e.g. "127.0.0.1:7484")
client_len  : u32 LE
client_addr : UTF-8 host:port
```

**Reference:** `encode_member_payload` / `decode_member_payload`

### REMOVE_MEMBER (opcode 8)

```
node_id : u64 LE
```

**Reference:** `encode_remove_member_payload` / `decode_remove_member_payload`

---

## 4. Payload Formats (Response)

### Status 0 (OK)

- **PUT, DELETE:** usually empty payload (frame_len = 2).
- **GET:** `value_len: u32 LE | value bytes`
- **SCAN:** 
  ```
  item_count : u32 LE
  [ key_len:u32 | key bytes | value_len:u32 | value bytes ] × item_count
  ```
- **HEALTH:** `b"leader"` or `b"follower"` (raw bytes, no length prefix).
- **STATS:** UTF-8 JSON document (see example below). No inner length.

**Reference encoders:** `encode_value_payload`, `encode_scan_response`

### Status 1 (INVALID_ARGUMENT) or 9 (ERROR)

```
msg_len : u32 LE
message : UTF-8 bytes (length = msg_len)
```

Client should surface the message to the user.

**Reference:** `encode_error_payload` / `decode_error_payload`

### Status 2 (NOT_FOUND) — GET only

Empty payload.

### Status 10 (NOT_LEADER) — critical for clustering

Payload contains the **current leader's client address** as a raw UTF-8 string (e.g. `127.0.0.1:7379`) **or empty** if unknown.

- No length prefix inside the payload — the outer `frame_len` tells you how many bytes.
- Client **must** parse this as `SocketAddr` / host:port and redirect the request.
- If empty or unparsable → fall back to trying other configured endpoints.

**Reference (server):** `get_leader_hint`

---

## 5. Example Frames (hex)

**PUT "hello" → "world" (opcode 1)**

Request (simplified, frame_len shown):

```
Frame len (4) : 00 00 00 11   (17 = 1 opcode + 16 payload)
Opcode        : 01
key_len       : 00 00 00 05
value_len     : 00 00 00 05
key           : 68 65 6c 6c 6f
value         : 77 6f 72 6c 64
```

**GET "hello" response OK:**

```
Frame len : 00 00 00 0A   (10 = 2 + 8)
status    : 00 00
value_len : 00 00 00 05
value     : 77 6f 72 6c 64
```

**NOT_LEADER response (status 10):**

```
Frame len : 00 00 00 10   (example)
status    : 00 0A
payload   : 31 32 37 2e 30 2e 30 2e 31 3a 37 33 37 39   ("127.0.0.1:7379")
```

---

## 6. Important Behavioral Rules (Wire Level)

- Every mutating operation (PUT, DELETE) that reaches the leader is durably committed via Raft before `STATUS_OK` is returned.
- GET and SCAN are linearizable (served via ReadIndex on the leader).
- Clients should **not** assume a connection stays with the same leader forever. On `STATUS_NOT_LEADER` (or connection error), re-resolve and redirect.
- Connections can be long-lived (recommended). Use connection pooling / keep-alive where the language supports it.
- On network error or timeout: apply the retry policy from the high-level spec. Writes should be retried only when safe (idempotency key support is planned).
- Frame length limits: clients should reject or truncate absurdly large responses (server already protects itself).

---

## 7. STATS JSON Shape (current)

```json
{
  "role": "leader" | "follower" | "candidate",
  "term": 42,
  "commit_index": 1234,
  "applied_index": 1234,
  "peer_count": 2,
  "engine": {
    "put_count": 987,
    "get_count": 1203,
    "delete_count": 45,
    "scan_count": 67,
    "wal_bytes_written": 1048576,
    "wal_fsync_count": 1234,
    "memtable_entries": 312,
    "sstable_count": 7,
    "last_sequence": 56789
  }
}
```

This shape is stable for v1 but may gain fields.

---

## 8. Health Check Notes

- `HEALTH` (opcode 5) returns `OK` + role string.
- It does **not** imply the node is the leader. Use it only for basic connectivity/role awareness.
- For operations, always respect `NOT_LEADER`.

---

## 9. Error Handling on the Wire

- `INVALID_ARGUMENT`: The request bytes could not be parsed or violated basic rules (e.g. oversized key in current limits). Client should not retry blindly.
- `NOT_FOUND`: Only meaningful for GET. Never an error for DELETE or PUT.
- Treat unknown status codes ≥ 1000 as `ERROR`.

---

## 10. Versioning

There is currently no protocol version byte in the client frames (v1 implicit). Future protocol evolution will introduce a version field. Until then, treat the formats in this document as the contract.

---

## 11. Reference Implementations (to study)

- **Rust (canonical):** 
  - Framing: `crates/kaya-net/src/transport.rs` (`read_client_frame`, `write_client_response`, `encode_client_frame`, `roundtrip`)
  - Payloads: `crates/kaya-net/src/codec.rs`
  - Server dispatch + ReadIndex + leader hint: `crates/kaya-server/src/cluster.rs`
- **Rust high-level client:** `crates/kaya-client/src/lib.rs` (especially `send_with_retry`)
- **CLI reference:** `crates/kayactl/src/main.rs`

Any correct client must produce identical bytes for the same logical operation.

---

**This wire specification + the high-level [client-protocol-spec.md](client-protocol-spec.md) together are the complete contract for client authors.**

When in doubt, match the behavior and byte layout of the Rust reference exactly.
