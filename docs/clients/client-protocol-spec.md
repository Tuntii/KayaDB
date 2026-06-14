# KayaDB Client Protocol Specification

**Status:** Review-Ready v0.2  
**Date:** 2026-06-14  
**Scope:** High-level client-facing protocol for multi-language clients (Go, Python, TypeScript, Zig, etc.)

This document defines the expected behavior and semantics that all KayaDB clients must implement. It is intentionally high-level (not wire format) so that different language implementations can follow the same rules while using the most idiomatic patterns for their ecosystem.

For the exact byte layout of frames, opcodes, and payloads, see the companion document **[client-wire-protocol.md](client-wire-protocol.md)** (authoritative reference for implementers).

---

## 1. Goals

- Provide consistent behavior across all client languages.
- Make leader redirection, retries, and error handling predictable.
- Support observability and tracing out of the box.
- Enable production-grade usage for medium to large scale workloads.
- Allow independent evolution of wire protocol while keeping client semantics stable.

---

## 2. Connection Model

### 2.1 Endpoints
A client is given one or more server addresses (client ports).

Clients **must** support multiple endpoints for:
- Initial connection
- Failover
- Leader discovery

### 2.2 Connection Lifecycle
- Clients should use connection pooling / keep-alive where the language supports it.
- Connections should be established lazily (on first operation) unless explicitly configured otherwise.
- Clients should detect broken connections and reconnect automatically.

### 2.3 Session Concept (Future)
In the long term, a lightweight session identifier may be introduced for better idempotency and server-side state. Clients should be prepared to carry a session token if the server returns one.

---

## 3. Leader Discovery and Redirection

This is one of the most important parts of the protocol.

### 3.1 Initial Leader Discovery
When a client connects to any node:
- The node may respond with `NOT_LEADER` + the current leader's client address (if known).
- If no leader is known, the node may return a list of known peers or ask the client to try another endpoint.

### 3.2 Handling `NOT_LEADER`
- Clients **must** automatically redirect the request to the leader address provided in the response.
- After redirection, the client should update its internal "preferred endpoint" cache.
- Redirection should happen transparently to the user (unless configured otherwise).

### 3.3 Stale Leader Detection
- If a redirected request also fails with `NOT_LEADER`, the client must clear its leader cache and retry the full discovery process (possibly trying other known endpoints).

### 3.4 Leader Cache
Clients should maintain a short-lived leader cache (e.g. 5-30 seconds) to avoid hitting followers repeatedly.

---

## 4. Request Lifecycle

### 4.1 Basic Flow
1. Client prepares the request.
2. Client sends to current target (initially any endpoint or cached leader).
3. If the response is successful → return to caller.
4. If the response is `NOT_LEADER` → redirect (see section 3) and retry.
5. On network error / timeout → apply retry policy (see section 5).

### 4.2 Idempotency
- All mutating operations (PUT, DELETE) should be safe to retry.
- Clients should support an optional `idempotency_key` (passed by the user or generated internally).
- The server may use this key to deduplicate requests.

---

## 5. Error Handling and Retries

### 5.1 Error Categories
Clients should distinguish between:

- **Transient errors** (retryable): Network issues, timeouts, temporary `NOT_LEADER`.
- **Permanent errors** (non-retryable): Invalid argument, authentication failure, etc.
- **Leadership errors**: `NOT_LEADER` (special handling required).

### 5.2 Retry Policy
Default recommended policy (can be overridden by user):

- Exponential backoff with jitter.
- Maximum number of attempts (default: 3–5 for reads, 5–10 for writes).
- Do not retry on permanent errors.
- On leadership change, reset backoff and redirect immediately.

### 5.3 Read vs Write Retries
- Reads can be retried more aggressively.
- Writes should prefer idempotency keys over blind retries when possible.

### 5.4 Canonical Error Codes

All official clients **must** understand and act on the following status codes (returned as `u16 LE` in the response frame header):

| Code | Symbolic Name           | Description                                      | Client Action                          | Retry Policy          |
|------|-------------------------|--------------------------------------------------|----------------------------------------|-----------------------|
| 0    | `STATUS_OK`             | Operation completed successfully.                | Return success / value to caller       | N/A                   |
| 1    | `STATUS_INVALID_ARGUMENT` | Request was malformed or violated limits (bad key length, etc.). | Surface error to user immediately      | Never                 |
| 2    | `STATUS_NOT_FOUND`      | Key does not exist (only for GET).               | Return "not found" / None / null       | Never (normal case)   |
| 9    | `STATUS_ERROR`          | Unexpected internal server error.                | Log details, surface generic error     | At most once (then fail) |
| 10   | `STATUS_NOT_LEADER`     | The contacted node is not currently the leader.  | Extract leader hint from payload (UTF-8 "host:port" or empty) and redirect. | Immediate redirect (see §3) |

**Notes on `NOT_LEADER`:**
- The response payload for `STATUS_NOT_LEADER` contains the current leader's *client* address as a UTF-8 string when known (e.g. `127.0.0.1:7379`). Clients must parse it and switch target.
- If the hint is empty or unparseable, fall back to full discovery using the configured endpoint list.
- `NOT_LEADER` is the primary mechanism for transparent leader following.

Additional codes may be introduced in minor protocol revisions. Unknown codes ≥ 1000 should be treated as `STATUS_ERROR`.

---

## 6. Observability and Tracing

All official clients **must** support the following:

- Request latency metrics (per operation type).
- Error rate metrics (categorized).
- Optional distributed tracing (OpenTelemetry compatible where possible).
- Ability to attach user-provided trace/span context.

Clients should expose hooks or configuration so users can integrate with their existing observability stack.

---

## 7. Linearizability Tracing (Optional but Recommended)

To support the existing `kaya-sim` linearizability checker:

- Clients should be able to record the full history of operations with:
  - Operation type and arguments
  - Result
  - Start and end timestamps (or logical ticks if using simulation)
- This can be enabled via `enable_tracing()` style API (as already exists in the Rust client).

This feature is especially valuable for companies that frequently hit subtle correctness bugs.

---

## 8. Configuration

Minimum configuration every client must support:

- List of initial endpoints
- Timeout settings (connect, request, etc.)
- Retry policy (max attempts, backoff strategy)
- Durability preference (strict vs relaxed) when applicable
- Tracing / logging level

---

## 9. Versioning and Compatibility

- The client protocol has its own version (independent of the server binary version).
- Breaking changes must be versioned.
- Clients should declare the protocol version they support.
- Servers should be able to reject clients with incompatible protocol versions.

Backward compatibility rule (recommended):
- Minor protocol versions should be backward compatible for read operations.
- Write operations may require stricter compatibility.

---

## 10. Core Operations (Request/Response Shapes)

All operations below are described at the **semantic level**. Concrete wire encoding (length-prefixed frames, opcodes 1-6, payload layouts) is defined in the reference Rust implementation (`kaya-net` crate) and must be replicated faithfully by other language clients.

### 10.1 PUT
**Purpose:** Store or overwrite a key with a value (durable according to server defaults or per-request options in future).

**Inputs:**
- `key`: arbitrary bytes (recommended max ~64 KiB for now)
- `value`: arbitrary bytes
- Optional: durability hint, idempotency key (future)

**Success response:** `STATUS_OK` (empty or minimal payload)

**Failure modes:** `INVALID_ARGUMENT`, `NOT_LEADER` (with hint), `ERROR`

**Client requirements:**
- Must support transparent redirection on `NOT_LEADER`.
- Should offer a way to pass or generate idempotency keys for safe retries.
- After successful commit, the write is visible to subsequent linearizable reads.

### 10.2 GET
**Purpose:** Retrieve the latest committed value for a key.

**Inputs:**
- `key`: bytes

**Success:** `STATUS_OK` + value bytes (or empty value)

**Not found:** `STATUS_NOT_FOUND` (empty payload)

**Other:** `INVALID_ARGUMENT`, `NOT_LEADER`, `ERROR`

**Semantics:** Linearizable read (currently implemented via ReadIndex on leader).

### 10.3 DELETE
**Purpose:** Remove a key (tombstone semantics in the LSM).

**Inputs:** `key`

**Success:** `STATUS_OK`

**Behavior:** Same durability and replication guarantees as PUT. Subsequent GETs return NOT_FOUND.

### 10.4 SCAN
**Purpose:** Iterate over keys with a given prefix, in lexicographic order.

**Inputs:**
- `prefix`: bytes

**Success:** `STATUS_OK` + encoded list of `(key, value)` pairs (see `encode_scan_response` reference).

**Empty result:** OK with zero items.

**Limits:** Clients should be prepared for large responses and/or offer pagination / streaming in later versions.

### 10.5 HEALTH
**Purpose:** Lightweight liveness / connectivity check.

**Inputs:** none

**Response:** `STATUS_OK` (no payload required)

**Note:** Does **not** guarantee leadership. Use for quick probes only.

### 10.6 STATS
**Purpose:** Retrieve server-side metrics and Raft state (role, term, commit index, engine counters, etc.).

**Inputs:** none

**Response:** `STATUS_OK` + JSON document (current schema is implementation-defined but stable enough for dashboards; see `kayactl status --json` and server handler).

---

## 11. Conformance

To be considered an official KayaDB client, an implementation should:

1. Implement the behaviors described in sections 3 (Leader Discovery), 5 (Error Handling & Retries, including 5.4 Error Codes), and 10 (Core Operations).
2. Provide a way to enable linearizability tracing (see section 7).
3. Follow the exact error model, status codes, and retry/redirect semantics.
4. Pass future conformance tests (when a language-agnostic suite exists).

Clients that deviate from the documented leader redirection, retry, or error handling rules are considered non-conforming.

---

## 12. Future Extensions (Not in v0.2)

- Multi-key transactional API
- Watch / subscription primitives
- Batch operations
- Server-side sessions with explicit attach/detach
- Per-request durability controls exposed in client API

---

## Next Steps (Current State & Roadmap)

**Completed in v0.2 (2026-06-14):**
- Error codes fully documented with client actions (5.4).
- Core operations (PUT/GET/DELETE/SCAN/HEALTH/STATS) described with semantic shapes and edge cases (section 10).

**Completed in M11 (2026-06-14):**
- Membership admin opcodes ADD_MEMBER (7) and REMOVE_MEMBER (8) on the wire (see [client-wire-protocol.md](client-wire-protocol.md)).
- `kayactl add-node` / `remove-node` wrappers for operators.

**Remaining for client ecosystem:**
1. Create conformance test suite (language-agnostic where possible, e.g. using test vectors + a reference runner).
2. Stand up the first non-Rust client repository (recommended order: **Go** → Python → TypeScript).
3. Implement leader redirection + basic retry + tracing enablement in the first client.
4. Add protocol version handshake (future minor version).

---

**This document is the single source of truth for client behavior.**  
Any deviation in a language client should be discussed and either justified or fixed.

Last updated: 2026-06-14 (error codes + operations added; review-ready)