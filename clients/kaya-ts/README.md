# kaya-ts — KayaDB TypeScript client

Minimal zero-dependency Node.js TCP client for KayaDB's client wire protocol.
Byte-compatible with the Rust reference (`crates/kaya-client`), Go
(`clients/kaya-go`), and Python (`clients/kaya-py`) clients. See
[`docs/clients/client-wire-protocol.md`](../../docs/clients/client-wire-protocol.md).

## Features

- `put` / `get` / `delete` / `health` / `hello`
- Snapshot Isolation transactions: `beginTxn` → `get` / `put` / `delete` → `commit` / `rollback`
- Connection reuse with reconnect on transport errors
- `RetryPolicy` (exponential backoff + full jitter + per-attempt timeout)
- Leader redirect on `NOT_LEADER` (follows the returned `host:port` hint; redirects do **not** consume a retry attempt)
- Optional client token auth (`CLIENT\x00` framing) for data-path and TXN ops
- Per-request timeout

Requires **Node.js 22+** (native TypeScript strip-types, or run through any TS loader).

## Usage

```ts
import { KayaClient } from "kaya-ts";

const db = new KayaClient({
  addr: "127.0.0.1:7379",
  clientToken: "secret", // optional
  timeoutMs: 5000,
});

try {
  await db.hello(); // protocol version (1)
  await db.put(Buffer.from("hello"), Buffer.from("world"));
  const v = await db.get(Buffer.from("hello"));
  console.log(v?.toString()); // "world"
  console.log(await db.health()); // "leader" | "follower"
} finally {
  db.close();
}
```

`get` returns `null` when the key is absent. Protocol errors throw `KayaError`
(or `InvalidArgument` / `NotFound` / `TxnConflict`).

### Transactions

```ts
import { KayaClient, TxnConflict } from "kaya-ts";

const db = new KayaClient("127.0.0.1:7379");
try {
  const txn = await db.beginTxn();
  await txn.put(Buffer.from("k"), Buffer.from("v"));
  const v = await txn.get(Buffer.from("k")); // local read-your-writes
  try {
    const commitTs = await txn.commit();
    console.log("committed at", commitTs);
  } catch (err) {
    if (err instanceof TxnConflict) {
      // write-write conflict; start a new txn
    } else {
      throw err;
    }
  }
} finally {
  db.close();
}
```

`rollback()` discards staged intents. After `commit` or `rollback` (including a
failed commit) the handle cannot be reused.

### Retry policy

Defaults match Go/Rust: 4 attempts, 50ms base backoff, 2s cap, full jitter, 5s
per-attempt timeout. Leader redirects have a separate budget (`maxRedirects`,
default 3) and do not consume a retry attempt.

```ts
import { KayaClient, defaultRetryPolicy, retryPolicyNone } from "kaya-ts";

const db = new KayaClient({
  addr: "127.0.0.1:7379",
  retryPolicy: defaultRetryPolicy(),
});

db.setRetryPolicy(retryPolicyNone()); // single shot, no extra timeout
```

## Opcode coverage

| Opcode | Name | Status |
|--------|------|--------|
| 0 | HELLO | implemented |
| 1 | PUT | implemented |
| 2 | GET | implemented |
| 3 | DELETE | implemented |
| 4 | SCAN | **gap** |
| 5 | HEALTH | implemented |
| 6 | STATS | **gap** |
| 7–8 | ADD/REMOVE_MEMBER | **gap** (admin) |
| 9 | TXN_BEGIN | implemented |
| 10 | TXN_OP (Get=1, Put=2, Delete=3) | implemented |
| 11 | TXN_COMMIT | implemented |
| 12 | TXN_ROLLBACK | implemented |
| 13 | CDC_POLL | **gap** |
| 14 | CDC_CHECKPOINT | **gap** |
| 15 | LIST_RANGES | **gap** |
| 16–22 | SPLIT/MERGE/MOVE_RANGE, admin | **gap** (admin) |

Wire integers are little-endian. TXN_BEGIN OK is `txn_id(u64) \| snapshot_ts(u64)`;
TXN_OP is `txn_id(u64) \| op(u8) \| key_len(u32) \| key \| [value_len(u32) \| value for put]`;
TXN_COMMIT/ROLLBACK request is `txn_id(u64)`; TXN_COMMIT OK is `commit_ts(u64)`.
Status `STATUS_TXN_CONFLICT = 3`. Data-path and TXN opcodes are wrapped with
`CLIENT\x00` token framing when a client token is configured.

Conformance vectors live in [`docs/clients/conformance/`](../../docs/clients/conformance/).
This client does not run the vector suite; the table above is the coverage map.

## Testing

```bash
cd clients/kaya-ts
npm test
```

CI runs the same command on Node 22 (job `kaya-ts` in `.github/workflows/ci.yml`).

## Status

Experimental, matching the KayaDB v0.1.x prototype. TLS is not implemented in
this client (front with a TLS sidecar if needed).
