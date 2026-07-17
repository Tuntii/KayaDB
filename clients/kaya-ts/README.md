# kaya-ts — KayaDB TypeScript client

Minimal zero-dependency Node.js TCP client for KayaDB's client wire protocol.
Byte-compatible with the Rust reference (`crates/kaya-client`), Go
(`clients/kaya-go`), and Python (`clients/kaya-py`) clients. See
[`docs/clients/client-wire-protocol.md`](../../docs/clients/client-wire-protocol.md).

## Features

- `put` / `get` / `delete` / `health` / `hello`
- Connection reuse with reconnect on transport errors
- Leader redirect on `NOT_LEADER` (follows the returned `host:port` hint)
- Optional client token auth (`CLIENT\x00` framing) for data-path ops
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
(or `InvalidArgument` / `NotFound`).

## Testing

```bash
cd clients/kaya-ts
npm test
```

## Status

Experimental, matching the KayaDB v0.1.x prototype. TLS is not implemented in
this client (front with a TLS sidecar if needed).
