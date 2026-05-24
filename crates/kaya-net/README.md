# kaya-net

Wire codec, TCP transport layer, and node roster management for KayaDB clusters.

`kaya-net` is the boundary between protocol logic and sockets. It contains the framing and helper utilities used by both the server and the async client.

## What it provides

- client request/response framing
- Raft envelope encoding and decoding
- TCP helpers for sending batches of Raft messages
- TCP helpers for single-request client round trips
- cluster roster management with client and peer addresses
- status codes and opcodes shared by server/client implementations

## Public API highlights

- `roundtrip(...)`
- `encode_client_frame(...)`
- `read_client_frame(...)`
- `write_client_response(...)`
- `send_envelopes(...)`
- `start_raft_listener(...)`
- `NodeRoster`
- client payload codec helpers such as `encode_put_payload`, `decode_key_payload`, and `decode_scan_response`

## Example

```rust
use std::net::SocketAddr;

use kaya_net::roundtrip;

async fn health(addr: SocketAddr) -> std::io::Result<String> {
    let (status, body) = roundtrip(addr, 5, &[]).await?;
    assert_eq!(status, 0);
    Ok(String::from_utf8_lossy(&body).into_owned())
}
```

Opcode `5` is the current `HEALTH` request in the KayaDB client protocol.

## Client protocol

Request frame format:

- `frame_len(u32 LE)`
- `opcode(u8)`
- `payload`

Response frame format:

- `frame_len(u32 LE)`
- `status(u16 LE)`
- `payload`

Current exported status codes include:

- `STATUS_OK`
- `STATUS_NOT_FOUND`
- `STATUS_ERROR`
- `STATUS_NOT_LEADER`

## Why it is separate

Keeping the wire layer in its own crate makes it easier to:

- test codecs without the full server runtime,
- share transport helpers between `kaya-client` and `kaya-server`,
- evolve protocol framing independently from storage internals.

## Related crates

- `../kaya-server` — uses the transport for cluster and client traffic
- `../kaya-client` — uses `roundtrip` and payload codecs
- `../kaya-raft` — source of the replicated message types carried on the wire

See the [workspace README](../../README.md) for project-wide context.
