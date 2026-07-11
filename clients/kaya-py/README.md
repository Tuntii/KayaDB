# kaya-py — KayaDB Python client

A pure-standard-library (no dependencies) synchronous client for KayaDB's TCP
client protocol. Byte-compatible with the Rust reference (`crates/kaya-client`)
and the Go client (`clients/kaya-go`); see
[`docs/clients/client-wire-protocol.md`](../../docs/clients/client-wire-protocol.md).

## Features

- `put` / `get` / `delete` / `scan` / `health` / `stats` / `hello`
- Connection reuse (keep-alive) with automatic reconnect
- Leader redirect on `NOT_LEADER` (follows the returned `host:port` hint)
- Optional client token auth (`CLIENT\x00` framing) for data-path ops
- Per-request timeout

## Usage

```python
from kaya import KayaClient

with KayaClient("127.0.0.1:7379", client_token="secret", timeout=5.0) as db:
    db.put(b"hello", b"world")
    assert db.get(b"hello") == b"world"
    for key, value in db.scan(b"he"):
        print(key, value)
    print(db.health())   # "leader" or "follower"
```

`get` returns `None` when the key is absent. Protocol errors raise
`KayaError` (or the subclasses `InvalidArgument` / `NotFound`).

## Testing

The tests use only the standard library and an in-process mock server:

```bash
cd clients/kaya-py
python -m unittest discover -s tests -p 'test_*.py' -v
# or, if pytest is installed:
pytest -q
```

## Status

Experimental, matching the KayaDB v0.1.x prototype. TLS is not yet implemented
in this client (front with a TLS sidecar if needed); the Rust client has native
TLS support.
