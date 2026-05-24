# kaya-client

Official async Rust client library for KayaDB.

`kaya-client` talks to a running `kayadb-server` over TCP and offers a small ergonomic API for common database operations. It also handles leader redirection automatically, so a request sent to a follower can be retried against the active leader without extra application code.

## Features

- Async client API built on `tokio`
- `put`, `get`, `delete`, `scan`, `health`, and `stats`
- Automatic retry on `NOT_LEADER` responses
- Minimal dependency surface inside the KayaDB workspace

## Example

```rust
use kaya_client::KayaClient;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:7379".parse()?;
    let mut client = KayaClient::connect(addr).await?;

    client.put(b"hello", b"world").await?;

    if let Some(value) = client.get(b"hello").await? {
        println!("{}", String::from_utf8_lossy(&value));
    }

    let role = client.health().await?;
    println!("node role: {role}");

    Ok(())
}
```

More complete examples live in:

- [`examples/client_tcp.rs`](examples/client_tcp.rs)
- [`examples/kaya_client_example.rs`](examples/kaya_client_example.rs)

## API overview

- `KayaClient::connect(addr)`
- `put(key, value)`
- `get(key)`
- `delete(key)`
- `scan(prefix)`
- `health()`
- `stats()`
- `set_max_redirects(max)`

## Leader redirection

In clustered mode, follower nodes may return a leader hint. `kaya-client` parses that hint, reconnects to the leader, and retries the request up to a configurable maximum number of redirects.

## When to use this crate

Use `kaya-client` if your application connects to a separate KayaDB server process.

If you want to embed the storage engine in-process without TCP, use `kaya-engine` instead.

## Related crates

- `../kaya-server` — server process exposing the protocol
- `../kaya-net` — wire codec and transport layer
- `../kayactl` — CLI client for debugging and manual inspection

See the [workspace README](../../README.md) for the full project overview.
