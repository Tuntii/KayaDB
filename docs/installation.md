# Installation

KayaDB ships as Rust crates on [crates.io](https://crates.io) and as pre-built binaries on [GitHub Releases](https://github.com/Tuntii/KayaDB/releases). You can also build everything from source.

**Current stable tag:** `v0.1.45` — see [Releases](releases.md) for notes.

---

## Requirements

| Requirement | Version |
|---|---|
| Rust toolchain | 1.85+ ([`rust-toolchain.toml`](../rust-toolchain.toml)) |
| Platform | Linux, macOS, Windows (x86_64 and Apple Silicon for releases) |

---

## Option 1 — Install from crates.io (recommended)

### CLI only

```bash
cargo install kayactl
kayactl --help
```

Embedded mode works out of the box — no server required:

```bash
kayactl --data ./data put key value
```

### Server binary

```bash
cargo install kaya-server --bin kayadb-server
kayadb-server --help
```

### Rust library (embedded engine)

Add to `Cargo.toml`:

```toml
[dependencies]
kaya-engine = "0.1.44"
kaya-io = "0.1.44"
kaya-core = "0.1.44"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

For remote access:

```toml
kaya-client = "0.1.44"
```

Published crates (dependency order): `kaya-core`, `kaya-io`, `kaya-wal`, `kaya-lsm`, `kaya-engine`, `kaya-raft`, `kaya-net`, `kaya-client`, `kaya-server`, `kayactl`.

---

## Option 2 — Download release binaries

On each `v*` tag, CI builds `kayadb-server` and `kayactl` for:

| Target | Archive |
|---|---|
| `x86_64-unknown-linux-gnu` | `.tar.gz` |
| `x86_64-pc-windows-msvc` | `.zip` |
| `x86_64-apple-darwin` | `.tar.gz` |
| `aarch64-apple-darwin` | `.tar.gz` |

1. Open [GitHub Releases](https://github.com/Tuntii/KayaDB/releases)
2. Download the archive for your platform (e.g. `kayadb-v0.1.44-x86_64-unknown-linux-gnu.tar.gz`)
3. Extract and add the binaries to your `PATH`

```bash
tar -xzf kayadb-v0.1.44-x86_64-unknown-linux-gnu.tar.gz
./kayadb-server --data ./data --client-addr 127.0.0.1:7379
```

Release packages include `README.md`, `CHANGELOG.md`, and `security.md`.

---

## Option 3 — Build from source

```bash
git clone https://github.com/Tuntii/KayaDB.git
cd KayaDB
cargo build --release --workspace
```

Binaries land in `target/release/`:

- `kayadb-server`
- `kayactl`

Run without installing:

```bash
cargo run -p kayactl -- --data ./data put hello world
cargo run -p kaya-server --bin kayadb-server -- --data ./data
```

CI gates (run before contributing):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

---

## Optional features

### TLS (`tls` feature)

Native TLS (rustls) for Raft and client listeners:

```bash
cargo install kaya-server --bin kayadb-server --features tls
cargo install kayactl --features tls
```

Or from source:

```bash
cargo build --release -p kaya-server --features tls
cargo build --release -p kayactl --features tls
```

See [Security → TLS](security.md#3-transport-layer-encryption-tls-wrapper) and the [mTLS sidecar runbook](runbooks/mtls-sidecar.md).

### Operator token

Membership changes (`ADD_MEMBER` / `REMOVE_MEMBER`) accept an operator token when configured:

```bash
export KAYA_OPERATOR_TOKEN=your-secret
kayadb-server --operator-token "$KAYA_OPERATOR_TOKEN" ...
kayactl --operator-token "$KAYA_OPERATOR_TOKEN" add-node ...
```

---

## Verify installation

```bash
# Embedded smoke test
kayactl --data /tmp/kayadb-check put ping pong
kayactl --data /tmp/kayadb-check get ping
# expected: pong

# Server smoke test (two terminals)
kayadb-server --data /tmp/kayadb-srv --client-addr 127.0.0.1:7379
kayactl --server 127.0.0.1:7379 put ping pong
```

---

## Next steps

- [Getting started](getting-started.md) — first commands, cluster setup, client library
- [CLI reference](cli-reference.md) — full `kayactl` documentation
- [Releases](releases.md) — versioning and changelog