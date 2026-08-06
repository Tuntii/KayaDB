#!/usr/bin/env bash
# Validate publish packaging on PRs when workspace version is not yet on crates.io.
# Patches workspace crates to path deps so cargo package can resolve them locally.
set -euo pipefail

mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[patch.crates-io]
kaya-core = { path = "crates/kaya-core" }
kaya-io = { path = "crates/kaya-io" }
kaya-raft = { path = "crates/kaya-raft" }
kaya-wal = { path = "crates/kaya-wal" }
kaya-lsm = { path = "crates/kaya-lsm" }
kaya-engine = { path = "crates/kaya-engine" }
kaya-net = { path = "crates/kaya-net" }
kaya-client = { path = "crates/kaya-client" }
kaya-ebpf = { path = "crates/kaya-ebpf" }
kaya-server = { path = "crates/kaya-server" }
EOF

cargo package --no-verify -p kaya-engine
cargo package --no-verify -p kaya-server
cargo package --no-verify -p kayactl