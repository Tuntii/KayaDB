#!/usr/bin/env bash
# Package workspace crates for PR CI when workspace version is not yet on crates.io.
# Publishes to a temporary local registry in dependency order, then validates packaging.
set -euo pipefail

REGISTRY_DIR="${RUNNER_TEMP:-/tmp}/kaya-local-registry"
mkdir -p "${REGISTRY_DIR}/index" .cargo

cat >> .cargo/config.toml <<EOF

[registries.local]
index = "sparse+file://${REGISTRY_DIR}/index"
EOF

export CARGO_REGISTRIES_LOCAL_TOKEN="ci-dry-run"

# Dependency order (kaya-net required by kayactl but omitted from crates.io publish list).
ORDER=(
  kaya-core
  kaya-io
  kaya-raft
  kaya-wal
  kaya-lsm
  kaya-engine
  kaya-net
  kaya-client
  kayactl
)

for crate in "${ORDER[@]}"; do
  echo "::group::dry-run publish ${crate}"
  cargo publish -p "${crate}" --registry local --no-verify --allow-dirty
  echo "::endgroup::"
done