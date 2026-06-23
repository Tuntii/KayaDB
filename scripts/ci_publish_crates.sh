#!/usr/bin/env bash
# Publish workspace crates in dependency order; skip crates already on crates.io.
set -euo pipefail

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

workspace_version() {
  grep -m1 '^version = ' Cargo.toml | sed 's/.*"\(.*\)".*/\1/'
}

local_version() {
  local crate="$1"
  local manifest="crates/${crate}/Cargo.toml"
  if grep -q 'version\.workspace = true' "$manifest"; then
    workspace_version
  else
    grep -m1 '^version = ' "$manifest" | sed 's/.*"\(.*\)".*/\1/'
  fi
}

remote_version() {
  local crate="$1"
  curl -fsSL "https://crates.io/api/v1/crates/${crate}" \
    | grep -o '"max_version":"[^"]*"' \
    | head -1 \
    | sed 's/"max_version":"\(.*\)"/\1/' || true
}

LOCAL_WS_VER="$(workspace_version)"

for crate in "${ORDER[@]}"; do
  local_ver="$(local_version "$crate")"
  remote_ver="$(remote_version "$crate")"

  if [[ "$local_ver" == "$remote_ver" ]]; then
    echo "SKIP ${crate}: already at ${local_ver} on crates.io"
    continue
  fi

  echo "::group::Publishing ${crate} (${remote_ver:-none} -> ${local_ver})"
  for attempt in 1 2 3; do
    if cargo publish -p "${crate}" --no-verify --allow-dirty; then
      break
    fi
    if [[ $attempt -eq 3 ]]; then
      echo "FAILED to publish ${crate} after 3 attempts"
      exit 1
    fi
    echo "Publish attempt ${attempt} failed; retrying in 15s..."
    sleep 15
  done
  echo "Waiting 30s for crates.io index propagation..."
  sleep 30
  echo "::endgroup::"
done