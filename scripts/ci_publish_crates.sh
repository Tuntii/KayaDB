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
  curl -fsSL -A "kaya-ci-publish/1.0 (https://github.com/Tuntii/KayaDB)" \
    "https://crates.io/api/v1/crates/${crate}" \
    | grep -o '"max_version":"[^"]*"' \
    | head -1 \
    | sed 's/"max_version":"\(.*\)"/\1/' || true
}

publish_crate() {
  local crate="$1"
  local output
  if output="$(cargo publish -p "${crate}" --no-verify --allow-dirty 2>&1)"; then
    echo "$output"
    return 0
  fi
  echo "$output"
  if echo "$output" | grep -q 'already exists on crates.io'; then
    echo "SKIP ${crate}: already published"
    return 0
  fi
  return 1
}

for crate in "${ORDER[@]}"; do
  local_ver="$(local_version "$crate")"
  remote_ver="$(remote_version "$crate")"

  if [[ -n "$remote_ver" && "$local_ver" == "$remote_ver" ]]; then
    echo "SKIP ${crate}: already at ${local_ver} on crates.io"
    continue
  fi

  echo "::group::Publishing ${crate} (${remote_ver:-none} -> ${local_ver})"
  for attempt in 1 2 3; do
    if publish_crate "$crate"; then
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