#!/usr/bin/env bash
# Package workspace crates for PR CI when workspace version is not yet on crates.io.
# Publishes to a temporary local git registry in dependency order, then validates packaging.
set -euo pipefail

REGISTRY_DIR="${RUNNER_TEMP:-/tmp}/kaya-local-registry"
mkdir -p .cargo "${REGISTRY_DIR}/dl"

pushd "${REGISTRY_DIR}" >/dev/null
git init index >/dev/null
printf '{"dl":"file://%s/dl"}\n' "${REGISTRY_DIR}" > index/config.json
git -C index add config.json
git -C index -c user.email=ci@kaya.dev -c user.name=ci commit -m "init" >/dev/null
popd >/dev/null

cat > .cargo/config.toml <<EOF
[registries.local]
index = "file://${REGISTRY_DIR}/index"
EOF

export CARGO_REGISTRIES_LOCAL_TOKEN="ci-dry-run"

# Dependency order (kaya-net required by kayactl).
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