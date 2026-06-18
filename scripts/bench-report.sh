#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export KAYADB_GIT_COMMIT="${KAYADB_GIT_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export KAYADB_RUSTC="${KAYADB_RUSTC:-$(rustc --version 2>/dev/null || echo unknown)}"

cargo bench -p kaya-bench --bench smoke -- --noplot

OUT_DIR="$ROOT/target/bench-reports"
mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="$OUT_DIR/smoke-$STAMP.jsonl"

printf '%s\n' "{\"bench\":\"smoke_put_get\",\"commit\":\"$KAYADB_GIT_COMMIT\",\"profile\":\"release\",\"durability\":\"relaxed\",\"ops\":10}" > "$REPORT"
echo "Wrote benchmark report: $REPORT"