#!/usr/bin/env bash
# Assemble the full GitHub Pages / Docsify site from docs/ plus repo-root companions.
# Usage: scripts/prepare_docs_site.sh [output-dir]
# Default output: build/docs-site/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/build/docs-site}"

rm -rf "$OUT"
mkdir -p "$OUT"

cp -r "$ROOT/docs/." "$OUT/"
for f in "$ROOT/docs"/.*; do
  [ -e "$f" ] || continue
  base="$(basename "$f")"
  [ "$base" = "." ] || [ "$base" = ".." ] && continue
  cp -a "$f" "$OUT/" 2>/dev/null || true
done

touch "$OUT/.nojekyll"
rm -rf "$OUT/superpowers"

for f in ROADMAP.md CHANGELOG.md CONTRIBUTING.md BENCHMARKS.md CODE_OF_CONDUCT.md; do
  if [ -f "$ROOT/$f" ]; then
    cp "$ROOT/$f" "$OUT/"
  fi
done

if [ -f "$OUT/ROADMAP.md" ]; then
  if sed --version >/dev/null 2>&1; then
    sed -i -E 's|\]\(docs/|\]\(|g' "$OUT/ROADMAP.md"
  else
    sed -i '' -E 's|\]\(docs/|\]\(|g' "$OUT/ROADMAP.md"
  fi
fi

mkdir -p "$OUT/deploy/docker" "$OUT/deploy/k8s"
cp "$ROOT/deploy/docker/README.md" "$OUT/deploy/docker/"
cp "$ROOT/deploy/k8s/README.md" "$OUT/deploy/k8s/"

mkdir -p "$OUT/spec/docs" "$OUT/spec/issues"
cp -r "$ROOT/spec/docs/." "$OUT/spec/docs/"
if [ -f "$ROOT/spec/issues/expanded-implementation-roadmap.md" ]; then
  cp "$ROOT/spec/issues/expanded-implementation-roadmap.md" "$OUT/spec/issues/"
fi

echo "Docs site prepared at $OUT"