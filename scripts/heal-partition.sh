#!/usr/bin/env bash
# Heal / un-partition a specific node
set -euo pipefail

NODE_ID="${1:-}"
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"

if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <node-id>"
  echo "  node-id: 1, 2, or 3"
  exit 1
fi

COMMENT="kaya-partition-n${NODE_ID}"

echo "[Heal] Restoring connectivity for node $NODE_ID (comment: $COMMENT) ..."

# Remove any rules with our comment (both OUTPUT and INPUT)
for CHAIN in OUTPUT INPUT; do
  # Find and delete rules with the comment (loop in case multiple)
  while sudo iptables -L "$CHAIN" -n --line-numbers 2>/dev/null | grep -q "$COMMENT"; do
    LINE=$(sudo iptables -L "$CHAIN" -n --line-numbers 2>/dev/null | grep "$COMMENT" | head -1 | awk '{print $1}')
    if [ -n "$LINE" ]; then
      sudo iptables -D "$CHAIN" "$LINE" 2>/dev/null || true
    else
      break
    fi
  done
done

echo "[Heal] Partition rules for node $NODE_ID removed (if any existed)."
echo "Connectivity should be restored."