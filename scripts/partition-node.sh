#!/usr/bin/env bash
# Partition a specific node (Jepsen nemesis: network isolation)
# Linux version using iptables (usually requires sudo/root).
set -euo pipefail

NODE_ID="${1:-}"
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"

if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <node-id>   (or set NODE_ID env)"
  echo "  node-id: 1, 2, or 3"
  exit 1
fi

case "$NODE_ID" in
  1) CLIENT_PORT=7379; RAFT_PORT=7481 ;;
  2) CLIENT_PORT=7380; RAFT_PORT=7482 ;;
  3) CLIENT_PORT=7381; RAFT_PORT=7483 ;;
  *)
    echo "Error: Invalid node ID $NODE_ID (must be 1, 2, or 3)"
    exit 1
    ;;
esac

COMMENT="kaya-partition-n${NODE_ID}"

echo "[Partition] Isolating node $NODE_ID (ports: $CLIENT_PORT,$RAFT_PORT) ..."

# Add DROP rules for OUTPUT (and INPUT for good measure) targeting the node's ports on loopback.
# Using a comment allows easy targeted removal on heal.
for PORT in $CLIENT_PORT $RAFT_PORT; do
  sudo iptables -I OUTPUT 1 -p tcp -d 127.0.0.1 --dport "$PORT" -m comment --comment "$COMMENT" -j DROP 2>/dev/null || true
  sudo iptables -I INPUT  1 -p tcp -s 127.0.0.1 --sport "$PORT" -m comment --comment "$COMMENT" -j DROP 2>/dev/null || true
done

echo "[Partition] iptables rules added for node $NODE_ID (comment: $COMMENT)."
echo "Node $NODE_ID is now partitioned. Use heal-partition.sh to restore."
echo "Note: requires iptables + appropriate privileges. On some distros 'sudo' may be needed."