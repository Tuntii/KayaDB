#!/usr/bin/env bash
# Kill a specific node (Jepsen nemesis: crash simulation)
set -euo pipefail

NODE_ID="${1:-}"
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"

if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <node-id>"
  echo "  node-id: 1, 2, or 3"
  exit 1
fi

pid_file="$CLUSTER_DIR/node$NODE_ID.pid"

if [ ! -f "$pid_file" ]; then
  echo "Error: Node $NODE_ID PID file not found"
  exit 1
fi

pid=$(cat "$pid_file")

if ! kill -0 "$pid" 2>/dev/null; then
  echo "Node $NODE_ID (PID $pid) already stopped"
  exit 0
fi

echo "Killing node $NODE_ID (PID $pid) with SIGKILL..."
kill -9 "$pid"
rm -f "$pid_file"

echo "Node $NODE_ID killed"
echo "To restart: ./scripts/restart-node.sh $NODE_ID"
