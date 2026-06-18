#!/usr/bin/env bash
# Restart a specific node (Jepsen nemesis: recovery simulation)
set -euo pipefail

NODE_ID="${1:-}"
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"
KAYA_SERVER="${KAYA_SERVER:-kayadb-server}"

if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <node-id>"
  echo "  node-id: 1, 2, or 3"
  exit 1
fi

pid_file="$CLUSTER_DIR/node$NODE_ID.pid"

# Stop if running
if [ -f "$pid_file" ]; then
  pid=$(cat "$pid_file")
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping node $NODE_ID (PID $pid)..."
    kill "$pid"
    for _ in {1..10}; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid"
    fi
  fi
  rm -f "$pid_file"
fi

# Start node
echo "Starting node $NODE_ID..."

case $NODE_ID in
  1)
    $KAYA_SERVER \
      --node-id 1 \
      --raft-addr 127.0.0.1:7481 \
      --client-addr 127.0.0.1:7379 \
      --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
      --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
      --data "$CLUSTER_DIR/node1" &
    ;;
  2)
    $KAYA_SERVER \
      --node-id 2 \
      --raft-addr 127.0.0.1:7482 \
      --client-addr 127.0.0.1:7380 \
      --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
      --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
      --data "$CLUSTER_DIR/node2" &
    ;;
  3)
    $KAYA_SERVER \
      --node-id 3 \
      --raft-addr 127.0.0.1:7483 \
      --client-addr 127.0.0.1:7381 \
      --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
      --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
      --data "$CLUSTER_DIR/node3" &
    ;;
  *)
    echo "Error: Invalid node ID $NODE_ID (must be 1, 2, or 3)"
    exit 1
    ;;
esac

echo $! > "$pid_file"
echo "Node $NODE_ID restarted (PID $(cat "$pid_file"))"
