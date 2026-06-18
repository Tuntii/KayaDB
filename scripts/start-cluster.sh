#!/usr/bin/env bash
# Start a 3-node KayaDB cluster for Jepsen-style testing
set -euo pipefail

CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"
KAYA_SERVER="${KAYA_SERVER:-kayadb-server}"

mkdir -p "$CLUSTER_DIR"

echo "Starting 3-node KayaDB cluster in $CLUSTER_DIR..."

# Node 1
$KAYA_SERVER \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data "$CLUSTER_DIR/node1" &
echo $! > "$CLUSTER_DIR/node1.pid"
echo "Node 1 started (PID $(cat "$CLUSTER_DIR/node1.pid"))"

# Node 2
$KAYA_SERVER \
  --node-id 2 \
  --raft-addr 127.0.0.1:7482 \
  --client-addr 127.0.0.1:7380 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data "$CLUSTER_DIR/node2" &
echo $! > "$CLUSTER_DIR/node2.pid"
echo "Node 2 started (PID $(cat "$CLUSTER_DIR/node2.pid"))"

# Node 3
$KAYA_SERVER \
  --node-id 3 \
  --raft-addr 127.0.0.1:7483 \
  --client-addr 127.0.0.1:7381 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --data "$CLUSTER_DIR/node3" &
echo $! > "$CLUSTER_DIR/node3.pid"
echo "Node 3 started (PID $(cat "$CLUSTER_DIR/node3.pid"))"

echo ""
echo "Cluster started. Client endpoints:"
echo "  Node 1: 127.0.0.1:7379"
echo "  Node 2: 127.0.0.1:7380"
echo "  Node 3: 127.0.0.1:7381"
echo ""
echo "To stop: ./scripts/stop-cluster.sh"
