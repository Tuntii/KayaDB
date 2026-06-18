#!/usr/bin/env bash
# Stop a running KayaDB cluster
set -euo pipefail

CLUSTER_DIR="${CLUSTER_DIR:-/tmp/kayadb-cluster}"

echo "Stopping KayaDB cluster..."

for i in 1 2 3; do
  pid_file="$CLUSTER_DIR/node$i.pid"
  if [ -f "$pid_file" ]; then
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping node $i (PID $pid)..."
      kill "$pid"
      # Wait for graceful shutdown
      for _ in {1..10}; do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.5
      done
      # Force kill if still running
      if kill -0 "$pid" 2>/dev/null; then
        echo "  Force killing node $i..."
        kill -9 "$pid"
      fi
    else
      echo "Node $i (PID $pid) already stopped"
    fi
    rm -f "$pid_file"
  else
    echo "Node $i PID file not found"
  fi
done

echo "Cluster stopped"
