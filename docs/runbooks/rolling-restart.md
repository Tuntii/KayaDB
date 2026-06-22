# Rolling Restart (Day-2 Operations)

Goal: restart nodes one-by-one with minimal disruption.

## Prerequisites
- Cluster has a healthy leader.
- You can monitor `kayactl status` and applied indices.
- Operator token if required for any admin actions.

## Procedure (one node at a time)

1. Pick a **follower** (never the leader first).
   ```bash
   kayactl --server <any-node> status
   ```

2. Stop the follower gracefully (or kill if testing).
   - Record its applied index before stopping if possible.

3. Wait for the cluster to stabilize (new leader elected if needed).

4. Restart the node with the same `--data`, addresses, and peer list.
   ```bash
   ./kayadb-server \
     --node-id N \
     --data ./data/nodeN \
     --raft-addr 127.0.0.1:748N \
     --client-addr 127.0.0.1:737N \
     --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
     --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
     --peer 3=127.0.0.1:7483,127.0.0.1:7381
   ```
   Or use `scripts/restart-node.sh <N>` / `scripts/restart-node.ps1 -NodeId N` for the bundled 3-node demo.

5. Wait until the node reports as follower and its applied index catches up.
   ```bash
   kayactl --server <this-node-client-port> status
   # Check "applied_index"
   ```

6. Repeat for the next follower.

7. Finally restart the original leader (after a new stable leader exists).

## Verification
- After every restart, `peer_count` should return to the expected value.
- You can read/write through any client port.
- No linearizability violations in ongoing workloads (if running chaos or client load).

## With Operator Token
The token is not usually needed for pure restarts (only for membership changes). If using `--tls-*` for native TLS, ensure clients/kayactl connect via the TLS-wrapped ports.

## Tips
- Do not restart more than one node at a time.
- In production, combine with load balancer / client retry logic.
- Use `scripts/restart-node.sh` (or `.ps1`) for local/scripted testing.
- When TLS is enabled, verify the restarted node re-establishes TLS handshakes successfully.

See also:
- `docs/runbooks/add-remove-node.md`
- `docs/runbooks/detecting-split-brain.md`
- `docs/runbooks/mtls-sidecar.md` and native TLS in security.md