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

### Optional: transfer leadership before restarting the leader

M22 exposes admin opcode **TRANSFER_LEADER (18)** so you can ask the current leader of a Raft group to step down before you stop it. Request body:

```
group_id        : u64 LE   (0 for the primary / single group)
target_node_id  : u64 LE   (preferred successor voter; may be self for a no-op check)
```

Requires the operator token when the cluster is started with `--operator-token` / `KAYA_OPERATOR_TOKEN` (same framing as ADD/REMOVE_MEMBER: optional `ADMIN\x00` prefix via `encode_admin_payload`).

**Semantics (minimal M22):** if the callee is not leader → `STATUS_NOT_LEADER`; if `target == self` → success no-op; otherwise the leader becomes a follower. This implementation does **not** send TimeoutNow to force the target to win the next election — the subsequent election is free among voters. Prefer transferring only when the target is caught up and healthy; then wait for a new stable leader via `kayactl status` before stopping the old leader.

## Verification
- After every restart, `peer_count` should return to the expected value.
- You can read/write through any client port.
- No linearizability violations in ongoing workloads (if running chaos or client load).

## With Operator Token
The token is not usually needed for pure restarts (only for membership changes and TRANSFER_LEADER). If using `--tls-*` for native TLS, ensure clients/kayactl connect via the TLS-wrapped ports.

## Tips
- Do not restart more than one node at a time.
- In production, combine with load balancer / client retry logic.
- Use `scripts/restart-node.sh` (or `.ps1`) for local/scripted testing.
- When TLS is enabled, verify the restarted node re-establishes TLS handshakes successfully.

See also:
- `docs/runbooks/add-remove-node.md`
- `docs/runbooks/detecting-split-brain.md`
- `docs/runbooks/mtls-sidecar.md` and native TLS in security.md