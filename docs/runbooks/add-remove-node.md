# Add / Remove Node (Day-2 Operations)

This runbook covers safely adding and removing nodes from a KayaDB cluster using `kayactl`.

## Prerequisites
- Cluster is healthy (majority of nodes up, a stable leader).
- You have network access to at least one node (preferably current leader).
- If the server was started with an operator token (`--operator-token` or `KAYA_OPERATOR_TOKEN`), you must provide it for membership operations.

## Add a Node

### 1. Start the new node in join mode
```bash
./kayadb-server \
  --id 4 \
  --data-dir ./data/node4 \
  --raft-addr 127.0.0.1:7484 \
  --client-addr 127.0.0.1:7384 \
  --join-cluster 127.0.0.1:7481,127.0.0.1:7482,127.0.0.1:7483
```

The node will appear in the cluster as a non-voter until you promote it.

### 2. Add it to the voter set (from any node, ideally current leader)
```bash
# With token (if required)
kayactl --server 127.0.0.1:7379 \
  --operator-token "$KAYA_OPERATOR_TOKEN" \
  add-node 4 127.0.0.1:7484 127.0.0.1:7384

# Without token
kayactl --server 127.0.0.1:7379 add-node 4 127.0.0.1:7484 127.0.0.1:7384
```

Wait for the config change to apply:
```bash
kayactl --server 127.0.0.1:7379 status
# Look for peer_count increasing and role=Follower on the new node
```

### 3. Verify
- New node shows as `voter` in status on all nodes.
- You can read/write through the new node's client port.

## Remove a Node

**Important:** You cannot shrink below 2 voters in a 3-node (or larger) cluster without careful planning.

```bash
kayactl --server 127.0.0.1:7379 \
  --operator-token "$KAYA_OPERATOR_TOKEN" \
  remove-node 4
```

Monitor until `peer_count` decreases on all remaining nodes.

## Tips
- Always target a current leader when possible (`kayactl status` shows role).
- Use `--operator-token` (or set `KAYA_OPERATOR_TOKEN`) if the servers require it.
- After removal, you can safely shut down the node process and delete its data dir (after making sure it is no longer in the roster).

See also:
- `docs/runbooks/rolling-restart.md`
- `docs/security.md` (operator credentials section)
- `scripts/start-cluster.sh` and related scripts for local testing.