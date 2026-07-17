# Decommission a Node (Day-2 Operations)

Goal: remove a node from a live cluster without stranding leadership or data, then wipe its local store.

Safe order: **transfer leaders away → remove member → stop process → wipe `data_dir`**.

## Prerequisites

- Cluster is healthy (majority of voters up, stable leader(s)).
- You can reach at least one remaining node with `kayactl` (preferably a current leader).
- Operator token if the cluster was started with `--operator-token` / `KAYA_OPERATOR_TOKEN`.
- Target node identity (`--node-id N`) and its `data_dir`.

## Drain mode (recommended)

Restart (or start) the target node with drain enabled so operators and tooling can see intent:

```bash
# Flag
./kayadb-server --drain --node-id N ...same peers and data_dir...

# Or environment
KAYA_DRAIN=1 ./kayadb-server --node-id N ...
```

Confirm via status JSON (STATS opcode / `kayactl status`):

```bash
kayactl --server <target-client-addr> status
# Expect "drain": true in the JSON (and Drain: true in human output)
```

**What drain does today (M22):**

| Behavior | Effect |
|----------|--------|
| Status / health JSON | Includes `"drain": true` |
| Existing leadership | Still serves writes/reads as leader until you transfer |
| New range hosting | `SPLIT_RANGE` on this node is rejected |
| Elections | Node still votes; it may still start or win elections until leaders are transferred |

Drain is a **marker + placement guard**, not a full “never lead again” implementation. Always transfer leadership before removal.

## Procedure

### 1. Transfer leaders away from the target

For every Raft group where the target is leader (start with group `0`; multi-raft: each hosted group), call **TRANSFER_LEADER (18)** on the current leader of that group with a healthy voter as `target_node_id`.

Request body (LE):

```
group_id        : u64
target_node_id  : u64   # preferred successor voter (not the decommissioned node)
```

Requires the operator token when configured (`ADMIN\x00` framing via `encode_admin_payload`, same as ADD/REMOVE_MEMBER).

**Semantics (minimal M22):** if the callee is not leader → `STATUS_NOT_LEADER`; if `target == self` → success no-op; otherwise the leader becomes a follower. TimeoutNow is **not** sent; the next election is free among voters. Prefer a caught-up target, then wait for a stable new leader:

```bash
kayactl --server <any-remaining-node> status
# role should not be Leader on the node you are decommissioning
```

Repeat until the draining node is a follower (or non-leader) on all groups.

### 2. Remove the member from the voter set

From a current leader (or any node that will redirect):

```bash
kayactl --server 127.0.0.1:7379 \
  --operator-token "$KAYA_OPERATOR_TOKEN" \
  remove-node N
```

Wait until remaining nodes show a reduced `peer_count` and the cluster still has a leader:

```bash
kayactl --server <remaining-node> status
```

Do not shrink below a safe majority plan (e.g. do not go from 3 voters to 1 without intent).

### 3. Stop the process on the decommissioned node

Stop `kayadb-server` on node `N` (SIGTERM / service stop / `scripts/stop-cluster` for local demos). It must no longer be in the roster before you treat disk as reclaimable.

### 4. Wipe `data_dir`

Only after step 2 has applied cluster-wide and the process is stopped:

```bash
# Example: local demo layout
rm -rf ./data/nodeN
# Or whatever path was passed as --data
rm -rf /var/lib/kayadb/nodeN
```

Wiping while the node is still a voter risks rejoin with stale Raft state. Prefer a clean empty directory if the machine is ever reused with a **new** node id.

## Verification

- `kayactl status` on every remaining node: expected `peer_count`, stable leader, no references to `N` in operational roster checks you rely on.
- Client put/get through remaining client ports succeeds.
- Decommissioned host has no running `kayadb-server` and empty (or deleted) data directory.

## Rollback notes

- If you have not yet called `remove-node`, clear drain by restarting **without** `--drain` / `KAYA_DRAIN` and transfer leadership back if needed.
- After `remove-node` has applied, re-adding requires the join path (`--join-cluster` + `add-node`); do not reuse a wiped directory under the old id without a deliberate rejoin.

## Tips

- Prefer decommissioning a **follower** after transfer, never “kill the sole leader and delete disk.”
- Combine with [rolling restart](rolling-restart.md) only when replacing hardware in place; full decommission always ends with membership remove + data wipe.
- With native TLS (`--tls-*`), point `kayactl` at the TLS client port. See [mtls-sidecar](mtls-sidecar.md) and `docs/security.md`.

See also:

- [Add / remove node](add-remove-node.md)
- [Rolling restart](rolling-restart.md) (TRANSFER_LEADER details)
- [Detecting split-brain](detecting-split-brain.md)
