# Backup & Restore (Day-2 Operations)

KayaDB stores all durable state under `--data-dir`.

## Backup

**Simple full backup (recommended for small/medium clusters):**

```bash
# Stop the node first (cleanest) or do a live copy with care
tar -czf kaya-node-$(date +%Y%m%d-%H%M%S).tar.gz ./data/nodeN
```

Or with rsync for incremental-style:

```bash
rsync -a --delete ./data/nodeN/ /backups/kaya/nodeN/
```

**What is backed up:**
- WAL files
- SSTables
- Manifest
- `raft-snapshot.bin` (if present)
- `cluster-roster.json`
- Any `raft-*.` files from durable Raft

## Restore

1. Stop the node.
2. Replace the data directory with the backup.
3. Start the node.
   ```bash
   ./kayadb-server --data-dir ./restored-data/nodeN ...
   ```
4. The node will recover from WAL + latest snapshot on startup.
5. Verify with `kayactl status` and sample reads.

## Cautions
- Never restore a node while it is still considered part of the active Raft cluster unless you are doing a full cluster restore.
- For a full cluster restore, restore **all** nodes to the same consistent backup point (or use snapshots + log from a point in time).
- After restore, you may need to re-add the node to the cluster using `add-node` if its membership state is stale.
- Backups containing real data are sensitive — protect them.

## With Operator Token
Restores themselves do not require the token; membership changes after restore do.

When native TLS or sidecars are in use, the backed-up data (including any persisted TLS-related state if any) can be restored to nodes that are started with the matching TLS configuration.

## Automation
See `kayactl recover --dry-run` for inspecting a data directory before reuse.

See also:
- `docs/runbooks/rolling-restart.md`
- `docs/security.md` (directory permissions + TLS)
- `docs/runbooks/mtls-sidecar.md`