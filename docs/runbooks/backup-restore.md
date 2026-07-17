# Backup & Restore (Day-2 Operations)

KayaDB stores all durable state under `--data` (the node data directory).

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
   ./kayadb-server --data ./restored-data/nodeN --node-id N --raft-addr ... --client-addr ... --peer ...
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

## Built-in backup command

`kayactl backup` copies a node's durable state to a backup directory:

```bash
# Full backup
kayactl backup --data ./data/nodeN --out /backups/kaya/nodeN

# Incremental: skips immutable files (SSTables, sealed WAL segments) already
# present in the destination with the same size; copies only new/changed files.
kayactl backup --data ./data/nodeN --out /backups/kaya/nodeN --incremental
```

Each file is copied via a temp file + rename so a partial copy never leaves a
truncated file at the destination. For a point-in-time-consistent snapshot,
stop the node first — a live backup is safe for the immutable SSTables but the
WAL/manifest may be mid-write. Add `--json` for machine-readable output
(`copied`, `skipped`, `bytes_copied`).


## CDC checkpoints and backup watermarks (M19 polish)

`Engine::cdc_checkpoint(consumer_id)` persists a consumer's last delivered
sequence under `{data_dir}/cdc/cursors/{id}`.

Link a filesystem backup to that watermark:

```bash
# After consuming/checkpointing as consumer "backup":
kayactl backup --data ./data/nodeN --out /backups/kaya/nodeN \
  --incremental --cdc-consumer backup
```

This writes `dest/cdc/backup_watermark` with the consumer's durable sequence
and includes `cdc_watermark` in `--json` output. See `spec/docs/cdc-spec.md`.

## Automation
See `kayactl recover --dry-run` for inspecting a data directory before reuse.

See also:
- `docs/runbooks/rolling-restart.md`
- `docs/security.md` (directory permissions + TLS)
- `docs/runbooks/mtls-sidecar.md`