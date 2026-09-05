# Detecting Split-Brain or Divergence

## Quick Health Check
```bash
# Run against multiple nodes
for port in 7379 7380 7381; do
  echo "=== Node on :$port ==="
  kayactl --server 127.0.0.1:$port status
done
```

Look for:
- Different `term`
- Very different `applied_index` or `commit_index`
- `peer_count` not matching expected voter count
- Different leaders reported

## Using kayactl status (JSON)
```bash
kayactl --server 127.0.0.1:7379 --json status | jq '.'
```

Key fields to compare across nodes (from `kayactl status --json`):
- `term`
- `applied_index`
- `commit_index`
- `role`
- `peer_count`

`last_log_index` is not exposed in status JSON; use `inspect wal` on each node's data dir for log tail comparison.

## Using Inspect
```bash
kayactl inspect manifest ./data/nodeN/MANIFEST
kayactl inspect wal ./data/nodeN/wal-000001.wal
```

Compare `last_sequence` in manifest output and recent WAL records across nodes.

## Common Split-Brain Indicators
- Two different nodes both claim to be leader at the same time.
- Writes succeed on one partition but are not visible on another after healing.
- `term` has jumped on some nodes but not others without corresponding membership change.

## Recovery Steps (high level)
1. Identify the partition with the highest term + longest log.
2. Stop the minority/wrong side nodes.
3. Use `kayactl --data ./data/nodeN recover --dry-run` or manual inspection to understand state.
4. Restart the wrong-side nodes so they rejoin the correct partition (they will catch up via Raft).
5. If necessary, force-remove bad nodes with `remove-node` (requires operator token if configured) and re-add clean nodes.

**Never** blindly take the highest term node as truth without verifying data.

## Prevention
- Proper firewall / network segmentation.
- Use `--operator-token` to protect membership changes.
- When using TLS (native or sidecar), ensure all nodes and clients have correct certs and CAs to prevent connection failures that look like split-brain.
- Monitor terms and applied indices continuously (e.g. via Prometheus + kayactl status scraping).

See also:
- `docs/runbooks/add-remove-node.md`
- `docs/runbooks/dashboard.md` (`GET /v1/leadership` and per-range `healthy` on each node's `--dashboard-addr`)
- `docs/security.md` (TLS and operator token sections)
- `docs/runbooks/mtls-sidecar.md`
- Chaos tests in `kaya-jepsen-test` (they exercise these scenarios).