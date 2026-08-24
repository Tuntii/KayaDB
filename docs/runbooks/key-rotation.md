# Encryption Key Rotation (#28)

Goal: rotate the AES-256-GCM key that seals a node's data-at-rest files (WAL, SSTables, manifest via `EncryptedDisk`) without downtime and without losing access to already-written data.

See `docs/security.md` §7.1 for the on-disk format and guarantees. Summary: reads decrypt by the key id stored in each file's own header (legacy files are id `0`); writes always reseal under the active key. There is no background re-encrypt job — old files upgrade to the new key lazily, the next time something writes them. This is an accepted, documented limitation, not a bug.

## Prerequisites

- `kayactl` built from this checkout (`cargo build -p kayactl` or the released binary).
- Shell/file access to the node's config location (to update `--encryption-keyring-file` and restart), or your deployment automation's equivalent.
- If migrating an existing single-key deployment: the current `--encryption-key-file` path.

## One-time: adopt a keyring

If the node is not encrypted yet:

```bash
kayactl encryption init --keyring /etc/kayadb/keyring.txt
# active_id=0, key_ids=[0]
```

If the node already runs with `--encryption-key-file /etc/kayadb/key.bin` (single, non-rotating key), migrate without re-encrypting anything — the existing key becomes id 0:

```bash
kayactl encryption init --keyring /etc/kayadb/keyring.txt --from-key-file /etc/kayadb/key.bin
```

Restart the node with the keyring instead of the raw key file:

```bash
# before
./kayadb-server ... --encryption-key-file /etc/kayadb/key.bin
# after
./kayadb-server ... --encryption-keyring-file /etc/kayadb/keyring.txt
```

`--encryption-key-file` and `--encryption-keyring-file` are mutually exclusive. Existing `KAYAENC1` files decrypt unchanged (they're implicitly key id 0); nothing is rewritten by this step alone.

Protect `keyring.txt` the same way you would `key.bin`: file mode `0600`, not committed to version control, ideally sourced from a secrets manager.

## Rotate

```bash
kayactl encryption rotate --keyring /etc/kayadb/keyring.txt
# rotated /etc/kayadb/keyring.txt: active_id=1 key_ids=[0, 1]
```

This generates a fresh 32-byte key from the OS CSPRNG, makes it the active id, and keeps every previously-active key in the ring so files not yet rewritten stay readable.

Distribute the updated `keyring.txt` to the node (and to every replica in the cluster — each node has its own local `EncryptedDisk`, so rotate per node or push the same keyring to all of them) and restart the process so it picks up the new active key:

```bash
# rolling restart, one node at a time (see docs/runbooks/rolling-restart.md)
systemctl restart kayadb-server
```

From this point:
- **New writes** (new WAL bytes, new SSTables from flush/compaction, manifest updates) are sealed under the new active key.
- **Existing files** sealed under the previous key are still fully readable — the dual-key window is unconditional and does not expire on its own.

## Verify

Before and after a rotation, confirm the keyring can open everything currently on disk:

```bash
kayactl encryption verify --data /var/lib/kayadb --keyring /etc/kayadb/keyring.txt
# checked N encrypted file(s): N ok, 0 failed
```

Non-zero exit status and a `FAIL <path>: <reason>` line per failure if anything doesn't decrypt. The command never prints key material, only paths and ids.

## Complete the migration off a retired key (optional)

Because there is no background rewrite, a key you rotated away from remains *needed* until every file that used it has been naturally rewritten. To force that:

- Let normal operation run: WAL segments roll, memtables flush to new SSTables, compaction rewrites old SSTables — all under the active key.
- Or force it: `kayactl backup --data <dir> --out <fresh-dir>` performs a filesystem copy (files keep whatever key sealed them, so this alone does not upgrade anything), so instead stand up a fresh node/directory and let the engine's normal write path (a full compaction pass, or replaying data through PUTs) rewrite everything under the active key, then decommission the old directory.
- Confirm completion by checking a keyring **without** the retired id:

```bash
kayactl encryption verify --data /var/lib/kayadb --keyring /tmp/pruned-keyring.txt
```

Only remove the retired `key <id> <hex>` line from the real keyring file once verify reports zero failures against a keyring lacking that id — removing it too early makes any not-yet-rewritten file permanently unreadable.

## Crash mid-rotation

- **Keyring file**: `kayactl encryption rotate`/`init` write the whole keyring file in one `std::fs::write`; `load_keyring_file` rejects a keyring whose `active` id has no matching `key` line, so a torn write is caught at node startup rather than silently accepted. Keep the previous `keyring.txt` as a backup until you've confirmed the node started cleanly with the new one.
- **Data files**: a crash while `EncryptedDisk` is resealing a file mid-write is bounded by the same durability discipline the engine already relies on for WAL/SST/manifest writes (fsync + recovery on reopen); the encryption layer adds no new unsynced window. Run `kayactl recover --dry-run --data <dir>` after an unclean shutdown as usual (see `docs/security.md` §6).

## Rollback

If a rotation was pushed in error, restore the previous `keyring.txt` (which still lists every key id, including the one you just made active) and restart — no data was re-encrypted destructively, so any files already rewritten under the new key are still readable as long as that key's `key <id> <hex>` line stays in the ring.
