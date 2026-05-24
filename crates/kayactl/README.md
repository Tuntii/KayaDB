# kayactl

Command-line interface for KayaDB.

`kayactl` is the operator and debugging tool for the workspace. It can talk either to:

- a local embedded engine on disk, or
- a running `kayadb-server` over TCP.

It also includes format-inspection and recovery-report commands for WAL, SSTable, and manifest files.

## Command groups

### Local engine mode

Operate directly on a data directory:

- `put <key> <value>`
- `get <key>`
- `delete <key>`
- `scan <prefix>`
- `stats`
- `recover --dry-run`

### Server mode

Send commands to a running node with `--server <addr>`:

- `put <key> <value>`
- `get <key>`
- `delete <key>`
- `scan <prefix>`
- `health`
- `status`

### Inspect mode

Inspect on-disk files without custom tooling:

- `inspect wal <path>`
- `inspect sstable <path>`
- `inspect manifest <path>`

## Examples

### Local mode

```bash
cargo run -p kayactl -- put hello world
cargo run -p kayactl -- get hello
cargo run -p kayactl -- scan he
cargo run -p kayactl -- stats
cargo run -p kayactl -- recover --dry-run
```

### Server mode

```bash
cargo run -p kayactl -- --server 127.0.0.1:7379 put hello world
cargo run -p kayactl -- --server 127.0.0.1:7379 get hello
cargo run -p kayactl -- --server 127.0.0.1:7379 health
cargo run -p kayactl -- --server 127.0.0.1:7379 status
```

### JSON output

Most commands support `--json` for machine-readable output:

```bash
cargo run -p kayactl -- --json stats
cargo run -p kayactl -- --server 127.0.0.1:7379 --json status
```

## Flags

- `--data <dir>` — local engine data directory (default: `./data`)
- `--durability strict|relaxed` — local write durability mode
- `--server <addr>` — target remote server instead of local engine
- `--json` — print JSON output when supported

## Why this tool matters

KayaDB emphasizes inspectability. `kayactl` is a major part of that story because it lets you:

- inspect storage formats by hand,
- verify recovery state after a crash,
- poke a cluster without writing application code,
- compare local and remote behavior quickly.

## Related crates

- `../kaya-engine` — local embedded engine API
- `../kaya-server` — remote TCP server process
- `../kaya-wal` / `../kaya-lsm` — inspected file formats

See the [CLI reference](../../docs/cli-reference.md) and [workspace README](../../README.md) for more.
