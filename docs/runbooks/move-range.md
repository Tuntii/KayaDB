# Move a Range (Live Rebalance)

Goal: move one key range onto a different Raft group while the cluster keeps
serving reads and writes, so load can be shifted off a hot node.

Safe order: **plan → move → land the leader → verify**.

Protocol, failure modes and invariants: [range-routing-spec §3d](../../spec/docs/range-routing-spec.md).

## Prerequisites

- Cluster healthy (majority of voters up, a stable leader for group 0).
- `kayactl` can reach a node; admin ops redirect to the group-0 leader on their own.
- Operator token if the cluster was started with `--operator-token` / `KAYA_OPERATOR_TOKEN`.
- The range's **exact** start key (`range list` prints it). A move matches the
  start key exactly — it will not "find the range containing" a key.

## What a move does (and does not) do

| Does | Does not |
|---|---|
| Reassigns `[start, end)` to the target group | Move keys between disks — all groups in a process share one engine |
| Bumps the range `epoch` and `meta_epoch` | Split or merge — bounds and `range_id` are unchanged |
| Commits through the group-0 Raft log + `range-table.bin` | Move leadership — use `TRANSFER_LEADER` (18) after the move |
| Hosts the target group before cutover | Tear down the source group (it stays hosted and idle) |

Clients with a stale cache get `RANGE_MOVED` (11) plus a full table and retry —
a move is visible to clients as a brief refresh, not an error.

## Procedure

### 1. Look at the current layout

```bash
kayactl --server 127.0.0.1:7379 range list
# meta_epoch=4
# range_id=1 epoch=2 group=0 start="" end="m"
# range_id=2 epoch=1 group=1 start="m" end=""
```

Optionally take the advisory plan as a starting point (it suggests moves, it
does not apply them):

```bash
kayactl --server 127.0.0.1:7379 --operator-token "$KAYA_OPERATOR_TOKEN" range rebalance-plan
```

### 2. Move the range

```bash
kayactl --server 127.0.0.1:7379 --operator-token "$KAYA_OPERATOR_TOKEN" \
  range move m 5
# OK move start="m" -> group=5; meta_epoch=5
#   range_id=2 epoch=2 group=5 ["m", "")
```

Start-key forms: `""` / `@empty` for the first range, `0x…` / `hex:…` for raw
bytes, otherwise UTF-8 (same parsing as `range merge`).

Pick a target group id that is **not** already in the table unless you mean to
co-locate two ranges on one group. Fresh ids are safe: the table advances its
`next_group_id` past whatever you pass, so a later split will not collide.

### 3. Land the new group's leader where you want the load

The move changes *which group* owns the range, not *which node* leads that
group. Follow with **TRANSFER_LEADER (18)** on the new group if its leader is
not on the intended node — there is no `kayactl` subcommand for it; send the
opcode with `ADMIN\x00` framing (request body `group_id: u64 | target_node_id:
u64`, LE), exactly as in
[decommission-node.md](decommission-node.md#1-transfer-leaders-away-from-the-target).

### 4. Verify

```bash
kayactl --server 127.0.0.1:7379 range list          # new group_id, higher meta_epoch
curl -s http://127.0.0.1:7380/v1/ranges | jq .      # same view from the dashboard
kayactl --server 127.0.0.1:7379 get <a-key-in-range>
```

Every node's `{data_dir}/range-table.bin` should carry the new `meta_epoch`
after the entry applies; a restart restores the moved layout.

## Failure handling

| Symptom | Meaning | Action |
|---|---|---|
| `STATUS_NOT_LEADER` | Callee is not the group-0 leader | Retry — `kayactl` follows the redirect; check for an election in progress |
| `range already owned by the target group` | The move is a no-op | Nothing to do; re-read `range list` |
| `no range with the given start_key` | Start key is not a range boundary | Copy the exact `start` from `range list` |
| `range meta CAS failed: …` | A concurrent split/merge/move won | Re-read `range list` and reissue |
| `node is draining; refuse new range hosting` | Target node is drained | Move to a node that is not draining |
| Clients see a burst of `RANGE_MOVED` | Expected cache refresh at cutover | None — retries are automatic in `kaya-client` |

If the group-0 leader crashes mid-move, the move either committed (the new
owner is in `range list` on every node) or it did not (the old owner is). There
is no partial state: the cutover is a single committed meta entry. Re-issue the
move if it did not land.

## Verification in CI

- Sim, crash at every point mid-migrate: `cargo test -p kaya-sim --lib move_range`
- Integration, migrate under concurrent puts/gets:
  `cargo test -p kaya-server --lib test_range_move_under_concurrent_load`
- Chaos (nightly, documented subset — move + kill):
  `KAYA_JEPSEN_FAST=1 cargo test -p kaya-jepsen-test --test grand_matrix \
     multi_range_bank_move_range_chaos -- --ignored --nocapture --test-threads=1`

## Related

- [decommission-node.md](decommission-node.md) — drain + transfer + remove
- [add-remove-node.md](add-remove-node.md) — membership changes
- [../deployment-guide-v2.md](../deployment-guide-v2.md) — range ops overview
