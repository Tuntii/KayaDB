# Read-only HTTP dashboard

Optional JSON listener on `kayadb-server --dashboard-addr HOST:PORT`. There is no default bind. The listener is **not authenticated**: keep it on loopback or a private admin network, the same way you treat Prometheus `/metrics`.

Phase A of Dashboard v2 (issue #31) is the current surface. Phase B (eBPF / fsync attribution) and Phase C (scheduled profiling CI) are **deferred**: they need a Linux runner with `perf` / `CAP_PERFMON` (or equivalent privileged bpf/stap). Building blocks already exist under `crates/kaya-ebpf` and `scripts/ebpf/`; they are not wired into this HTTP API.

## Enable

```bash
./kayadb-server \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  --dashboard-addr 127.0.0.1:7380 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381
```

Public binds follow the same `--allow-public-bind` policy as Raft, client, and metrics.

## Endpoints

All bodies are JSON. `GET /health` is frozen at `{"ok":true}` for probes; richer node state lives under `/v1/*`.

| Path | Status | Body |
|---|---|---|
| `GET /health` | 200 | `{"ok":true}` (v1, unchanged) |
| `GET /v1/cluster` | 200 | `node_id`, `drain`, `range_count`, `leader_group_ids`, `meta_epoch` |
| `GET /v1/ranges` | 200 | `meta_epoch` + range descriptors + per-range `healthy` |
| `GET /v1/raft` | 200 | Per hosted group: `group_id`, `leader_id`, `term`, `commit`, `role`, `is_leader` |
| `GET /v1/leadership` | 200 | Map of `group_id` → `{leader_id, term, role, is_leader}` |
| `GET /v1/errors` | 200 | Recent error ring (cap 50): `{ts_unix_ms, kind, message}` |
| anything else | 404 | `{"error":"not_found"}` (also recorded on the error ring) |

```bash
curl -s http://127.0.0.1:7380/health
curl -s http://127.0.0.1:7380/v1/cluster | jq .
curl -s http://127.0.0.1:7380/v1/ranges | jq .
curl -s http://127.0.0.1:7380/v1/raft | jq .
curl -s http://127.0.0.1:7380/v1/leadership | jq .
curl -s http://127.0.0.1:7380/v1/errors | jq .
```

### Range health

`healthy` is true when the range's hosting Raft group has a **known leader** on this node (the group is hosted here and `leader_id` is set, or this node is the leader). A range whose group is not hosted locally, or is mid-election, reports `healthy: false`. That is a view from **this process**, not a cluster-wide quorum check. Compare `/v1/leadership` across nodes when hunting split-brain; see [detecting-split-brain.md](detecting-split-brain.md).

### Drain

`GET /v1/cluster` includes `"drain": true|false`, the same marker as `kayactl status` / `--drain` / `KAYA_DRAIN`. Drain does not stop this node from leading; transfer leaders before remove. See [decommission-node.md](decommission-node.md).

### Recent errors

The ring is in-memory, oldest-first, cap 50. Sources today:

| `kind` | When |
|---|---|
| `http_404` | Dashboard path that is not one of the routes above |
| `auth_deny` | Client `STATUS_ERROR` whose payload is ACL deny or a missing/invalid client/operator credential |
| `status_error` | Other client `STATUS_ERROR` (propose failure, engine error, …) |

This is a recent-window debug aid, not an audit log. Durable operator history remains `{data_dir}/audit.jsonl` when `--audit-log` is on.

## What this is not

- Not a GUI / trace timeline.
- Not a substitute for Prometheus `/metrics`.
- Not authenticated.
- **Phase B deferred:** kernel + userspace fsync attribution and io_uring completion tracing. Needs a Linux host that can load BPF / attach probes (`CAP_BPF` / `CAP_PERFMON`, often root). See `crates/kaya-ebpf/README.md` and `scripts/ebpf/README.md`.
- **Phase C deferred:** scheduled profiling CI (flamegraph artifacts on a timer). Needs a Linux `perf` / capability runner; default GitHub-hosted runners are not sufficient.

## Related

- [deployment-guide-v2.md](../deployment-guide-v2.md) — flags and staging profile
- [decommission-node.md](decommission-node.md) — drain
- [move-range.md](move-range.md) — `GET /v1/ranges` after a move
- [detecting-split-brain.md](detecting-split-brain.md) — compare leadership across nodes
