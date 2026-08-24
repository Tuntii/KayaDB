# Deployment Guide v2 (M22–M24)

**Status:** Living document (M25 close-out track)  
**Supersedes for flags:** extends [deployment.md](deployment.md) (Docker/K8s layouts still apply)  
**Audience:** operators running a lab or staging cluster with range routing, drain, encryption, and ACL

> KayaDB remains a correctness-first prototype. This guide documents **what ships and how to wire it**, not a production SLA. Pair with [security.md](security.md), [slo-envelope.md](slo-envelope.md), and day-2 [runbooks](runbooks/rolling-restart.md).

---

## 1. What changed since deployment v1 (M15)

| Area | M15 baseline | M22–M24 additions |
|---|---|---|
| Placement / ops | Static 3-node roster | Drain mode, leadership transfer, learners, advisory rebalance plan, Dashboard v1 |
| Ranges | Single group / early multi-raft | LIST / SPLIT / MERGE range ops; shared-engine routing (no physical key migrate) |
| Transactions | Point KV | Single-group SI multi-key + cross-range 2PC (client-transparent) |
| Security | Client/operator tokens, optional TLS, audit | Engine AES-GCM at rest, per-prefix ACL file |
| Observability | Prometheus `/metrics` | Read-only JSON dashboard (`/health`, `/v1/ranges`, `/v1/raft`) |

Docker Compose and Kubernetes examples under `deploy/` remain the reference layouts. Wire the flags below into the same images or StatefulSet command lines.

---

## 2. Server flags (M22–M24)

When running `kayadb-server` (flag wins over env where both exist unless noted):

### 2.1 Drain and decommission (M22)

| Flag / env | Purpose |
|---|---|
| `--drain` / `KAYA_DRAIN=1` (or `true` / `yes`) | Mark this node draining: status JSON reports `"drain": true`; rejects new `SPLIT_RANGE` on this node |

Drain is a **marker + placement guard**, not full “never lead”. Safe decommission order:

1. Start/restart target with `--drain`
2. Confirm via `kayactl --server HOST:PORT status` (`"drain": true`)
3. Transfer leadership off the node (admin `TRANSFER_LEADER` / runbook)
4. `kayactl remove-node N` (operator token if configured)
5. Stop process and wipe `data_dir`

Full procedure: [runbooks/decommission-node.md](runbooks/decommission-node.md).

### 2.2 Dashboard v1 (M22)

| Flag / env | Purpose |
|---|---|
| `--dashboard-addr HOST:PORT` | Bind read-only HTTP JSON dashboard (optional; no default bind) |

Endpoints:

| Path | Body (summary) |
|---|---|
| `GET /health` | `{"ok":true}` |
| `GET /v1/ranges` | `meta_epoch` + range descriptors |
| `GET /v1/raft` | Per-group leader / term / commit |

Bind rules match client/Raft/metrics: public binds require the same allow-public policy as other listeners. Prefer loopback or a private admin network.

```bash
./kayadb-server --node-id 1 --data-dir ./data1 \
  --raft-addr 127.0.0.1:7481 --client-addr 127.0.0.1:7379 \
  --dashboard-addr 127.0.0.1:7380 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381

curl -s http://127.0.0.1:7380/v1/ranges | jq .
curl -s http://127.0.0.1:7380/v1/raft | jq .
```

Dashboard v1 is **not** authenticated. Do not expose it on the public internet.

### 2.3 Encryption at rest (M24)

| Flag / env | Purpose |
|---|---|
| `--encryption-key-file PATH` / `KAYA_ENCRYPTION_KEY_FILE` | Path to **exactly 32 raw bytes** (AES-256), single non-rotating key. Wraps engine `Disk` with `EncryptedDisk` |
| `--encryption-keyring-file PATH` / `KAYA_ENCRYPTION_KEYRING_FILE` | Path to a keyring (active + previous key ids) for online rotation (#28). Mutually exclusive with `--encryption-key-file`; managed via `kayactl encryption init/rotate/list/verify` |

On-disk layout for sealed blobs: `KAYAENC1 | plain_len | nonce | ciphertext+tag` (legacy, implicit key id 0) or, once rotated, `KAYAENC2 | key_id | plain_len | nonce | ciphertext+tag`. See [security.md](security.md) §7.1 for the format and [key-rotation runbook](runbooks/key-rotation.md) for the rotation procedure.

```bash
# Generate a key once; store offline / secret manager; mode 600
openssl rand -out /etc/kaya/encryption.key 32
chmod 600 /etc/kaya/encryption.key

./kayadb-server ... --encryption-key-file /etc/kaya/encryption.key

# Or, to support future rotation:
kayactl encryption init --keyring /etc/kaya/keyring.txt --from-key-file /etc/kaya/encryption.key
./kayadb-server ... --encryption-keyring-file /etc/kaya/keyring.txt
```

Notes:

- All nodes that open the same engine data must share the same key(s).
- Enabling encryption on an existing plaintext `data_dir` is **not** a migration tool; start with a fresh directory or restore into an encrypted layout deliberately.
- Raft peer state and non-engine files may still sit outside `EncryptedDisk`; combine with volume encryption for full disk coverage. See [security.md](security.md).

### 2.4 Per-prefix ACL (M24)

| Flag / env | Purpose |
|---|---|
| `--acl-file PATH` / `KAYA_ACL_FILE` | JSON object mapping key **prefix** → client token |

Rules:

- Longest-prefix match on PUT / GET / DELETE / SCAN / TXN_*.
- Empty map denies all data ops.
- Prefix keys may be UTF-8 or hex (`0x…` / `hex:…`).
- TXN_BEGIN / COMMIT / ROLLBACK accept any token present on at least one rule.
- HEALTH stays open (liveness probes).

Example `acl.json`:

```json
{
  "user:": "token-users",
  "acct:": "token-ledger",
  "0x00meta": "token-admin"
}
```

```bash
./kayadb-server ... --acl-file /etc/kaya/acl.json \
  --client-token token-users
```

This is key-space isolation, not multi-tenancy (no quotas, tenant IDs, or resource accounting).

### 2.5 Still available (M15 baseline)

| Flag / env | Purpose |
|---|---|
| `--client-token` / `KAYA_CLIENT_TOKEN` | Require token on data opcodes |
| `--operator-token` / `KAYA_OPERATOR_TOKEN` | Require token on membership / admin ops |
| `--audit-log` / `--no-audit-log` | JSONL audit under `{data_dir}/audit.jsonl` |
| `--metrics-addr` / `--no-metrics` | Prometheus `/metrics` (default `127.0.0.1:9090`) |
| `--tls-cert`, `--tls-key`, `--tls-ca` | Native TLS (feature build) |
| `--max-client-connections` / `KAYA_MAX_CLIENT_CONNECTIONS` | Cap concurrent clients (default 1024) |

---

## 3. Range operations (M21–M22)

Ranges are **routing partitions** over a shared engine: split/merge update the meta table; keys are not physically moved between disks.

| Op | Wire | CLI |
|---|---|---|
| List ranges | `LIST_RANGES` (15) | `kayactl --server HOST:PORT range list` |
| Split at key | `SPLIT_RANGE` (16) | `kayactl --server HOST:PORT range split <key>` |
| Merge with next | `MERGE_RANGE` (17) | `kayactl --server HOST:PORT range merge <left-start>` |
| Advisory rebalance | `REBALANCE_PLAN` (20) | `kayactl --server HOST:PORT range rebalance-plan` |

```bash
kayactl --server 127.0.0.1:7379 range list
kayactl --server 127.0.0.1:7379 range split m
kayactl --server 127.0.0.1:7379 range merge ""    # left start empty = first range
kayactl --server 127.0.0.1:7379 range rebalance-plan
```

Admin (operator token when configured):

| Op | Wire | Notes |
|---|---|---|
| Transfer leader | `TRANSFER_LEADER` (18) | Leader steps down; free election (no TimeoutNow) |
| Promote learner | `PROMOTE_LEARNER` (19) | Learner → voter |
| Rebalance plan | `REBALANCE_PLAN` (20) | **Advisory only** — does not move ranges or leases |

Cross-range transactions: clients use the same TXN opcodes; the server coordinator runs 2PC when a commit spans more than one group. No client API change.

---

## 4. Recommended staging profile

Minimal 3-node lab with drain-aware ops, ACL, encryption, metrics, and dashboard:

```bash
# Shared secrets (example paths)
KEY=/etc/kaya/encryption.key
ACL=/etc/kaya/acl.json
OP_TOKEN=op-secret
# Clients present tokens that match ACL entries

./kayadb-server \
  --node-id 1 \
  --data-dir /var/lib/kaya/n1 \
  --raft-addr 10.0.0.1:7481 \
  --client-addr 10.0.0.1:7379 \
  --peer 2=10.0.0.2:7482,10.0.0.2:7379 \
  --peer 3=10.0.0.3:7483,10.0.0.3:7379 \
  --operator-token "$OP_TOKEN" \
  --acl-file "$ACL" \
  --encryption-key-file "$KEY" \
  --metrics-addr 127.0.0.1:9090 \
  --dashboard-addr 127.0.0.1:7380
```

Firewall: client and Raft ports only on the private net; metrics and dashboard on loopback or bastion-only.

---

## 5. Health, metrics, and SLO envelope

```bash
# Liveness (open even with client token / ACL)
kayactl --server HOST:7379 health

# Prometheus
curl -s http://127.0.0.1:9090/metrics | head

# Dashboard
curl -s http://127.0.0.1:7380/health
```

Operating limits and correctness SLOs: [slo-envelope.md](slo-envelope.md).  
CI perf envelope (put/get + multi-key txn + multi-range 2PC smoke budgets): [BENCHMARKS.md](../BENCHMARKS.md) and `cargo test -p kaya-bench --test perf_gate --release`.

---

## 6. Explicit non-goals (still out of path)

- Live range migrate / MOVE_RANGE with physical key movement
- Parallel-commit 2PC stretch and durable global decision log
- Background re-encrypt after a key rotation (#28 ships an online dual-key read window; old files upgrade lazily on next write — see `docs/security.md` §7.1)
- Full multi-tenancy beyond per-prefix ACL
- Dashboard v2 (trace timeline, eBPF, range health UI)
- Contractual latency SLA or north-star production claim before M25 exit proof

---

## Related

- [deployment.md](deployment.md) — Docker Compose + Kubernetes
- [security.md](security.md) — network model, tokens, encryption, accepted risks
- [runbooks/decommission-node.md](runbooks/decommission-node.md) — drain workflow
- [runbooks/rolling-restart.md](runbooks/rolling-restart.md) — rolling restart + transfer note
- [slo-envelope.md](slo-envelope.md) — hard limits and design SLOs
- [BENCHMARKS.md](../BENCHMARKS.md) — performance envelope v2 gates
