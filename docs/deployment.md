# Deployment

KayaDB **v0.1.46 (M15)** ships example deployment layouts under `deploy/` in the repository. These are **reference configurations** for local/staging — not a managed cloud product.

**M22–M24 operators:** see **[Deployment guide v2](deployment-guide-v2.md)** for drain, dashboard, encryption, ACL, and range ops flags layered on these layouts.

---

## Docker Compose (3-node cluster)

**Path:** [`deploy/docker/`](deploy/docker/README.md)

```bash
cd deploy/docker
docker compose up --build -d

docker exec kayadb-node1 kayactl --server kayadb-node1:7379 put hello world
docker exec kayadb-node1 kayactl --server kayadb-node1:7379 get hello
```

| Node | Client port (host) | Raft port (host) |
|---|---|---|
| node1 | 7379 | 7481 |
| node2 | 7380 | 7482 |
| node3 | 7381 | 7483 |

See [deploy/docker/README.md](deploy/docker/README.md) for build details, volumes, and security notes.

---

## Kubernetes (StatefulSet)

**Path:** [`deploy/k8s/`](deploy/k8s/README.md)

```bash
docker build -f deploy/docker/Dockerfile -t kayadb:latest .
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
kubectl -n kayadb port-forward pod/kayadb-0 7379:7379
```

Headless service DNS: `kayadb-0.kayadb-headless`, `kayadb-1.kayadb-headless`, …

See [deploy/k8s/README.md](deploy/k8s/README.md) for peer roster template and rollout notes.

---

## Server flags (M15)

When running `kayadb-server` directly or in containers:

| Flag / env | Purpose |
|---|---|
| `--client-token` / `KAYA_CLIENT_TOKEN` | Require token on PUT/GET/DELETE/SCAN/STATS (opcodes 1–4, 6) |
| `--operator-token` / `KAYA_OPERATOR_TOKEN` | Require token on ADD_MEMBER / REMOVE_MEMBER |
| `--audit-log` / `--no-audit-log` | JSONL audit at `{data_dir}/audit.jsonl` (default on when any token set) |
| `--metrics-addr` / `--no-metrics` | Prometheus HTTP `/metrics` (default `127.0.0.1:9090`) |
| `--tls-cert`, `--tls-key`, `--tls-ca` | Native TLS (feature build) |
| `--max-client-connections` / `KAYA_MAX_CLIENT_CONNECTIONS` | Cap concurrent client connections (default 1024); excess connections wait in the TCP backlog |

**Health checks:** opcode 5 (HEALTH) stays open when client token is configured — use for liveness probes.

**Shutdown:** the server exits cleanly on Ctrl-C (all platforms) and SIGTERM (Unix) — eBPF probes are detached and OTel spans flushed before exit. Durability never depends on clean shutdown (acknowledged strict writes are already fsynced).

---

## Observability

```bash
curl http://127.0.0.1:9090/metrics    # when --metrics-addr enabled
kayactl --server HOST:7379 watch status --interval 2
```

---

## Security before production-like use

Read [Security & deployment](security.md) — firewall client/Raft ports, configure tokens, prefer mTLS sidecar or native TLS for multi-host clusters. Remaining accepted risks (data-at-rest, multi-tenant, SIEM export) are in [§7](security.md#7-accepted-risks-and-future-hardening-m15-exit).

---

## Related

- [Deployment guide v2](deployment-guide-v2.md) — M22–M24 flags, ranges, staging profile
- [SLO envelope](slo-envelope.md) — hard limits and design SLOs
- [Getting started](getting-started.md) — manual 3-node cluster
- [Runbooks](runbooks/rolling-restart.md) — day-2 operations
- [Go client](clients/go-client.md) — application connectivity