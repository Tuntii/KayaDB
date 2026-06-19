# mTLS Sidecar Runbook (Ghostunnel)

This runbook covers day-2 operations for the recommended mTLS sidecar pattern using ghostunnel.

**See also:** [Security & Deployment Guide](../security.md#production-mtls-with-sidecar-copy-paste-demo)

The sidecar pattern keeps KayaDB itself free of TLS dependencies.

**Native in-process TLS** (via `tls` feature + `--tls-cert` / `--tls-key` / `--tls-ca` flags or env vars) is now implemented for Raft and client listeners. Sidecar remains useful for:
- Builds without the `tls` feature
- Additional proxy features (e.g. rate limiting, logging)
- Gradual migration

Use `--operator-token` together with either approach for authenticated membership changes.

**Prerequisites**
- `ghostunnel` binary or the Docker image (`ghostunnel/ghostunnel`)
- The cert generation script and/or proper PKI certs
- KayaDB nodes started with `--operator-token` (for membership protection)
- `scripts/mtls-sidecar/setup-certs.sh` + `docker-compose.mtls.yml` (for demo)

## Generating / Rotating Certificates (Demo)

```bash
# (Re)generate self-signed set (demo only)
CERTS_DIR=./certs ./scripts/mtls-sidecar/setup-certs.sh

ls -l certs/
```

**Production:** Obtain node + client certs from your CA. Never reuse demo certs.

To rotate:
1. Generate new certs with overlapping validity.
2. Deploy new keystores to sidecars (zero-downtime: start new ghostunnel instances on temp ports then cut over, or restart sidecars one-by-one).
3. Update any client proxies.
4. Remove old certs after all connections migrated.

## Starting Sidecars

### Using the Docker Compose example (single-host demo)

```bash
# Ensure plain KayaDB nodes are running on localhost (internal ports)
CLUSTER_DIR=/tmp/kayadb-demo ./scripts/start-cluster.sh

cd scripts/mtls-sidecar
CERTS_DIR=../../certs docker compose -f docker-compose.mtls.yml up -d

# Check
docker compose -f docker-compose.mtls.yml logs --tail=20
```

Ports (host network):
- Raft mTLS (server sidecars): 8481 (node1), 8482, 8483
- Client mTLS (server sidecars): 8379, 8380, 8381
- Local client proxy (for plain clients): 7399 → node1 client mTLS

Stop: `docker compose -f docker-compose.mtls.yml down`

### Manual per-node

See exact `ghostunnel server` commands in `docs/security.md`.

Run one server-sidecar for Raft + one for Client per node.

Additionally run client-mode proxies on client machines:

```bash
ghostunnel client \
  --listen 127.0.0.1:7399 \
  --target <public-or-node-ip>:8379 \
  --keystore certs/client.p12 \
  --cacert certs/ca.crt
```

## Connecting Clients and kayactl

Always talk to the **local plain proxy** (or a load-balanced mTLS endpoint if you front it):

```bash
kayactl --server 127.0.0.1:7399 status

# Admin ops also pass the operator token
kayactl --server 127.0.0.1:7399 \
  --operator-token "$KAYA_OPERATOR_TOKEN" \
  add-node ...
```

The token protects `ADD_MEMBER`/`REMOVE_MEMBER` at the KayaDB layer. mTLS protects the bytes on the wire and authenticates the caller identity via certificate CN.

## Verifying the Setup

```bash
# From a client host (through local proxy)
kayactl --server 127.0.0.1:7399 status

# With operator token for membership
kayactl --server 127.0.0.1:7399 \
  --operator-token "$KAYA_OPERATOR_TOKEN" \
  status
```

On the server sidecar logs you should see successful `TLS handshake complete` from allowed CNs.

When using **native TLS** (no sidecar), point kayactl directly at the TLS port and use `--tls --tls-ca-cert`.

Use `kayactl --server ... status` and confirm cluster is healthy. Also run `kayactl recover --dry-run` on data dirs after any restore.

## Inter-node Raft over mTLS (production hosts)

1. Each host runs its **server** sidecars (as above) on the public IPs.
2. Each host also runs **client** ghostunnel proxies for every peer:
   ```bash
   # On host of node1, for reaching node2 raft
   ghostunnel client \
     --listen 127.0.0.1:9482 \
     --target node2.example.com:8482 \
     --keystore certs/node1.p12 \
     --cacert certs/ca.crt
   ```
3. Start node1 with:
   ```bash
   --peer 2=127.0.0.1:9482,127.0.0.1:xxxx   # the local client proxies
   ```
4. Only the public mTLS ports (848x / 83xx) are reachable across hosts.

In this single-machine demo the inter-node raft still uses plain localhost for simplicity.

## Firewall Rules Reminder

- Expose **only** mTLS ports publicly.
- Allow-list source IPs or security groups.
- Block direct access to KayaDB's plain ports (`7379*`, `7481*`) from untrusted networks.
- See `docs/security.md` for checklist.

## Troubleshooting

- **Handshake failures / "bad certificate"**: CN mismatch in `--allow-cn` vs the presented cert. Check `openssl x509 -in nodeX.crt -noout -subject`.
- **Connection refused on proxy port**: sidecar not running or wrong target inside container/host.
- **Membership add fails with auth error**: mismatched `--operator-token` between server(s) and kayactl.
- **Docker network_mode host issues** (Windows/Mac): run sidecars directly on the host instead of compose, or use port publishing + internal Docker DNS targets.
- Ghostunnel logs are your friend (`--quiet=false` or Docker logs).

## References

- `scripts/mtls-sidecar/setup-certs.sh`
- `scripts/mtls-sidecar/docker-compose.mtls.yml`
- `docs/security.md` (full production guidance + operator token)
- Ghostunnel docs: https://github.com/ghostunnel/ghostunnel

Keep this pattern until native mTLS + auth is implemented inside KayaDB.
