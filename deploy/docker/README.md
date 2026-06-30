# KayaDB Docker deployment

Run a **3-node Raft cluster** locally with Docker Compose. The image ships both `kayadb-server` and `kayactl`.

## Prerequisites

- Docker 24+ with Compose v2
- ~2 GB free disk for the Rust build layer (first build only)

## Build

From the repository root:

```bash
docker build -f deploy/docker/Dockerfile -t kayadb:latest .
```

Or let Compose build on first `up`:

```bash
cd deploy/docker
docker compose build
```

## Run the cluster

```bash
cd deploy/docker
docker compose up -d
```

| Node | Client (host) | Raft (host) | Container |
|------|---------------|-------------|-----------|
| 1    | `127.0.0.1:7379` | `127.0.0.1:7481` | `kayadb-node1` |
| 2    | `127.0.0.1:7380` | `127.0.0.1:7482` | `kayadb-node2` |
| 3    | `127.0.0.1:7381` | `127.0.0.1:7483` | `kayadb-node3` |

Each node persists data in a named Docker volume (`kayadb-node1-data`, etc.).

### Logs and status

```bash
docker compose ps
docker compose logs -f kayadb-node1
```

### Stop and remove

```bash
docker compose down          # keep volumes
docker compose down -v       # delete persisted data
```

## kayactl via `docker exec`

The runtime image includes `kayactl`. Use `docker exec` against any node container.

**Write a key** (talk to node 1 over the Docker network):

```bash
docker exec kayadb-node1 kayactl \
  --server kayadb-node1:7379 \
  put hello world
```

**Read it back:**

```bash
docker exec kayadb-node1 kayactl \
  --server kayadb-node1:7379 \
  get hello
```

You can also target the host-mapped port from inside the same container:

```bash
docker exec kayadb-node1 kayactl --server 127.0.0.1:7379 get hello
```

**Cluster status** (follow redirects to the Raft leader):

```bash
docker exec kayadb-node1 kayactl --server kayadb-node1:7379 status
```

## Configuration notes

Compose uses the same CLI flags as `kayadb-server` (`crates/kaya-server/src/main.rs`):

- `--node-id`, `--raft-addr`, `--client-addr`, `--peer`, `--data`
- `--allow-public-bind` — required for `0.0.0.0` binds inside containers
- `--no-metrics` — avoids exposing a metrics listener in this demo layout

Optional flags you can add per service in `docker-compose.yml`:

- `--operator-token` / `KAYA_OPERATOR_TOKEN` — admin/membership ops
- `--client-token` / `KAYA_CLIENT_TOKEN` — data-path auth
- `--tls-cert`, `--tls-key`, `--tls-ca` — native mTLS (see `docs/security.md`)

For production, place nodes behind firewall rules or an mTLS sidecar (`scripts/mtls-sidecar/`).