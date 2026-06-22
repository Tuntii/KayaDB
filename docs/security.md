# KayaDB Security and Deployment Guide

This document outlines the security architecture, networking requirements, and best practices for deploying **KayaDB** securely in production environments.

---

## 1. Network Security Architecture

By default, KayaDB's Raft consensus protocol (`kaya-net`) and client communication protocols run over **raw, unencrypted TCP**.
* **Raft Transport:** Exchanged state machine replicates, heartbeats, and leader election envelopes without payload encryption.
* **Client Protocol:** Client PUT, GET, DELETE, and SCAN operations are transmitted in plain text.
* **Authentication:** There is no built-in password, token-based, or TLS certificate authentication in the current version.

Because of these design choices (focused on high performance and zero external dependencies), **security must be enforced at the infrastructure level**.

### Current trust model

KayaDB currently assumes:

- clients are trusted,
- cluster peers are trusted,
- the network is private,
- the data directory is owned by the database process user,
- malformed files and frames should return errors, not panic the process.

If any of those assumptions are false in your environment, treat KayaDB as a local experiment only until you add the missing infrastructure controls around it.

---

## 2. Port Exposure & Firewall Guidance

### Critical Warning
> [!CAUTION]
> **NEVER expose KayaDB Raft or Client ports directly to the public Internet.** Doing so allows anyone to read all stored keys and values, modify the database state, or trigger cluster-wide disruptions.

### Best Practices:
1. **Private Networks (VPCs):**
   * Deploy all KayaDB cluster nodes inside a isolated virtual private cloud (VPC) or private subnet.
   * Clients accessing the database should reside in the same VPC or be connected via a secure VPN/VPC Peering.
2. **Restrictive Firewalls:**
   * Configure strict firewall rules (using `iptables`, `ufw`, Windows Defender Firewall, or Cloud Security Groups).
   * **Raft Port:** Allow incoming TCP traffic ONLY from other designated nodes in the `NodeRoster`.
   * **Client Port:** Allow incoming TCP traffic ONLY from authorized application server IP addresses.
3. **Bind Address:**
   * Do not bind to wildcard addresses (`0.0.0.0`) if the machine has multiple network interfaces. Bind strictly to the node's private IP address (e.g., `10.0.0.5:7481`).

### Port checklist

| Endpoint | Default example | Who may connect? | Public internet? |
|---|---|---|---|
| Raft peer port | `127.0.0.1:7481` | Other KayaDB nodes only | Never |
| Client port | `127.0.0.1:7379` | Trusted application hosts/operators only | Never |
| Metrics/status through client protocol | same as client port | Trusted operators/automation only | Never |

For local demos, bind to `127.0.0.1`. For multi-host experiments, bind to a private subnet address and enforce firewall rules before starting the node.

### Server enforcement (M11 + M13 final)

| Control | Default | Override / Location | Effect | Enforced in code? |
|---|---|---|---|---|
| Bind address | `127.0.0.1` | `--raft-addr` / `--client-addr` | Loopback-only unless widened | ✅ `security::validate_bind_addr` |
| Public bind guard | rejects public/wildcard | `--allow-public-bind` | Banner + allow; no built-in auth/TLS | ✅ startup + security.rs |
| Raft / client frame size | 64 MiB max | compile-time in codec | Oversize → decode error | ✅ |
| Roster / unknown peer | drop | static at start (RaftNode) | Unknown `from` ids ignored | ✅ |
| Snapshot file protection (refcounts) | pinned SSTs during active snapshot | engine refcounts + release on new snapshot | Compaction cannot delete live snap data | ✅ kaya-engine |
| Durable snapshot on restart | loads `raft-snapshot.bin` + engine state | startup in cluster.rs | Follower/leader restart preserves applied state | ✅ |
| Crash safety on snapshot persist | tmp + rename + fsync + dir sync | compaction path | Atomic snapshot file | ✅ |
| Operator credential on admin ops | none (open) | `--operator-token` / `KAYA_OPERATOR_TOKEN` (server + kayactl) | ADD/REMOVE_MEMBER (op 7/8) require matching token when configured | ✅ (M13) kaya-server + kayactl |
| TLS configuration validation | no TLS by default | `--tls-cert` / `--tls-key` / `--tls-ca` + env vars (when `tls` feature enabled) | Listeners use rustls; invalid paths/config fail startup | ✅ kaya-server + kaya-net (feature-gated) |
| mTLS sidecar support | documented | ghostunnel/stunnel + runbook + scripts | Full transport auth via sidecar | ✅ |
| Native TLS transport (raft + client) | `tls` feature + --tls-* flags | kaya-net + kaya-server + kaya-client | In-process rustls encryption (mTLS optional) | ✅ (M13) |
| Client-side TLS + token usage | plain TCP + no token | `kayactl --tls --tls-ca-cert ... --operator-token ...` | Authenticated + encrypted client + admin ops | ✅ kayactl + kaya-client |

`kayadb-server` calls security checks before binding listeners. See `crates/kaya-server/src/security.rs` and `cluster.rs` (snapshot load + compaction, TLS listener setup).

Treat `--allow-public-bind` as explicit ack that you have perimeter controls (firewall + mTLS sidecar or native TLS).

**M13 progress:** Operator token (admin auth) + native TLS transport are implemented (feature-gated). See runbooks for day-2 usage.

---

## 3. Transport Layer Encryption (TLS Wrapper)

If your network spans across non-trusted environments or requires data-in-transit encryption to comply with security standards (e.g., SOC2, PCI-DSS), you must wrap KayaDB network interfaces in a TLS proxy.

We recommend using **[ghostunnel](https://github.com/ghostunnel/ghostunnel)**, a simple SSL/TLS proxy with mutual authentication support, or **stunnel**.

### mTLS Wrapping Example with Ghostunnel (basic)

For each KayaDB node (single-node sketch):
1. **Secure Raft Port:**
   Set up `ghostunnel` on each node to listen on public port `8481` (with mTLS certificates) and proxy to local KayaDB Raft listener on `127.0.0.1:7481`.
   ```bash
   ghostunnel server \
     --listen 0.0.0.0:8481 \
     --target 127.0.0.1:7481 \
     --keystore certs/node-server.p12 \
     --cacert certs/ca.crt \
     --allow-cn node1.kaya.local \
     --allow-cn node2.kaya.local
   ```
2. **Secure Client Port:**
   Configure a similar wrapper for the client endpoint to ensure client-to-server traffic is fully encrypted.

---

### Production mTLS with Sidecar (copy-paste demo)

For production-like authenticated transport use **ghostunnel sidecars** (mTLS on "public" ports, plain TCP only to localhost KayaDB).

**Together with `--operator-token`** (see operator auth section below) this gives:
- Encrypted + mutually-authenticated transport (mTLS)
- Authorization for sensitive membership operations (operator token)

**Native TLS is now available** (behind `tls` feature). Sidecar remains a zero-change option for existing deploys.

### Step-by-step (3-node demo)

#### 1. Generate certs (self-signed for demo only)

```bash
# From repo root
mkdir -p certs
CERTS_DIR=./certs ./scripts/mtls-sidecar/setup-certs.sh
```

This creates:
- `ca.crt` / `ca.key`
- `node1.p12`, `node2.p12`, `node3.p12` (for sidecars + inter-node client auth)
- `client.p12` (for external clients / kayactl via proxy)

**Production warning:** Never use these self-signed certs in real deployments. Use your CA, short lifetimes, and secrets management. Protect all `.key`/`.p12` files (chmod 600, never commit).

#### 2. Start plain KayaDB nodes (localhost only)

Use the usual scripts or manual (bind to 127.0.0.1, **never** 0.0.0.0 without sidecar + firewall).

```bash
# Example: start internal plain cluster
CLUSTER_DIR=/tmp/kayadb-mtls-demo ./scripts/start-cluster.sh
```

Each node listens only on `127.0.0.1:7481` (raft) / `127.0.0.1:7379` (client) etc.

Start servers with the operator token for protected membership:

```bash
# (when not using the start script directly)
kayadb-server \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  ... \
  --operator-token "super-secret-demo-token-CHANGE-ME"
```

#### 3. Start the mTLS sidecar wrappers

**Option A: Manual (one shell / node)**

For node 1 (repeat for 2/3 with incremented ports):

```bash
# Raft sidecar (mTLS public 8481 -> plain internal 7481)
ghostunnel server \
  --listen 0.0.0.0:8481 \
  --target 127.0.0.1:7481 \
  --keystore certs/node1.p12 \
  --cacert certs/ca.crt \
  --allow-cn node1.kaya.local \
  --allow-cn node2.kaya.local \
  --allow-cn node3.kaya.local \
  --allow-cn admin-client.kaya.local

# Client sidecar (in another terminal)
ghostunnel server \
  --listen 0.0.0.0:8379 \
  --target 127.0.0.1:7379 \
  --keystore certs/node1.p12 \
  --cacert certs/ca.crt \
  --allow-cn node1.kaya.local \
  --allow-cn node2.kaya.local \
  --allow-cn node3.kaya.local \
  --allow-cn admin-client.kaya.local
```

**Option B: Docker Compose (recommended for local 3-node demo)**

```bash
# From repo root (after generating certs)
cd scripts/mtls-sidecar
CERTS_DIR=../../certs docker compose -f docker-compose.mtls.yml up -d

# Verify
docker compose -f docker-compose.mtls.yml ps
```

See the compose file comments for exposed ports:
- Raft mTLS: `8481,8482,8483`
- Client mTLS: `8379,8380,8381`
- Convenience local proxy for kayactl: `127.0.0.1:7399`

#### 4. Connect clients / kayactl to the TLS side (via local proxy)

Because kayactl (and most current clients) speak plain TCP, run a **client-mode** ghostunnel proxy locally:

```bash
# One-time: proxy plain local port to the mTLS client sidecar
ghostunnel client \
  --listen 127.0.0.1:7399 \
  --target 127.0.0.1:8379 \
  --keystore certs/client.p12 \
  --cacert certs/ca.crt
```

Now use the plain proxy port:

```bash
# Status (no token needed for read ops)
kayactl --server 127.0.0.1:7399 status --json

# Write
kayactl --server 127.0.0.1:7399 put hello world

# Membership operations REQUIRE the operator token
# (servers must also be started with --operator-token)
kayactl --server 127.0.0.1:7399 \
  --operator-token "super-secret-demo-token-CHANGE-ME" \
  add-node 4 127.0.0.1:7484 127.0.0.1:7383
```

**Point kayactl / clients at the local proxy port (or any node’s client mTLS via its own client proxy).** The sidecar performs the mTLS handshake on your behalf.

If your custom client supports TLS + client certs, you can point it directly at `127.0.0.1:8379` (or remote public equivalent) presenting `client.p12` (or equiv).

#### 5. Firewall / network rules

- **Allow** inbound TCP to the **mTLS ports only** (`8481-8483`, `8379-8381`) **from**:
  - Other cluster nodes (for raft)
  - Authorized app servers + operator machines (for client)
- **Deny** everything else to those ports.
- **Never** allow direct access to the plain internal ports (`7481-7483`, `7379-7381`) from outside localhost / the sidecar containers.
- On multi-host: use security groups / iptables / cloud firewalls. Sidecar ports become the only externally reachable.

Example (ufw):

```bash
# Only from the other node IPs + your client hosts
ufw allow from 10.0.0.2 to any port 8481
ufw allow from 10.0.0.2 to any port 8379
# ... repeat for 8482/3 + 8380/1
# No rules for 7xxx
```

#### Full production notes

- Run ghostunnel under the same unprivileged user or as a systemd unit / container sidecar.
- Mount certs read-only.
- Monitor ghostunnel logs for auth failures.
- Rotate certs before expiry.
- Combine with `--operator-token` (required for `add-node` / `remove-node` when set on servers).
- In K8s consider cert-manager + ghostunnel or Envoy / Linkerd / Istio for automatic mTLS.
- See `scripts/mtls-sidecar/` for the cert script and compose example, and `docs/runbooks/` for day-2 procedures:
- `add-remove-node.md`
- `rolling-restart.md`
- `backup-restore.md`
- `detecting-split-brain.md`
- `mtls-sidecar.md` (sidecar operations + native TLS notes)

---

## 4. Operational & Local System Security

1. **Process Privilege:**
   * Never run the `kayadb-server` daemon as the `root` or `Administrator` user.
   * Create a dedicated unprivileged user (e.g., `kaya`) with read/write access restricted ONLY to the database directory (`data_dir`).
2. **Directory Permissions:**
   * Set file permissions on the storage directory (e.g., `./data`) to `0700` (readable/writable only by the database owner user).
   ```bash
   chmod 700 /var/lib/kaya-data
   ```
3. **Data At Rest Encryption:**
   * Since KayaDB stores SSTables as raw binary files on disk, use filesystem-level encryption (like DM-Crypt/LUKS on Linux or BitLocker on Windows) if storage hardware theft is a threat model.

---

## 5. Safe Local Development Profiles

### Laptop / single-node demo

- Bind to `127.0.0.1` only.
- Store data under a disposable directory such as `./data` or a temp directory.
- Use `kayactl recover --dry-run` before reusing a directory after crash testing.
- Delete demo directories when finished.

### Private lab cluster

- Use private IP addresses only.
- Restrict Raft ports to the static node roster.
- Restrict client ports to trusted application or operator hosts.
- Prefer an isolated VM/container network.
- Capture node logs and `kayactl status --json` output when testing failures.

### Anything production-like

Do not run KayaDB as a production system yet. If you still run a production-like experiment:

- wrap client and Raft traffic with mTLS or a private encrypted tunnel,
- use filesystem or block-device encryption for data at rest,
- run under an unprivileged service account,
- back up the full data directory before upgrades or experiments,
- keep a rollback plan,
- document which security controls live outside KayaDB.

---

## 6. Recovery and Inspection Safety

Inspection commands are designed for local operators and debugging. Treat their output as sensitive because it may include keys, values, paths, and operational metadata.

Recommended workflow after an unclean shutdown:

1. Stop the node.
2. Copy the data directory if you need forensic evidence.
3. Run `kayactl --data <dir> recover --dry-run --json`.
4. Inspect WAL/manifest/SSTable files only on trusted machines.
5. Restart the node only after the recovery report is understood.

Never paste inspection output from real datasets into public issue trackers unless you have scrubbed secrets and user data.

---

## 7. Accepted risks and future hardening (M13 exit)

M13 delivers operator-token auth for membership ops, native TLS (feature-gated), durable Raft snapshots, and documented day-2 runbooks. The items below are **explicitly accepted risks** for M13 — not correctness bugs. Mitigate them with infrastructure controls documented in sections 2–5.

| Gap | Status | Mitigation (operator responsibility) | Code / docs reference |
|---|---|---|---|
| Full authZ for all client ops (GET/PUT/DELETE/SCAN) | Accepted risk | Firewall client ports; mTLS sidecar or native TLS; app-layer auth in front of KayaDB | Operator token enforces only opcodes 7/8: `crates/kaya-server/src/cluster.rs` (admin opcode handler ~L934) |
| Data at rest encryption | Accepted risk | LUKS/DM-Crypt, BitLocker, or encrypted block volumes on the data directory | Section 4 above; no engine-level encryption |
| Multi-tenant isolation | Accepted risk | One cluster per tenant; network segmentation; separate credentials per deployment | No tenant IDs in engine or protocol |
| Client cert enforcement on every connection | Accepted risk (partial impl.) | Enable native TLS with CA (`require_client_cert: true` when `--tls-ca` set); or ghostunnel `--allow-cn` | `crates/kaya-server/src/main.rs`, `crates/kaya-net/src/transport.rs` |
| Compliance-grade audit logging | Accepted risk | Ship node logs + `kayactl status --json` to your SIEM; ghostunnel access logs for mTLS | No structured audit trail in engine |
| Hardened remote admin API | Accepted risk | Restrict `kayactl` to bastion/VPN; require `--operator-token` for membership | `kayactl` over client protocol only |

**No known correctness gaps** are listed as accepted risk. Remaining items are deployment hardening, not storage or consensus defects.

Native TLS + operator token provide transport encryption and basic admin auth. Firewall rules, mTLS (native or sidecar), and operator token remain mandatory for any production-like deployment.
