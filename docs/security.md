# KayaDB Security and Deployment Guide

This document outlines the security architecture, networking requirements, and best practices for deploying **KayaDB** securely in production environments.

---

## 1. Network Security Architecture

**Default (no flags):** Raft and client traffic use **plain TCP** on localhost. This is intentional for local development and deterministic testing.

**M13+ (optional):** Enable native TLS with the `tls` feature and `--tls-*` flags, or wrap ports with an mTLS sidecar (ghostunnel/stunnel). Membership admin ops (`ADD_MEMBER` / `REMOVE_MEMBER`) accept an **operator token** when configured. **M15:** Data-path ops (PUT/GET/DELETE/SCAN/STATS) accept a **client token** when configured.

| Layer | Default | Hardened option |
|---|---|---|
| Raft transport | Plain TCP | `--features tls` + cert flags, or mTLS sidecar |
| Client protocol | Plain TCP | Same as Raft |
| Admin / membership | Open on client port | `--operator-token` / `KAYA_OPERATOR_TOKEN` |
| Full client authZ | Optional `--client-token` | Perimeter + sidecar/TLS + client token for data ops (opcodes 1–4, 6); see §7 |

When TLS and operator token are **not** enabled, **security must be enforced at the infrastructure level** (private network, firewall, bind to loopback).

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
| Client credential on data ops | none (open) | `--client-token` / `KAYA_CLIENT_TOKEN` (server + kayactl + kaya-client) | PUT/GET/DELETE/SCAN/STATS (opcodes 1–4, 6) require matching `CLIENT\x00` token prefix when configured; HEALTH (op 5) stays open | ✅ (M15) kaya-server + kaya-net codec + kayactl + kaya-client |
| Structured audit logging | off | `--audit-log` / `--no-audit-log` (default on when any token configured) | Append-only JSONL at `{data_dir}/audit.jsonl` for all client opcodes | ✅ (M15) `crates/kaya-server/src/audit.rs` |
| Prometheus metrics | disabled | `--metrics-addr` (default `127.0.0.1:9090`) | HTTP `/metrics` text exposition (WAL fsync, SSTable count, Raft role/term) | ✅ (M15) `crates/kaya-server/src/metrics.rs` |
| Concurrent client connections | 1024 max | `--max-client-connections` / `KAYA_MAX_CLIENT_CONNECTIONS` | Accept-loop semaphore; excess connections wait in TCP backlog (no unbounded task spawn) | ✅ `crates/kaya-server/src/cluster/client_ops.rs` |
| Scan result / byte caps | 100 000 entries / 64 MiB | `EngineConfig.limits.max_scan_results` / `max_scan_bytes` | Unbounded SCAN cannot exhaust memory; merge window bounded; oversized prefixes → `STATUS_INVALID_ARGUMENT` | ✅ `crates/kaya-engine/src/memtable.rs` |
| Data-dir exclusive lock | on | `EngineConfig.disable_locking` | `KAYA_LOCK` file (share-mode 0 on Windows, `flock` on Unix) prevents two processes corrupting one data dir | ✅ `crates/kaya-engine/src/lib.rs` |
| Encryption at rest (engine Disk) | off | `--encryption-key-file` / `KAYA_ENCRYPTION_KEY_FILE` (32 raw bytes) | Engine files sealed with AES-256-GCM via `EncryptedDisk` | ✅ (M24) `kaya-io` + server open path |
| Per-prefix ACL | off | `--acl-file` / `KAYA_ACL_FILE` (JSON `prefix → token`) | Longest-prefix authorize on PUT/GET/DELETE/SCAN/TXN_OP; any-rule token on TXN_BEGIN/COMMIT/ROLLBACK, CDC_POLL/CHECKPOINT, SPLIT/MERGE; empty map denies all; HEALTH stays open | ✅ (M24) `crates/kaya-server/src/acl.rs` + `client_ops` |

`kayadb-server` calls security checks before binding listeners. See `crates/kaya-server/src/security.rs` and `cluster.rs` (snapshot load + compaction, TLS listener setup).

Treat `--allow-public-bind` as explicit ack that you have perimeter controls (firewall + mTLS sidecar or native TLS).

**M13 progress:** Operator token (admin auth) + native TLS transport are implemented (feature-gated). See runbooks for day-2 usage.

**M15 progress:** Client token auth for data-path ops, structured audit logging, and Prometheus `/metrics` are implemented. See `deploy/` for Docker and Kubernetes examples.

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
   * **M24 engine-level AES-256-GCM:** pass `--encryption-key-file <path>` (or `KAYA_ENCRYPTION_KEY_FILE`) where the file is exactly **32 raw bytes**. The server wraps the engine `Disk` with `EncryptedDisk` so WAL/SST/manifest bytes are sealed as `KAYAENC1 | plain_len | nonce | ciphertext+tag`. v1 uses the same key as KEK and DEK; key rotation is a follow-on.
   * Still recommended for full-volume protection (Raft peer state and non-engine files): filesystem-level encryption (DM-Crypt/LUKS, BitLocker, or encrypted block volumes).
4. **Per-prefix ACL (M24):**
   * Optional JSON file via `--acl-file` / `KAYA_ACL_FILE`: object mapping key prefix → client token. Prefix keys may be UTF-8 text or hex (`0x…` / `hex:…`).
   * When configured, data-path ops authorize with **longest-prefix** match against the presented client token; an empty ACL map denies every data op. TXN_BEGIN/COMMIT/ROLLBACK accept any token that appears on at least one rule. HEALTH stays open.
   * This is key-space isolation only — not full multi-tenancy (no tenant IDs, quotas, or resource accounting).

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

## 7. Accepted risks and future hardening (M24 exit)

M15 closed data-path authZ and structured audit; M24 closes engine encryption-at-rest and optional per-prefix ACL. Items still marked **accepted risk** below are deployment / isolation hardening — not correctness bugs. Mitigate them with infrastructure controls in sections 2–5.

| Gap | Status | Mitigation (operator responsibility) | Code / docs reference |
|---|---|---|---|
| Full authZ for all client ops (GET/PUT/DELETE/SCAN/STATS) | ✅ Implemented when `--client-token` set | Configure `--client-token` / `KAYA_CLIENT_TOKEN`; combine with firewall + mTLS; HEALTH (op 5) remains open for probes | `CLIENT\x00` framing in `crates/kaya-net/src/codec.rs`; enforcement in `crates/kaya-server/src/cluster/client_ops.rs` (opcodes 1–4, 6) |
| Structured audit logging (local JSONL) | ✅ Implemented | Enable `--audit-log` (default on when any token configured); rotate/archive `{data_dir}/audit.jsonl` | `crates/kaya-server/src/audit.rs` |
| Data at rest encryption | ✅ Optional engine-level AES-GCM (M24) | Set `--encryption-key-file` / `KAYA_ENCRYPTION_KEY_FILE` (32-byte key, non-rotating) or `--encryption-keyring-file` / `KAYA_ENCRYPTION_KEYRING_FILE` for rotation (#28); combine with volume encryption for Raft/non-Disk files | `EncryptedDisk` / `Keyring` in `crates/kaya-io/src/encrypted.rs`; server open path in `cluster/mod.rs` |
| Per-prefix ACL | ✅ Optional when `--acl-file` set (M24) | JSON `prefix → token`; longest-prefix on PUT/GET/DELETE/SCAN/TXN_OP; any-rule token on TXN_BEGIN/COMMIT/ROLLBACK + CDC_POLL/CHECKPOINT + SPLIT/MERGE; empty map denies all data/admin-range ops | `PrefixAcl` in `crates/kaya-server/src/acl.rs`; enforcement in `client_ops` |
| Multi-tenant isolation | Partial via per-prefix ACL; full tenancy still accepted risk | Use `--acl-file` for key-space isolation, or one cluster per tenant + network segmentation; no tenant IDs, quotas, or resource accounting in engine/protocol | `acl.rs`; ROADMAP: full multi-tenancy out of M16–M25 scope |
| Encryption key rotation (KEK/DEK) | ✅ Implemented (#28): online dual-key read window, no background rewrite | Use `kayactl encryption init/rotate/list/verify` against a `--encryption-keyring-file`; old keys stay readable until every file naturally rewrites under the active key (see §7.1 and `docs/runbooks/key-rotation.md`) | `Keyring` in `crates/kaya-io/src/encrypted.rs`; `crates/kayactl/src/encryption_cmd.rs` |
| Client cert enforcement on every connection | Accepted risk (partial impl.) | Enable native TLS with CA (`require_client_cert: true` when `--tls-ca` set); or ghostunnel `--allow-cn` | `crates/kaya-server/src/main.rs`, `crates/kaya-net/src/transport.rs` |
| Compliance-grade audit export to SIEM | ✅ Optional built-in UDP syslog sink | Set `--audit-syslog <host:port>` / `KAYA_AUDIT_SYSLOG` (RFC 5424 over UDP, requires `--audit-log`); for TCP/TLS transport or delivery guarantees, front with a local syslog agent (rsyslog/vector) | `SyslogSink` in `crates/kaya-server/src/audit.rs`; UDP best-effort, no on-wire encryption |
| Hardened remote admin API | Accepted risk | Restrict `kayactl` to bastion/VPN; require `--operator-token` for membership and `--client-token` / ACL for data ops | `kayactl` over client protocol only |

### 7.1 Encryption key rotation (#28): format and guarantees

**Envelope format.** Each file `EncryptedDisk` manages carries a small header identifying which key sealed it:

```text
Legacy v1 (KAYAENC1, still readable, implicit key id 0):
  magic(8)="KAYAENC1"  plain_len(u64 LE)  nonce(12)  ciphertext||tag

v2 (KAYAENC2, written once a keyring has rotated):
  magic(8)="KAYAENC2"  key_id(u32 LE)  plain_len(u64 LE)  nonce(12)  ciphertext||tag
```

`key_id` is bound into the AES-GCM AAD (with the magic and length), so an attacker cannot repoint a ciphertext at a different key id without failing authentication. A non-rotating deployment (single key, id 0) always writes the original `KAYAENC1` bytes — **zero on-disk format change** unless you actually rotate.

**Keyring.** A keyring is a small text file (`active <id>` + one or more `key <id> <64-hex-char>` lines) holding the active key plus every previous-generation key still needed to decrypt old files. It supersedes the single raw-bytes `--encryption-key-file` for deployments that want rotation; the two flags are mutually exclusive on `kayadb-server`. `save_keyring_file` (used by `kayactl encryption init`/`rotate`) writes it atomically — a sibling `<path>.tmp` is created (mode `0600` on unix), fsynced, then renamed over the real path, so a crash mid-write can never leave a truncated/empty keyring on disk (`load_keyring_file` would refuse it and the node would fail to start, rather than silently losing key material) and the key bytes are never briefly world/group-readable. This matches the trust level of `--encryption-key-file`, whose loader does no permission check either — `kayactl` does not currently warn or refuse on a pre-existing world-readable keyring, so if you hand-edit or copy a keyring file in, `chmod 600` it yourself.

**Read/write semantics (multi-key decrypt window).**
- **Read path**: decrypts using whichever key id is named in the file's own header (0 for legacy files), looked up in the keyring. Any key still present in the ring — active or previous — can be read.
- **Write path**: every mutating op (`write_at`/`append`/`truncate`) always re-seals the **whole file** under the currently active key, upgrading a legacy or older-generation file to the new key and format as a side effect.

**Rotation procedure (no data loss).**
1. `kayactl encryption init --keyring <path>` once, to bootstrap id 0 (fresh key, or `--from-key-file <old-path>` to migrate an existing single-key deployment without re-encrypting anything).
2. Point the server at the keyring: `--encryption-keyring-file <path>` (or `KAYA_ENCRYPTION_KEYRING_FILE`) instead of `--encryption-key-file`.
3. `kayactl encryption rotate --keyring <path>` generates a new key, makes it active, and keeps every previous key in the ring. Distribute the updated keyring file to the node(s) and restart (or have the node re-read it, once hot-reload exists — today this is a restart).
4. `kayactl encryption verify --data <dir> --keyring <path>` walks the data directory and confirms every sealed file still decrypts with the ring in hand — safe to run before/after a rotation, and it never prints key material.
5. `kayactl encryption list --keyring <path>` reports the active id and all retained ids (ids only, never key bytes).

**Crash safety.** Rotation only ever *replaces the keyring file* and *appends/rewrites engine files under a lock already required for durability* — it does not introduce a new crash window. A crash mid-write leaves that one file however the engine's existing WAL/fsync discipline already tolerates (the encryption layer seals/unseals whole-file contents inside the same `write_at`/`append`/`truncate` calls the engine was already crash-tested against); a half-written keyring file is caught at load (`load_keyring_file` rejects a keyring whose `active` id has no matching `key` line) and the node refuses to start rather than silently losing rotation state.

**Accepted limitation (documented, not shipped).** There is no background re-encrypt daemon. Rotation opens an **online dual-key read window**: old and new files coexist and are both readable indefinitely, and files upgrade to the active key lazily (the next time something writes them — WAL segment rolls, compaction, manifest updates). To force full migration off a retired key, trigger normal write traffic/compaction (or `kayactl backup` + restore into a fresh directory) until `kayactl encryption verify --keyring <ring-without-the-old-key>` reports zero failures, then remove that key line from the keyring. This is called out explicitly, per the issue's acceptance criteria, as the intentional v1 scope: a background rewrite is a valid future enhancement, not required for correctness or data-loss safety today.

**No known correctness gaps** are listed as accepted risk. Remaining items are deployment hardening, not storage or consensus defects.

Native TLS + operator token + client token / per-prefix ACL provide transport encryption, admin auth, and optional data-path auth. Engine AES-GCM seals WAL/SST/manifest when configured. Firewall rules, mTLS (native or sidecar), volume encryption for Raft peer files, and configured tokens remain mandatory for any production-like deployment.
