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

### Server enforcement (M11 + M13 progress)

| Control | Default | Override / Location | Effect | Enforced in code? |
|---|---|---|---|---|
| Bind address | `127.0.0.1` | `--raft-addr` / `--client-addr` | Loopback-only unless widened | ✅ `security::validate_bind_addr` |
| Public bind guard | rejects public/wildcard | `--allow-public-bind` | Banner + allow; no built-in auth/TLS | ✅ startup + security.rs |
| Raft / client frame size | 64 MiB max | compile-time in codec | Oversize → decode error | ✅ |
| Roster / unknown peer | drop | static at start (RaftNode) | Unknown `from` ids ignored | ✅ |
| Snapshot file protection (refcounts) | pinned SSTs during active snapshot | engine refcounts + release on new snapshot | Compaction cannot delete live snap data | ✅ kaya-engine (create/install/release) |
| Durable snapshot on restart | loads `raft-snapshot.bin` + engine state | startup in cluster.rs | Follower/leader restart preserves applied state | ✅ (improved M13 for T7) |
| Crash safety on snapshot persist | tmp + rename + fsync + dir sync | compaction path | Atomic snapshot file | ✅ |

`kayadb-server` calls security checks before binding listeners. See `crates/kaya-server/src/security.rs` and `cluster.rs` (snapshot load + compaction).

Treat `--allow-public-bind` as explicit ack that you have perimeter controls (firewall + mTLS sidecar or equivalent).

---

## 3. Transport Layer Encryption (TLS Wrapper)

If your network spans across non-trusted environments or requires data-in-transit encryption to comply with security standards (e.g., SOC2, PCI-DSS), you must wrap KayaDB network interfaces in a TLS proxy.

We recommend using **[ghostunnel](https://github.com/ghostunnel/ghostunnel)**, a simple SSL/TLS proxy with mutual authentication support, or **stunnel**.

### mTLS Wrapping Example with Ghostunnel:

For each KayaDB node:
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

## 7. What KayaDB Does Not Yet Provide

- Authentication or authorization.
- TLS/mTLS built into the server.
- Encrypted storage files.
- Multi-tenant isolation.
- Automatic TLS or auth on membership/admin RPCs (ADD_MEMBER is leader-only but unauthenticated).
- Audit logging suitable for compliance.
- A hardened remote administration API.

These are future hardening areas. Until they exist, infrastructure-level isolation is mandatory.
