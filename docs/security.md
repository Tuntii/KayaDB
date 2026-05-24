# KayaDB Security and Deployment Guide

This document outlines the security architecture, networking requirements, and best practices for deploying **KayaDB** securely in production environments.

---

## 1. Network Security Architecture

By default, KayaDB's Raft consensus protocol (`kaya-net`) and client communication protocols run over **raw, unencrypted TCP**.
* **Raft Transport:** Exchanged state machine replicates, heartbeats, and leader election envelopes without payload encryption.
* **Client Protocol:** Client PUT, GET, DELETE, and SCAN operations are transmitted in plain text.
* **Authentication:** There is no built-in password, token-based, or TLS certificate authentication in the current version.

Because of these design choices (focused on high performance and zero external dependencies), **security must be enforced at the infrastructure level**.

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
