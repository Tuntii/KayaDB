# M15 Remaining Tracks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close all post-M14 roadmap gaps: client authZ, audit logging, conformance suite, Go client, Prometheus metrics, eBPF crate, Docker/K8s deployment, protocol handshake, kayactl watch, and docs.

**Architecture:** Extend existing `ADMIN\x00` token pattern with `CLIENT\x00` for data-path ops; add optional JSONL audit sink in server; conformance vectors as JSON + Rust runner; Go client as separate module under `clients/kaya-go/`; Prometheus via lightweight HTTP sidecar in `kaya-server`; deployment under `deploy/`.

**Tech Stack:** Rust (kaya-net, kaya-server, kayactl, kaya-client), Go 1.22+, Docker, Kubernetes YAML, Prometheus text format.

**Worktree:** `.worktrees/feat-remaining-tracks` on branch `feat/remaining-tracks`

---

### Task 1: Client token auth for data-path ops

**Files:**
- Create: `crates/kaya-server/src/client_auth.rs`
- Modify: `crates/kaya-net/src/codec.rs`, `crates/kaya-net/src/lib.rs`
- Modify: `crates/kaya-server/src/cluster/mod.rs`, `client_ops.rs`, `main.rs`
- Modify: `crates/kaya-client/src/lib.rs`, `crates/kayactl/src/main.rs`, `cli.rs`, `server.rs`
- Test: `crates/kaya-server/src/integration_tests.rs`, `crates/kaya-net/src/codec.rs` (unit tests)

**Wire format (mirror ADMIN):**
```
CLIENT\x00 | token_len(u16 LE) | token_bytes | [original payload unchanged]
```
When `--client-token` is set, opcodes 1–4 and 6 require matching token prefix. Opcode 5 (HEALTH) stays open for probes.

- [ ] **Step 1:** Add `CLIENT_AUTH_PREFIX`, `encode_client_auth_payload`, `decode_client_auth_payload` to `codec.rs` with roundtrip unit tests
- [ ] **Step 2:** Add `client_token` to `ClusterConfig`, wire `--client-token` / `KAYA_CLIENT_TOKEN` in `main.rs`
- [ ] **Step 3:** Enforce in `client_ops::dispatch` before handling PUT/GET/DELETE/SCAN/STATS
- [ ] **Step 4:** Update `kaya-client` and `kayactl --server` to send token when configured
- [ ] **Step 5:** Integration test `data_ops_require_client_token` in `integration_tests.rs`
- [ ] **Step 6:** Commit `feat(server): client token auth for data-path ops`

---

### Task 2: Structured audit logging

**Files:**
- Create: `crates/kaya-server/src/audit.rs`
- Modify: `crates/kaya-server/src/cluster/client_ops.rs`, `main.rs`, `cluster/mod.rs`

Audit events as JSONL lines to `{data_dir}/audit.jsonl`:
```json
{"ts":"2026-06-30T12:00:00Z","node_id":1,"peer":"127.0.0.1:1234","opcode":1,"key_hash":"...","status":0,"auth":"client"}
```

- [ ] **Step 1:** `AuditLog` struct with `record(opcode, peer, status, auth_kind)` async-safe via `tokio::sync::Mutex`
- [ ] **Step 2:** Hook into `dispatch` return path for all opcodes
- [ ] **Step 3:** `--audit-log` flag (default on when any token configured)
- [ ] **Step 4:** Unit test for JSONL line format
- [ ] **Step 5:** Commit `feat(server): structured audit logging`

---

### Task 3: Protocol conformance vectors + Rust runner

**Files:**
- Create: `docs/clients/conformance/vectors.json`
- Create: `crates/kaya-net/tests/conformance_vectors.rs`

Vectors cover encode/decode roundtrips for PUT/GET payloads, admin framing, client auth framing, response status codes.

- [ ] **Step 1:** Write `vectors.json` with 20+ cases
- [ ] **Step 2:** Runner test loads JSON and asserts codec functions
- [ ] **Step 3:** Commit `test(net): protocol conformance vectors`

---

### Task 4: Go client

**Files:**
- Create: `clients/kaya-go/go.mod`, `codec.go`, `client.go`, `errors.go`, `client_test.go`, `README.md`

Implement Put/Get/Delete/Scan/Health/Stats with leader redirect, client token, TLS optional stub.

- [ ] **Step 1:** `codec.go` matching `kaya-net` wire format
- [ ] **Step 2:** `client.go` with retry + NOT_LEADER redirect
- [ ] **Step 3:** Unit tests for codec; integration test skipped without server
- [ ] **Step 4:** Commit `feat(clients): Go client bootstrap`

---

### Task 5: Prometheus /metrics exporter

**Files:**
- Create: `crates/kaya-server/src/metrics.rs`
- Modify: `crates/kaya-server/src/cluster/mod.rs`, `main.rs`

HTTP listener on `--metrics-addr` (default `127.0.0.1:9090`) exposing:
- `kaya_wal_fsync_total_us`, `kaya_engine_live_sstables`, `kaya_raft_role`, `kaya_raft_term`

- [ ] **Step 1:** `MetricsSnapshot` from engine stats + raft status
- [ ] **Step 2:** `render_prometheus()` text format
- [ ] **Step 3:** Tokio HTTP accept loop (hyper or tiny hand-rolled)
- [ ] **Step 4:** Unit test for render output
- [ ] **Step 5:** Commit `feat(server): Prometheus metrics endpoint`

---

### Task 6: kaya-ebpf crate restoration

**Files:**
- Create: `crates/kaya-ebpf/Cargo.toml`, `src/lib.rs`
- Modify: root `Cargo.toml` workspace members

Linux-gated stub with `probe_catalog()` returning script paths; no hard dep on libbpf.

- [ ] **Step 1:** Scaffold crate with `#[cfg(target_os = "linux")]` module
- [ ] **Step 2:** Export `available_scripts()` listing `scripts/ebpf/*.bt`
- [ ] **Step 3:** `cargo test -p kaya-ebpf` passes on all platforms (no-op on Windows)
- [ ] **Step 4:** Commit `feat(observe): restore kaya-ebpf stub crate`

---

### Task 7: Docker deployment

**Files:**
- Create: `deploy/docker/Dockerfile`, `deploy/docker/docker-compose.yml`, `deploy/docker/README.md`

3-node cluster + kayactl sidecar pattern.

- [ ] **Step 1:** Multi-stage Rust build Dockerfile
- [ ] **Step 2:** docker-compose with 3 kayadb-server services
- [ ] **Step 3:** Commit `feat(deploy): Docker compose 3-node cluster`

---

### Task 8: Kubernetes manifests

**Files:**
- Create: `deploy/k8s/namespace.yaml`, `statefulset.yaml`, `service.yaml`, `configmap.yaml`, `README.md`

- [ ] **Step 1:** StatefulSet 3 replicas with headless service
- [ ] **Step 2:** ConfigMap for peer roster template
- [ ] **Step 3:** Commit `feat(deploy): Kubernetes StatefulSet manifests`

---

### Task 9: Protocol version handshake

**Files:**
- Modify: `crates/kaya-net/src/codec.rs`, `transport.rs`, `client_ops.rs`

New opcode 0 = HELLO: client sends `proto_version(u16)=1`, server responds OK with `server_version(u16)=1`.

- [ ] **Step 1:** Define `PROTO_VERSION = 1`, encode/decode helpers
- [ ] **Step 2:** Optional: client sends HELLO on connect; server rejects unknown versions
- [ ] **Step 3:** Tests + update wire protocol doc
- [ ] **Step 4:** Commit `feat(protocol): version handshake opcode 0`

---

### Task 10: kayactl watch + EngineStats v2

**Files:**
- Modify: `crates/kayactl/src/stats_cmd.rs`, `cli.rs`
- Modify: `crates/kaya-engine/src/stats.rs`, `crates/kaya-lsm/src/block_cache.rs`

Add `kayactl watch [--interval 2] status` and expose `block_cache_hits/misses`, `recovery_duration_us` in stats JSON.

- [ ] **Step 1:** Track cache hits/misses in `BlockCache`
- [ ] **Step 2:** Recovery timer in engine open
- [ ] **Step 3:** `kayactl watch` loop printing stats
- [ ] **Step 4:** Commit `feat(cli): watch mode and cache stats`

---

### Task 11: Documentation updates

**Files:**
- Modify: `docs/security.md`, `ROADMAP.md`, `CHANGELOG.md`, `docs/clients/client-wire-protocol.md`

- [ ] **Step 1:** Update security §7 — client token closes authZ gap for data ops
- [ ] **Step 2:** ROADMAP parallel tracks status
- [ ] **Step 3:** CHANGELOG Unreleased section
- [ ] **Step 4:** Commit `docs: M15 remaining tracks closure`