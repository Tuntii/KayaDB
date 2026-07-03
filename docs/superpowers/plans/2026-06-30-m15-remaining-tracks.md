# M15 Remaining Tracks Implementation Plan

**Status:** ✅ Complete — shipped in **v0.1.46** (2026-06-30)  
**Primary commit:** `db7dcc3` (`feat-M15-remaining-tracks-v0.1.46`)  
**Docs sync:** `7e20696` (`docs-M15-v0.1.46-sync-and-ci-actions-guide`)

> **For agentic workers:** This plan is closed. Do not start new M15 work here. Post-M15 items live in [ROADMAP.md](../../../ROADMAP.md) parallel tracks (A–H). eBPF hardening continued after M15 exit (`db79cde` … `b1919f1`) — see Task 6 notes.

**Goal:** Close all post-M14 roadmap gaps: client authZ, audit logging, conformance suite, Go client, Prometheus metrics, eBPF crate, Docker/K8s deployment, protocol handshake, kayactl watch, and docs.

**Architecture:** Extend existing `ADMIN\x00` token pattern with `CLIENT\x00` for data-path ops; add optional JSONL audit sink in server; conformance vectors as JSON + Rust runner; Go client as separate module under `clients/kaya-go/`; Prometheus via lightweight HTTP sidecar in `kaya-server`; deployment under `deploy/`.

**Tech Stack:** Rust (kaya-net, kaya-server, kayactl, kaya-client), Go 1.23+, Docker, Kubernetes YAML, Prometheus text format.

**Worktree:** Merged to `main` from `feat/remaining-tracks` (`.worktrees/feat-remaining-tracks` may be stale).

**Exit verification:**
```powershell
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1
cd clients/kaya-go && go test ./...
```

Linux-only (CI): `scripts/linux_verify_ebpf_kernel.sh` — BPF object compile + `bpf_object_loads_without_cap_bpf`.

---

### Task 1: Client token auth for data-path ops ✅

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

- [x] **Step 1:** Add `CLIENT_AUTH_PREFIX`, `encode_client_auth_payload`, `decode_client_auth_payload` to `codec.rs` with roundtrip unit tests
- [x] **Step 2:** Add `client_token` to `ClusterConfig`, wire `--client-token` / `KAYA_CLIENT_TOKEN` in `main.rs`
- [x] **Step 3:** Enforce in `client_ops::dispatch` before handling PUT/GET/DELETE/SCAN/STATS
- [x] **Step 4:** Update `kaya-client` and `kayactl --server` to send token when configured
- [x] **Step 5:** Integration test `data_ops_require_client_token` in `integration_tests.rs`
- [x] **Step 6:** Commit `feat(server): client token auth for data-path ops`

---

### Task 2: Structured audit logging ✅

**Files:**
- Create: `crates/kaya-server/src/audit.rs`
- Modify: `crates/kaya-server/src/cluster/client_ops.rs`, `main.rs`, `cluster/mod.rs`

Audit events as JSONL lines to `{data_dir}/audit.jsonl`:
```json
{"ts":"2026-06-30T12:00:00Z","node_id":1,"peer":"127.0.0.1:1234","opcode":1,"key_hash":"...","status":0,"auth":"client"}
```

- [x] **Step 1:** `AuditLog` struct with `record(opcode, peer, status, auth_kind)` async-safe via `tokio::sync::Mutex`
- [x] **Step 2:** Hook into `dispatch` return path for all opcodes
- [x] **Step 3:** `--audit-log` flag (default on when any token configured)
- [x] **Step 4:** Unit test for JSONL line format
- [x] **Step 5:** Commit `feat(server): structured audit logging`

---

### Task 3: Protocol conformance vectors + Rust runner ✅

**Files:**
- Create: `docs/clients/conformance/vectors.json`
- Create: `crates/kaya-net/tests/conformance_vectors.rs`

Vectors cover encode/decode roundtrips for PUT/GET payloads, admin framing, client auth framing, response status codes.

- [x] **Step 1:** Write `vectors.json` with 20+ cases
- [x] **Step 2:** Runner test loads JSON and asserts codec functions
- [x] **Step 3:** Commit `test(net): protocol conformance vectors`

---

### Task 4: Go client ✅

**Files:**
- Create: `clients/kaya-go/go.mod`, `codec.go`, `client.go`, `errors.go`, `codec_test.go`, `README.md`

Implement Put/Get/Delete/Scan/Health/Stats with leader redirect, client token, TLS optional stub.

- [x] **Step 1:** `codec.go` matching `kaya-net` wire format
- [x] **Step 2:** `client.go` with retry + NOT_LEADER redirect
- [x] **Step 3:** Unit tests for codec (`codec_test.go`); integration test skipped without server
- [x] **Step 4:** Commit `feat(clients): Go client bootstrap`

**Post-exit notes:** Module path `github.com/Tuntii/KayaDB/clients/kaya-go`. Stays in monorepo per `docs/clients/go-client.md`. TLS stub and `client_test.go` (end-to-end) remain optional follow-ups under Track D.

---

### Task 5: Prometheus /metrics exporter ✅

**Files:**
- Create: `crates/kaya-server/src/metrics.rs`
- Modify: `crates/kaya-server/src/cluster/mod.rs`, `main.rs`

HTTP listener on `--metrics-addr` (default `127.0.0.1:9090`) exposing:
- `kaya_wal_fsync_total_us`, `kaya_engine_live_sstables`, `kaya_raft_role`, `kaya_raft_term`

- [x] **Step 1:** `MetricsSnapshot` from engine stats + raft status
- [x] **Step 2:** `render_prometheus()` text format
- [x] **Step 3:** Tokio HTTP accept loop (hyper or tiny hand-rolled)
- [x] **Step 4:** Unit test for render output
- [x] **Step 5:** Commit `feat(server): Prometheus metrics endpoint`

---

### Task 6: kaya-ebpf crate restoration ✅

**Files:**
- Create: `crates/kaya-ebpf/Cargo.toml`, `src/lib.rs`
- Modify: root `Cargo.toml` workspace members

Linux-gated stub with `probe_catalog()` returning script paths; no hard dep on libbpf.

- [x] **Step 1:** Scaffold crate with `#[cfg(target_os = "linux")]` module
- [x] **Step 2:** Export `available_scripts()` listing `scripts/ebpf/*.bt`
- [x] **Step 3:** `cargo test -p kaya-ebpf` passes on all platforms (no-op on Windows)
- [x] **Step 4:** Commit `feat(observe): restore kaya-ebpf stub crate`

**Post-M15 scope expansion (Track A, not required for M15 exit):** In-process runtime (`db79cde`), KernelPreferred backend, BPF ringbuf tests, `kayadb-server --ebpf`, kayactl `ebpf` subcommands, Linux CI gate (`scripts/linux_verify_ebpf_kernel.sh`). Windows uses kernel-simulated fallback; live kernel attach is `#[ignore]` + `KAYA_EBPF_LIVE_KERNEL=1`.

---

### Task 7: Docker deployment ✅

**Files:**
- Create: `deploy/docker/Dockerfile`, `deploy/docker/docker-compose.yml`, `deploy/docker/README.md`

3-node cluster + kayactl sidecar pattern.

- [x] **Step 1:** Multi-stage Rust build Dockerfile
- [x] **Step 2:** docker-compose with 3 kayadb-server services
- [x] **Step 3:** Commit `feat(deploy): Docker compose 3-node cluster`

**Note:** Manifests are reference-only; local Docker verification is optional. Operators may use `deploy/k8s/` or systemd instead.

---

### Task 8: Kubernetes manifests ✅

**Files:**
- Create: `deploy/k8s/namespace.yaml`, `statefulset.yaml`, `service.yaml`, `configmap.yaml`, `README.md`

- [x] **Step 1:** StatefulSet 3 replicas with headless service
- [x] **Step 2:** ConfigMap for peer roster template
- [x] **Step 3:** Commit `feat(deploy): Kubernetes StatefulSet manifests`

---

### Task 9: Protocol version handshake ✅

**Files:**
- Modify: `crates/kaya-net/src/codec.rs`, `transport.rs`, `client_ops.rs`

New opcode 0 = HELLO: client sends `proto_version(u16)=1`, server responds OK with `server_version(u16)=1`.

- [x] **Step 1:** Define `PROTO_VERSION = 1`, encode/decode helpers
- [x] **Step 2:** Optional: client sends HELLO on connect; server rejects unknown versions
- [x] **Step 3:** Tests + update wire protocol doc
- [x] **Step 4:** Commit `feat(protocol): version handshake opcode 0`

---

### Task 10: kayactl watch + EngineStats v2 ✅

**Files:**
- Modify: `crates/kayactl/src/stats_cmd.rs`, `cli.rs`, `watch.rs`
- Modify: `crates/kaya-engine/src/stats.rs`, `crates/kaya-lsm/src/block_cache.rs`

Add `kayactl watch [--interval 2] status` and expose `block_cache_hits/misses`, `recovery_duration_us` in stats JSON.

- [x] **Step 1:** Track cache hits/misses in `BlockCache`
- [x] **Step 2:** Recovery timer in engine open
- [x] **Step 3:** `kayactl watch` loop printing stats
- [x] **Step 4:** Commit `feat(cli): watch mode and cache stats`

---

### Task 11: Documentation updates ✅

**Files:**
- Modify: `docs/security.md`, `ROADMAP.md`, `CHANGELOG.md`, `docs/clients/client-wire-protocol.md`

- [x] **Step 1:** Update security §7 — client token closes authZ gap for data ops
- [x] **Step 2:** ROADMAP parallel tracks status
- [x] **Step 3:** CHANGELOG Unreleased section
- [x] **Step 4:** Commit `docs: M15 remaining tracks closure`

---

## M15 exit summary

All 11 tasks shipped in v0.1.46. Accepted risks documented in `docs/security.md` §7: data-at-rest encryption, multi-tenant isolation, SIEM audit export.

**Optional follow-ups (not M15 blockers):**

| Item | Track | Notes |
|------|-------|-------|
| Go client CI job (`go test ./...`) | D | Not in `.github/workflows/ci.yml` yet |
| Go TLS + `client_test.go` e2e | D | Stub deferred |
| OpenTelemetry spans | A | Prometheus done; OTel ⬜ |
| Python / TS / Zig clients | D | ⬜ planned |
| Incremental backup | E | Runbook exists; incremental TBD |
| SLO / error budget envelopes | E | ⬜ planned |

See [ROADMAP.md](../../../ROADMAP.md) § "Long-term Expanded Roadmap — Parallel Tracks" for next work.