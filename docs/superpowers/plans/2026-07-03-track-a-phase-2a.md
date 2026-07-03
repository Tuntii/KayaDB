# Track A Phase 2A — kayactl ebpf CLI & Correlation

**Status:** ✅ Complete — docs sync 2026-07-03  
**Primary commits:** `a98e2ec` … `fb27af5` (implementation), docs commit `docs: Track A phase 2A CLI and correlation`

> **For agentic workers:** This plan is closed. Next Track A work lives in [ROADMAP.md](../../../ROADMAP.md) § Track A (medium/long-term: USDT, flamegraphs, per-file bpftrace filters, OpenTelemetry).

**Goal:** Harden `kayactl ebpf` for day-2 observability: PID/script discovery, bpftrace wrappers with timed `--run`, userspace↔kernel correlation report, Makefile helpers, and documentation sync.

**Architecture:** Split `kayactl` ebpf into `ebpf.rs` (dispatch + in-process artifacts), `ebpf_bpftrace.rs` (script resolution, pgrep discovery, bpftrace child spawn), `ebpf_correlate.rs` (engine stats + `trace.jsonl`/`status.json` comparison). Non-Linux graceful degradation; bpftrace scripts remain standalone in `scripts/ebpf/`.

**Tech Stack:** Rust (`kayactl` with `ebpf` feature), bpftrace `.bt` scripts, `kaya-ebpf` probe artifacts, Makefile, Docker verify harness.

**Exit verification:**
```powershell
cargo test -p kayactl --features ebpf
# Linux only:
cd scripts/ebpf && make verify
```

---

### Task 1: ebpf script resolution and pid discovery ✅

**Files:**
- Create: `crates/kayactl/src/ebpf_bpftrace.rs`
- Modify: `crates/kayactl/src/ebpf.rs`, `main.rs`

- [x] `resolve_script()` with `KAYA_EBPF_SCRIPT_DIR`, cwd, and walk-up search
- [x] `discover_server_pids()` / `pgrep` helpers
- [x] Commit `feat(kayactl): ebpf script resolution and pid discovery` (`a98e2ec`)

---

### Task 2: ebpf list subcommand ✅

**Files:**
- Modify: `crates/kayactl/src/ebpf.rs`, `ebpf_bpftrace.rs`

- [x] `kayactl ebpf list` — server PIDs + cmdline, active bpftrace PIDs, catalog script names
- [x] Commit `feat(kayactl): ebpf list subcommand` (`eb10521`)

---

### Task 3: bpftrace wrapper subcommands ✅

**Files:**
- Modify: `crates/kayactl/src/ebpf.rs`, `ebpf_bpftrace.rs`, `main.rs`, `Cargo.toml`

- [x] `fsync-latency`, `block-latency`, `syscall-timeline` wrappers
- [x] `--pid` auto-detect, `--run` spawn + stream, `--duration` (default 10s, SIGTERM)
- [x] Without `--run`: print manual `sudo bpftrace -p <PID> <script>`
- [x] Commit `feat(kayactl): bpftrace wrapper subcommands` (`e1db56a`)

---

### Task 4: ebpf correlate userspace-kernel report ✅

**Files:**
- Create: `crates/kayactl/src/ebpf_correlate.rs`
- Modify: `crates/kayactl/src/ebpf.rs`, `stats_cmd.rs`, `main.rs`, `Cargo.toml`

- [x] `kayactl ebpf correlate` — userspace `wal_fsync_*` + `flush_*` vs kernel trace averages
- [x] Delta hints, missing-trace guidance (`kayadb-server --ebpf`)
- [x] Unit tests with fixture `trace.jsonl` / `status.json`
- [x] Commit `feat(kayactl): ebpf correlate userspace-kernel report` (`bd7fb9a`)

---

### Task 5: scripts/ebpf Makefile helpers ✅

**Files:**
- Create: `scripts/ebpf/Makefile`
- Modify: `scripts/ebpf/README.md`

- [x] `make list|fsync|block|timeline|verify` targets
- [x] Commit `chore(ebpf): scripts/ebpf Makefile helpers` (`34b532c`)

---

### Task 6: Docker kernel verify harness ✅

**Files:**
- Create: `scripts/docker_verify_ebpf_kernel.sh`, `scripts/docker_verify_ebpf_kernel.ps1`

- [x] Containerized kernel eBPF gate for hosts without native BPF dev env
- [x] Commit `fix(ebpf): harden docker kernel verify harness` (`fb27af5`)

---

### Task 7: Documentation sync ✅

**Files:**
- Modify: `docs/cli-reference.md`, `ROADMAP.md`, `spec/docs/observability-spec.md` §7, `CHANGELOG.md` Unreleased
- Create: `docs/superpowers/plans/2026-07-03-track-a-phase-2a.md` (this file)

- [x] CLI reference aligned with all ebpf subcommands: list, status, trace wal, correlate, fsync/block/syscall-timeline (--run, --duration)
- [x] ROADMAP Track A short-term items marked ✅/🟡 for Phase 2A
- [x] observability-spec §7: bpftrace wrapper + correlate
- [x] CHANGELOG Unreleased section
- [x] Commit `docs: Track A phase 2A CLI and correlation`

---

## Phase 2A exit summary

All 7 tasks shipped. Track A short-term observability goals (list/status/correlate, bpftrace wrappers, userspace latency pairing) are complete for current scope.

**Optional follow-ups (not Phase 2A blockers):**

| Item | Track | Notes |
|------|-------|-------|
| Per-file / data-dir bpftrace filters | A | Extend `.bt` scripts or add filtered variants |
| Parallel multi-script `--run` | A | Manual via separate terminals / Makefile today |
| OpenTelemetry spans | A | Prometheus done; OTel ⬜ |
| USDT markers in engine | A | Medium-term ROADMAP item |
| Flamegraph integration | A | bpftrace + collapse helper |

See [ROADMAP.md](../../../ROADMAP.md) § "Track A: Observability" for next work.