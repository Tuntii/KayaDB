#!/usr/bin/env bash
# Run Linux eBPF verification inside Docker (Windows dev host harness).
# Writes logs to KAYA_GOAL_SCRATCH or /scratch when mounted.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRATCH="${KAYA_GOAL_SCRATCH:-/scratch}"
FAIL_LOG="${SCRATCH}/docker-verify-failure.log"
mkdir -p "$SCRATCH"
cd "$ROOT"

log() { echo "$@" | tee -a "$SCRATCH/kernel-probes-build.log"; }

write_failure() {
  local rc="$1"
  local msg="${2:-}"
  {
    echo "status=FAIL"
    echo "exit_code=${rc}"
    echo "timestamp=$(date -Iseconds)"
    echo "message=${msg}"
    echo "uname=$(uname -a)"
    echo "cargo=$(command -v cargo >/dev/null 2>&1 && cargo --version || echo missing)"
    echo "rustc=$(command -v rustc >/dev/null 2>&1 && rustc --version || echo missing)"
  } >>"$FAIL_LOG"
}

on_err() {
  local rc=$?
  local line="${BASH_LINENO[0]:-0}"
  local cmd="${BASH_COMMAND:-}"
  write_failure "$rc" "line=${line} cmd=${cmd}"
  exit "$rc"
}
trap on_err ERR

ensure_rust_toolchain() {
  if [[ -f "${HOME}/.cargo/env" ]]; then
    # shellcheck source=/dev/null
    source "${HOME}/.cargo/env"
  fi

  if ! command -v cargo >/dev/null 2>&1; then
    log "cargo missing; installing rustup (stable)..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq curl ca-certificates
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
    # shellcheck source=/dev/null
    source "${HOME}/.cargo/env"
  fi

  if command -v rustup >/dev/null 2>&1; then
    log "rustup default stable"
    rustup default stable 2>&1 | tee -a "$SCRATCH/kernel-probes-build.log"
    log "rustup component add rustfmt clippy"
    rustup component add rustfmt clippy 2>&1 | tee -a "$SCRATCH/kernel-probes-build.log" || log "warn: optional rustup components install failed"
  else
    log "rustup=not-installed (using preinstalled cargo)"
  fi

  log "cargo=$(cargo --version)"
  log "rustc=$(rustc --version)"
}

ensure_rust_toolchain

log "host=linux-docker"
log "timestamp=$(date -Iseconds)"
log "uname=$(uname -a)"

export DEBIAN_FRONTEND=noninteractive
if ! command -v clang >/dev/null 2>&1; then
  log "installing clang llvm..."
  apt-get update -qq
  apt-get install -y -qq clang llvm libelf-dev
fi
log "clang=$(clang --version | head -1)"

log "==> cargo build -p kaya-ebpf --features kernel-probes"
cargo build -p kaya-ebpf --features kernel-probes 2>&1 | tee -a "$SCRATCH/kernel-probes-build.log"

log "==> tier B: bpf_object_loads + kernel_load_object_and_drain_injected_ringbuf"
cargo test -p kaya-ebpf --features kernel-probes bpf_object_loads_without_cap_bpf kernel_load_object_and_drain_injected_ringbuf -- --test-threads=1 2>&1 | tee "$SCRATCH/kernel-ringbuf-tier-b.log"

log "==> full kaya-ebpf kernel-probes tests"
cargo test -p kaya-ebpf --features kernel-probes -- --test-threads=1 2>&1 | tee "$SCRATCH/kernel-ringbuf-kernel-probes.log"

BPF_OBJ="$(find target -name 'fsync_latency.bpf.o' -print -quit || true)"
if [[ -z "${BPF_OBJ}" ]]; then
  log "FAIL: fsync_latency.bpf.o not found"
  write_failure 1 "fsync_latency.bpf.o not found under target/"
  exit 1
fi
log "kaya_ebpf_bpf_built=yes"
log "bpf_object=${BPF_OBJ}"
ls -la "$BPF_OBJ" | tee -a "$SCRATCH/kernel-probes-build.log"

if [[ "${KAYA_EBPF_LIVE_KERNEL:-}" == "1" ]]; then
  log "==> tier C: live_kernel_attach_streams_events (--privileged required)"
  cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach_streams_events -- --ignored --test-threads=1 2>&1 | tee "$SCRATCH/kernel-live-attach.log"
else
  log "live_kernel_attach=skipped (set KAYA_EBPF_LIVE_KERNEL=1)"
fi

log "docker ebpf verification complete"