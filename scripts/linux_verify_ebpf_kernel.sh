#!/usr/bin/env bash
# Linux-only gate: compile bpf/fsync_latency.bpf.c and verify aya object load + ringbuf tests.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "skip: linux_verify_ebpf_kernel.sh requires Linux (host=$(uname -s))"
  exit 0
fi

if ! command -v clang >/dev/null 2>&1; then
  echo "installing clang/llvm for bpf compile..."
  sudo apt-get update -qq
  sudo apt-get install -y clang llvm
fi

echo "==> clang: $(clang --version | head -1)"

# GitHub-hosted runners often disable unprivileged BPF map creation. Enable it
# for the CAP-free object-load gate (test name: bpf_object_loads_without_cap_bpf).
if [[ -w /proc/sys/kernel/unprivileged_bpf_disabled ]] || command -v sudo >/dev/null 2>&1; then
  sudo sysctl -w kernel.unprivileged_bpf_disabled=0 2>/dev/null \
    || sysctl -w kernel.unprivileged_bpf_disabled=0 2>/dev/null \
    || echo "warn: could not set unprivileged_bpf_disabled=0 (continuing)"
fi
# Large maps need raised memlock; ignore if not permitted.
ulimit -l unlimited 2>/dev/null || true

echo "==> cargo build -p kaya-ebpf --features kernel-probes"
# Force rebuild of build.rs so clang is re-invoked after apt install.
cargo clean -p kaya-ebpf -q || true
cargo build -p kaya-ebpf --features kernel-probes 2>&1 | tee /tmp/kaya-ebpf-build.log

echo "==> locate fsync_latency.bpf.o"
BPF_OBJ="$(find target -name 'fsync_latency.bpf.o' -print -quit || true)"
if [[ -z "${BPF_OBJ}" ]]; then
  echo "clang/bpf compile did not produce fsync_latency.bpf.o; build log tail:"
  tail -n 80 /tmp/kaya-ebpf-build.log || true
  # Fallback: compile the object explicitly into target/ so the gate is deterministic.
  mkdir -p target/ebpf
  EXPLICIT_OBJ="target/ebpf/fsync_latency.bpf.o"
  if clang -g -O2 -target bpf -D__TARGET_ARCH_x86 \
      -I crates/kaya-ebpf/bpf/include \
      -c crates/kaya-ebpf/bpf/fsync_latency.bpf.c \
      -o "${EXPLICIT_OBJ}" 2>&1 | tee /tmp/kaya-ebpf-clang-direct.log; then
    BPF_OBJ="${EXPLICIT_OBJ}"
    echo "explicit clang bpf compile ok: ${BPF_OBJ}"
  else
    echo "FAIL: could not build fsync_latency.bpf.o (cargo build.rs + direct clang both failed)"
    cat /tmp/kaya-ebpf-clang-direct.log || true
    exit 1
  fi
fi

echo "bpf_object=${BPF_OBJ}"

echo "==> cargo test -p kaya-ebpf --features kernel-probes (tier B names, one filter each)"
# Modern cargo accepts a single TESTNAME; run each tier-B filter separately.
cargo test -p kaya-ebpf --features kernel-probes bpf_object_loads_without_cap_bpf -- --test-threads=1
cargo test -p kaya-ebpf --features kernel-probes kernel_load_object_and_drain_injected_ringbuf -- --test-threads=1

echo "==> cargo test -p kaya-ebpf --features kernel-probes (full crate)"
cargo test -p kaya-ebpf --features kernel-probes -- --test-threads=1

echo "kaya_ebpf_bpf_built=yes"
echo "bpf_object_loads=exercised via bpf_object_loads_without_cap_bpf (kernel_ringbuf + unit tests)"

if [[ "${KAYA_EBPF_LIVE_KERNEL:-}" == "1" ]]; then
  echo "==> live kernel attach (KAYA_EBPF_LIVE_KERNEL=1)"
  cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored
else
  echo "live_kernel_attach=skipped (set KAYA_EBPF_LIVE_KERNEL=1 with CAP_BPF to run)"
fi
