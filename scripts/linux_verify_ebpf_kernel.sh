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
  echo "installing clang for bpf compile..."
  sudo apt-get update -qq
  sudo apt-get install -y clang llvm
fi

echo "==> cargo test -p kaya-ebpf --features kernel-probes (tier B: bpf load + injected decode)"
cargo test -p kaya-ebpf --features kernel-probes bpf_object_loads_without_cap_bpf kernel_load_object_and_drain_injected_ringbuf -- --test-threads=1
echo "==> cargo test -p kaya-ebpf --features kernel-probes (full crate)"
cargo test -p kaya-ebpf --features kernel-probes -- --test-threads=1

BPF_OBJ="$(find target -name 'fsync_latency.bpf.o' -print -quit || true)"
if [[ -z "${BPF_OBJ}" ]]; then
  echo "FAIL: fsync_latency.bpf.o not found under target/ after kernel-probes build"
  exit 1
fi

echo "kaya_ebpf_bpf_built=yes"
echo "bpf_object=${BPF_OBJ}"
echo "bpf_object_loads=exercised via bpf_object_loads_without_cap_bpf (kernel_ringbuf + unit tests)"

if [[ "${KAYA_EBPF_LIVE_KERNEL:-}" == "1" ]]; then
  echo "==> live kernel attach (KAYA_EBPF_LIVE_KERNEL=1)"
  cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored
else
  echo "live_kernel_attach=skipped (set KAYA_EBPF_LIVE_KERNEL=1 with CAP_BPF to run)"
fi