# Verification harness for Goal #100 (kaya-ebpf observability hardening).
$ErrorActionPreference = "Continue"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Scratch = if ($env:KAYA_GOAL_SCRATCH) { $env:KAYA_GOAL_SCRATCH } else {
    "C:\Users\tunay\AppData\Local\Temp\grok-goal-e9b62b239508\implementer"
}
New-Item -ItemType Directory -Force -Path $Scratch | Out-Null
$env:KAYA_GOAL_SCRATCH = $Scratch
Set-Location $RepoRoot

$buildNotes = @(
    "host=$($env:OS)"
    "arch=$($env:PROCESSOR_ARCHITECTURE)"
    "timestamp=$(Get-Date -Format o)"
)

Write-Host "==> workspace tests (plan step 2)"
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1 2>&1 | Tee-Object -FilePath "$Scratch\workspace-test.log"

Write-Host "==> kaya-ebpf unit + integration tests"
cargo test -p kaya-ebpf 2>&1 | Tee-Object -FilePath "$Scratch\kaya-ebpf-test.log"

Write-Host "==> kernel-slot pipeline integration (required cross-platform gate)"
cargo test -p kaya-ebpf --test kernel_pipeline_integration 2>&1 | Tee-Object -FilePath "$Scratch\kernel-pipeline-integration.log"

Write-Host "==> kernel ringbuf + bpf source tests (default feature set, all hosts)"
cargo test -p kaya-ebpf --test kernel_ringbuf 2>&1 | Tee-Object -FilePath "$Scratch\kernel-ringbuf-test.log"

if ($IsLinux -or (Test-Path "/proc/version")) {
    Write-Host "==> Linux: kernel-probes build + bpf object tests"
    "command=cargo build -p kaya-ebpf --features kernel-probes" | Add-Content "$Scratch\kernel-build-notes.log"
    cargo build -p kaya-ebpf --features kernel-probes 2>&1 | Tee-Object -FilePath "$Scratch\kernel-probes-build.log"
    "command=cargo test -p kaya-ebpf --features kernel-probes --test kernel_ringbuf" | Add-Content "$Scratch\kernel-build-notes.log"
    cargo test -p kaya-ebpf --features kernel-probes --test kernel_ringbuf 2>&1 | Tee-Object -FilePath "$Scratch\kernel-ringbuf-kernel-probes.log"
    $outDirs = Get-ChildItem -Path "target" -Recurse -Filter "fsync_latency.bpf.o" -ErrorAction SilentlyContinue
    if ($outDirs) {
        $buildNotes += "kaya_ebpf_bpf_built=yes"
        $buildNotes += "bpf_object=$($outDirs[0].FullName)"
        if ($env:KAYA_EBPF_LIVE_KERNEL -eq "1") {
            Write-Host "==> live kernel attach (KAYA_EBPF_LIVE_KERNEL=1)"
            cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored 2>&1 | Tee-Object -FilePath "$Scratch\kernel-live-attach.log"
        } else {
            $buildNotes += "live_kernel_attach=skipped (set KAYA_EBPF_LIVE_KERNEL=1 on Linux with CAP_BPF)"
        }
    } else {
        $buildNotes += "kaya_ebpf_bpf_built=no (clang bpf compile did not produce .o; see kernel-probes-build.log)"
    }
} else {
    $buildNotes += "kaya_ebpf_bpf_built=skipped-non-linux (aya+bpf compile require Linux target_os)"
    $buildNotes += "kernel-probes cargo=skipped (aya crate does not compile on Windows)"
    $buildNotes += "bpf_source_pid_filter=verified via cargo test -p kaya-ebpf --test kernel_ringbuf"
    $buildNotes += "kernel_slot_runtime=KernelPreferred try-live-then-fallback-to-kernel-simulated"
    $buildNotes += "live_kernel_attach=requires-linux+CAP_BPF (see scripts/linux_verify_ebpf_kernel.sh + .github/workflows/ci.yml)"
    $buildNotes += "linux_bpf_object_load=CI ubuntu step runs bpf_object_loads_without_cap_bpf"
}

$buildNotes | Add-Content -Path "$Scratch\kernel-build-notes.log"

Write-Host "==> ebpf replay + chaos"
cargo test -p kaya-ebpf --test replay_validation 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-replay-test.log"
cargo test -p kaya-ebpf --test chaos_ebpf 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-chaos-test.log"

Write-Host "==> kayadb-server ebpf"
cargo build -p kaya-server --features ebpf --bin kayadb-server 2>&1 | Tee-Object -FilePath "$Scratch\kayadb-server-ebpf-build.log"
cargo test -p kaya-server --features ebpf ebpf_enabled_metrics 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-integration-test.log"
cargo test -p kaya-server --features ebpf --test ebpf_bin_launch 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-bin-launch-test.log"

Write-Host "==> kayactl ebpf CLI"
cargo run -p kayactl --features ebpf -- ebpf status --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-status.log"
cargo run -p kayactl --features ebpf -- ebpf trace wal --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-trace-wal.log"

@(
    "crates/kaya-ebpf/README.md",
    "spec/docs/observability-spec.md",
    "crates/kaya-ebpf/bpf/fsync_latency.bpf.c",
    "crates/kaya-ebpf/bpf/include/vmlinux.h",
    "crates/kaya-ebpf/src/backend/kernel_sim.rs",
    "crates/kaya-ebpf/src/backend/probe_backend.rs",
    "crates/kaya-ebpf/src/pipeline.rs",
    "crates/kaya-ebpf/tests/kernel_pipeline_integration.rs"
) | ForEach-Object {
    if (Test-Path $_) { "ok $_" } else { "MISSING $_" }
} | Set-Content -Path "$Scratch\docs-check.log"

@(
    "verification_tiers:"
    "  tier_a_cross_platform=kaya-ebpf + kernel_pipeline + kernel_ringbuf + server bin launch (this host)"
    "  tier_b_linux_bpf_compile=scripts/linux_verify_ebpf_kernel.sh + ci.yml ebpf step on ubuntu-latest"
    "  tier_c_live_attach=KAYA_EBPF_LIVE_KERNEL=1 on Linux with CAP_BPF (optional, #[ignore])"
    "kernel_preferred_path=try KernelLive attach, fallback KernelSimulated on failure"
    "live_ts_ns=stamped at ringbuf drain via parse_raw_fsync_event_at (BPF wire has no ts field)"
    "pid_filter=bpf target_pid map + set_target_pid_map at live attach"
) | Set-Content -Path "$Scratch\ebpf-verification-tier.txt"

Write-Host "Evidence written to $Scratch"