# Verification harness for Goal #100 (kaya-ebpf observability hardening).
$ErrorActionPreference = "Continue"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Scratch = if ($env:KAYA_GOAL_SCRATCH) { $env:KAYA_GOAL_SCRATCH } else {
    "C:\Users\tunay\AppData\Local\Temp\grok-goal-e9b62b239508\implementer"
}
New-Item -ItemType Directory -Force -Path $Scratch | Out-Null
$env:KAYA_GOAL_SCRATCH = $Scratch
Set-Location $RepoRoot

Write-Host "==> kaya-ebpf unit + integration tests"
cargo test -p kaya-ebpf 2>&1 | Tee-Object -FilePath "$Scratch\kaya-ebpf-test.log"

Write-Host "==> kernel-slot pipeline integration (required cross-platform gate)"
cargo test -p kaya-ebpf --test kernel_pipeline_integration 2>&1 | Tee-Object -FilePath "$Scratch\kernel-pipeline-integration.log"

Write-Host "==> ebpf replay + chaos"
cargo test -p kaya-ebpf --test replay_validation 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-replay-test.log"
cargo test -p kaya-ebpf --test chaos_ebpf 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-chaos-test.log"

$buildNotes = @(
    "host=$($env:OS)"
    "arch=$($env:PROCESSOR_ARCHITECTURE)"
    "timestamp=$(Get-Date -Format o)"
)

if ($IsLinux -or (Test-Path "/proc/version")) {
    Write-Host "==> Linux: kernel-probes build + test"
    cargo build -p kaya-ebpf --features kernel-probes 2>&1 | Tee-Object -FilePath "$Scratch\kernel-probes-build.log"
    cargo test -p kaya-ebpf --features kernel-probes --test kernel_ringbuf 2>&1 | Tee-Object -FilePath "$Scratch\kernel-ringbuf-test.log"
    $outDirs = Get-ChildItem -Path "target" -Recurse -Filter "fsync_latency.bpf.o" -ErrorAction SilentlyContinue
    if ($outDirs) {
        $buildNotes += "kaya_ebpf_bpf_built=yes"
        $buildNotes += "bpf_object=$($outDirs[0].FullName)"
    } else {
        $buildNotes += "kaya_ebpf_bpf_built=no (clang bpf compile did not produce .o)"
    }
} else {
    Write-Host "==> Non-Linux: skip kernel-probes compile (kernel-simulated slot verified above)"
    $buildNotes += "kaya_ebpf_bpf_built=skipped-non-linux"
    $buildNotes += "kernel_slot_test=kernel_pipeline_integration (kernel-simulated)"
}

$buildNotes | Set-Content -Path "$Scratch\kernel-build-notes.log"

Write-Host "==> kayadb-server ebpf"
cargo build -p kaya-server --features ebpf --bin kayadb-server 2>&1 | Out-Null
cargo test -p kaya-server --features ebpf ebpf_enabled_metrics 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-integration-test.log"
cargo test -p kaya-server --features ebpf --test ebpf_bin_launch 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-bin-launch-test.log"

Write-Host "==> kayactl ebpf CLI"
cargo run -p kayactl --features ebpf -- ebpf status --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-status.log"
cargo run -p kayactl --features ebpf -- ebpf trace wal --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-trace-wal.log"

@(
    "crates/kaya-ebpf/README.md",
    "spec/docs/observability-spec.md",
    "scripts/ebpf/README.md",
    "crates/kaya-ebpf/bpf/fsync_latency.bpf.c",
    "crates/kaya-ebpf/bpf/include/vmlinux.h",
    "crates/kaya-ebpf/src/backend/kernel_sim.rs",
    "crates/kaya-ebpf/src/backend/probe_backend.rs",
    "crates/kaya-ebpf/src/pipeline.rs",
    "crates/kaya-ebpf/tests/kernel_pipeline_integration.rs"
) | ForEach-Object {
    if (Test-Path $_) { "ok $_" } else { "MISSING $_" }
} | Set-Content -Path "$Scratch\docs-check.log"

Write-Host "Evidence written to $Scratch"