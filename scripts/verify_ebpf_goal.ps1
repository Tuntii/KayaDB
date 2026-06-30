# Verification harness for Goal #100 (kaya-ebpf observability hardening).
# Writes evidence to $env:KAYA_GOAL_SCRATCH or the default implementer scratch dir.

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Scratch = if ($env:KAYA_GOAL_SCRATCH) { $env:KAYA_GOAL_SCRATCH } else {
    "C:\Users\tunay\AppData\Local\Temp\grok-goal-e9b62b239508\implementer"
}
New-Item -ItemType Directory -Force -Path $Scratch | Out-Null
$env:KAYA_GOAL_SCRATCH = $Scratch

Set-Location $RepoRoot

Write-Host "==> kaya-ebpf tests"
cargo test -p kaya-ebpf 2>&1 | Tee-Object -FilePath "$Scratch\kaya-ebpf-test.log"

Write-Host "==> kernel ringbuf + bpf object tests"
cargo test -p kaya-ebpf --test kernel_ringbuf 2>&1 | Tee-Object -FilePath "$Scratch\kernel-ringbuf-test.log"

Write-Host "==> ebpf replay validation"
cargo test -p kaya-ebpf --test replay_validation 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-replay-test.log"

Write-Host "==> ebpf chaos test"
cargo test -p kaya-ebpf --test chaos_ebpf 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-chaos-test.log"

Write-Host "==> workspace tests (exclude jepsen)"
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1 2>&1 | Tee-Object -FilePath "$Scratch\workspace-test.log"

Write-Host "==> kayadb-server ebpf integration + bin launch"
cargo build -p kaya-server --features ebpf --bin kayadb-server 2>&1 | Out-Null
cargo test -p kaya-server --features ebpf ebpf_enabled_metrics 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-integration-test.log"
cargo test -p kaya-server --features ebpf --test ebpf_bin_launch 2>&1 | Tee-Object -FilePath "$Scratch\ebpf-bin-launch-test.log"

Write-Host "==> kayactl ebpf CLI (non-Linux graceful)"
cargo run -p kayactl --features ebpf -- ebpf status --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-status.log"
cargo run -p kayactl --features ebpf -- ebpf trace wal --data ./data 2>&1 | Tee-Object -FilePath "$Scratch\kayactl-ebpf-trace-wal.log"

@(
    "crates/kaya-ebpf/README.md",
    "spec/docs/observability-spec.md",
    "scripts/ebpf/README.md",
    "scripts/analyze_ebpf_trace.ps1"
) | ForEach-Object {
    $path = Join-Path $RepoRoot $_
    if (-not (Test-Path $path)) { throw "missing doc: $_" }
    "ok $_" 
} | Set-Content -Path "$Scratch\docs-check.log"

Write-Host "Evidence written to $Scratch"