# Windows wrapper: run scripts/docker_verify_ebpf_kernel.sh in privileged Linux Docker.
# Mounts repo + scratch; forwards optional KAYA_EBPF_LIVE_KERNEL=1 for tier-C live attach.
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Scratch = if ($env:KAYA_GOAL_SCRATCH) { $env:KAYA_GOAL_SCRATCH } else {
    Join-Path $env:TEMP "kaya-docker-ebpf-verify"
}
New-Item -ItemType Directory -Force -Path $Scratch | Out-Null
$env:KAYA_GOAL_SCRATCH = $Scratch

Write-Host "==> docker ebpf kernel verify"
Write-Host "repo=$RepoRoot"
Write-Host "scratch=$Scratch"
if ($env:KAYA_EBPF_LIVE_KERNEL -eq "1") {
    Write-Host "tier_c=live_kernel_attach_streams_events (KAYA_EBPF_LIVE_KERNEL=1)"
} else {
    Write-Host "tier_c=skipped (set KAYA_EBPF_LIVE_KERNEL=1 for live attach)"
}

$dockerArgs = @(
    "run", "--rm", "--privileged",
    "-v", "${RepoRoot}:/workspace",
    "-v", "${Scratch}:/scratch",
    "-e", "KAYA_GOAL_SCRATCH=/scratch",
    "-w", "/workspace"
)
if ($env:KAYA_EBPF_LIVE_KERNEL -eq "1") {
    $dockerArgs += "-e", "KAYA_EBPF_LIVE_KERNEL=1"
}
$dockerArgs += "rust:1-bookworm", "bash", "scripts/docker_verify_ebpf_kernel.sh"

& docker @dockerArgs
if ($LASTEXITCODE -ne 0) {
    $failLog = Join-Path $Scratch "docker-verify-failure.log"
    if (-not (Test-Path $failLog)) {
        @(
            "status=FAIL"
            "exit_code=$LASTEXITCODE"
            "timestamp=$(Get-Date -Format o)"
            "message=docker run exited non-zero (see kernel-probes-build.log)"
        ) | Set-Content -Path $failLog
    }
    throw "docker ebpf kernel verify failed (exit=$LASTEXITCODE); evidence in $Scratch"
}

Write-Host "Evidence written to $Scratch"