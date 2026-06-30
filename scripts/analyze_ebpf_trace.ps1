# Simple trace.jsonl inspector for eBPF durability events.
param(
    [Parameter(Mandatory = $true)]
    [string]$TracePath
)

if (-not (Test-Path $TracePath)) {
    Write-Error "trace file not found: $TracePath"
    exit 1
}

$lines = Get-Content $TracePath
$header = $lines | Select-Object -First 1
Write-Host "header: $header"

$fsync = 0
$fdatasync = 0
$maxLatency = 0

foreach ($line in $lines | Select-Object -Skip 1) {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    if ($line -match '"syscall"\s*:\s*"fsync"') { $fsync++ }
    if ($line -match '"syscall"\s*:\s*"fdatasync"') { $fdatasync++ }
    if ($line -match '"latency_us"\s*:\s*(\d+)') {
        $us = [int64]$Matches[1]
        if ($us -gt $maxLatency) { $maxLatency = $us }
    }
}

Write-Host "fsync_events=$fsync fdatasync_events=$fdatasync max_latency_us=$maxLatency"