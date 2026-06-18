# Run smoke benchmarks and emit a metadata report (BENCH-004).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$commit = try { git rev-parse --short HEAD } catch { "unknown" }
$rustc = try { rustc --version } catch { "unknown" }
$env:KAYADB_GIT_COMMIT = $commit
$env:KAYADB_RUSTC = $rustc

cargo bench -p kaya-bench --bench smoke -- --noplot

$outDir = Join-Path $root "target\bench-reports"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportPath = Join-Path $outDir "smoke-$stamp.jsonl"

$line = (@{
    bench = "smoke_put_get"
    commit = $commit
    profile = "release"
    os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
    arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    rustc = $rustc
    durability = "relaxed"
    ops = 10
} | ConvertTo-Json -Compress)

Set-Content -Path $reportPath -Value $line -Encoding UTF8
Write-Host "Wrote benchmark report: $reportPath"