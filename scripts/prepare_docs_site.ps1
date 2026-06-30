# Assemble the full GitHub Pages / Docsify site (Windows).
# Usage: .\scripts\prepare_docs_site.ps1 [-OutputDir build\docs-site]
param(
    [string]$OutputDir = (Join-Path $PSScriptRoot "..\build\docs-site")
)

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Out = Resolve-Path -Path $OutputDir -ErrorAction SilentlyContinue
if (-not $Out) {
    New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
    $Out = Resolve-Path $OutputDir
}

if (Test-Path $Out) {
    Remove-Item -Recurse -Force $Out
}
New-Item -ItemType Directory -Force -Path $Out | Out-Null

Copy-Item -Path (Join-Path $Root "docs\*") -Destination $Out -Recurse -Force
Get-ChildItem (Join-Path $Root "docs") -Force -Filter ".*" -File | ForEach-Object {
    Copy-Item $_.FullName -Destination $Out -Force
}

New-Item -ItemType File -Force -Path (Join-Path $Out ".nojekyll") | Out-Null
$superpowers = Join-Path $Out "superpowers"
if (Test-Path $superpowers) {
    Remove-Item -Recurse -Force $superpowers
}

@("ROADMAP.md", "CHANGELOG.md", "CONTRIBUTING.md", "BENCHMARKS.md", "CODE_OF_CONDUCT.md") | ForEach-Object {
    $src = Join-Path $Root $_
    if (Test-Path $src) {
        Copy-Item $src -Destination $Out -Force
    }
}

$roadmap = Join-Path $Out "ROADMAP.md"
if (Test-Path $roadmap) {
    (Get-Content $roadmap -Raw) -replace '\]\(docs/', '](' | Set-Content $roadmap -NoNewline
}

New-Item -ItemType Directory -Force -Path (Join-Path $Out "deploy\docker") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Out "deploy\k8s") | Out-Null
Copy-Item (Join-Path $Root "deploy\docker\README.md") (Join-Path $Out "deploy\docker\") -Force
Copy-Item (Join-Path $Root "deploy\k8s\README.md") (Join-Path $Out "deploy\k8s\") -Force

New-Item -ItemType Directory -Force -Path (Join-Path $Out "spec\docs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Out "spec\issues") | Out-Null
Copy-Item (Join-Path $Root "spec\docs\*") (Join-Path $Out "spec\docs\") -Recurse -Force
$expanded = Join-Path $Root "spec\issues\expanded-implementation-roadmap.md"
if (Test-Path $expanded) {
    Copy-Item $expanded (Join-Path $Out "spec\issues\") -Force
}

Write-Host "Docs site prepared at $Out"