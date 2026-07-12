param (
    [switch]$DryRun,
    [switch]$SkipVersionUpdate
)

$ErrorActionPreference = "Stop"

# Ensure we are running from the project root
$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

# Define crates in dependency order (excluding unpublished bench crate)
$Crates = @(
    @{ Name = "kaya-core"; Path = "crates/kaya-core" },
    @{ Name = "kaya-io"; Path = "crates/kaya-io" },
    @{ Name = "kaya-raft"; Path = "crates/kaya-raft" },
    @{ Name = "kaya-wal"; Path = "crates/kaya-wal" },
    @{ Name = "kaya-lsm"; Path = "crates/kaya-lsm" },
    @{ Name = "kaya-engine"; Path = "crates/kaya-engine" },
    @{ Name = "kaya-net"; Path = "crates/kaya-net" },
    @{ Name = "kaya-sim"; Path = "crates/kaya-sim" },
    @{ Name = "kaya-client"; Path = "crates/kaya-client" },
    # Unpublished-no-longer: optional dep of server/kayactl `ebpf` feature
    @{ Name = "kaya-ebpf"; Path = "crates/kaya-ebpf" },
    @{ Name = "kaya-server"; Path = "crates/kaya-server" },
    @{ Name = "kayactl"; Path = "crates/kayactl" }
)

# Read workspace version once
$WorkspaceContent = Get-Content "Cargo.toml" -Raw

$NewVersion = $null

if ($SkipVersionUpdate) {
    Write-Host "Skipping version auto-increment, using existing workspace version..." -ForegroundColor Yellow
} else {
    # --- COMMIT-BASED VERSIONING LOGIC ---
    Write-Host "Calculating version from git commits..." -ForegroundColor Cyan
    $CommitCount = git rev-list --count HEAD
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to get git commit count"
    }
    $NewVersion = "0.1.$CommitCount"
    Write-Host "Target Version: $NewVersion" -ForegroundColor Green

    # Update Cargo.toml workspace version
    # Matches 'version = "..."' at the start of a line under [workspace.package]
    $NewWorkspaceContent = $WorkspaceContent -replace '(?m)^\s*version\s*=\s*".*?"', "version = `"$NewVersion`""
    if ($NewWorkspaceContent -ne $WorkspaceContent) {
        if ($DryRun) {
            Write-Host "[DRY RUN] Would update root Cargo.toml version to $NewVersion" -ForegroundColor Yellow
        } else {
            Set-Content "Cargo.toml" -Value $NewWorkspaceContent
            Write-Host "Updated workspace version to $NewVersion" -ForegroundColor Yellow
        }
        # Refresh content for regex match below
        $WorkspaceContent = $NewWorkspaceContent
    } else {
        Write-Host "Workspace version already up to date ($NewVersion)" -ForegroundColor DarkGray
    }
}

# Extract Workspace Version
$WorkspaceVersion = $null
if ($WorkspaceContent -match '\[workspace\.package\][\s\S]*?version\s*=\s*"(.*?)"') {
    $WorkspaceVersion = $matches[1]
}

if (-not $WorkspaceVersion -and $SkipVersionUpdate) {
    throw "Could not find [workspace.package] version in root Cargo.toml"
}

function Get-LocalVersion {
    param ([string]$Path)
    $Content = Get-Content "$Path/Cargo.toml" -Raw
    
    # Check for specific version
    if ($Content -match '(?m)^version\s*=\s*"(.*?)"') {
        return $matches[1]
    }
    
    # Check for workspace inheritance
    if ($Content -match 'version\.workspace\s*=\s*true') {
        if ($WorkspaceVersion) {
            return $WorkspaceVersion
        }
        throw "Crate uses workspace version but could not find version in root Cargo.toml"
    }
    
    throw "Could not find version in $Path/Cargo.toml"
}

function Get-RemoteVersion {
    param ([string]$Name)
    try {
        $Url = "https://crates.io/api/v1/crates/$Name"
        $Response = Invoke-RestMethod -Uri $Url -Method Get -ErrorAction Stop
        return $Response.crate.max_version
    } catch {
        # Check if the status code is 404
        if ($_.Exception -and $_.Exception.Response -and $_.Exception.Response.StatusCode -eq 404) {
            return $null
        }
        # Invoke-RestMethod error format on 404 might not have StatusCode exposed the same way in all PS versions,
        # so check message or default to null.
        if ($_.Message -like "*404*") {
            return $null
        }
        Write-Warning "Failed to check crates.io for $Name : $($_.Exception.Message)"
        return $null
    }
}

Write-Host "Starting Smart Publish Process..." -ForegroundColor Magenta
if ($DryRun) {
    Write-Host "--- RUNNING IN DRY-RUN MODE ---" -ForegroundColor Yellow
}

foreach ($Crate in $Crates) {
    $Name = $Crate.Name
    $Path = $Crate.Path

    Write-Host "Checking $Name..." -NoNewline

    $LocalVer = Get-LocalVersion -Path $Path
    $RemoteVer = Get-RemoteVersion -Name $Name

    Write-Host " Local: $LocalVer | Remote: $RemoteVer" -ForegroundColor DarkGray

    $LocalStr = [string]$LocalVer.Trim()
    $RemoteStr = if ($RemoteVer) { [string]$RemoteVer.Trim() } else { "" }

    $NeedsPublish = $false
    
    if ([string]::IsNullOrEmpty($RemoteStr)) {
        Write-Host " [NEW]" -ForegroundColor Cyan
        $NeedsPublish = $true
    }
    elseif ($LocalStr -ne $RemoteStr) {
        Write-Host " [UPDATE] $RemoteStr -> $LocalStr" -ForegroundColor Green
        $NeedsPublish = $true
    }
    else {
        Write-Host " [SKIP] Version matches" -ForegroundColor Yellow
    }
    
    if ($NeedsPublish) {
        if ($DryRun) {
            Write-Host "   [DRY RUN] Would publish $Name at $LocalStr..." -ForegroundColor Cyan
        } else {
            Write-Host "   Publishing $LocalStr..." -ForegroundColor Cyan
            $prevEap = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            try {
                $publishLines = & cargo publish -p $Name --allow-dirty --no-verify 2>&1
                $publishExit = $LASTEXITCODE
            } finally {
                $ErrorActionPreference = $prevEap
            }
            $publishOutput = ($publishLines | Out-String)
            if ($publishExit -ne 0) {
                if ($publishOutput -match 'already uploaded') {
                    Write-Host " [SKIP] $LocalStr already on crates.io (race or stale index)" -ForegroundColor Yellow
                } else {
                    throw "Failed to publish ${Name}: $publishOutput"
                }
            }

            Write-Host "   Waiting 5s for propagation..." -ForegroundColor DarkGray
            Start-Sleep -Seconds 5
        }
    }
}

Write-Host "Smart Publish Completed!" -ForegroundColor Magenta
