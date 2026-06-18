# Partition a specific node (Jepsen nemesis: network isolation simulation)
# Windows PowerShell - requires Administrator for firewall rules in most cases.
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(1, 2, 3)]
    [int]$NodeId,
    
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster"
)

$ErrorActionPreference = "Continue"

# Port map (must match start-cluster.ps1)
$ports = switch ($NodeId) {
    1 { @(7379, 7481) }
    2 { @(7380, 7482) }
    3 { @(7381, 7483) }
    default { @() }
}

if ($ports.Count -eq 0) {
    Write-Error "Invalid node ID $NodeId"
    exit 1
}

$ruleName = "KayaDB-Partition-Node$NodeId"

Write-Host "[Partition] Isolating node $NodeId (client/raft ports: $($ports -join ',')) ..."

# Outbound block (prevents this machine from reaching the node's ports)
try {
    New-NetFirewallRule `
        -DisplayName $ruleName `
        -Direction Outbound `
        -Protocol TCP `
        -RemoteAddress 127.0.0.1 `
        -RemotePort $ports `
        -Action Block `
        -ErrorAction Stop | Out-Null
    Write-Host "[Partition] Created outbound block rule '$ruleName'"
} catch {
    Write-Host "[Partition] Warning: Could not create firewall rule (are you running as Administrator?). Error: $($_.Exception.Message)"
    Write-Host "[Partition] Partition effect may be limited (continuing for test)."
}

# Also try inbound for completeness (node can't be reached)
try {
    New-NetFirewallRule `
        -DisplayName "$ruleName-In" `
        -Direction Inbound `
        -Protocol TCP `
        -LocalAddress 127.0.0.1 `
        -LocalPort $ports `
        -Action Block `
        -ErrorAction SilentlyContinue | Out-Null
} catch {}

Write-Host "[Partition] Node $NodeId is now partitioned (for ~duration). Use heal-partition to restore."
Write-Host "To heal manually: .\scripts\heal-partition.ps1 -NodeId $NodeId"