# Heal / un-partition a specific node (Jepsen nemesis recovery)
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(1, 2, 3)]
    [int]$NodeId,
    
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster"
)

$ErrorActionPreference = "Continue"

$ruleName = "KayaDB-Partition-Node$NodeId"
$ruleNameIn = "$ruleName-In"

Write-Host "[Heal] Restoring connectivity for node $NodeId ..."

$removed = 0
try {
    if (Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue) {
        Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
        $removed++
    }
} catch {}

try {
    if (Get-NetFirewallRule -DisplayName $ruleNameIn -ErrorAction SilentlyContinue) {
        Remove-NetFirewallRule -DisplayName $ruleNameIn -ErrorAction SilentlyContinue
        $removed++
    }
} catch {}

if ($removed -gt 0) {
    Write-Host "[Heal] Removed $removed firewall rule(s) for node $NodeId. Partition healed."
} else {
    Write-Host "[Heal] No active partition rule found for node $NodeId (already healed or never partitioned in this session)."
}