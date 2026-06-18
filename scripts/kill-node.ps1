# Kill a specific node - Jepsen nemesis: crash simulation (Windows PowerShell)
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(1, 2, 3)]
    [int]$NodeId,
    
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster"
)

$pidFile = "$ClusterDir\node$NodeId.pid"

if (-not (Test-Path $pidFile)) {
    Write-Error "Node $NodeId PID file not found"
    exit 1
}

$nodePid = Get-Content $pidFile | Select-Object -First 1

try {
    $proc = Get-Process -Id $nodePid -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "Killing node $NodeId (PID $nodePid) with SIGKILL..."
        Stop-Process -Id $nodePid -Force
        Remove-Item $pidFile -Force
        Write-Host "Node $NodeId killed"
        Write-Host "To restart: .\scripts\restart-node.ps1 -NodeId $NodeId"
    } else {
        Write-Host "Node $NodeId (PID $nodePid) already stopped"
        Remove-Item $pidFile -Force
    }
} catch {
    Write-Host "Node $NodeId (PID $nodePid) already stopped"
    Remove-Item $pidFile -Force
}
