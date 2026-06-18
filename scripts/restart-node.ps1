# Restart a specific node (Jepsen nemesis: recovery simulation) - Windows PowerShell
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(1, 2, 3)]
    [int]$NodeId,
    
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster",
    [string]$KayaServer = "kayadb-server"
)

$ErrorActionPreference = "Stop"

$pidFile = "$ClusterDir\node$NodeId.pid"

# Stop if running
if (Test-Path $pidFile) {
    $pid = Get-Content $pidFile | Select-Object -First 1
    try {
        $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host "Stopping node $NodeId (PID $pid)..."
            Stop-Process -Id $pid -Force
            Start-Sleep -Seconds 1
        }
    } catch {
        # already gone
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}

# Start node with the correct per-node arguments (mirrors start-cluster.ps1)
Write-Host "Starting node $NodeId..."

$commonArgs = @("--node-id", "$NodeId", "--data", "$ClusterDir\node$NodeId")

switch ($NodeId) {
    1 {
        $args = $commonArgs + @(
            "--raft-addr", "127.0.0.1:7481",
            "--client-addr", "127.0.0.1:7379",
            "--peer", "2=127.0.0.1:7482,127.0.0.1:7380",
            "--peer", "3=127.0.0.1:7483,127.0.0.1:7381"
        )
    }
    2 {
        $args = $commonArgs + @(
            "--raft-addr", "127.0.0.1:7482",
            "--client-addr", "127.0.0.1:7380",
            "--peer", "1=127.0.0.1:7481,127.0.0.1:7379",
            "--peer", "3=127.0.0.1:7483,127.0.0.1:7381"
        )
    }
    3 {
        $args = $commonArgs + @(
            "--raft-addr", "127.0.0.1:7483",
            "--client-addr", "127.0.0.1:7381",
            "--peer", "1=127.0.0.1:7481,127.0.0.1:7379",
            "--peer", "2=127.0.0.1:7482,127.0.0.1:7380"
        )
    }
    default {
        Write-Error "Invalid node ID"
        exit 1
    }
}

$proc = Start-Process -FilePath $KayaServer -ArgumentList $args -PassThru -NoNewWindow
$proc.Id | Out-File $pidFile
Write-Host "Node $NodeId restarted (PID $($proc.Id))"
Write-Host "To kill: .\scripts\kill-node.ps1 -NodeId $NodeId"