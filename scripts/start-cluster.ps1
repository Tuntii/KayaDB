# Start a 3-node KayaDB cluster for Jepsen-style testing (Windows PowerShell)
param(
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster",
    [string]$KayaServer = "kayadb-server"
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path $ClusterDir | Out-Null

Write-Host "Starting 3-node KayaDB cluster in $ClusterDir..."

# Node 1
$proc1 = Start-Process -FilePath $KayaServer -ArgumentList @(
    "--node-id", "1",
    "--raft-addr", "127.0.0.1:7481",
    "--client-addr", "127.0.0.1:7379",
    "--peer", "2=127.0.0.1:7482,127.0.0.1:7380",
    "--peer", "3=127.0.0.1:7483,127.0.0.1:7381",
    "--data", "$ClusterDir\node1"
) -PassThru -NoNewWindow
$proc1.Id | Out-File "$ClusterDir\node1.pid"
Write-Host "Node 1 started (PID $($proc1.Id))"

# Node 2
$proc2 = Start-Process -FilePath $KayaServer -ArgumentList @(
    "--node-id", "2",
    "--raft-addr", "127.0.0.1:7482",
    "--client-addr", "127.0.0.1:7380",
    "--peer", "1=127.0.0.1:7481,127.0.0.1:7379",
    "--peer", "3=127.0.0.1:7483,127.0.0.1:7381",
    "--data", "$ClusterDir\node2"
) -PassThru -NoNewWindow
$proc2.Id | Out-File "$ClusterDir\node2.pid"
Write-Host "Node 2 started (PID $($proc2.Id))"

# Node 3
$proc3 = Start-Process -FilePath $KayaServer -ArgumentList @(
    "--node-id", "3",
    "--raft-addr", "127.0.0.1:7483",
    "--client-addr", "127.0.0.1:7381",
    "--peer", "1=127.0.0.1:7481,127.0.0.1:7379",
    "--peer", "2=127.0.0.1:7482,127.0.0.1:7380",
    "--data", "$ClusterDir\node3"
) -PassThru -NoNewWindow
$proc3.Id | Out-File "$ClusterDir\node3.pid"
Write-Host "Node 3 started (PID $($proc3.Id))"

Write-Host ""
Write-Host "Cluster started. Client endpoints:"
Write-Host "  Node 1: 127.0.0.1:7379"
Write-Host "  Node 2: 127.0.0.1:7380"
Write-Host "  Node 3: 127.0.0.1:7381"
Write-Host ""
Write-Host "To stop: .\scripts\stop-cluster.ps1"
