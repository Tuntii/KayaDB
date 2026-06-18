# Stop a running KayaDB cluster (Windows PowerShell)
param(
    [string]$ClusterDir = "$env:TEMP\kayadb-cluster"
)

Write-Host "Stopping KayaDB cluster..."

foreach ($i in 1..3) {
    $pidFile = "$ClusterDir\node$i.pid"
    if (Test-Path $pidFile) {
        $nodePid = Get-Content $pidFile | Select-Object -First 1
        try {
            $proc = Get-Process -Id $nodePid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "Stopping node $i (PID $nodePid)..."
                Stop-Process -Id $nodePid -Force
                Start-Sleep -Seconds 1
            } else {
                Write-Host "Node $i (PID $nodePid) already stopped"
            }
        } catch {
            Write-Host "Node $i (PID $nodePid) already stopped"
        }
        Remove-Item $pidFile -Force
    } else {
        Write-Host "Node $i PID file not found"
    }
}

Write-Host "Cluster stopped"
