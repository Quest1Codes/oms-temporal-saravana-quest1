# OMS Temporal POC — start script
#
# Prerequisites (Terminal 1, already running):
#   .\temporal-start.ps1
#
# Usage (Terminal 2):
#   .\run.ps1
#
# What this does:
#   1. Restores NuGet packages
#   2. Starts the OMS API process in a new window
#   3. Waits until the worker is actually polling (not just a fixed sleep)
#   4. Promotes the worker deployment version so Temporal dispatches workflow tasks
#
# For CI/CD pipelines, call promote.ps1 as a separate pipeline step after
# health checks confirm the new pod is polling.

$deploymentName = "oms-order-worker"
$buildId        = "dev"
$address        = "localhost:7233"
$maxWaitSeconds = 60

dotnet restore

Write-Host "Starting OMS API..."
$proc = Start-Process -FilePath "dotnet" `
    -ArgumentList "run --project src/OMS.Api" `
    -PassThru -NoNewWindow

Write-Host "Waiting for worker to appear in Temporal pollers (up to $maxWaitSeconds s)..."
$elapsed = 0
$promoted = $false

while ($elapsed -lt $maxWaitSeconds) {
    Start-Sleep -Seconds 2
    $elapsed += 2

    # Check whether our build ID has appeared as a reachable version.
    # 'temporal worker deployment describe' exits 0 when the version exists.
    $descOutput = temporal worker deployment describe `
        --deployment-name $deploymentName `
        --address $address 2>&1

    if ($descOutput -match $buildId) {
        Write-Host "Worker '$deploymentName/$buildId' is polling. Promoting..."
        temporal worker deployment set-current-version `
            --deployment-name $deploymentName `
            --build-id $buildId `
            --address $address `
            --yes
        $promoted = $true
        break
    }
}

if (-not $promoted) {
    Write-Warning "Worker did not appear within $maxWaitSeconds s. Promoting anyway (may fail)."
    temporal worker deployment set-current-version `
        --deployment-name $deploymentName `
        --build-id $buildId `
        --address $address `
        --yes
}

Write-Host ""
Write-Host "OMS API is running (PID $($proc.Id))."
Write-Host "  Temporal UI : http://localhost:8233"
Write-Host "  Swagger     : http://localhost:5000/swagger  (replace port as needed)"
Write-Host "  Health      : http://localhost:5000/health"
Write-Host ""
Write-Host "Press Ctrl+C to stop. Then stop the API with: Stop-Process -Id $($proc.Id)"

# Keep script alive so Ctrl+C is catchable.
try { $proc.WaitForExit() } catch { }
