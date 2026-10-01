# promote.ps1 — promote a worker deployment version to current.
#
# Use this as a CI/CD pipeline step after health checks confirm
# the new pod/container is polling.
#
# Parameters
#   -DeploymentName  Name of the Temporal worker deployment (default: oms-order-worker)
#   -BuildId         Immutable build identifier, e.g. container image digest (default: dev)
#   -Address         Temporal frontend address (default: localhost:7233)
#
# Example (GitHub Actions):
#   - run: pwsh promote.ps1 -BuildId ${{ github.sha }} -Address $TEMPORAL_HOST

param(
    [string]$DeploymentName = "oms-order-worker",
    [string]$BuildId        = "dev",
    [string]$Address        = "localhost:7233"
)

Write-Host "Promoting $DeploymentName / $BuildId on $Address"
temporal worker deployment set-current-version `
    --deployment-name $DeploymentName `
    --build-id $BuildId `
    --address $Address `
    --yes

Write-Host "Done."
