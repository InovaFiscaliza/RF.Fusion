# setup-network.ps1
# Force context to "podman-machine-default-root"
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$networkName = "rede-direct"
$expectedSubnet = "172.21.48.0/20"
$expectedGateway = "172.21.48.1"

Write-Host "=== Checking network $networkName ==="
$network = podman network inspect $networkName 2>$null

if ($LASTEXITCODE -eq 0) {
    if ($network | Select-String $expectedSubnet -Quiet) {
        Write-Host "Network $networkName already exists and is correct."
    } else {
        Write-Host "Network $networkName exists but is incorrect. Recreating..."
        podman network rm $networkName
        podman network create -d ipvlan -o parent=eth0 -o mode=l2 `
          --subnet $expectedSubnet --gateway $expectedGateway $networkName
    }
} else {
    Write-Host "Network $networkName not found. Creating..."
    podman network create -d ipvlan -o parent=eth0 -o mode=l2 `
      --subnet $expectedSubnet --gateway $expectedGateway $networkName
}

Write-Host "Network $networkName configured successfully."
