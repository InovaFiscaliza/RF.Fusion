# ============================================================
# setup-network.ps1
# Cria ou recria a rede bridge "rffusion-net" (10.99.0.0/24)
# usada pelos containers do ambiente RF.Fusion.
# ============================================================

$ErrorActionPreference = "Stop"

Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

# --- Configuração da rede ---
$networkName = "rffusion-net"
$expectedSubnet = "10.99.0.0/24"
$expectedGateway = "10.99.0.1"
$bridgeName = "br99"

Write-Host "=== Checking network $networkName ==="
$network = podman network inspect $networkName 2>$null

if ($LASTEXITCODE -eq 0) {
    # Rede já existe — verificar se está correta
    if ($network | Select-String $expectedSubnet -Quiet) {
        Write-Host "✅ Network $networkName already exists and is correctly configured."
        exit 0
    } else {
        Write-Host "⚠️  Network $networkName exists but configuration differs. Recreating..."
        podman network rm $networkName | Out-Null
    }
} else {
    Write-Host "⚠️  Network $networkName not found. Creating..."
}

# (Re)criação da rede bridge
try {
    podman network create `
        --driver bridge `
        --subnet $expectedSubnet `
        --gateway $expectedGateway `
        --opt com.docker.network.bridge.name=$bridgeName `
        $networkName | Out-Null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Network $networkName successfully created."
        Write-Host "    Subnet : $expectedSubnet"
        Write-Host "    Gateway: $expectedGateway"
        Write-Host "    Bridge : $bridgeName"
        exit 0
    } else {
        Write-Host "❌ ERROR: Failed to create network $networkName."
        exit 1
    }
}
catch {
    Write-Host "❌ Exception: $($_.Exception.Message)"
    exit 1
}
