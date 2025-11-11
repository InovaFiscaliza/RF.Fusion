#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# setup-network.sh
# Cria ou recria a rede bridge "rffusion-net" (10.99.0.0/24)
# usada pelos containers do ambiente RF.Fusion.
# Versão Linux (RHEL) convertida de setup-network.ps1
# ============================================================

echo "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# --- Configuração da rede ---
networkName="rffusion-net"
expectedSubnet="10.99.0.0/24"
expectedGateway="10.99.0.1"
bridgeName="br99"

echo "=== Checking network $networkName ==="

# Verifica se a rede já existe
if podman network exists "$networkName"; then
    echo "Rede $networkName já existe, inspecionando..."
    networkInfo=$(podman network inspect "$networkName" | jq -r '.[0].subnets[0].subnet // empty')

    if [[ "$networkInfo" == "$expectedSubnet" ]]; then
        echo "Rede $networkName já está configurada corretamente ($expectedSubnet)"
        exit 0
    else
        echo "Rede $networkName existente, mas com configuração incorreta. Removendo..."
        podman network rm -f "$networkName"
    fi
else
    echo "Rede $networkName não encontrada, criando..."
fi

# --- Criação da rede ---
echo "=== Creating network $networkName ==="
podman network create \
    --subnet "$expectedSubnet" \
    --gateway "$expectedGateway" \
    --driver bridge \
    --opt "com.docker.network.bridge.name=$bridgeName" \
    "$networkName"

# --- Validação ---
echo "=== Validating network configuration ==="
podman network inspect "$networkName" | grep -E '("subnet"|"gateway")' || true

echo "=== Network $networkName setup complete ==="
