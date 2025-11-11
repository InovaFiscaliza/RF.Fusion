#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# setup-network.sh
# Cria ou recria a rede bridge "rffusion-net" (10.99.0.0/24)
# usada pelos containers do ambiente RF.Fusion.
# Versão Linux (RHEL) aprimorada.
# ============================================================

# --- Funções auxiliares ---
color_info()  { echo -e "\033[1;36m$1\033[0m"; }   # Cyan
color_warn()  { echo -e "\033[1;33m$1\033[0m"; }   # Yellow
color_error() { echo -e "\033[1;31m$1\033[0m"; }   # Red
color_ok()    { echo -e "\033[1;32m$1\033[0m"; }   # Green

# --- Configurações ---
networkName="rffusion-net"
expectedSubnet="10.99.0.0/24"
expectedGateway="10.99.0.1"
bridgeName="br99"

color_info "=== [1/5] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

color_info "=== [2/5] Checking existing network '$networkName' ==="
if podman network exists "$networkName"; then
    color_info "Rede '$networkName' encontrada. Validando configuração..."
    # tenta extrair o subnet sem depender de jq
    currentSubnet=$(podman network inspect "$networkName" | grep -m1 '"subnet"' | grep -oE '([0-9]+\.){3}[0-9]+/[0-9]+') || currentSubnet=""

    if [[ "$currentSubnet" == "$expectedSubnet" ]]; then
        color_ok "Rede '$networkName' já configurada corretamente ($currentSubnet)."
    else
        color_warn "Rede existente com configuração incorreta ou corrompida. Removendo..."
        podman network rm -f "$networkName" >/dev/null 2>&1 || true
    fi
else
    color_warn "Rede '$networkName' não encontrada."
fi

# --- Se a rede não existe ou foi removida ---
if ! podman network exists "$networkName"; then
    color_info "=== [3/5] Creating network '$networkName' ==="
    podman network create \
        --subnet "$expectedSubnet" \
        --gateway "$expectedGateway" \
        --driver bridge \
        --opt "com.docker.network.bridge.name=$bridgeName" \
        "$networkName"

    color_ok "Rede '$networkName' criada com sucesso."
fi

# --- Verificação da bridge ---
color_info "=== [4/5] Validating bridge '$bridgeName' existence ==="
if ip link show "$bridgeName" >/dev/null 2>&1; then
    color_ok "Bridge '$bridgeName' detectada no sistema."
else
    color_error "Atenção: bridge '$bridgeName' não encontrada no kernel!"
    color_warn "Tentando corrigir..."
    sudo ip link add name "$bridgeName" type bridge || true
    sudo ip addr add "$expectedGateway/24" dev "$bridgeName" || true
    sudo ip link set "$bridgeName" up || true
fi

# --- Validação final ---
color_info "=== [5/5] Final validation ==="
podman network inspect "$networkName" | grep -E '("subnet"|"gateway"|"network_interface")' || true

if ip addr show "$bridgeName" | grep -q "$expectedGateway"; then
    color_ok "Rede '$networkName' e bridge '$bridgeName' ativas e configuradas."
else
    color_error "Erro: gateway $expectedGateway não configurado na bridge '$bridgeName'."
fi

color_ok "=== ✅ Network setup complete ==="
