#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# setup-network.sh
# Cria ou recria a rede bridge "rffusion-net" (10.99.0.0/24)
# usada pelos containers do ambiente RF.Fusion.
# Versão Linux (RHEL) revisada para NÃO corromper a CNI.
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

color_info "=== [1/4] Checking existing network '$networkName' ==="
if podman network exists "$networkName"; then
    color_info "Rede '$networkName' encontrada. Validando configuração..."

    # Extrai subnet atual
    currentSubnet=$(podman network inspect "$networkName" | grep -m1 '"subnet"' | grep -oE '([0-9]+\.){3}[0-9]+/[0-9]+') || currentSubnet=""

    if [[ "$currentSubnet" != "$expectedSubnet" ]]; then
        color_warn "Configuração incorreta ou rede corrompida. Removendo..."
        podman network rm -f "$networkName" >/dev/null 2>&1 || true
    else
        color_ok "Rede '$networkName' já configurada corretamente."
    fi
else
    color_warn "Rede '$networkName' não encontrada."
fi

# --- Criar novamente se não existir ---
if ! podman network exists "$networkName"; then
    color_info "=== [2/4] Creating network '$networkName' ==="
    podman network create \
        --subnet "$expectedSubnet" \
        --gateway "$expectedGateway" \
        --driver bridge \
        --opt "com.docker.network.bridge.name=$bridgeName" \
        "$networkName"

    color_ok "Rede '$networkName' criada com sucesso."
fi

# --- Verificar estado da bridge (SOMENTE LER, NUNCA ALTERAR) ---
color_info "=== [3/4] Validating bridge '$bridgeName' ==="
if ip link show "$bridgeName" >/dev/null 2>&1; then
    brState=$(ip link show "$bridgeName" | grep -oE "state [A-Z]+")
    color_info "Estado atual da bridge: $brState"

    if [[ "$brState" =~ "DOWN" || "$brState" =~ "NO-CARRIER" ]]; then
        color_warn "Atenção: bridge '$bridgeName' está DOWN/NO-CARRIER."
        color_warn "Isso ocorre quando a rede CNI foi corrompida."
        color_warn "Recriar containers e a rede irá restaurá-la."
    else
        color_ok "Bridge '$bridgeName' está ativa e operacional."
    fi
else
    color_error "ERRO: bridge '$bridgeName' não existe no kernel!"
    color_error "Isso indica corrupção da rede CNI."
    color_warn "Remova containers e recrie a rede."
fi

# --- Validação final ---
color_info "=== [4/4] Final CNI validation ==="
podman network inspect "$networkName" | grep -E '("subnet"|"gateway"|"network_interface")' || true

color_ok "=== ✅ Network setup complete ==="
