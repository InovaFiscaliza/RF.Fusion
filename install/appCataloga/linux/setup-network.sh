#!/bin/bash
set -e
echo -e "\e[32m=== Setting up Podman network ===\e[0m"

NET_NAME="rffusion-net"

# Verifica se a rede já existe
if podman network exists "$NET_NAME"; then
  echo "Network '$NET_NAME' already exists — skipping creation."
  exit 0
fi

# Verifica se a rede padrão 'podman' já usa 10.88.0.0/16
if podman network inspect podman &>/dev/null; then
  USED_SUBNET=$(podman network inspect podman | grep -oP '"subnet":\s*"\K[0-9./]+')
  echo "Default 'podman' network uses subnet $USED_SUBNET"
else
  USED_SUBNET="10.88.0.0/16"
fi

# Escolhe uma sub-rede diferente para evitar conflito
NEW_SUBNET="10.99.0.0/24"
if [[ "$USED_SUBNET" == "$NEW_SUBNET" ]]; then
  NEW_SUBNET="10.77.0.0/24"
fi

echo "Creating new network '$NET_NAME' using subnet $NEW_SUBNET..."
podman network create "$NET_NAME" --subnet="$NEW_SUBNET" --gateway="${NEW_SUBNET%.*}.1"
echo "Network '$NET_NAME' created successfully."
