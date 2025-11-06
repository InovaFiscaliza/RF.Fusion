#!/bin/bash
set -e
NETWORK_NAME="rffusion-net"
SUBNET="10.99.0.0/24"
GATEWAY="10.99.0.1"

echo -e "\e[32m=== Setting up Podman network ($NETWORK_NAME) ===\e[0m"

if podman network exists "$NETWORK_NAME"; then
    echo -e "\e[36mNetwork $NETWORK_NAME already exists.\e[0m"
else
    echo -e "\e[33mCreating network $NETWORK_NAME...\e[0m"
    podman network create "$NETWORK_NAME" --subnet=$SUBNET --gateway=$GATEWAY
fi

echo -e "\e[32mNetwork setup complete.\e[0m"
