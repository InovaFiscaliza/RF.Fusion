#!/bin/bash
set -e
echo -e "\e[32m=== Setting up Podman network ===\e[0m"
NET_NAME="rffusion-net"
if ! podman network exists $NET_NAME; then
  podman network create $NET_NAME --subnet=10.88.0.0/16
  echo "Created network $NET_NAME"
else
  echo "Network $NET_NAME already exists"
fi
