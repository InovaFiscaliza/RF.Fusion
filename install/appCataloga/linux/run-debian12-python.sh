#!/bin/bash
set -e
echo -e "\e[32m=== Running container debian12-python ===\e[0m"
CONTAINER_NAME="debian12-python"
NETWORK="rffusion-net"
SSH_PORT=2222
APP_PORT=5555
HOST_VOLUME_RFF="/RFFusion-dev/RF.Fusion"
HOST_VOLUME_REPOS="/mnt/reposfi"
CONTAINER_VOLUME_RFF="/RFFusion"
CONTAINER_VOLUME_REPOS="/mnt/reposfi"

if ! podman network exists "$NETWORK"; then
    podman network create "$NETWORK" --subnet=10.99.0.0/24 --gateway=10.99.0.1
fi

if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

podman run -d   --name "$CONTAINER_NAME"   --hostname "$CONTAINER_NAME"   --network "$NETWORK"   -p ${SSH_PORT}:22   -p ${APP_PORT}:5555   -e SSH_PASSWORD=changeme   -v "${HOST_VOLUME_RFF}:${CONTAINER_VOLUME_RFF}:Z"   -v "${HOST_VOLUME_REPOS}:${CONTAINER_VOLUME_REPOS}:Z"   debian12-python

echo -e "\e[32mContainer started successfully!\e[0m"
podman ps
