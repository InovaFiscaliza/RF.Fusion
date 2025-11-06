#!/bin/bash
set -e
echo -e "\e[32m=== Running container rffusion-debian12-python ===\e[0m"
CONTAINER_NAME="debian12-python"
NETWORK="rffusion-net"
podman run -d --name $CONTAINER_NAME --hostname $CONTAINER_NAME           --network $NETWORK -p 2222:22           -e SSH_PASSWORD=changeme           rffusion-debian12-python
podman ps
