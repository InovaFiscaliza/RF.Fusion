#!/usr/bin/env bash
set -Eeuo pipefail

PodName="rffusion-pod"

PortPythonSSH=2828
PortPythonAPI=5555

PortMariaDBSSH=2224
PortMariaDBDB=9081

echo "=== Checking if POD already exists ==="
if podman pod exists "$PodName"; then
    echo "POD $PodName already exists. Nothing to do."
    exit 0
fi

echo "=== Creating POD $PodName with ALL required ports ==="
podman pod create \
    --name "$PodName" \
    -p "${PortPythonSSH}:22" \
    -p "${PortPythonAPI}:5555" \
    -p "${PortMariaDBSSH}:22" \
    -p "${PortMariaDBDB}:3306"

echo "=== ✅ POD created successfully ==="
