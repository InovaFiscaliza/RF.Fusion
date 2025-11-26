#!/usr/bin/env bash
set -Eeuo pipefail

PodName="rffusion-pod"

echo "=== Checking if POD already exists ==="
if podman pod exists "$PodName"; then
    echo "POD $PodName already exists. Nothing to do."
    exit 0
fi

echo "=== Creating POD $PodName (NO PORTS HERE) ==="
podman pod create \
    --name "$PodName"

echo "=== ✅ POD created successfully ==="
echo "Containers will publish ONLY their own ports."
