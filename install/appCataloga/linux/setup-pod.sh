#!/usr/bin/env bash
set -Eeuo pipefail

PodName="rffusion-pod"

echo "=== Checking if POD already exists ==="
if podman pod exists "$PodName"; then
    echo "POD $PodName already exists. Nothing to do."
    exit 0
fi

echo "=== Creating POD $PodName with required ports ==="
podman pod create \
    --name "$PodName" \
    -p "2828:22" \
    -p "5555:5555" \
    -p "9081:3306" \
    -p "2224:2222"

echo "=== ✅ POD created successfully ==="
echo "Python SSH  = 2828 -> 22"
echo "Python API  = 5555 -> 5555"
echo "MariaDB DB  = 9081 -> 3306"
echo "MariaDB SSH = 2224 -> 2222"
