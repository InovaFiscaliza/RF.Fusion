#!/usr/bin/env bash
set -e

echo "=== Deploying Debian 12 MariaDB + SSH container (Linux native) ==="

# -------------------------------------------------------------------
# 1) Parâmetros
# -------------------------------------------------------------------
CONTAINER_NAME="debian12-mariadb"
IMAGE_NAME="debian12-mariadb"
HOST_IP="10.99.0.3"
HOST_VOLUME="/RFFusion-dev/RF.Fusion/src/appCataloga/server_volume"

# -------------------------------------------------------------------
# 2) Build da imagem
# -------------------------------------------------------------------
echo "--- STEP 1: Building image ---"
podman build -t "$IMAGE_NAME" -f ./Containerfile .

# -------------------------------------------------------------------
# 3) Criação da rede, se não existir
# -------------------------------------------------------------------
NETWORK_NAME="rffusion-net"
if ! podman network exists "$NETWORK_NAME"; then
    echo "--- STEP 2: Creating Podman network '$NETWORK_NAME' ---"
    podman network create --subnet 10.99.0.0/24 "$NETWORK_NAME"
else
    echo "[info] Network '$NETWORK_NAME' already exists."
fi

# -------------------------------------------------------------------
# 4) Execução do container
# -------------------------------------------------------------------
podman run -dit \
    --name "$CONTAINER_NAME" \
    --hostname "$CONTAINER_NAME" \
    --network "$NETWORK_NAME" \
    --ip "$HOST_IP" \
    -p 3306:3306 \
    -p 2222:22 \
    -v "${HOST_VOLUME}:/server_volume:Z" \
    -e MARIADB_ROOT_PASSWORD="changeme" \
    -e SSH_PASSWORD="changeme" \
    "$IMAGE_NAME"


# -------------------------------------------------------------------
# 5) Verificação
# -------------------------------------------------------------------
echo "--- STEP 4: Checking container status ---"
podman ps --filter "name=$CONTAINER_NAME"

echo "=== Deployment complete ==="
echo "Container IP: $HOST_IP"
echo "Mapped volume: $HOST_VOLUME"
echo "MariaDB port: 3306"
echo "SSH port: 22"
