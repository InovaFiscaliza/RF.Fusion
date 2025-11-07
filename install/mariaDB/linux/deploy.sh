#!/usr/bin/env bash
set -e

echo "=== Deploying Debian 12 MariaDB + SSH container (Rootful mode) ==="

# -------------------------------------------------------------------
# 1) Parâmetros principais
# -------------------------------------------------------------------
CONTAINER_NAME="debian12-mariadb"
IMAGE_NAME="debian12-mariadb-ssh"
NETWORK_NAME="rffusion-net"

# Subnet e IP consistentes
SUBNET="10.99.0.0/24"
HOST_IP="10.99.0.3"

# Volume persistente do host
HOST_VOLUME="/RF.Fusion/src/appCataloga/server_volume"

# -------------------------------------------------------------------
# 2) Build da imagem
# -------------------------------------------------------------------
echo "--- STEP 1: Building image ---"
sudo podman build -t "$IMAGE_NAME" -f ./Containerfile .

# -------------------------------------------------------------------
# 3) Criação da rede bridge rootful
# -------------------------------------------------------------------
if ! sudo podman network exists "$NETWORK_NAME"; then
    echo "--- STEP 2: Creating Podman network '$NETWORK_NAME' ---"
    sudo podman network create --subnet "$SUBNET" "$NETWORK_NAME"
else
    echo "[info] Network '$NETWORK_NAME' already exists."
fi

# -------------------------------------------------------------------
# 4) Parar e remover container antigo (se existir)
# -------------------------------------------------------------------
if sudo podman ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
    echo "[info] Removing old container '$CONTAINER_NAME'..."
    sudo podman stop "$CONTAINER_NAME" || true
    sudo podman rm "$CONTAINER_NAME" || true
fi

# -------------------------------------------------------------------
# 5) Execução do container (modo rootful, acessível externamente)
# -------------------------------------------------------------------
echo "--- STEP 3: Running container ---"
sudo podman run -dit \
    --name "$CONTAINER_NAME" \
    --hostname "$CONTAINER_NAME" \
    --network "$NETWORK_NAME" \
    --ip "$HOST_IP" \
    -p 3336:3306 \
    -p 2222:22 \
    -v "${HOST_VOLUME}:/server_volume:Z" \
    -e MARIADB_ROOT_PASSWORD="changeme" \
    -e SSH_PASSWORD="changeme" \
    "$IMAGE_NAME"

# -------------------------------------------------------------------
# 6) Verificação
# -------------------------------------------------------------------
echo "--- STEP 4: Checking container status ---"
sudo podman ps --filter "name=$CONTAINER_NAME"

# -------------------------------------------------------------------
# 7) Teste simples de conectividade interna (MariaDB)
# -------------------------------------------------------------------
echo "--- STEP 5: Testing internal MariaDB connection ---"
sudo podman exec "$CONTAINER_NAME" mariadb -uroot -pchangeme -e "SELECT VERSION();" || {
    echo "❌ MariaDB connection failed inside container."
    exit 1
}

echo
echo "✅ Deployment successful!"
echo "Container name: $CONTAINER_NAME"
echo "Container IP (bridge): $HOST_IP"
echo "MariaDB available at: 172.16.18.11:3336"
echo "SSH available at: 172.16.18.11:2222"
echo "Mapped volume: $HOST_VOLUME"
