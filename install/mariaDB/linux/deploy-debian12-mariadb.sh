#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build + Deploy do container Debian 12 + MariaDB + SSH
#            agora usando volume Podman rootless persistente
# =======================================================================

ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="rffusion-net"
IPAddress="10.99.0.3"
SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# =======================================================================
# 1. Garantir volume persistente do MariaDB
# =======================================================================
echo "=== Ensuring podman volume 'mariadb_data' exists ==="
if ! podman volume inspect mariadb_data >/dev/null 2>&1; then
    echo "Volume not found. Creating..."
    podman volume create mariadb_data >/dev/null
else
    echo "Volume mariadb_data already exists."
fi

# =======================================================================
# 2. Rede
# =======================================================================
echo "=== Ensuring network ${NetworkName} exists ==="
networkScript="${projectRoot}/setup-network.sh"
if [[ -f "$networkScript" ]]; then
    bash "$networkScript"
else
    echo "⚠️  setup-network.sh not found — skipping."
fi

# =======================================================================
# 3. Build da imagem
# =======================================================================
echo "=== [3/6] Building image ${ImageName} ==="
if podman images --format '{{.Repository}}' | grep -q "^${ImageName}$"; then
    podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
fi

podman build --no-cache -t "${ImageName}" .
echo "✔ Image build completed"

# =======================================================================
# 4. Deploy
# =======================================================================
echo "=== [4/6] Deploying container ${ContainerName} ==="

if podman ps -a --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "Container exists. Removing..."
    podman rm -f "${ContainerName}" >/dev/null 2>&1 || true
fi

echo "Starting new container..."

podman run -d \
  --name "${ContainerName}" \
  --hostname "${ContainerName}" \
  --network "${NetworkName}" \
  --ip "${IPAddress}" \
  --cap-add=NET_RAW \
  --cap-add=NET_ADMIN \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -p "${HostSSHPort}:22" \
  -p "${HostDBPort}:3306" \
  -v mariadb_data:/var/lib/mysql:Z \
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

sleep 5

# =======================================================================
# 5. Verificação do container
# =======================================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")

if [[ "$state" != "running" ]]; then
    echo "❌ ERROR: Container failed to start (state: ${state})"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

echo "✔ Container is running"

# =======================================================================
# 6. Testes de rede e porta
# =======================================================================
echo "Testing exposed ports..."

nc -z localhost "${HostSSHPort}" && echo "✔ SSH ok" || echo "⚠ SSH FAIL"
nc -z localhost "${HostDBPort}" && echo "✔ DB ok" || echo "⚠ DB FAIL"

echo "=== Deploy READY ==="
echo "MariaDB: 127.0.0.1:${HostDBPort} (root / ${DBPassword})"
