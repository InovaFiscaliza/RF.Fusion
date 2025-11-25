#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build + Deploy Debian 12 + MariaDB + SSH (DB interno)
# =======================================================================

ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="rffusion-net"
IPAddress="10.99.0.3"
SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# =======================================================================
# 1. Rede
# =======================================================================
echo "=== Ensuring network ${NetworkName} exists ==="
networkScript="${projectRoot}/setup-network.sh"
if [[ -f "$networkScript" ]]; then
    bash "$networkScript"
else
    echo "⚠️  setup-network.sh not found — skipping."
fi

# =======================================================================
# 2. Build da imagem
# =======================================================================
echo "=== [2/5] Building image ${ImageName} ==="

if podman images --format '{{.Repository}}' | grep -q "^${ImageName}$"; then
    echo "Removing old image..."
    podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
fi

podman build -t "${ImageName}" .
echo "✔ Image build completed"

# =======================================================================
# 3. Deploy do container
# =======================================================================
echo "=== [3/5] Deploying container ${ContainerName} ==="

if podman ps -a --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "Container exists. Removing..."
    podman rm -f "${ContainerName}" >/dev/null || true
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
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

echo "Waiting MariaDB startup..."
sleep 10

# =======================================================================
# 4. Verificação do estado
# =======================================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")

if [[ "$state" != "running" ]]; then
    echo "❌ ERROR: Container failed to start (state: ${state})"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

echo "✔ Container is running"

# =======================================================================
# 5. Testes rápidos
# =======================================================================
echo "Testing exposed ports..."

nc -z localhost "${HostSSHPort}" && echo "✔ SSH ok" || echo "⚠ SSH FAIL"
nc -z localhost "${HostDBPort}" && echo "✔ DB ok" || echo "⚠ DB FAIL"

echo "Checking MariaDB availability..."
if podman exec "${ContainerName}" mariadb -uroot -p"${DBPassword}" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "✔ MariaDB is responding"
else
    echo "⚠ MariaDB not responding yet — check logs"
fi

echo
echo "=== Deploy READY ==="
echo "MariaDB: 127.0.0.1:${HostDBPort} (root / ${DBPassword})"
