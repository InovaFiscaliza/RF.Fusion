#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build + Deploy Debian 12 + MariaDB + SSH com volume interno
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
# 1. Garantir volume persistente
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
    echo "Removing old image..."
    podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
fi

podman build -t "${ImageName}" .
echo "✔ Image build completed"

# =======================================================================
# 4. Deploy do container
# =======================================================================
echo "=== [4/6] Deploying container ${ContainerName} ==="

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
  -v mariadb_data:/var/lib/mysql \
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

echo "Waiting MariaDB startup..."
sleep 10

# =======================================================================
# 5. Verificação do estado
# =======================================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")

if [[ "$state" != "running" ]]; then
    echo "❌ ERROR: Container failed to start (state: ${state})"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

echo "✔ Container is running"

# =======================================================================
# 6. Teste das portas
# =======================================================================
echo "Testing exposed ports..."

nc -z localhost "${HostSSHPort}" && echo "✔ SSH ok" || echo "⚠ SSH FAIL"
nc -z localhost "${HostDBPort}" && echo "✔ DB ok" || echo "⚠ DB FAIL"

# =======================================================================
# 7. Teste inicial do banco (health-check leve)
# =======================================================================
echo "Checking MariaDB availability..."
if podman exec "${ContainerName}" mariadb -uroot -p"${DBPassword}" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "✔ MariaDB is responding"
else
    echo "⚠ MariaDB not responding yet — check logs"
fi

echo
echo "=== Deploy READY ==="
echo "MariaDB: 127.0.0.1:${HostDBPort} (root / ${DBPassword})"
