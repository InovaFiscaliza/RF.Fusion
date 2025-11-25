#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build e deploy do container Debian 12 + MariaDB + SSH
#            com volume persistente seguro em /mnt/reposfi/database
# =======================================================================

# ------------------------------
# Parâmetros configuráveis
# ------------------------------
ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="rffusion-net"
IPAddress="10.99.0.3"

SSHPassword="changeme"
DBPassword="changeme"

HostSSHPort="2224"
HostDBPort="9081"

# ------------------------------
# Caminhos internos
# ------------------------------
sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

repoRoot="/RFFusion-dev/RF.Fusion"
dbVolumeHost="/mnt/reposfi/database"

projectRoot="$(dirname "$(realpath "$0")")"

# =======================================================================
# 1. Contexto
# =======================================================================
echo "=== [1/6] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# =======================================================================
# 2. Garantir rede
# =======================================================================
echo "=== [2/6] Ensuring network ${NetworkName} exists ==="
networkScript="${projectRoot}/setup-network.sh"

if [[ -f "$networkScript" ]]; then
    echo "Running setup-network.sh..."
    bash "$networkScript"
else
    echo "⚠️ WARNING: setup-network.sh not found. Skipping network setup."
fi

# =======================================================================
# 3. Preparar volume persistente
# =======================================================================
echo "=== Preparing persistent database volume at ${dbVolumeHost} ==="

sudo mkdir -p "${dbVolumeHost}"
sudo chown -R 999:999 "${dbVolumeHost}"
sudo chmod -R 775 "${dbVolumeHost}"

# SELinux context para Podman no RHEL
sudo chcon -R system_u:object_r:container_file_t:s0 "${dbVolumeHost}" || true

# =======================================================================
# 4. Build da imagem
# =======================================================================
echo "=== [3/6] Building image ${ImageName} ==="

if podman images --format '{{.Repository}}' | grep -q "^${ImageName}$"; then
    echo "Removing old image..."
    podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
fi

podman build --no-cache -t "${ImageName}" .
echo "✔ Image build completed"

# =======================================================================
# 5. Deploy do container
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
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  -v "${dbVolumeHost}:/var/lib/mysql" \
  "${ImageName}:latest" >/dev/null

sleep 8

# =======================================================================
# 6. Verificação do container
# =======================================================================
containerStatus=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "$containerStatus" != "running" ]]; then
    echo "❌ ERROR: Container failed to start (state: ${containerStatus})"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

echo "✔ Container is running."

echo "Testing host ports..."
nc -z localhost "${HostSSHPort}" && echo "✔ SSH OK" || echo "⚠️ SSH NOT OK"
nc -z localhost "${HostDBPort}" && echo "✔ DB OK" || echo "⚠️ DB NOT OK"

# =======================================================================
# 7. Inicialização automática dos bancos
# =======================================================================
echo "=== [6/6] Initializing MariaDB databases ==="

podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlProcessing}" || true
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlMeasure}" || true

echo "✔ DB initialization scripts executed"

# =======================================================================
# 8. Teste interno
# =======================================================================
echo "=== Testing network to Python Node (10.99.0.2) ==="

if podman exec "${ContainerName}" ping -c 2 10.99.0.2 >/dev/null 2>&1; then
    echo "✔ Connectivity OK"
else
    echo "⚠️ Python node not reachable"
fi

echo "=== ✅ Deployment completed successfully ==="
