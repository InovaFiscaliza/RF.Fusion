#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build do zero e deploy do container Debian 12 + MariaDB + SSH
# =======================================================================

# ------------------------------
# Parâmetros configuráveis
# ------------------------------
ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="podman"
IPAddress="10.88.0.33"
SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

# ------------------------------
# Caminhos dos scripts SQL (NOVO LAYOUT)
# ------------------------------
sqlProcessing="/RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql"
sqlMeasure="/RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql"

# ------------------------------
# Caminhos do host
# ------------------------------
repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# =======================================================================
# 1. Contexto
# =======================================================================
echo "=== [1/6] Using default Podman context ==="
podman context use default >/dev/null 2>&1 || true

# =======================================================================
# 2. Rede
# =======================================================================
echo "=== [2/6] Network ${NetworkName} is managed by Podman and already exists ==="

# =======================================================================
# 3. Build da imagem
# =======================================================================
echo "=== [3/6] Building image ${ImageName} ==="
if podman images --format '{{.Repository}}' | grep -q "^${ImageName}$"; then
    echo "Removing old image..."
    podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
fi

podman build --no-cache -t "${ImageName}" .
if [[ $? -ne 0 ]]; then
    echo "❌ ERROR: Failed to build image ${ImageName}"
    exit 1
fi

# =======================================================================
# 4. Deploy do container
# =======================================================================
echo "=== [4/6] Deploying container ${ContainerName} ==="
if podman ps -a --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "Container ${ContainerName} found. Removing..."
    podman rm -f "${ContainerName}" >/dev/null 2>&1 || true
fi

echo "Starting new container..."
podman run -d \
  --name "${ContainerName}" \
  --hostname "${ContainerName}" \
  --network "${NetworkName}" \
  --ip "${IPAddress}" \
  --cpus=1 \
  --memory=1g \
  --memory-swap=1g \
  --pids-limit=1024 \
  --cap-add=NET_RAW \
  --cap-add=NET_ADMIN \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -p "${HostSSHPort}:22" \
  -p "${HostDBPort}:3306" \
  -v "${repoRoot}:/RFFusion:Z" \
  "${ImageName}:latest"

sleep 8

# =======================================================================
# 5. Verificação do container
# =======================================================================
containerStatus=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "$containerStatus" != "running" ]]; then
    echo "❌ ERROR: Container failed to start. Current state: ${containerStatus}"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

echo "✅ Container is running."

# =======================================================================
# 6. Inicialização do banco
# =======================================================================
echo "=== [6/6] Initializing MariaDB databases ==="
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlProcessing}" || true
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlMeasure}" || true

echo "=== ✅ Deployment completed successfully ==="
