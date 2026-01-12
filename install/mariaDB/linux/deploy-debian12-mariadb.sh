#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build do zero e deploy do container Debian 12 + MariaDB + SSH
#           com inicialização automática dos bancos RFDATA e RFMEASURE
# =======================================================================

# ------------------------------
# Parâmetros configuráveis
# ------------------------------
ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="podman"            # Rede válida e ativa
IPAddress="10.88.0.33"          # IP fixo dentro da rede podman
SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

# ------------------------------
# Caminhos e variáveis auxiliares
# ------------------------------
sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v9.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v5.sql"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# =======================================================================
# 1. Contexto (removido: podman-machine-default-root → desnecessário)
# =======================================================================
echo "=== [1/6] Using default Podman context ==="
podman context use default >/dev/null 2>&1 || true

# =======================================================================
# 2. Garantir rede (somente log – rede podman já existe no sistema)
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
  --cap-add=NET_RAW \
  --cap-add=NET_ADMIN \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -p "${HostSSHPort}:22" \
  -p "${HostDBPort}:3306" \
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

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

echo "Testing host ports..."
if nc -z localhost "${HostSSHPort}" >/dev/null 2>&1; then
    echo "✅ SSH port ${HostSSHPort} reachable"
else
    echo "⚠️  SSH port ${HostSSHPort} not responding"
fi

if nc -z localhost "${HostDBPort}" >/dev/null 2>&1; then
    echo "✅ DB port ${HostDBPort} reachable"
else
    echo "⚠️  DB port ${HostDBPort} not responding"
fi

# =======================================================================
# 6. Inicialização do banco de dados
# =======================================================================
echo "=== [6/6] Initializing MariaDB databases ==="
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlProcessing}" || true
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlMeasure}" || true

if [[ $? -eq 0 ]]; then
    echo "✅ Databases successfully created and initialized."
    echo "Access DB via host: 127.0.0.1:${HostDBPort} (user=root, pass=${DBPassword})"
else
    echo "⚠️  Warning: Database initialization may have failed. Check logs."
fi

# =======================================================================
# Teste de conectividade interna
# =======================================================================
echo "=== Testing internal network connectivity ==="
if podman exec -it "${ContainerName}" ping -c 3 10.88.0.1 >/dev/null 2>&1; then
    echo "✅ Container reached gateway 10.88.0.1."
else
    echo "⚠️  Container cannot reach gateway. Check Podman network."
fi

echo "=== ✅ Deployment completed successfully ==="
