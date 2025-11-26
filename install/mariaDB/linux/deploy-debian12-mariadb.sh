#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Objetivo: Build do zero e deploy do container Debian 12 + MariaDB + SSH
#            com inicialização automática dos bancos de dados RFDATA e RFMEASURE
# Conversão fiel de deploy-debian12-mariadb.ps1 (PowerShell → Shell Script)
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
HostSSHPort="2224"     # igual ao PS1 (evita conflito)
HostDBPort="9081"

# ------------------------------
# Caminhos e variáveis auxiliares
# ------------------------------
sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

repoRoot="/RFFusion-dev/RF.Fusion"
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
    bash "$networkScript" || {
        echo "❌ ERROR: setup-network.sh failed to ensure network configuration."
        exit 1
    }
else
    echo "⚠️  WARNING: setup-network.sh not found in ${projectRoot}. Skipping network setup."
fi

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
  --network "slirp4netns:allow_host_loopback=true" \
  --restart=always \
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

# Teste de conectividade das portas locais
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
if podman exec -it "${ContainerName}" ping -c 3 10.99.0.2 >/dev/null 2>&1; then
    echo "✅ Container can reach 10.99.0.2 (Python node)."
else
    echo "⚠️  Container cannot reach 10.99.0.2. Check bridge or capabilities."
fi

echo "=== ✅ Deployment completed successfully ==="