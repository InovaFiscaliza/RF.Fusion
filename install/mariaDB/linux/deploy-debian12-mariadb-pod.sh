#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh (versão POD)
# Objetivo: Build e deploy do container Debian 12 + MariaDB + SSH
#           dentro do POD rffusion-pod
# =======================================================================

ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
PodName="rffusion-pod"

SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

echo "=== [1/6] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# =======================================================================
# 2. Criar POD (se não existir)
# =======================================================================
echo "=== [2/6] Ensuring POD ${PodName} exists ==="

if ! podman pod exists "$PodName"; then
    echo "Creating POD ${PodName}..."
    podman pod create \
      --name "$PodName" \
      -p "${HostSSHPort}:22" \
      -p "${HostDBPort}:3306" \
      >/dev/null
else
    echo "POD ${PodName} already exists."
fi

# =======================================================================
# 3. Build
# =======================================================================
echo "=== [3/6] Building image ${ImageName} ==="
cd "$projectRoot"
podman build --no-cache -t "${ImageName}" .

# =======================================================================
# 4. Deploy
# =======================================================================
echo "=== [4/6] Deploying container ${ContainerName} ==="

if podman ps -a --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    podman rm -f "$ContainerName" >/dev/null 2>&1 || true
fi

podman run -d \
  --name "${ContainerName}" \
  --pod "${PodName}" \
  --hostname "${ContainerName}" \
  --restart=always \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

sleep 8

# =======================================================================
# 5. Verificação
# =======================================================================
containerStatus=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "$containerStatus" != "running" ]]; then
    echo "❌ ERROR: MariaDB container failed to start"
    exit 1
fi

echo "Testing ports on host..."
nc -z localhost "${HostSSHPort}" && echo "SSH OK (${HostSSHPort})"
nc -z localhost "${HostDBPort}" && echo "DB  OK (${HostDBPort})"

# =======================================================================
# 6. Inicialização do banco
# =======================================================================
echo "=== [6/6] Initializing databases ==="
podman exec -i "${ContainerName}" mysql -u root -p"${DBPassword}" < "${sqlProcessing}" || true
podman exec -i "${ContainerName}" mysql -u root -p"${DBPassword}" < "${sqlMeasure}" || true

echo "=== ✅ MariaDB deployment complete ==="
