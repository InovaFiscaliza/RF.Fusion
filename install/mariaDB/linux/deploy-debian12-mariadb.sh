#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# Deploy Debian 12 + MariaDB + SSH — banco interno
# ============================================================

ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
NetworkName="rffusion-net"
IPAddress="10.99.0.3"
SSHPassword="changeme"
DBPassword="changeme"
HostSSHPort="2224"
HostDBPort="9081"

# Scripts SQL (no host)
sqlProcessing="/RFFusion-dev/RF.Fusion/src/appCataloga/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/RFFusion-dev/RF.Fusion/src/appCataloga/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

projectRoot="$(dirname "$(realpath "$0")")"

# ============================================================
# 1. Rede
# ============================================================
echo "=== Ensuring network ${NetworkName} ==="
networkScript="${projectRoot}/setup-network.sh"
[[ -f "$networkScript" ]] && bash "$networkScript"

# ============================================================
# 2. Build da imagem
# ============================================================
echo "=== Building ${ImageName} ==="
podman rmi -f "${ImageName}" >/dev/null 2>&1 || true
podman build -t "${ImageName}" .
echo "✔ Image built"

# ============================================================
# 3. Deploy
# ============================================================
echo "=== Deploying container ${ContainerName} ==="
podman rm -f "${ContainerName}" >/dev/null 2>&1 || true

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
  -v "/RFFusion-dev/RF.Fusion/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

echo "Waiting MariaDB startup..."
sleep 15   # tempo necessário

# ============================================================
# 4. Verificar container
# ============================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "${state}" != "running" ]]; then
    echo "❌ ERROR: Container failed (state: ${state})"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi
echo "✔ Container running"

# ============================================================
# 5. Testar MariaDB
# ============================================================
echo "Checking MariaDB..."
if podman exec "${ContainerName}" \
    mariadb -uroot -p"${DBPassword}" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "✔ MariaDB OK"
else
    echo "⚠ MariaDB not ready — continuing anyway"
fi

# ============================================================
# 6. Executar SQL
# ============================================================
echo "=== Running project SQL scripts ==="

if [[ -f "${sqlProcessing}" ]]; then
    if podman exec -i "${ContainerName}" mariadb -uroot -p"${DBPassword}" < "${sqlProcessing}"; then
        echo "✔ createProcessingDB-v8.sql executed"
    else
        echo "⚠ Failed createProcessingDB-v8.sql"
    fi
else
    echo "⚠ File not found: ${sqlProcessing}"
fi

if [[ -f "${sqlMeasure}" ]]; then
    if podman exec -i "${ContainerName}" mariadb -uroot -p"${DBPassword}" < "${sqlMeasure}"; then
        echo "✔ createMeasureDB-v4.sql executed"
    else
        echo "⚠ Failed createMeasureDB-v4.sql"
    fi
else
    echo "⚠ File not found: ${sqlMeasure}"
fi

# ============================================================
# 7. Final
# ============================================================
echo "=== Deploy Ready ==="
echo "MariaDB host access: 127.0.0.1:${HostDBPort}"
echo "SSH root@127.0.0.1 -p ${HostSSHPort}"
