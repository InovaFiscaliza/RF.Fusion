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

# Scripts SQL originais
sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# ============================================================
# 1. Garantir rede
# ============================================================
echo "=== Ensuring network ${NetworkName} ==="
networkScript="${projectRoot}/setup-network.sh"
if [[ -f "$networkScript" ]]; then
    bash "$networkScript"
fi

# ============================================================
# 2. Build
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
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  "${ImageName}:latest" >/dev/null

echo "Waiting MariaDB startup..."
sleep 10

# ============================================================
# 4. Check running state
# ============================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "$state" != "running" ]]; then
    echo "❌ ERROR: Container failed (state: ${state})"
    exit 1
fi

echo "✔ Container running"

# ============================================================
# 5. Teste inicial
# ============================================================
echo "Checking MariaDB..."
if podman exec "${ContainerName}" \
    mariadb -uroot -p"${DBPassword}" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "✔ MariaDB OK"
else
    echo "⚠ MariaDB not responding"
fi

# ============================================================
# 6. Executar arquivos SQL originais
# ============================================================
echo "=== Running project SQL scripts ==="

if podman exec "${ContainerName}" mariadb -uroot -p"${DBPassword}" < "${sqlProcessing}"; then
    echo "✔ createProcessingDB-v8.sql executed"
else
    echo "⚠ Failed createProcessingDB-v8.sql"
fi

if podman exec "${ContainerName}" mariadb -uroot -p"${DBPassword}" < "${sqlMeasure}"; then
    echo "✔ createMeasureDB-v4.sql executed"
else
    echo "⚠ Failed createMeasureDB-v4.sql"
fi

# ============================================================
# 7. Final
# ============================================================
echo "=== Deploy Ready ==="
echo "MariaDB host access: 127.0.0.1:${HostDBPort}"
echo "SSH root@127.0.0.1 -p ${HostSSHPort}"
