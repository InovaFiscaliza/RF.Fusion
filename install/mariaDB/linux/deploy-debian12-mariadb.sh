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
sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

projectRoot="$(dirname "$(realpath "$0")")"

# ============================================================
# 1. Rede
# ============================================================
echo "=== Ensuring network ${NetworkName} ==="

if ! podman network exists "${NetworkName}"; then
    echo "Network ${NetworkName} not found — creating..."
    networkScript="${projectRoot}/setup-network.sh"
    [[ -f "$networkScript" ]] && bash "$networkScript"
else
    echo "✔ Network exists"
fi

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

# remove container anterior
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

echo "✔ Container started"

# ============================================================
# 4. Esperar MariaDB de verdade (sem sleep fixo)
# ============================================================
echo "Waiting MariaDB startup (max 40s)..."

for i in {1..40}; do
    if podman exec "${ContainerName}" mariadb -uroot -p"${DBPassword}" -e "SELECT 1;" >/dev/null 2>&1; then
        echo "✔ MariaDB OK"
        break
    fi
    sleep 1
done

if [[ $i -eq 40 ]]; then
    echo "❌ MariaDB did not start"
    echo "Use: podman logs ${ContainerName}"
    exit 1
fi

# ============================================================
# 5. Verificar estado final do container
# ============================================================
state=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")
if [[ "${state}" != "running" ]]; then
    echo "❌ ERROR: Container failed (state: ${state})"
    echo "Logs:"
    podman logs "${ContainerName}"
    exit 1
fi
echo "✔ Container running"

# ============================================================
# 6. Executar SQL
# ============================================================
echo "=== Running project SQL scripts ==="

run_sql() {
    local file="$1"
    local name="$2"

    if [[ ! -f "${file}" ]]; then
        echo "⚠ SQL file not found: ${file}"
        return
    fi

    echo "→ Executing ${name}"

    if podman exec -i "${ContainerName}" \
        mariadb -uroot -p"${DBPassword}" < "${file}"
    then
        echo "✔ ${name} executed"
    else
        echo "❌ ERROR running ${name}"
    fi
}

run_sql "${sqlProcessing}" "createProcessingDB-v8.sql"
run_sql "${sqlMeasure}"    "createMeasureDB-v4.sql"

# ============================================================
# 7. Final
# ============================================================
echo "=== Deploy Ready ==="
echo "MariaDB host access: 127.0.0.1:${HostDBPort}"
echo "SSH root@127.0.0.1 -p ${HostSSHPort}"
