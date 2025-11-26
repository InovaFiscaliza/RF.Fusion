#!/usr/bin/env bash
set -Eeuo pipefail

ContainerName="debian12-mariadb"
ImageName="debian12-mariadb"
PodName="rffusion-pod"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

DBPassword="changeme"
SSHPassword="changeme"

sqlProcessing="/server_volume/tmp/appCataloga/createProcessingDB-v8.sql"
sqlMeasure="/server_volume/tmp/appCataloga/createMeasureDB-v4.sql"

echo "=== Ensuring POD exists ==="
if ! podman pod exists "$PodName"; then
    echo "❌ ERROR: POD $PodName does not exist. Run create-pod.sh first."
    exit 1
fi

echo "=== Building image ${ImageName} ==="
cd "$projectRoot"
podman build --no-cache -t "${ImageName}" .

echo "=== Deploying container ${ContainerName} ==="
podman rm -f "$ContainerName" >/dev/null 2>&1 || true

podman run -d \
    --name "${ContainerName}" \
    --pod "${PodName}" \
    --restart=always \
    -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
    -e "SSH_PASSWORD=${SSHPassword}" \
    -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
    "${ImageName}:latest" >/dev/null

sleep 8

echo "=== Initializing databases ==="
podman exec -i "${ContainerName}" mysql -u root -p"${DBPassword}" < "${sqlProcessing}" || true
podman exec -i "${ContainerName}" mysql -u root -p"${DBPassword}" < "${sqlMeasure}" || true

echo "=== ✅ MariaDB ready on localhost:9081 ==="
echo "Connect: mysql -h 127.0.0.1 -P 9081 -uroot -p${DBPassword}"
