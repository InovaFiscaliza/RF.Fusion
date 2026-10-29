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
SSHAppUser="rffusion"
SSHAppPassword="changeme"
SSHAppPublicKey="${SSH_APP_PUBLIC_KEY:-ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICOV2QzbKI1es3i5dc93j9zNtyfQAVPdrtQCpFjrdcWF rffusion-service}"
RuntimeHealthPublicKey="${RUNTIME_HEALTH_SSH_PUBLIC_KEY:-}"

# ------------------------------
# Caminhos dos scripts SQL (NOVO LAYOUT)
# ------------------------------
sqlProcessing="/RFFusion/src/mariadb/scripts/createProcessingDB.sql"
sqlMeasure="/RFFusion/src/mariadb/scripts/createMeasureDB.sql"
sqlFusionSummary="/RFFusion/src/mariadb/scripts/createFusionSummaryDB.sql"
sqlWebFusion="/RFFusion/src/mariadb/scripts/createWebFusionDB.sql"

# ------------------------------
# Caminhos do host
# ------------------------------
repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

source "${projectRoot}/../../common/environment.sh"
rffusion_load_environment "$repoRoot" mariadb

# =======================================================================
# 1. Contexto
# =======================================================================
echo "=== [1/6] Using default Podman context ==="
podman context use default >/dev/null 2>&1 || true

# Query the selected Podman host before replacing an existing container.
podmanInfo="$(podman info --format '{{.Host.Security.Rootless}} {{.Host.CgroupsVersion}} {{range .Host.CgroupControllers}}{{.}} {{end}}')"
read -r rootless cgroupVersion controllers <<< "$podmanInfo"
echo "Podman: rootless=${rootless}, cgroups=${cgroupVersion}, controllers=${controllers:-none}"

# Rootless resource limits require cgroup v2. Rootful v1 keeps the existing limits.
if [[ "$cgroupVersion" == "v1" ]]; then
    if [[ "$rootless" == "true" ]]; then
        controllers=""
    else
        controllers="cpu memory pids"
    fi
fi

resourceArgs=()
if [[ " ${controllers} " == *" cpu "* ]]; then
    resourceArgs+=(--cpus=1)
else
    echo "WARNING: CPU controller unavailable; container will have no CPU limit."
fi
if [[ " ${controllers} " == *" memory "* ]]; then
    resourceArgs+=(--memory=2g --memory-swap=3g)
else
    echo "WARNING: Memory controller unavailable; container will have no memory/swap limit."
fi
if [[ " ${controllers} " == *" pids "* ]]; then
    resourceArgs+=(--pids-limit=1024)
else
    resourceArgs+=(--pids-limit=-1)
    echo "WARNING: PIDs controller unavailable; container will have no process limit."
fi

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
runtimeHealthArgs=()
if [[ -n "${RuntimeHealthPublicKey}" ]]; then
  runtimeHealthArgs+=(-e "RUNTIME_HEALTH_SSH_PUBLIC_KEY=${RuntimeHealthPublicKey}")
fi

podman run -d \
  --name "${ContainerName}" \
  "${rffusion_identity_args[@]}" \
  --network "${NetworkName}" \
  --ip "${IPAddress}" \
  "${resourceArgs[@]}" \
  --cap-add=NET_RAW \
  --cap-add=NET_ADMIN \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -e "SSH_APP_USER=${SSHAppUser}" \
  -e "SSH_APP_PASSWORD=${SSHAppPassword}" \
  -e "SSH_APP_PUBLIC_KEY=${SSHAppPublicKey}" \
  "${runtimeHealthArgs[@]}" \
  -p "${HostSSHPort}:2828" \
  -p "${HostDBPort}:3306" \
  -v "${repoRoot}:/RFFusion:z" \
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
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlFusionSummary}" || true
podman exec -i "${ContainerName}" bash -c "mysql -u root -p${DBPassword} < ${sqlWebFusion}" || true

echo "=== ✅ Deployment completed successfully ==="
