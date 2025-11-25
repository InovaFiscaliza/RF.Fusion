#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-python.sh
# Objetivo: Build e inicialização do container Debian 12 com suporte a Python e SSH
# =======================================================================

# ------------------------------
# Parâmetros configuráveis
# ------------------------------
ContainerName="debian12-python"
ImageName="debian12-python"
NetworkName="rffusion-net"
IPAddress="10.99.0.2"
SSHPassword="changeme"
HostSSHPort="2828"
HostAppPort="5555"

# ------------------------------
# Diretórios e volumes
# ------------------------------
repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

# Ajuste importante:
# /mnt/reposfi NÃO pode ter :Z porque o filesystem não suporta xattrs/SELinux
volumes=(
  "${repoRoot}:/RFFusion:Z"
  "/mnt/reposfi:/mnt/reposfi"
)

echo "=== [1/6] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# =======================================================================
# 2. Garantir rede "rffusion-net"
# =======================================================================
echo "=== [2/6] Checking network environment ==="
networkScript="${projectRoot}/setup-network.sh"

if [[ -f "$networkScript" ]]; then
    echo "Running setup-network.sh to validate or create network..."
    bash "$networkScript" || {
        echo "❌ ERROR: setup-network.sh failed to ensure network configuration."
        exit 1
    }
else
    echo "⚠️  WARNING: setup-network.sh not found in $projectRoot. Skipping network setup."
fi

# =======================================================================
# 3. Build da imagem
# =======================================================================
echo "=== [3/6] Validating required files ==="
requiredFiles=("Containerfile" "docker-entrypoint.sh" "environment.yml")
for file in "${requiredFiles[@]}"; do
    if [[ ! -f "${projectRoot}/${file}" ]]; then
        echo "❌ ERROR: Missing required file: ${file} in ${projectRoot}"
        exit 1
    fi
done

echo "=== Building image ${ImageName} ==="
cd "$projectRoot"
podman build -t "$ImageName" -f "${projectRoot}/Containerfile" .
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
    podman rm -f "$ContainerName" >/dev/null 2>&1 || true
fi

args=(
    run -d
    --name "$ContainerName"
    --hostname "$ContainerName"
    --network "$NetworkName"
    --ip "$IPAddress"
    --cap-add=NET_RAW
    --cap-add=NET_ADMIN
    --restart=always
    -e "SSH_PASSWORD=${SSHPassword}"
    -p "${HostSSHPort}:22"
    -p "${HostAppPort}:5555"
)

for mapping in "${volumes[@]}"; do
    host_path="${mapping%%:*}"
    if [[ ! -d "$host_path" ]]; then
        echo "❌ ERROR: Directory ${host_path} does not exist!"
        exit 1
    fi
    echo "Mapped: ${mapping}"
    args+=(-v "$mapping")
done

args+=("$ImageName")

echo "Starting container..."
podman "${args[@]}" >/dev/null
sleep 5

# =======================================================================
# 5. Verificação
# =======================================================================
if podman ps --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "✅ Container ${ContainerName} is running at ${IPAddress}"
    echo "SSH/SFTP:  ssh root@localhost -p ${HostSSHPort}"
    echo "Python API: http://localhost:${HostAppPort}/"
    echo "Attach:     podman exec -it ${ContainerName} bash"
else
    echo "❌ ERROR: Failed to start container ${ContainerName}"
    exit 1
fi

# =======================================================================
# 6. Conclusão
# =======================================================================
echo "=== ✅ Deployment completed successfully ==="
