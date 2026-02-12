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
NetworkName="podman"            # Rede padrão do Podman
IPAddress="10.88.0.2"
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

echo "=== [1/5] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# =======================================================================
# 2. Build da imagem
# =======================================================================
echo "=== [2/5] Validating required files ==="
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
# 3. Deploy do container
# =======================================================================
echo "=== [3/5] Deploying container ${ContainerName} ==="
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

    # === LIMITES DE RECURSOS ===
    --cpus=2
    --memory=2g
    --memory-swap=2g
    --pids-limit=2048

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
# 4. Verificação
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
# 5. Conclusão
# =======================================================================
echo "=== ✅ Deployment completed successfully ==="
