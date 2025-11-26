#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-python.sh (versão POD)
# Objetivo: Build e inicialização do container Debian 12 com Python e SSH
#           dentro do POD rffusion-pod
# =======================================================================

ContainerName="debian12-python"
ImageName="debian12-python"
PodName="rffusion-pod"

SSHPassword="changeme"
HostSSHPort="2828"
HostAppPort="5555"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

volumes=(
  "${repoRoot}:/RFFusion:Z"
  "/mnt/reposfi:/mnt/reposfi"
)

echo "=== [1/6] Switching Podman context ==="
podman context use podman-machine-default-root >/dev/null 2>&1 || true

# =======================================================================
# 2. Criação do POD
# =======================================================================
echo "=== [2/6] Ensuring POD ${PodName} exists ==="

if ! podman pod exists "$PodName"; then
    echo "Creating POD ${PodName}..."
    podman pod create \
      --name "$PodName" \
      -p "${HostSSHPort}:22" \
      -p "${HostAppPort}:5555" \
      >/dev/null
else
    echo "POD ${PodName} already exists."
fi

# =======================================================================
# 3. Build da imagem
# =======================================================================
echo "=== [3/6] Validating required files ==="
requiredFiles=("Containerfile" "docker-entrypoint.sh" "environment.yml")
for file in "${requiredFiles[@]}"; do
    [[ -f "${projectRoot}/${file}" ]] || {
        echo "❌ ERROR: Missing required file: ${file}"
        exit 1
    }
done

echo "=== Building image ${ImageName} ==="
cd "$projectRoot"
podman build -t "$ImageName" -f Containerfile .

# =======================================================================
# 4. Container dentro do POD
# =======================================================================
echo "=== [4/6] Deploying container ${ContainerName} inside POD ==="

if podman ps -a --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "Removing existing container ${ContainerName}..."
    podman rm -f "${ContainerName}" >/dev/null 2>&1 || true
fi

args=(
    run -d
    --name "$ContainerName"
    --pod "$PodName"
    --hostname "$ContainerName"
    --restart=always
    -e "SSH_PASSWORD=${SSHPassword}"
)

for m in "${volumes[@]}"; do
    args+=(-v "$m")
done

args+=("$ImageName")

echo "Starting container..."
podman "${args[@]}" >/dev/null
sleep 4

# =======================================================================
# 5. Verificação
# =======================================================================
if podman ps --format '{{.Names}}' | grep -q "^${ContainerName}$"; then
    echo "✅ Container ${ContainerName} running inside POD"
    echo "SSH:     ssh root@localhost -p ${HostSSHPort}"
    echo "APP:     http://localhost:${HostAppPort}/"
else
    echo "❌ ERROR: Failed to start ${ContainerName}"
    exit 1
fi

echo "=== ✅ Python deployment complete ==="
