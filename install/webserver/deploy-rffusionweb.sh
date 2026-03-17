#!/usr/bin/env bash
set -Eeuo pipefail

scriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ======================================================================
# RF.Fusion - Deploy Web UI + Python Dispatcher
# ======================================================================

ContainerName="rffusion-web"
ImageName="rffusion-web"

NetworkName="podman"
IPAddress="10.88.0.34"

HostHTTPPort="9082"
ContainerHTTPPort="80"

HostSSHPort="2225"
ContainerSSHPort="22"

# ----------------------------------------------------------------------
# Volume: raiz do repositório
# ----------------------------------------------------------------------
repoRoot="/RFFusion-dev/RF.Fusion"
projectVolume="${repoRoot}"

# ----------------------------------------------------------------------
# Volume: repositório CIFS (somente leitura)
# ----------------------------------------------------------------------
reposVolume="/mnt/reposfi"

if [[ ! -d "${reposVolume}" ]]; then
    echo "❌ ERROR: reposfi mount not found on host:"
    echo "    ${reposVolume}"
    exit 1
fi

# ----------------------------------------------------------------------
# Credenciais (root)
# ----------------------------------------------------------------------
ROOT_USER="root"
ROOT_PASSWORD="changeme"

# ======================================================================
# 1) Validação do volume no host
# ======================================================================
echo "=== [1/7] Validating host repository path ==="
if [[ ! -d "${projectVolume}" ]]; then
    echo "❌ ERROR: Repository path does not exist on host:"
    echo "    ${projectVolume}"
    exit 1
fi

# ======================================================================
# 2) Normalização de line endings (CRLF -> LF)
# ======================================================================
echo "=== [2/7] Normalizing shell scripts (CRLF -> LF) ==="
find "${projectVolume}" -type f -name "*.sh" -print0 | while IFS= read -r -d '' f; do
    sed -i 's/\r$//' "$f"
done
echo "✅ Line endings normalized."

# ======================================================================
# 3) Contexto Podman
# ======================================================================
echo "=== [3/7] Using default Podman context ==="
podman context use default >/dev/null 2>&1 || true

# ======================================================================
# 4) Build da imagem
# ======================================================================
echo "=== [4/7] Building image ${ImageName} ==="
podman rmi -f "${ImageName}" >/dev/null 2>&1 || true

podman build --no-cache -t "${ImageName}" -f "${scriptDir}/Containerfile" "${scriptDir}"
if [[ $? -ne 0 ]]; then
    echo "❌ ERROR: Failed to build image ${ImageName}"
    exit 1
fi

# ======================================================================
# 5) Deploy do container
# ======================================================================
echo "=== [5/7] Deploying container ${ContainerName} ==="
podman rm -f "${ContainerName}" >/dev/null 2>&1 || true

podman run -d \
  --name "${ContainerName}" \
  --hostname "${ContainerName}" \
  --restart unless-stopped \
  --network "${NetworkName}" \
  --ip "${IPAddress}" \
  -e "ROOT_USER=${ROOT_USER}" \
  -e "ROOT_PASSWORD=${ROOT_PASSWORD}" \
  -e "SSH_PASSWORD=${ROOT_PASSWORD}" \
  -p "${HostHTTPPort}:${ContainerHTTPPort}" \
  -p "${HostSSHPort}:${ContainerSSHPort}" \
  -v "${projectVolume}:/RF.Fusion:Z" \
  -v "${reposVolume}:/mnt/reposfi:ro" \
  "${ImageName}:latest" >/dev/null

sleep 6

# ======================================================================
# 6) Verificação de estado
# ======================================================================
echo "=== [6/7] Verifying container status ==="
containerStatus=$(podman inspect -f '{{.State.Status}}' "${ContainerName}")

if [[ "${containerStatus}" != "running" ]]; then
    echo "❌ ERROR: Container failed to start. State: ${containerStatus}"
    echo "---- Container logs ----"
    podman logs "${ContainerName}"
    exit 1
fi

echo "✅ Container is running."

# ======================================================================
# 7) Testes básicos
# ======================================================================
echo "=== [7/7] Testing exposed services ==="

if curl -fsS "http://127.0.0.1:${HostHTTPPort}/health" >/dev/null; then
    echo "✅ HTTP ${HostHTTPPort} OK"
else
    echo "⚠️ HTTP healthcheck failed"
fi

nc -z localhost "${HostSSHPort}" && echo "✅ SSH ${HostSSHPort} OK" || echo "⚠️ SSH FAIL"

echo
echo "=== Deployment completed successfully ==="
echo "Web URL : http://127.0.0.1:${HostHTTPPort}"
echo "SSH     : ssh root@127.0.0.1 -p ${HostSSHPort}"
echo "IP      : ${IPAddress}"
echo "Volume  : ${projectVolume} -> /RF.Fusion"
