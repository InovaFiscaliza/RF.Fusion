#!/usr/bin/env bash
set -Eeuo pipefail

scriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtimeHealthBootstrap="${scriptDir}/../../service/rffusion-runtime-health-bootstrap.sh"

# ======================================================================
# RF.Fusion - Deploy Web UI + Python Dispatcher
# ======================================================================

ContainerName="rffusion-web"
ImageName="rffusion-web"

NetworkName="podman"
IPAddress="10.88.0.34"

HostHTTPPort="9082"
ContainerHTTPPort="80"
PublicBasePath="/rffusion"

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
RuntimeHealthPrivateKeyPath="${RUNTIME_HEALTH_SSH_PRIVATE_KEY_PATH:-}"
RuntimeHealthKnownHostsPath="${RUNTIME_HEALTH_SSH_KNOWN_HOSTS_PATH:-}"

runtimeHealthMounts=()
if [[ -n "${RuntimeHealthPrivateKeyPath}" || -n "${RuntimeHealthKnownHostsPath}" ]]; then
    if [[ -z "${RuntimeHealthPrivateKeyPath}" || -z "${RuntimeHealthKnownHostsPath}" ]]; then
        echo "❌ ERROR: Configure private key and known_hosts together for container health."
        exit 1
    fi

    if [[ ! -f "${RuntimeHealthPrivateKeyPath}" || ! -f "${RuntimeHealthKnownHostsPath}" ]]; then
        echo "❌ ERROR: Runtime health SSH key or known_hosts file was not found."
        exit 1
    fi

    runtimeHealthMounts=(
        -v "${RuntimeHealthPrivateKeyPath}:/run/secrets/rffusion_runtime_health_ed25519:ro"
        -v "${RuntimeHealthKnownHostsPath}:/run/secrets/rffusion_runtime_health_known_hosts:ro"
    )
fi

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
  "${runtimeHealthMounts[@]}" \
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

# Reinstall health credentials after recreating the WebFusion container.
# The health panel remains optional when internal services are unavailable.
if bash "${runtimeHealthBootstrap}"; then
    echo "✅ Runtime health SSH configuration completed."
else
    echo "⚠️ Runtime health SSH configuration failed."
fi

# ======================================================================
# 7) Testes básicos
# ======================================================================
echo "=== [7/7] Testing exposed services ==="

if curl -fsS "http://127.0.0.1:${HostHTTPPort}${PublicBasePath}/health" >/dev/null; then
    echo "✅ HTTP ${HostHTTPPort} OK"
else
    echo "⚠️ HTTP healthcheck failed"
fi

nc -z localhost "${HostSSHPort}" && echo "✅ SSH ${HostSSHPort} OK" || echo "⚠️ SSH FAIL"

echo
echo "=== Deployment completed successfully ==="
echo "Web URL : http://127.0.0.1:${HostHTTPPort}${PublicBasePath}/"
echo "SSH     : ssh root@127.0.0.1 -p ${HostSSHPort}"
echo "IP      : ${IPAddress}"
echo "Volume  : ${projectVolume} -> /RF.Fusion"
