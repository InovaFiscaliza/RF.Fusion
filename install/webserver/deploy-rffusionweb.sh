#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== Starting WebFusion deploy ==="
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

# Local database credentials are deliberately excluded from Git.
webfusionSecretPath="${projectVolume}/src/webfusion/.secret"
if [[ ! -f "${webfusionSecretPath}" || ! -r "${webfusionSecretPath}" || ! -s "${webfusionSecretPath}" ]]; then
    echo "❌ ERROR: WebFusion database configuration is missing, unreadable or empty:"
    echo "    ${webfusionSecretPath}"
    echo "Provision the local .secret file with this environment's database settings before deploying."
    exit 1
fi

# ----------------------------------------------------------------------
# Volume: repositório CIFS (somente leitura)
# ----------------------------------------------------------------------
reposVolume="/mnt/reposfi"
uploadVolume="${reposVolume}/upload"

echo "Checking mount: ${reposVolume}"
if ! mountpoint -q "${reposVolume}"; then
    echo "❌ ERROR: reposfi mount not found on host:"
    echo "    ${reposVolume}"
    exit 1
fi

# Uploads are the only writable area of the repository exposed to WebFusion.
echo "Preparing upload directory: ${uploadVolume}"
mkdir -p "${uploadVolume}"

# Fail before replacing the running container if CIFS rejects writes.
echo "Creating upload write probe in: ${uploadVolume}"
if ! uploadProbeDir=$(mktemp -d "${uploadVolume}/.webfusion-deploy.XXXXXX"); then
    echo "❌ ERROR: Cannot create a probe directory in ${uploadVolume}."
    exit 1
fi
uploadProbe="${uploadProbeDir}/probe"
# Create and write once; reopening an empty CIFS file with O_TRUNC can block.
echo "Writing upload probe."
if ! (set -C; printf 'WebFusion upload write check\n' > "${uploadProbe}"); then
    rm -f -- "${uploadProbe}"
    rmdir -- "${uploadProbeDir}"
    echo "❌ ERROR: Cannot write to ${uploadVolume}."
    exit 1
fi
echo "Removing upload write probe."
rm -- "${uploadProbe}"
rmdir -- "${uploadProbeDir}"
echo "Upload write check completed."

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

# CIFS cannot be relabeled with :z/:Z. Scope the label exception to this container.
volumeSecurityArgs=()
reposFsType="$(findmnt -rn -T "${reposVolume}" -o FSTYPE)"
selinuxEnabled="$(podman info --format '{{.Host.Security.SELinuxEnabled}}')"
if [[ "${reposFsType}" == "cifs" && "${selinuxEnabled}" == "true" ]]; then
    volumeSecurityArgs+=(--security-opt label=disable)
    echo "WARNING: CIFS volume detected; SELinux label separation is disabled for ${ContainerName} only."
    echo "Repository remains read-only; only upload is writable. Host SELinux policy is unchanged."
fi

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
  "${volumeSecurityArgs[@]}" \
  -e "ROOT_USER=${ROOT_USER}" \
  -e "ROOT_PASSWORD=${ROOT_PASSWORD}" \
  -e "SSH_PASSWORD=${ROOT_PASSWORD}" \
  -p "${HostHTTPPort}:${ContainerHTTPPort}" \
  -p "${HostSSHPort}:${ContainerSSHPort}" \
  -v "${projectVolume}:/RF.Fusion:Z" \
  -v "${reposVolume}:/mnt/reposfi:ro" \
  -v "${uploadVolume}:/mnt/reposfi/upload:rw" \
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

# Verify access as the container user, including rootless UID mapping.
if ! podman exec "${ContainerName}" /bin/sh -eu -c '
    cd /
    probe_dir=$(mktemp -d /mnt/reposfi/upload/.webfusion-deploy.XXXXXX)
    trap '\''rm -f -- "$probe_dir/probe"; rmdir -- "$probe_dir"'\'' EXIT
    (set -C; printf "WebFusion upload write check\n" > "$probe_dir/probe")
'; then
    echo "❌ ERROR: Upload storage is not writable inside ${ContainerName}."
    exit 1
fi
echo "✅ Upload storage is writable inside the container."

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

if curl -fsS "http://127.0.0.1:${HostHTTPPort}${PublicBasePath}/api/server/runtime-health" >/dev/null; then
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
echo "Repository: ${reposVolume} -> /mnt/reposfi (read-only)"
echo "Upload  : ${uploadVolume} -> /mnt/reposfi/upload (read-write)"
