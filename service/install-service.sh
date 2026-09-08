#!/usr/bin/env bash
# =============================================================================
# install-service.sh
# Purpose  : Install rffusion-containers.service on the Red Hat host VM so
#            the container stack starts automatically on every boot.
#
# IMPORTANT: Run this script as root, DIRECTLY on the VM host (172.16.18.11).
#            Do NOT run it from inside a container.
#
# Usage:
#   bash install-service.sh [REPO_ROOT] [CONTAINER_USER]
#
# Arguments:
#   REPO_ROOT  Path on the HOST where the RF.Fusion repository is located.
#              Default: /RFFusion-dev/RF.Fusion
#   CONTAINER_USER  User that owns the rootless Podman containers.
#                   Default: RFFUSION_CONTAINER_USER, then SUDO_USER.
#
# Examples:
#   bash /RFFusion-dev/RF.Fusion/service/install-service.sh
#   bash install-service.sh /opt/RF.Fusion rffusion
# =============================================================================

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Host-side path to the RF.Fusion repository root.
# Override by passing it as the first argument.
REPO_ROOT="${1:-/RFFusion-dev/RF.Fusion}"
CONTAINER_USER="${2:-${RFFUSION_CONTAINER_USER:-${SUDO_USER:-}}}"

SERVICE_DIR="${REPO_ROOT}/service"
SERVICE_TEMPLATE="${SERVICE_DIR}/rffusion-containers.service"
SERVICE_NAME="rffusion-containers"
SYSTEMD_DIR="/etc/systemd/system"
INSTALLED_UNIT="${SYSTEMD_DIR}/${SERVICE_NAME}.service"

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: This script must be run as root (use sudo or log in as root)."
    exit 1
fi

if [[ -z "${CONTAINER_USER}" ]] || ! id "${CONTAINER_USER}" >/dev/null 2>&1; then
    echo "ERROR: Provide the user that owns the rootless Podman containers."
    echo "       Usage: bash install-service.sh [REPO_ROOT] CONTAINER_USER"
    exit 1
fi

CONTAINER_UID="$(id -u "${CONTAINER_USER}")"
CONTAINER_GROUP="$(id -gn "${CONTAINER_USER}")"

if [[ ! -d "${SERVICE_DIR}" ]]; then
    echo "ERROR: Service directory not found: ${SERVICE_DIR}"
    echo "       Pass the correct repo root as an argument:"
    echo "       bash install-service.sh /path/to/RF.Fusion"
    exit 1
fi

for required in "${SERVICE_TEMPLATE}" \
                "${SERVICE_DIR}/rffusion-start.sh" \
                "${SERVICE_DIR}/rffusion-stop.sh" \
                "${SERVICE_DIR}/rffusion-runtime-health-bootstrap.sh"; do
    if [[ ! -f "${required}" ]]; then
        echo "ERROR: Required file not found: ${required}"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Make scripts executable on the host
# ---------------------------------------------------------------------------
log "Setting execute permission on scripts..."
chmod +x "${SERVICE_DIR}/rffusion-start.sh"
chmod +x "${SERVICE_DIR}/rffusion-stop.sh"
chmod +x "${SERVICE_DIR}/rffusion-runtime-health-bootstrap.sh"

log "Preparing runtime health secrets directory for ${CONTAINER_USER}..."
install -d -m 700 -o "${CONTAINER_USER}" -g "${CONTAINER_GROUP}" \
    "$(dirname "${REPO_ROOT}")/.secrets"

# ---------------------------------------------------------------------------
# Generate the final .service file with the real scripts path substituted
# The template uses __SCRIPTS_DIR__ as a placeholder so the file is portable
# across different host layouts without manual editing.
# ---------------------------------------------------------------------------
log "Generating systemd unit: ${INSTALLED_UNIT}"
sed -e "s|__SCRIPTS_DIR__|${SERVICE_DIR}|g" \
    -e "s|__CONTAINER_USER__|${CONTAINER_USER}|g" \
    -e "s|__CONTAINER_UID__|${CONTAINER_UID}|g" \
    "${SERVICE_TEMPLATE}" \
    > "${INSTALLED_UNIT}"

log "Unit written to: ${INSTALLED_UNIT}"

# ---------------------------------------------------------------------------
# Register and enable the service
# ---------------------------------------------------------------------------
log "Reloading systemd daemon..."
systemctl daemon-reload

log "Enabling lingering for ${CONTAINER_USER} (rootless Podman after boot)..."
loginctl enable-linger "${CONTAINER_USER}"

log "Enabling ${SERVICE_NAME} service (auto-start on boot)..."
systemctl enable "${SERVICE_NAME}.service"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "========================================================="
echo " RFFusion service installed successfully"
echo "========================================================="
echo ""
echo " Unit file : ${INSTALLED_UNIT}"
echo " Scripts   : ${SERVICE_DIR}"
echo " Podman user: ${CONTAINER_USER} (UID ${CONTAINER_UID})"
echo ""
echo " Useful commands:"
echo "   Start now  : systemctl start  ${SERVICE_NAME}"
echo "   Stop       : systemctl stop   ${SERVICE_NAME}"
echo "   Status     : systemctl status ${SERVICE_NAME}"
echo "   Live logs  : journalctl -u ${SERVICE_NAME} -f"
echo "   Disable    : systemctl disable ${SERVICE_NAME}"
echo ""
echo " To verify after next reboot:"
echo "   systemctl is-active ${SERVICE_NAME}"
echo "   podman ps"
echo "========================================================="
