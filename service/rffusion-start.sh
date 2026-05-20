#!/usr/bin/env bash
# =============================================================================
# rffusion-start.sh
# Purpose  : Start the RFFusion container stack in dependency order.
# Caller   : rffusion-containers.service (ExecStart) on system startup.
# Order    : (CIFS mount) → MariaDB → appCataloga → webfusion
# Assumption: Containers were already deployed via the install/*/deploy-*.sh
#             scripts. This script only STARTS existing containers; it does
#             not build images or create new containers.
#
# CIFS credentials: /root/.reposfi must already exist on the host.
#             Do NOT recreate it here — that would overwrite the password.
# =============================================================================

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Container names (must match those set in the deploy scripts)
# ---------------------------------------------------------------------------
MARIADB_CONTAINER="debian12-mariadb"
APPCATALOGA_CONTAINER="debian12-python"
WEBFUSION_CONTAINER="rffusion-web"

# ---------------------------------------------------------------------------
# CIFS share — shared measurement repository mounted into webfusion and
# appCataloga containers. Credentials are read from /root/.reposfi which
# must already exist on the host (managed separately, never overwritten here).
# ---------------------------------------------------------------------------
CIFS_SHARE="//reposfi/sfi\$/SENSORES"
CIFS_MOUNT="/mnt/reposfi"
CIFS_CREDENTIALS="/root/.reposfi"
CIFS_OPTIONS="credentials=${CIFS_CREDENTIALS},uid=987,gid=983,file_mode=0666,dir_mode=0777"

# ---------------------------------------------------------------------------
# appCataloga internal services
# Path inside the container filesystem. server_volume/ is NOT copied into the
# image — it is accessed via the /RFFusion volume mount (host repo → /RFFusion).
# tool_start_all.sh has an interactive confirmation prompt that is bypassed
# by piping 'y' via stdin (-i flag on podman exec).
# ---------------------------------------------------------------------------
APPCATALOGA_START_SCRIPT="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh"

# How long (seconds) to wait for MariaDB to accept connections before aborting
MARIADB_READY_TIMEOUT=120

# Seconds to pause after starting a container before proceeding to the next
STEP_DELAY=5

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Abort with an error message if a required container does not exist at all.
# A missing container means the deploy script was never run on this host.
require_container() {
    local name="$1"
    if ! podman ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
        log "ERROR: Container '${name}' not found."
        log "       Run the corresponding install/*/deploy-*.sh script first."
        exit 1
    fi
}

# Start a container only when it is not already in running state.
# Idempotent: safe to call even if the container is already up.
start_container() {
    local name="$1"
    if podman ps --format '{{.Names}}' | grep -q "^${name}$"; then
        log "Container '${name}' is already running — skipping."
    else
        log "Starting container '${name}'..."
        podman start "$name"
        log "Container '${name}' started."
    fi
}

# Poll until MariaDB accepts a ping or the timeout is reached.
# Uses mysqladmin ping inside the container so no host-side MySQL client
# is required, and no password is needed for the admin ping endpoint.
wait_for_mariadb() {
    log "Waiting for MariaDB to be ready (timeout: ${MARIADB_READY_TIMEOUT}s)..."
    local elapsed=0
    until podman exec "${MARIADB_CONTAINER}" mysqladmin ping -h localhost --silent 2>/dev/null; do
        if [[ $elapsed -ge $MARIADB_READY_TIMEOUT ]]; then
            log "ERROR: MariaDB did not become ready within ${MARIADB_READY_TIMEOUT}s."
            log "       Check logs with: podman logs ${MARIADB_CONTAINER}"
            exit 1
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    log "MariaDB is ready."
}

# Ensure the CIFS share is available before any container starts.
# Logic:
#   1. If already mounted → nothing to do, proceed.
#   2. If not mounted → attempt to mount.
#   3. If mount fails → abort startup. Containers that depend on the share
#      (webfusion, appCataloga) would malfunction without it, so it is safer
#      to fail loudly here than to start a broken stack silently.
#
# IMPORTANT: /root/.reposfi must already exist — this function never creates
# or modifies it to avoid overwriting the stored password.
ensure_cifs_mount() {
    if mountpoint -q "${CIFS_MOUNT}"; then
        log "CIFS share already mounted at ${CIFS_MOUNT}."
        return 0
    fi

    if [[ ! -f "${CIFS_CREDENTIALS}" ]]; then
        log "ERROR: CIFS credentials file not found: ${CIFS_CREDENTIALS}"
        log "       Create it manually on the host before starting the stack."
        exit 1
    fi

    log "Mounting CIFS share ${CIFS_SHARE} → ${CIFS_MOUNT} ..."
    mkdir -p "${CIFS_MOUNT}"
    if mount -t cifs -o "${CIFS_OPTIONS}" "${CIFS_SHARE}" "${CIFS_MOUNT}"; then
        log "CIFS share mounted successfully."
    else
        log "ERROR: Failed to mount CIFS share ${CIFS_SHARE}."
        log "       Verify network connectivity to 'reposfi' and that /root/.reposfi is correct."
        log "       Containers will NOT be started."
        exit 1
    fi
}

# Start all appCataloga internal worker daemons by calling tool_start_all.sh
# inside the running container. The script prompts for confirmation; we pipe
# 'y' automatically since this is a non-interactive boot context.
start_appcataloga_services() {
    log "Starting appCataloga internal services via ${APPCATALOGA_START_SCRIPT} ..."
    # -i is required so podman exec reads from stdin (the piped 'y' answer)
    if printf 'y\n' | podman exec -i "${APPCATALOGA_CONTAINER}" \
            bash "${APPCATALOGA_START_SCRIPT}"; then
        log "appCataloga internal services started."
    else
        log "WARNING: tool_start_all.sh exited with a non-zero code."
        log "         Check container logs: podman logs ${APPCATALOGA_CONTAINER}"
    fi
}

# ---------------------------------------------------------------------------
# Main startup sequence
# ---------------------------------------------------------------------------
log "=== Starting RFFusion container stack ==="

# Validate all containers exist before attempting to start anything.
# A missing container is caught early so the stack is never half-started.
require_container "$MARIADB_CONTAINER"
require_container "$APPCATALOGA_CONTAINER"
require_container "$WEBFUSION_CONTAINER"

# 0. Ensure the CIFS share is mounted — both webfusion and appCataloga need it.
#    Done before any container starts so the volume is ready when containers open files.
ensure_cifs_mount

# 1. MariaDB must be healthy before any other service tries to connect to it.
start_container "$MARIADB_CONTAINER"
wait_for_mariadb

# 2. appCataloga — start the container, wait briefly, then launch all internal
#    worker daemons (tool_start_all.sh). Workers depend on MariaDB being up.
start_container "$APPCATALOGA_CONTAINER"
sleep "$STEP_DELAY"
start_appcataloga_services

# 3. webfusion — web UI and dispatcher, depends on both DB and appCataloga.
start_container "$WEBFUSION_CONTAINER"

log "=== RFFusion stack is up ==="
