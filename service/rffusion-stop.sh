#!/usr/bin/env bash
# =============================================================================
# rffusion-stop.sh
# Purpose  : Gracefully stop the RFFusion container stack in reverse order.
# Caller   : rffusion-containers.service (ExecStop) during system shutdown.
# Order    : webfusion → appCataloga → MariaDB (reverse of startup)
# Note     : Called by systemd on service stop or system shutdown. At shutdown
#            time the system will power off regardless, so containers that have
#            --restart=always will not cause issues here.
# =============================================================================

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Container names (must match those set in the deploy scripts)
# ---------------------------------------------------------------------------
MARIADB_CONTAINER="debian12-mariadb"
APPCATALOGA_CONTAINER="debian12-python"
WEBFUSION_CONTAINER="rffusion-web"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Stop a container only when it is currently running.
# Using --ignore so the command succeeds even if the container no longer exists.
stop_container() {
    local name="$1"
    if podman ps --format '{{.Names}}' | grep -q "^${name}$"; then
        log "Stopping container '${name}'..."
        podman stop --ignore "$name"
        log "Container '${name}' stopped."
    else
        log "Container '${name}' is not running — skipping."
    fi
}

# ---------------------------------------------------------------------------
# Main shutdown sequence (reverse dependency order)
# ---------------------------------------------------------------------------
log "=== Stopping RFFusion container stack ==="

# 3 → 2 → 1: stop in reverse order so dependents go down before dependencies
stop_container "$WEBFUSION_CONTAINER"
stop_container "$APPCATALOGA_CONTAINER"
stop_container "$MARIADB_CONTAINER"

log "=== RFFusion stack stopped ==="
