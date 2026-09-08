#!/usr/bin/env bash
# =============================================================================
# rffusion-runtime-health-bootstrap.sh
# Purpose  : Provision the restricted SSH health-check key after containers run.
# Caller   : rffusion-start.sh on every VM boot, or an operator when needed.
# =============================================================================

set -Eeuo pipefail

MARIADB_CONTAINER="debian12-mariadb"
APPCATALOGA_CONTAINER="debian12-python"
WEBFUSION_CONTAINER="rffusion-web"
APPCATALOGA_HEALTH_SCRIPT="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/appCataloga_runtime_health.sh"
MARIADB_HEALTH_SCRIPT="/RFFusion/src/mariadb/scripts/mariadb_runtime_health.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_SECRETS_DIR="$(dirname "${REPO_ROOT}")/.secrets"
SECRETS_DIR="${RFFUSION_RUNTIME_HEALTH_SECRETS_DIR:-${DEFAULT_SECRETS_DIR}}"
PRIVATE_KEY_PATH="${RUNTIME_HEALTH_SSH_PRIVATE_KEY_PATH:-${SECRETS_DIR}/rffusion_runtime_health_ed25519}"
PUBLIC_KEY_PATH="${PRIVATE_KEY_PATH}.pub"
KNOWN_HOSTS_PATH="${RUNTIME_HEALTH_SSH_KNOWN_HOSTS_PATH:-${SECRETS_DIR}/rffusion_runtime_health_known_hosts}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

ensure_health_key() {
    install -d -m 700 "${SECRETS_DIR}"

    if [[ ! -f "${PRIVATE_KEY_PATH}" ]]; then
        log "Generating the runtime health SSH key."
        ssh-keygen -q -t ed25519 -f "${PRIVATE_KEY_PATH}" -N '' -C rffusion-runtime-health
    fi

    if [[ ! -f "${PUBLIC_KEY_PATH}" ]]; then
        ssh-keygen -y -f "${PRIVATE_KEY_PATH}" > "${PUBLIC_KEY_PATH}"
    fi

    chmod 600 "${PRIVATE_KEY_PATH}" "${PUBLIC_KEY_PATH}"
}

install_restricted_key() {
    local container_name="$1"
    local health_script="$2"
    local public_key
    public_key="$(<"${PUBLIC_KEY_PATH}")"

    log "Installing the restricted health key in ${container_name}."
    podman exec -e "RUNTIME_HEALTH_SSH_PUBLIC_KEY=${public_key}" "${container_name}" \
        bash -lc "
            install -d -m 700 /root/.ssh
            health_key=\"restrict,command=\\\"${health_script}\\\" \${RUNTIME_HEALTH_SSH_PUBLIC_KEY}\"
            touch /root/.ssh/authorized_keys
            grep -Fqx \"\${health_key}\" /root/.ssh/authorized_keys || printf '%s\\n' \"\${health_key}\" >> /root/.ssh/authorized_keys
            chmod 600 /root/.ssh/authorized_keys
            sshd -t && pkill -HUP sshd
        "
}

refresh_known_hosts() {
    local temporary_known_hosts
    temporary_known_hosts="$(mktemp "${SECRETS_DIR}/.rffusion_runtime_health_known_hosts.XXXXXX")"

    # Rootless Podman networks are reachable from WebFusion, not necessarily the host.
    if ! podman exec "${WEBFUSION_CONTAINER}" ssh-keyscan -H 10.88.0.2 \
            > "${temporary_known_hosts}" 2>/dev/null || \
       ! podman exec "${WEBFUSION_CONTAINER}" ssh-keyscan -H -p 2828 10.88.0.33 \
            >> "${temporary_known_hosts}" 2>/dev/null || \
       [[ ! -s "${temporary_known_hosts}" ]]; then
        rm -f "${temporary_known_hosts}"
        log "ERROR: Could not collect the SSH host keys for runtime health."
        return 1
    fi

    chmod 600 "${temporary_known_hosts}"
    mv -f "${temporary_known_hosts}" "${KNOWN_HOSTS_PATH}"
}

install_webfusion_secrets() {
    log "Refreshing runtime health secrets in ${WEBFUSION_CONTAINER}."
    podman exec "${WEBFUSION_CONTAINER}" mkdir -p /run/secrets
    podman cp "${PRIVATE_KEY_PATH}" "${WEBFUSION_CONTAINER}:/run/secrets/rffusion_runtime_health_ed25519"
    podman cp "${KNOWN_HOSTS_PATH}" "${WEBFUSION_CONTAINER}:/run/secrets/rffusion_runtime_health_known_hosts"
    podman exec "${WEBFUSION_CONTAINER}" sh -c \
        'chmod 600 /run/secrets/rffusion_runtime_health_ed25519 /run/secrets/rffusion_runtime_health_known_hosts'
}

main() {
    ensure_health_key
    install_restricted_key "${APPCATALOGA_CONTAINER}" "${APPCATALOGA_HEALTH_SCRIPT}"
    install_restricted_key "${MARIADB_CONTAINER}" "${MARIADB_HEALTH_SCRIPT}"
    refresh_known_hosts
    install_webfusion_secrets
    log "Runtime health SSH configuration is ready."
}

main "$@"