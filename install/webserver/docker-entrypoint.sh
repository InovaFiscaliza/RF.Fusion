#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] RF.Fusion Web container starting ==="

# -------------------------------------------------
# SSH
# -------------------------------------------------
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "[entrypoint] Generating SSH host keys..."
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# -------------------------------------------------
# RF.Fusion runtime
# -------------------------------------------------
export PYTHONUNBUFFERED=1
export WEBFUSION_HOST="${WEBFUSION_HOST:-127.0.0.1}"
export WEBFUSION_PORT="${WEBFUSION_PORT:-8000}"
export WEBFUSION_THREADS="${WEBFUSION_THREADS:-8}"
export WEBFUSION_CHANNEL_TIMEOUT="${WEBFUSION_CHANNEL_TIMEOUT:-300}"
export WEBFUSION_ACCEL_REDIRECT_PREFIX="${WEBFUSION_ACCEL_REDIRECT_PREFIX:-/_repo_download}"
export WEBFUSION_ACCEL_REDIRECT_ROOT="${WEBFUSION_ACCEL_REDIRECT_ROOT:-/mnt/reposfi}"

echo "[entrypoint] Python: $(python3 --version)"
echo "[entrypoint] RF.Fusion mount:"
ls -la /RF.Fusion || true
echo "[entrypoint] Repos mount:"
ls -ld /mnt/reposfi || true

cleanup() {
    if [[ -n "${NGINX_PID:-}" ]]; then
        kill "${NGINX_PID}" >/dev/null 2>&1 || true
        wait "${NGINX_PID}" 2>/dev/null || true
    fi

    if [[ -n "${WEBFUSION_PID:-}" ]]; then
        kill "${WEBFUSION_PID}" >/dev/null 2>&1 || true
        wait "${WEBFUSION_PID}" 2>/dev/null || true
    fi

    if [[ -n "${SSHD_PID:-}" ]]; then
        kill "${SSHD_PID}" >/dev/null 2>&1 || true
        wait "${SSHD_PID}" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

echo "[entrypoint] Starting SSH..."
/usr/sbin/sshd -D -e &
SSHD_PID=$!

echo "[entrypoint] Starting webfusion via waitress..."
python3 /RF.Fusion/src/webfusion/app.py &
WEBFUSION_PID=$!

for _ in $(seq 1 30); do
    if nc -z 127.0.0.1 "${WEBFUSION_PORT}"; then
        echo "[entrypoint] Webfusion is listening on 127.0.0.1:${WEBFUSION_PORT}"
        break
    fi

    if ! kill -0 "${WEBFUSION_PID}" >/dev/null 2>&1; then
        echo "[entrypoint] ERROR: webfusion exited before becoming ready"
        wait "${WEBFUSION_PID}"
        exit 1
    fi

    sleep 1
done

if ! nc -z 127.0.0.1 "${WEBFUSION_PORT}"; then
    echo "[entrypoint] ERROR: webfusion did not become ready on 127.0.0.1:${WEBFUSION_PORT}"
    exit 1
fi

echo "[entrypoint] Starting nginx..."
nginx -g "daemon off;" &
NGINX_PID=$!

wait -n "${WEBFUSION_PID}" "${NGINX_PID}"
exit_code=$?
echo "[entrypoint] ERROR: one of the core services exited (status=${exit_code})"
exit "${exit_code}"
