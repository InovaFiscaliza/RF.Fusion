#!/bin/bash
# =============================================================================
# Script: appCataloga_host_check.sh
# Purpose: Host availability and health check daemon (SINGLETON)
#
# Role:
#   - Periodically checks host reachability and basic health
#   - Supports discovery and backup pipelines
#
# Usage:
#   ./appCataloga_host_check.sh {start|stop|restart|status}
# =============================================================================

set -e

APP_NAME="appCataloga_host_check.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_DIR="/var/run/appCataloga"
LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_host_check.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

banner() {
    local w
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[32m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[32m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[32m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python binary not found: $PYTHON_BIN"; exit 1; }
    [[ -f "$APP_PATH"  ]] || { echo "[ERROR] Application file not found: $APP_PATH"; exit 1; }
}

start() {
    banner "STARTING appCataloga_host_check"
    check_env

    if pgrep -f "$APP_NAME" > /dev/null; then
        echo "appCataloga_host_check already running."
        exit 0
    fi

    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &

    echo "appCataloga_host_check started."
}

stop() {
    banner "STOPPING appCataloga_host_check"

    if ! pgrep -f "$APP_NAME" > /dev/null; then
        echo "appCataloga_host_check is not running."
        exit 0
    fi

    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true

    echo "appCataloga_host_check stopped."
}

status() {
    banner "STATUS appCataloga_host_check"
    pgrep -af "$APP_NAME" || echo "appCataloga_host_check not running."
}

case "$1" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

echo "bye"
