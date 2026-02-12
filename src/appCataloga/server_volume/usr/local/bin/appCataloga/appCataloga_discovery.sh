#!/bin/bash
# =============================================================================
# Script: appCataloga_discovery.sh
# Purpose: Remote file discovery daemon (SINGLETON)
# Usage: ./appCataloga_discovery.sh {start|stop|restart|status}
# =============================================================================

set -e

APP_NAME="appCataloga_discovery.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_DIR="/var/run/appCataloga"
LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_discovery.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

banner() {
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[36m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[36m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[36m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python not found"; exit 1; }
    [[ -f "$APP_PATH" ]] || { echo "[ERROR] Script not found"; exit 1; }
}

start() {
    banner "STARTING appCataloga_discovery"
    check_env

    if pgrep -f "$APP_NAME" >/dev/null; then
        echo "Already running."
        exit 0
    fi

    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &
    echo "Started."
}

stop() {
    banner "STOPPING appCataloga_discovery"
    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true
    echo "Stopped."
}

status() {
    banner "STATUS appCataloga_discovery"
    pgrep -af "$APP_NAME" || echo "Not running."
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac

echo "bye"
