#!/bin/bash
# =============================================================================
# Script: appCataloga_pub_metadata.sh
# Purpose: Metadata publication daemon (SINGLETON)
#
# Role:
#   - Publishes processed metadata to external systems
#   - Runs independently from discovery and backup loops
#
# Usage:
#   ./appCataloga_pub_metadata.sh {start|stop|restart|status}
# =============================================================================

set -e

APP_NAME="appCataloga_pub_metadata.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_DIR="/var/run/appCataloga"
LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_pub_metadata.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

banner() {
    local w
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[36m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[36m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[36m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python binary not found: $PYTHON_BIN"; exit 1; }
    [[ -f "$APP_PATH"  ]] || { echo "[ERROR] Application file not found: $APP_PATH"; exit 1; }
}

start() {
    banner "STARTING appCataloga_pub_metadata"
    check_env

    if pgrep -f "$APP_NAME" > /dev/null; then
        echo "appCataloga_pub_metadata already running."
        exit 0
    fi

    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &

    echo "appCataloga_pub_metadata started."
}

stop() {
    banner "STOPPING appCataloga_pub_metadata"

    if ! pgrep -f "$APP_NAME" > /dev/null; then
        echo "appCataloga_pub_metadata is not running."
        exit 0
    fi

    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true

    echo "appCataloga_pub_metadata stopped."
}

status() {
    banner "STATUS appCataloga_pub_metadata"
    pgrep -af "$APP_NAME" || echo "appCataloga_pub_metadata not running."
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
