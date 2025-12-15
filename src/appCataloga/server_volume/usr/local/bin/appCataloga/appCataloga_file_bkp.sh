#!/bin/bash
# =============================================================================
# Script: appCataloga_file_bkp.sh
# Purpose: Backup worker pool controller (MULTI-WORKER)
# =============================================================================

set -e

APP_NAME="appCataloga_file_bkp.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_file_bkp.log"

mkdir -p "$LOG_DIR" /var/run/appCataloga

banner() {
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[33m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[33m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[33m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python not found"; exit 1; }
    [[ -f "$APP_PATH" ]] || { echo "[ERROR] Script not found"; exit 1; }
}

start() {
    banner "STARTING appCataloga_file_bkp (POOL)"
    check_env
    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &
    echo "Worker pool started."
}

stop() {
    banner "STOPPING appCataloga_file_bkp (POOL)"
    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true
    rm -f /var/run/appCataloga/appCataloga_file_bkp*.pid
    echo "All workers stopped."
}

status() {
    banner "STATUS appCataloga_file_bkp"
    pgrep -af "$APP_NAME" || echo "No workers running."
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac

echo "bye"
