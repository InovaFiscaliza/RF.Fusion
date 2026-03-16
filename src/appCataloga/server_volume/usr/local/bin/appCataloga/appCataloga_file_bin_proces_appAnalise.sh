#!/bin/bash
# =============================================================================
# Script: appCataloga_file_bin_proces_appAnalise.sh
# Purpose: appAnalise-backed file processing daemon (SINGLETON)
# =============================================================================

set -e

APP_NAME="appCataloga_file_bin_proces_appAnalise.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_DIR="/var/run/appCataloga"
LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_file_bin_proces_appAnalise.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

banner() {
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[34m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[34m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[34m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python not found"; exit 1; }
    [[ -f "$APP_PATH" ]] || { echo "[ERROR] Script not found"; exit 1; }
}

start() {
    banner "STARTING appCataloga_file_bin_proces_appAnalise"
    check_env
    pgrep -f "$APP_NAME" >/dev/null && { echo "Already running."; exit 0; }
    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &
    echo "Started."
}

stop() {
    banner "STOPPING appCataloga_file_bin_proces_appAnalise"
    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true
    echo "Stopped."
}

status() {
    banner "STATUS appCataloga_file_bin_proces_appAnalise"
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
