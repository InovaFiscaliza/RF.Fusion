#!/bin/bash

# Caminhos principais
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_pub_metadata.py"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_FILE_PATH="/var/run/appCataloga"
LOG_FILE="/var/log/appCataloga/appCataloga_pub_metadata.log"
PID_FILE="$PID_FILE_PATH/appCataloga_pub_metadata.pid"

# Garante pastas
mkdir -p "$PID_FILE_PATH" /var/log/appCataloga

start() {
    echo "[DEBUG] Starting appCataloga_pub_metadata..."
    echo "  APP_PATH=$APP_PATH"
    echo "  PYTHON_BIN=$PYTHON_BIN"

    # Verifica se o binário Python existe
    if [ ! -f "$PYTHON_BIN" ]; then
        echo "[ERROR] Python binary not found: $PYTHON_BIN"
        exit 1
    fi

    # Verifica se o script Python existe
    if [ ! -f "$APP_PATH" ]; then
        echo "[ERROR] Application file not found: $APP_PATH"
        exit 1
    fi

    if [ -f "$PID_FILE" ]; then
        echo "Service already running with PID $(cat "$PID_FILE")."
        exit 0
    fi

    # Ajusta diretório de trabalho
    cd "$(dirname "$APP_PATH")" || exit 1

    # Inicia processo
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Service started with PID $(cat "$PID_FILE")."
}

stop() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        echo "Stopping service with PID $PID..."
        if kill "$PID" > /dev/null 2>&1; then
            rm -f "$PID_FILE"
            echo "Service stopped."
        else
            echo "Failed to stop process $PID. Cleaning up stale PID file."
            rm -f "$PID_FILE"
        fi
    else
        echo "The service is not running."
    fi
}

restart() {
    stop
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Service is running with PID $PID."
        else
            echo "PID file exists but process not running. Cleaning up."
            rm -f "$PID_FILE"
        fi
    else
        echo "Service not running."
    fi
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) restart ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac
