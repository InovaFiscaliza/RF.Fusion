#!/bin/bash

# Replace 'path_to_your_python_app' with the actual path to your Python application script.
# shellcheck source=/usr/local/bin/appCataloga/miniconda3/bin/activate
APP_PATH="/usr/local/bin/appCataloga/run_host_task.py"
CONDA_PATH="/usr/local/bin/appCataloga/miniconda3/bin/activate"
PID_FILE="/var/run/run_host_task.pid"
ENV_NAME="appdata"

# Replace 'path_to_your_python_app' with the actual path to your Python application script.

PID_FILE_ROOT="/var/run/run_host_task"
PID_FILE="$PID_FILE_ROOT.pid"

start() {

    if [ -f "$PID_FILE" ]; then
        echo "The service is already running."
    else
        source "$CONDA_PATH"
        source activate $ENV_NAME
        nohup python $APP_PATH >/dev/null 2>&1 &
        echo $! >$PID_FILE
        echo "Service started."
        while [ -f $PID_FILE ]; do
            sleep 2
        done
    fi
}

stop() {
    if [ -f $PID_FILE ]; then
        kill "$(cat "$PID_FILE")"
        rm $PID_FILE
        echo "Service stopped."
    else
        echo "The service is not running."
    fi
}

restart() {
    stop
    start
}

case "$1" in
start)
    start
    ;;
stop)
    stop
    ;;
restart)
    restart
    ;;
*)
    echo "Usage: $0 {start|stop|restart} [Worker #]"
    ;;
esac
