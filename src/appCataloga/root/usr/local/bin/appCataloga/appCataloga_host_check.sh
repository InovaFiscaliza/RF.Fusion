#!/bin/bash

# Replace 'path_to_your_python_app' with the actual path to your Python application script.
# shellcheck source=/usr/local/bin/appCataloga/miniconda3/bin/activate
APP_PATH="/usr/local/bin/appCataloga/appCataloga_host_check.py"
CONDA_PATH="/usr/local/bin/appCataloga/miniconda3/bin/activate"
ENV_NAME="appdata"

PID_FILE_PATH="/var/run/appCataloga"
PID_FILE="$PID_FILE_PATH/appCataloga_host_check.pid"

# test if PID_FILE_PATH folder exists and create it
if [ ! -d $PID_FILE_PATH ]; then
    mkdir $PID_FILE_PATH
fi

# Replace 'path_to_your_python_app' with the actual path to your Python application script.
start() {

    if [ -f "$PID_FILE_PATH/$PID_FILE" ]; then
        echo "The service is already running."
    else
        source "$CONDA_PATH"
        source activate $ENV_NAME
        nohup python $APP_PATH >/dev/null 2>&1 &
        echo $! >"$PID_FILE_PATH/$PID_FILE"
        while [ -f "$PID_FILE_PATH/$PID_FILE" ]; do
            sleep 2
        done
        echo "Service started."
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
