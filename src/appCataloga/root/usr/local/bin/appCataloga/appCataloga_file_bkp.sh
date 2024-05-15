#!/bin/bash

if [ -n "$2" ]; then
    WORKER=$2
else
    WORKER="0"
fi

# Replace 'path_to_your_python_app' with the actual path to your Python application script.
# shellcheck source=/usr/local/bin/appCataloga/miniconda3/bin/activate
APP_PATH="/usr/local/bin/appCataloga/appCataloga_file_bkp.py worker=$WORKER"
CONDA_PATH="/usr/local/bin/appCataloga/miniconda3/bin/activate"
ENV_NAME="appdata"

PID_FILE_PATH="/var/run/appCataloga"
PID_FILE="$PID_FILE_PATH/appCataloga_file_bkp_$WORKER.pid"

# test if PID_FILE_PATH folder exists and create it
if [ ! -d $PID_FILE_PATH ]; then
    mkdir $PID_FILE_PATH
fi

start() {

    if [ -f "$PID_FILE" ]; then
        echo "The service is already running."
    else
        source "$CONDA_PATH"
        source activate $ENV_NAME
        nohup python $APP_PATH >/dev/null 2>&1 &
        echo $! >$PID_FILE
        echo "Service started."
        # keep service running until the PID file is removed to avoid systemd restart
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
