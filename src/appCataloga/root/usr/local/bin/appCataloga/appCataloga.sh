#!/bin/bash

# Replace 'path_to_your_python_app' with the actual path to your Python application script.
APP_PATH="/usr/local/bin/appCataloga/appCataloga.py"
CONDA_PATH="/usr/local/bin/appCataloga/miniconda3/bin/activate"
ENV_NAME="appdata"
PID_FILE="/var/run/appCataloga.pid"

start() {
    if [ -f $PID_FILE ]; then
        echo "The service is already running."
    else
        source $CONDA_PATH
        source activate $ENV_NAME
        nohup python $APP_PATH &
        echo $! >$PID_FILE
        echo "Service started."
    fi
}

stop() {
    if [ -f $PID_FILE ]; then
        kill $(cat $PID_FILE)
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
    echo "Usage: $0 {start|stop|restart}"
    ;;
esac
