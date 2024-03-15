#!/bin/bash

# This script is used to deploy the appCataloga application to the development server, creating hard links from the repository to the test folders
MINICONDA_PATH="/root/miniconda3"
REPO_ROOT_PATH="/root/RF.Fusion/src/appCataloga/root"
CONF_PATH="/etc/appCataloga/"
APP_PATH="/usr/local/bin/appCataloga/"
LOG_FILE="/var/log/appCataloga.log"

repo_conf=$REPO_ROOT_PATH/$CONF_PATH
repo_app=$REPO_ROOT_PATH/$APP_PATH

# check if the required REPO folders are accessible
if [ ! -d $repo_conf ] || [ ! -d $repo_app ]; then
    echo "Error: Configure REPO_ROOT folder path and/or download the repo folder structure"
    exit 1
fi

# test if $APP_PATH folder exists and remove it
if [ -d $APP_PATH ]; then
    rm -r $APP_PATH
fi
mkdir $APP_PATH

# test if /etc/appCataloga exists, if not, create it
if [ -d $CONF_PATH ]; then
    rm -r $CONF_PATH    
fi
mkdir $CONF_PATH

conf_files=$(find "$repo_conf" -type f)

for file in $conf_files; do
    ln -f "$file" "$CONF_PATH"
done

app_files=$(find "$repo_app" -type f)

for file in $app_files; do
    ln -f "$file" "$APP_PATH"
done

if [ -f $LOG_FILE ]; then
    rm $LOG_FILE
fi

if ! ln -s $MINICONDA_PATH $APP_PATH/miniconda3; then
    echo "Error creating soft link for /etc/systemd/system/appCataloga.service. Do it manually."
fi

if ! ln -s /usr/local/bin/appCataloga/appCataloga.service /etc/systemd/system/appCataloga.service; then
    echo "Error creating soft link for /etc/systemd/system/appCataloga.service. Do it manually."
fi

if ! /sbin/restorecon -v /usr/local/bin/appCataloga/appCataloga.sh; then
    echo "Error setting SE Linux. Do it manually."
fi