#!/bin/bash

# This script is used to deploy the appCataloga application to the development server, creating hard links from the repository to the test folders
MINICONDA_PATH="/root/miniconda3"
REPO_ROOT_PATH="/root/RF.Fusion/src/appCataloga/root"
CONF_PATH="/etc/appCataloga/"
APP_PATH="/usr/local/bin/appCataloga/"
LOG_FILE="/var/log/appCataloga.log"

repo_conf=$REPO_ROOT_PATH/$CONF_PATH
repo_app=$REPO_ROOT_PATH/$APP_PATH

# create a list of services appCataloga.service, appCataloga_file_bkp@.service, appCataloga_file_bin_proces.service and appCataloga_host_check
services=("appCataloga.service" "appCataloga_file_bkp@.service" "appCataloga_file_bin_proces.service" "appCataloga_host_check.service")
scripts=("appCataloga.sh" "appCataloga_file_bkp.sh" "appCataloga_file_bin_proces.sh" "appCataloga_host_check.py")

# check if the required REPO folders are accessible
if [ ! -d $repo_conf ] || [ ! -d $repo_app ]; then
    echo "Error: Configure REPO_ROOT folder path and/or download the repo folder structure"
    exit 1
fi

# test if $APP_PATH folder exists and remove it
if [ -d $APP_PATH ]; then
    rm -r $APP_PATH
    echo "Removed $APP_PATH"
fi
mkdir $APP_PATH

# test if /etc/appCataloga exists, if not, create it
if [ -d $CONF_PATH ]; then
    rm -r $CONF_PATH
    echo "Removed $CONF_PATH"
fi
mkdir $CONF_PATH

# loop through the conf_files array and create the hard links
conf_files=$(find "$repo_conf" -type f)

for file in $conf_files; do
    ln -f "$file" "$CONF_PATH"
done
echo "Created new $CONF_PATH"

# loop through the app_files array and create the hard links
app_files=$(find "$repo_app" -type f)

for file in $app_files; do
    ln -f "$file" "$APP_PATH"
done

if [ -f $LOG_FILE ]; then
    rm $LOG_FILE
fi

# create link to MINICONDA_PATH within the APP_PATH
if ! ln -s "$MINICONDA_PATH" "$APP_PATH"; then
    echo "Error creating soft link for $MINICONDA_PATH. Do it manually."
fi

echo "Created new $APP_PATH"

# loop through script array and set the SE Linux context for each script
for i in "${!scripts[@]}"; do
    if ! /sbin/restorecon -v "$APP_PATH${scripts[$i]}"; then
        echo "Error setting SE Linux. Do it manually."
    fi
done

# loop through the service array and create the soft links if they don't already exist
for i in "${!services[@]}"; do
    if [ -f "/etc/systemd/system/${services[$i]}" ]; then
        if ! rm "/etc/systemd/system/${services[$i]}"; then
            echo "Error removing soft link for /etc/systemd/system/${services[$i]}. Do it manually and run again."
            exit 1
        fi
    fi
    if ! ln -s "$APP_PATH${services[$i]}" "/etc/systemd/system/${services[$i]}"; then
        echo "Error creating soft link for /etc/systemd/system/${services[$i]}. Do it manually and run again."
        exit 1
    fi
done
