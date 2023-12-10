#!/bin/bash

# Try to download files from github for zabbix external scripts
# Run as root this script as root

# define list of files to download
files=(
    "queryappColeta.py"
    "queryCataloga.py"
    "queryDigitizer.py"
    "queryLoggerUDP.py"
    "rfFusionLib.py"
    "defaultConfig.py"
)

# define Zabbix external script folder ( may improove this later by retrieving from zabbix config file )
zabbix_folder="/usr/lib/zabbix/externalscripts"

# Funciton to download all files from github
get_files() {

    for file in "${files[@]}"; do
        wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/$file

        # check if the file was downloaded, if not, exit
        if [ ! -f "$file" ]; then
            echo "Error downloading $file"
            # remove tmp folder and all content
            rm -rf /tmp/zabbix
            exit
        else
            dos2unix "$file"
            chmod 750 "$file"
        fi
    done
}

# try to create a temp folder, if it fails, exit
if [ ! -d /tmp/zabbix ]; then
    if ! mkdir /tmp/zabbix; then
        exit
    fi
fi

cd /tmp/zabbix || exit

# try to download files from github if it fails, exit
get_files

# move files to zabbix folder
mv -f "${files[@]}" $zabbix_folder

# remove tmp folder and all content
rm -rf /tmp/zabbix
