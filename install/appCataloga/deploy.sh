#!/bin/bash

# Download files from a repository and install
# Run as root this script as root

# parse arguments "-i", "-u", "-r", or "-h"
# if no argument is passed, exit
if [ $# -eq 0 ]; then
    echo "No arguments provided. Use -i to install, -u to update, -r to remove, or -h to for help"
    exit
fi

# define script control variables
repository="https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/appCataloga/src/root"

# declare an associative array with pairs of install required files to download and target folders
declare -A installFiles=(
    ["config.py"]="etc/appCataloga"
    ["equipmentType.csv"]="etc/appCataloga"
    ["fileType.csv"]="etc/appCataloga"
    ["IBGE-BR_Municipios_2020_BULKLOAD.csv"]="etc/appCataloga"
    ["IBGE-BR_UF_2020_BULKLOAD.csv"]="etc/appCataloga"
    ["measurementUnit.csv"]="etc/appCataloga"
    ["createMeasureDB.sql"]="usr/local/bin"
    ["createProcessingDB.sql"]="usr/local/bin"
)

# declare an associative array with pairs of update required files to download and target folders
declare -A updateFiles=(
    ["appCataloga.py"]="usr/local/bin"
    ["backup_control.py"]="usr/local/bin"
    ["processing_control.py"]="usr/local/bin"
    ["shared.py"]="usr/local/bin"
    ["backup_single_host.py"]="usr/local/bin"
    ["db_handler.py"]="usr/local/bin"
)

# define
tmp_folder="/tmp/appCataloga"

if [ "$1" == "-h" ]; then
    echo -e "\nThis script will download appCataloga files from a repository and install them in the required folders.\n"
    echo "Use -i to install, -u to update, -r to remove. Any additional argument will be ignored."
    echo "    Initially, the required files will be downloaded from '$repository' to the '$tmp_folder' folder"
    echo "    Afterwards, the files will be moved to the target folders at: /${installFiles[0]} and /${updateFiles[0]}"
    echo "    Install will create the target folders and include database reference data and sql scripts."
    echo "    Update will overwrite the python script files only. Database will not be affected."
    echo -e "\nExample: ./deploy.sh -i\n"
    exit
    # test if arguments are not -i, -u or -r
elif [ "$1" != "-i" ] && [ "$1" != "-u" ] && [ "$1" != "-r" ]; then
    echo "Invalid argument. Use -i, -u, -r, or -h"
fi

# Funciton to download files from the repository
get_files() {

    if [ "$1" == "-i" ]; then
        # install files
        for file in "${!installFiles[@]}"; do
            folder="${installFiles[$file]}"
            wget "$repository/$folder/${file}"
            # check if the file was downloaded, if not, exit
            if [ ! -f "$file" ]; then
                echo "Error downloading $file"
                # remove tmp folder and all content
                rm -rf "$tmp_folder"
                exit
            else
                dos2unix "$file"
                chmod 755 "$file"
            fi
        done
        # install files that are in the update list
        get_files "-u"
    elif [ "$1" == "-u" ]; then
        # update files
        for file in "${!updateFiles[@]}"; do
            folder="${updateFiles[$file]}"
            wget "$repository/$folder/${file}"
            # check if the file was downloaded, if not, exit
            if [ ! -f "$file" ]; then
                echo "Error downloading $file"
                # remove tmp folder and all content
                rm -rf "$tmp_folder"
                exit
            else
                dos2unix "$file"
                chmod 755 "$file"
            fi
        done
    fi
}

remove_files() {
    # remove files
    for file in "${!installFiles[@]}"; do
        folder="${installFiles[$file]}"
        rm -f "/$folder/$file}"
    done
    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
        rm -f "/$folder/$file}"
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
get_files "$1"

# remove tmp folder and all content
rm -rf /tmp/zabbix

172.16.17.11
