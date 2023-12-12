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

# declare folders to be used
dataFolder="etc/appCataloga"
scriptFolder="usr/local/bin"

# declare an associative array with pairs of install required files to download and target folders
declare -A installFiles=(
    ["config.py"]=$dataFolder
    ["equipmentType.csv"]=$dataFolder
    ["fileType.csv"]=$dataFolder
    ["IBGE-BR_Municipios_2020_BULKLOAD.csv"]=$dataFolder
    ["IBGE-BR_UF_2020_BULKLOAD.csv"]=$dataFolder
    ["measurementUnit.csv"]=$dataFolder
    ["createMeasureDB.sql"]=$scriptFolder
    ["createProcessingDB.sql"]=$scriptFolder
)

# declare an associative array with pairs of update required files to download and target folders
declare -A updateFiles=(
    ["appCataloga.py"]=$scriptFolder
    ["backup_control.py"]=$scriptFolder
    ["processing_control.py"]=$scriptFolder
    ["shared.py"]=$scriptFolder
    ["backup_single_host.py"]=$scriptFolder
    ["db_handler.py"]=$scriptFolder
)

# define
tmp_folder="/tmp/appCataloga"

print_help() {
    echo -e "\nThis script will download appCataloga files from a repository and install them in the required folders.\n"
    echo "Use -i to install, -u to update, -r to remove. Any additional argument will be ignored."
    echo "    Initially, the required files will be downloaded from '$repository' to the '$tmp_folder' folder"
    echo "    Afterwards, the files will be moved to the target folders at: /$dataFolder and /$scriptFolder"
    echo "    Install will create the target folders and include database reference data and sql scripts."
    echo "    Update will overwrite the python script files only. Database will not be affected and reference data not downloaded."
    echo -e "\nExample: ./deploy.sh -i\n"
    exit
}

create_tmp_folder() {
    # try to create a temp folder, if it fails, exit
    if [ ! -d "$tmp_folder" ]; then
        if ! mkdir $tmp_folder; then
            echo "Error creating $tmp_folder"
            exit
        fi
    fi

    if ! cd /$tmp_folder; then
        echo "Error changing to $tmp_folder"
        exit
    fi
}

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

# Function to move files from tmp to target folders
move_files() {
    # move files
    for file in "${!installFiles[@]}"; do
        folder="${installFiles[$file]}"
        if ! mv -f "$file" "/$folder"; then
            echo "Error moving $file to /$folder"
            scritpError=true
        fi
    done
    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
        if ! mv -f "$file" "/$folder"; then
            echo "Error moving $file to /$folder"
            scritpError=true
        fi
    done
}

# Function to remove files and folders
remove_files() {
    # remove files
    for file in "${!installFiles[@]}"; do
        folder="${installFiles[$file]}"
        if ! rm -f "/$folder/$file}"; then
            echo "Error removing /$folder/$file}"
            scritpError=true
        fi
    done
    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
        if ! rm -f "/$folder/$file}"; then
            echo "Error removing /$folder/$file}"
            scritpError=true
        fi
    done

    # test if folders are empty, if so, remove them
    if [ -z "$(ls -A "/$dataFolder")" ]; then
        rm -rf "/${dataFolder:?}"
    else
        echo "Error removing folder /${dataFolder:?}."
        scritpError=true
    fi
    if [ -z "$(ls -A "/$scriptFolder")" ]; then
        rm -rf "/${scriptFolder:?}"
    else
        echo "Error removing folder /${scriptFolder:?}."
        scritpError=true
    fi

    if [ "$scritpError" == true ]; then
        echo "Error removing files and folders. Please remove them manually."
    fi
}

if [ "$1" == "-h" ]; then
    print_help
elif [ "$1" != "-i" ] && [ "$1" != "-u" ]; then
    create_tmp_folder

elif [ "$1" == "-r" ]; then
    remove_files
else
    echo "Invalid argument. Use -i, -u, -r, or -h"
fi

# try to download files from github. If it fails, the function will exit the script at this point
get_files "$1"

# try to move files to target folders, If it fails, the function will exit the script at this point
move_files

# remove tmp folder and all content
rm -rf $tmp_folder

172.16.17.11
