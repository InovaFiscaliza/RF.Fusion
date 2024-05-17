#!/bin/bash

# Download files from a repository and install
# Run as root this script as root

#! initial system requirement and argument tests

# if no argument is passed, exit
simple_help="Use -i to install, -u to update, -r to remove, -du to update this deployment tool. Any additional argument will be ignored."

if [ $# -eq 0 ]; then
    echo "No arguments provided. $simple_help"
    exit
fi

case "$1" in
-i | -u | -r | -h | -du) ;;
*)
    echo "Invalid argument. $simple_help"
    exit 1
    ;;
esac

# test requirements
if ! which dos2unix >/dev/null; then
    echo "dos2unix is required to run this script. Please install it and try again."
    exit
fi

#! Declare constants that control the script

# define script control variables
repository="https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/appCataloga/root/"
deploy_tool_repo="https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/install/appCataloga/deploy.sh"

# declare folders to be used
tmpFolder="/tmp/appCataloga"
downloadFolder="/tmp/appCataloga/download"
dataFolder="/etc/appCataloga"
scriptFolder="/usr/local/bin/appCataloga"
# systemdFolder="etc/systemd/system" To be used for future systemd service files

#TODO: #2 Add group and user properties  individually, securing secret.py
# declare an associative array with pairs of install required files to download and target folders
declare -A installFiles=(
    [".secret"]=$dataFolder
)

# declare an associative array with pairs of update required files to download and target folders
declare -A updateFiles=(
    ["db_handler.py"]=$scriptFolder
    ["shared.py"]=$scriptFolder
    ["appCataloga_file_bkp.py"]=$scriptFolder
    ["appCataloga_file_bkp@.service"]=$scriptFolder
    ["appCataloga_file_bkp.sh"]=$scriptFolder
    ["appCataloga_file_bin_proces.py"]=$scriptFolder
    ["appCataloga_file_bin_proces.service"]=$scriptFolder
    ["appCataloga_file_bin_proces.sh"]=$scriptFolder
    ["appCataloga_host_check.py"]=$scriptFolder
    ["appCataloga_host_check.service"]=$scriptFolder
    ["appCataloga_host_check.sh"]=$scriptFolder
    ["appCataloga.py"]=$scriptFolder
    ["appCataloga.sh"]=$scriptFolder
    ["appCataloga.service"]=$scriptFolder
    ["environment.yml"]=$tmpFolder
    ["equipmentType.csv"]=$tmpFolder
    ["fileType.csv"]=$tmpFolder
    ["IBGE-BR_Municipios_2020_BULKLOAD.csv"]=$tmpFolder
    ["IBGE-BR_UF_2020_BULKLOAD.csv"]=$tmpFolder
    ["measurementUnit.csv"]=$tmpFolder
    ["createMeasureDB.sql"]=$tmpFolder
    ["createProcessingDB.sql"]=$tmpFolder
)

# declare an associative array with pairs of special required files to download and target folders
# these files may require special handling by the user if changed
declare -A special_files=(
    ["config.py"]=$dataFolder)

#! Varios functions to be used later
print_help() {
    echo -e "\nThis script will download appCataloga files from a repository and install them in the required folders.\n"
    echo "$simple_help"
    echo "    Install will create the target folders and include database reference data and sql scripts."
    echo "    Update will overwrite the python script files only. Database will not be affected and reference data not downloaded."
    echo "    Remove will delete all files that may be downloaded, but will not affect the database."
    echo "    Update deploy will update this deploy script."
    echo -e "\nThe install and update procedure starts by downloading the required files from '$repository' to the '$tmpFolder' folder."
    echo "    Afterwards, the files will be moved to the target folders at: $dataFolder and $scriptFolder and tmp folder will be removed."
    echo "    Changes in these folders and files to be copied must be performed by editing the script."
    echo "If any error occurs during the process, the script will exit and no changes will be made."
    echo -e "\nUsage example: ./deploy.sh -i\n"
    exit
}

create_folders() {
    # try to create a temp folder, if it fails, exit
    if [ ! -d "$tmpFolder" ]; then
        if ! mkdir $tmpFolder; then
            echo "Error creating $tmpFolder"
            exit
        fi
    fi

    # try to create a download folder, if it fails, exit
    if [ ! -d "$downloadFolder" ]; then
        if ! mkdir $downloadFolder; then
            echo "Error creating $downloadFolder"
            exit
        fi
    fi

    # create data folder if it does not exist
    if [ ! -d "$dataFolder" ]; then
        if ! mkdir $dataFolder; then
            echo "Error creating $dataFolder"
            exit
        fi
    fi

    # create script folder if it does not exist
    if [ ! -d "$scriptFolder" ]; then
        if ! mkdir $scriptFolder; then
            echo "Error creating $scriptFolder"
            exit
        fi
    fi
}

download_file() {
    # download file using name received as argument
    wget -q --show-progress "$1"

    # check if the file was downloaded, if not, exit
    if [ ! -f "${1##*/}" ]; then
        echo "Error downloading ${1##*/} from $1"
        # remove tmp folder and all content
        rm -rf "$tmpFolder"
        exit
    else
        # if file was downloaded, convert to unix format and set permissions according to file extension
        dos2unix -q "${1##*/}"

        case "${1##*.}" in
        csv | sql | yml | service)
            chmod 644 "${1##*/}"
            ;;
        py | sh)
            chmod 755 "${1##*/}"
            ;;
        esac
    fi
}
# Funciton to download files from the repository
get_files() {

    # change to downloadFolder folder for file download
    if ! cd "$downloadFolder"; then
        echo "Error changing to $downloadFolder"
        exit
    fi

    if [ "$1" == "-u" ]; then
        # download files that are in the update list
        for file in "${!updateFiles[@]}"; do
            folder="${updateFiles[$file]}"
            full_file_url="$repository$folder/${file}"
            download_file "$full_file_url"
        done

        # download files that are in the special list
        for file in "${!special_files[@]}"; do
            folder="${special_files[$file]}"
            full_file_url="$repository$folder/${file}"
            download_file "$full_file_url"
        done

    elif [ "$1" == "-i" ]; then
        # download files that are in the install list
        get_files "-u"

        # download files in the install list
        for file in "${!installFiles[@]}"; do
            folder="${installFiles[$file]}"
            full_file_url="$repository$folder/${file}"
            download_file "$full_file_url"
        done
    fi
}

handle_special() {
    # test if file is "config.py"
    if [ "$file" == "config.py" ]; then
        # check if file is different from the one in the target folder
        if [ -f "$folder/$file" ]; then
            if ! diff -q "$file" "$folder/$file"; then
                echo "Error: $folder/$file already exists and is different from the downloaded file."
                scritpError=true
            fi
        fi
    fi
}

# Function to move files from tmp to target folders
move_files() {
    scritpError=false

    # move files from the update list to the target folders
    if [ "$1" == "-u" ]; then
        for file in "${!updateFiles[@]}"; do
            folder="${updateFiles[$file]}"
            if ! mv -f "$file" "$folder"; then
                echo "Error moving $file to $folder"
                scritpError=true
            fi
        done
        for file in "${!special_files[@]}"; do
            folder="${special_files[$file]}"
            if [ -f "$folder/$file" ]; then
                if ! diff -q "$file" "$folder/$file"; then
                    echo "Warning: $folder/$file already exists. A backup will be created in the same folder."
                    if ! mv -f "$folder/$file" "$folder/$file.bak"; then
                        echo "Error moving $folder/$file to $folder/$file.bak"
                        scritpError=true
                    fi
                fi
            fi
            if ! mv -f "$file" "$folder"; then
                echo "Error moving $file to $folder"
                scritpError=true
            fi
        done
    # move files from the install list to the target folders
    elif [ "$1" == "-i" ]; then
        move_files "-u"

        for file in "${!installFiles[@]}"; do
            folder="${installFiles[$file]}"

            if ! mv -f "$file" "$folder"; then
                echo "Error moving $file to $folder"
                scritpError=true
            fi
        done
    fi

    if [ "$scritpError" == true ]; then
        echo "Error moving files. Check user and target folder permissions."

        echo "Rolling back files moved to $dataFolder and $scriptFolder"
        remove_files "$1" -v
        exit
    fi
}

# configure mysql database
config_database() {

    # test if mysql is installed
    if ! which mysql >/dev/null; then
        echo "mysql is required to run this script. Please install it and try again."
        exit
    fi

    # test if mysql is running
    if ! systemctl is-active --quiet mysql; then
        echo "mysql is not running. Please start it and try again."
        exit
    fi

    # test if mysql is enabled
    if ! systemctl is-enabled --quiet mysql; then
        echo "mysql is not enabled. Please enable it and try again."
        exit
    fi

    # test if mysql is configured
    if ! mysql -e "SELECT 1" >/dev/null; then
        echo "mysql is not configured. Please configure it and try again."
        exit
    fi

    # test if database RFDATA do not exists, create it
    if mysql -e "USE RFDATA" >/dev/null; then
        read -p "Database RFDATA already exists. Do you wish to remove it? [y/N]" -n 1 -r
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if ! mysql -e "DROP DATABASE RFDATA"; then
                echo "Error dropping database RFDATA. Please remove it manually."
                exit
            fi
        else
            echo "Please remove RFDATA manually and try again."
            exit
        fi

        # run the createMeasureDB.sql script to create and populate database
        if ! mysql -e "SOURCE $tmpFolder/createMeasureDB.sql"; then
            echo "Error creating database RFDATA. Please check the script and try again."
            exit
        fi
    fi

    # test if database BPDATA do not exists, create it
    if mysql -e "USE BPDATA" >/dev/null; then
        read -p "Database BPDATA already exists. Do you wish to remove it? [y/N]" -n 1 -r
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if ! mysql -e "DROP DATABASE BPDATA"; then
                echo "Error dropping database BPDATA. Please remove it manually."
                exit
            fi
        else
            echo "Please remove BPDATA manually and try again."
            exit
        fi

        # run the createProcessingDB.sql script to create and populate database
        if ! mysql -e "SOURCE $tmpFolder/createProcessingDB.sql"; then
            echo "Error creating database RFDATA. Please check the script and try again."
            exit
        fi
    fi
}

# set SE linux for shell script files and enable services
prepare_service() {

    for file in "${!updateFiles[@]}"; do
        folder="${special_files[$file]}"
        full_file_name="$folder/$file"

        # set SE linux for shell script files
        if [ "${file##*.}" == "sh" ]; then
            if ! /sbin/restorecon -v "$full_file_name"; then
                echo "Error setting SE Linux for $full_file_name. Do it manually."
            fi
        fi

        # enable services for restart in systemd
        if [ "${file##*.}" == "service" ]; then
            if ! /usr/bin/systemctl enable "$full_file_name"; then
                echo "Error enabling $full_file_name"
                scritpError=true
            fi
            #            if ! /usr/bin/systemctl start "$full_file_name"; then
            #                echo "Error starting $full_file_name"
            #                scritpError=true
            #            fi
        fi
    done
}

# Function to remove tmp folder
remove_tmp_folder() {
    # remove the downloadFolder
    if ! rm -rf "$downloadFolder"; then
        echo "Error removing $downloadFolder. Please remove it manually."
        exit
    fi
    # query user input to remove tmp folder
    echo "For inital install you will need to run the database creation scripts manually from the $tmpFolder folder."
    read -p "Remove $tmpFolder? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Please remove $tmpFolder manually afterwards."
        exit
    fi

    # remove tmp folder and all content
    if ! rm -rf "$tmpFolder"; then
        echo "Error removing $tmpFolder. Please remove it manually."
        exit
    fi
}

# Function to remove files and folders
remove_files() {
    scritpError=false

    if [ "$1" == "-u" ]; then
        for file in "${!updateFiles[@]}"; do
            folder="${updateFiles[$file]}"
            if ! rm -f "$folder/$file}"; then
                if [ "$2" == "-v" ]; then
                    echo "Error removing $folder/$file}"
                fi
                scritpError=true
            fi
        done
        for file in "${!special_files[@]}"; do
            folder="${special_files[$file]}"
            if ! rm -f "$folder/$file}"; then
                if [ "$2" == "-v" ]; then
                    echo "Error removing $folder/$file}"
                fi
                scritpError=true
            fi
        done
    elif [ "$1" == "-i" ]; then
        remove_files -u "$2"
        for file in "${!installFiles[@]}"; do
            folder="${installFiles[$file]}"
            if ! rm -f "$folder/$file}"; then
                if [ "$2" == "-v" ]; then
                    echo "Error removing $folder/$file}"
                fi
                scritpError=true
            fi
        done
    fi

    if [ "$scritpError" == true ]; then
        echo "Error removing files. Please remove them manually."
        exit
    fi

    # test if folders are empty, if so, remove them.
    # Splitting file removal and folder removal to avoid removing files created by other processes
    if [ -z "$(ls -A "$dataFolder")" ]; then
        rm -rf "${dataFolder:?}"
    else
        echo "Error removing folder ${dataFolder:?}. Folder not empty."
        scritpError=true
    fi
    if [ -z "$(ls -A "$scriptFolder")" ]; then
        rm -rf "${scriptFolder:?}"
    else
        echo "Error removing folder ${scriptFolder:?}. Folder not empty."
        scritpError=true
    fi

    if [ "$scritpError" == true ]; then
        echo "All files were removed but error removing folders. Please remove them manually as needed. "
        exit
    fi
}

update_deploy() {

    echo "WARNING: This will overwrite this deploy script and may cause it to stop working."
    echo "In case of error, just run it again with the -du option to ensure that it is updated correctly."

    wget -q --show-progress $deploy_tool_repo -O ./deploy.sh
    dos2unix -q deploy.sh
    chmod 755 deploy.sh
    echo -e "\nDeploy script updated."
}

#! Main script
case "$1" in
-h)
    print_help
    ;;
-i | -u)
    create_folders
    get_files "$1"
    move_files "$1"
    config_database
    prepare_service
    remove_tmp_folder
    ;;
-du)
    update_deploy
    ;;
-r)
    remove_files -i -v
    ;;
*)
    echo "Invalid option: $1"
    ;;
esac

echo -e "\nSuccess. Please check documentation for further instructions.\n"
