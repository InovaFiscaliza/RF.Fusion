#!/bin/bash

deploy_version=0.12

splash_banner() {

    terminal_width=$(tput cols)

    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
    printf "\e[32m%*s\e[0m\n" $((($terminal_width + ${#deploy_version}) / 2)) "appCataloga deploy script version $deploy_version"
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
}

# Download files from a repository and install
# Run as root this script as root

# Function to update this deploy script. Placed at start to reduce risk of script break before exit command
deploy_tool_repo="https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/install/appCataloga/deploy.sh"

update_deploy() {
    # print "\n- Updating deploy script..." using green color
    echo -e "\e[32m\n- Updating deploy script...\e[0m"

    wget -q --show-progress $deploy_tool_repo -O ./deploy.sh.new
    dos2unix -q deploy.sh.new
    chmod 755 deploy.sh.new
    new_version=$(grep -oP 'deploy_version=\K[^ ]+' deploy.sh.new | head -n 1)
    if [ "$new_version" == "$deploy_version" ]; then
        echo "No changes found in the deploy script."
        echo "Current version is $deploy_version."
        rm deploy.sh.new
        exit
    else
        echo -e "\nDeploy script updated from version $deploy_version to $new_version. Please check for changes and run the new script."
        mv deploy.sh.new deploy.sh
        exit
    fi
}

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
    if ! dnf install -y dos2unix; then
        echo "Error installing dos2unix. Please install it manually and try again."
        exit
    fi
fi

#! Declare constants that control the script

# define script control variables
repository="https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/appCataloga/root/"

# declare folders to be used
tmpFolder="/tmp/appCataloga"
downloadFolder="/tmp/appCataloga/download"
dataFolder="/etc/appCataloga"
scriptFolder="/usr/local/bin/appCataloga"
# systemdFolder="etc/systemd/system" To be used for future systemd service files

# declare global variables
mysql_user="to be defined"
password="to be defined"

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
    ["appCataloga_pub_metadata.py"]=$scriptFolder
    ["appCataloga_pub_metadata.service"]=$scriptFolder
    ["appCataloga_pub_metadata.sh"]=$scriptFolder
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

create_tmp_folders() {
    echo -e "\e[32m\n- Creting temporary folders...\e[0m"

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
}

create_install_folders() {

    echo -e "\e[32m\n- Creating folders that will be used by the application...\e[0m"
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
# Function to download files from the repository
get_files() {

    # change to downloadFolder folder for file download
    if ! cd "$downloadFolder"; then
        echo "Error changing to $downloadFolder"
        exit
    fi

    if [ "$1" == "-u" ]; then
        echo -e "\e[32m\n- Downloading files...\e[0m"

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

        echo -e "\e[32m\n- Moving files...\e[0m"

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
        if [ "$1" == "-u" ]; then
            remove_files_after_update
        elif [ "$1" == "-i" ]; then
            remove_install_folders
        fi
        exit
    fi
}

run_sql() {
    # create database and populate it with the createMeasureDB.sql script
    if ! mysql -u "$mysql_user" -p"$password" -e "SOURCE $tmpFolder/$2"; then
        echo "Error creating database $1. Please check the script and try again."
        exit 1
    else
        echo "Database $1 created successfully."
    fi
}

create_database() {
    if ! mysql -u "$mysql_user" -p"$password" -e "USE $1" >/dev/null 2>&1; then
        echo "Database $1 does not exist. Proceeding to create it..."
        # Run the createMeasureDB.sql script to create and populate database
        run_sql "$1" "$2"
    else
        read -p "Database $1 already exists. Do you wish to remove it? [y/N]" -n 1 -r
        echo " "
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if ! mysql -u "$mysql_user" -p"$password" -e "DROP DATABASE $1"; then
                echo "Error dropping database $1. Please remove it manually."
                exit 1
            else
                echo "Database $1 dropped successfully."
                # Proceed to create the database after dropping
                run_sql "$1" "$2"
            fi
        else
            read -p "Do you want to proceed with the installation process without the database setup? [y/N]" -n 1 -r
            echo " "
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                echo "For inital install you will need to run the database creation scripts manually from the $tmpFolder folder."
            else
                echo "Please remove $1 manually and try again."
                exit 1
            fi
        fi
    fi
}

install_database() {
    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Installing mysql (mariadb)...\e[0m"
    # test if mariadb is installed
    if ! which mysql >/dev/null; then
        echo "Mysql is not installed. Do you wish to install it? [y/N]"
        read -n 1 -r
        echo " "
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Please install mysql and try again."
            exit
        fi
    fi

    dnf module install -y mariadb

    updatesystemctl enable --now mariadb

    mysql_secure_installation
}
# configure mysql database
config_database() {

    echo -e "\e[32m\n- Configuring database...\e[0m"

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

    # prompt user for credentials to be used to access mysql
    read -p "Enter mysql user: " mysql_user
    read -s -p "Enter mysql password: " password
    echo " "

    # test if mysql is configured
    if ! mysql -u "$mysql_user" -p"$password" -e "SHOW DATABASES" >/dev/null 2>&1; then
        echo "mysql is not configured. Please configure it and try again."
        exit
    fi

    create_database "RFDATA" "createMeasureDB.sql"
    create_database "BPDATA" "createProcessingDB.sql"
}

update_database() {

    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Updating database...\e[0m"
    # Add required database update scripts here
    # run_sql "databaseUpdate.sql"
}

# Function to install miniconda
install_miniconda() {

    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Installing miniconda...\e[0m"

    # test if folder $scriptFolder/miniconda3 exist
    if [ -d "$scriptFolder/miniconda3" ]; then
        echo "Miniconda is already installed. Do you wish to reinstall it? [y/N]"
        read -n 1 -r
        echo " "
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$scriptFolder/miniconda3"
            exit
        fi
    fi

    # download miniconda installer
    wget -q --show-progress https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "$downloadFolder/miniconda.sh"

    # check if the file was downloaded, if not, exit
    if [ ! -f "$downloadFolder/miniconda.sh" ]; then
        echo "Error downloading miniconda.sh"
        exit
    else
        # if file was downloaded, convert to unix format and set permissions according to file extension
        dos2unix -q "$downloadFolder/miniconda.sh"
        chmod 755 "$downloadFolder/miniconda.sh"
    fi

    # install miniconda
    if ! "$downloadFolder/miniconda.sh" -b -p "$scriptFolder/miniconda3"; then
        echo "Error installing miniconda. Please check the script and try again."
        exit
    else
        echo "Miniconda installed successfully."
    fi
}

# Function to create conda enviorment from enviorment.yml
create_enviorment() {

    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Creating conda environment...\e[0m"

    # activate conda in the "$scriptFolder/miniconda3/bin/activate"
    if ! source "$scriptFolder/miniconda3/bin/activate"; then
        echo "Error activating conda. Please check the script and try again."
        exit
    fi

    if ! conda env create -f "$tmpFolder/environment.yml"; then
        echo "Error creating conda enviorment. Please check the script and try again."
        exit
    else
        echo "Conda enviorment created successfully."
    fi
}

# Function to update conda enviorment from enviorment.yml
update_enviorment() {

    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Updating conda environment...\e[0m"

    if ! conda env update -f "$tmpFolder/environment.yml"; then
        echo "Error updating conda enviorment. Please check the script and try again."
        exit
    else
        echo "Conda enviorment updated successfully."
    fi
}

# set SE linux for shell script files and enable services
prepare_service() {

    echo -e "\e[32m\n- Preparing services...\e[0m"

    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
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
        fi
    done
}

# Function to remove tmp folder
remove_tmp_folder() {

    echo -e "\e[32m\n- Removing temporary folders...\e[0m"

    # remove the downloadFolder
    if ! rm -rf "$downloadFolder"; then
        echo "Error removing $downloadFolder. Please remove it manually."
        exit
    fi
    # query user input to remove tmp folder
    read -p "Remove $tmpFolder? [y/N] " -n 1 -r
    echo " "
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

remove_install_folders() {
    # test if folders are empty, if so, remove them.
    # Splitting file removal and folder removal to avoid removing files created by other processes
    echo -e "\e[32m\n- Removing application folders...\e[0m"

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

# Function to remove files and folders
rollback_update() {

    scritpError=false

    echo -e "\e[32m\n- Rolling back installation...\e[0m"

    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
        if ! rm -f "$folder/$file}"; then
            echo "Error removing $folder/$file}"
            scritpError=true
        fi

    done
    for file in "${!special_files[@]}"; do
        folder="${special_files[$file]}"
        if ! rm -f "$folder/$file}"; then
            echo "Error removing $folder/$file}"
            scritpError=true
        fi
    done

    if [ "$scritpError" == true ]; then
        echo "Error removing files. Please remove them manually."
        exit
    fi
}

disable_services() {

    echo -e "\e[32m\n- Deactivating services from systemd...\e[0m"

    for file in "${!updateFiles[@]}"; do
        folder="${updateFiles[$file]}"
        full_file_name="$folder/$file"

        if [ "${file##*.}" == "service" ]; then
            if ! /usr/bin/systemctl disable "$full_file_name"; then
                echo "Error enabling $full_file_name"
                scritpError=true
            fi
        fi

    done
}

remove_mysql() {

    echo -e "\e[32m\n- Removing mysql...\e[0m"

    # test if mysql is running
    if ! systemctl is-active --quiet mysql; then
        # stop mysql
        if ! systemctl stop mysql; then
            echo "Error stopping mysql. Please stop it manually and try again."
            exit
        fi
    fi

    # test if mysql is enabled
    if ! systemctl is-enabled --quiet mysql; then
        # disable mysql
        if ! systemctl disable mysql; then
            echo "Error disabling mysql. Please disable it manually and try again."
            exit
        fi
    fi

    # remove mysql
    if ! dnf remove -y mariadb; then
        echo "Error removing mysql. Please remove it manually and try again."
        exit
    fi
}

remove_database() {

    if [ "$1" = "-skip" ]; then
        return
    fi

    echo -e "\e[32m\n- Removing database...\e[0m"

    # test if mysql is running
    if ! systemctl is-active --quiet mysql; then
        # try to start it
        if ! systemctl start mysql; then
            echo "Error starting mysql. Please start it and try again."
            exit
        fi
    fi

    # prompt user for credentials to be used to access mysql
    read -p "Enter mysql user: " mysql_user
    read -s -p "Enter mysql password: " password
    echo " "

    # test if mysql is configured
    if ! mysql -u "$mysql_user" -p"$password" -e "SHOW DATABASES" >/dev/null 2>&1; then
        echo "mysql is not configured. Please configure it and try again."
        exit
    fi

    # drop RFDATA and BPDATA databases
    if ! mysql -u "$mysql_user" -p"$password" -e "DROP DATABASE RFDATA" >/dev/null 2>&1; then
        echo "Error dropping database RFDATA. Please remove it manually."
        exit
    fi
    # drop BPDATA and BPDATA databases
    if ! mysql -u "$mysql_user" -p"$password" -e "DROP DATABASE BPDATA" >/dev/null 2>&1; then
        echo "Error dropping database RFDATA. Please remove it manually."
        exit
    fi

    echo "Do you wish to remove mysql? [y/N]"
    read -n 1 -r
    echo " "
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        remove_mysql
    fi
}

#! Main script
case "$1" in
-h)
    print_help
    ;;
-i)
    splash_banner
    create_tmp_folders
    create_install_folders
    get_files -i
    move_files -i
    install_database -skip
    config_database
    install_miniconda -skip
    create_enviorment -skip
    prepare_service
    remove_tmp_folder
    ;;
-u)
    splash_banner
    create_tmp_folders
    get_files -u
    move_files -u
    update_database -skip
    update_enviorment -skip
    prepare_service
    remove_tmp_folder
    ;;
-du)
    splash_banner
    update_deploy
    ;;
-r)
    splash_banner
    disable_services
    remove_install_folders
    remove_database -skip
    ;;
*)
    echo "Invalid option: $1"
    ;;
esac

echo -e "\nSuccess. Please check documentation for further instructions.\n"
