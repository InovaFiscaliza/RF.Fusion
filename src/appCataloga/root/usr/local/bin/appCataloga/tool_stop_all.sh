#!/bin/bash
# This script stops all appCataloga services

splash_banner() {
    # print full screen splash screen with the message received as argument

    terminal_width=$(tput cols)

    echo
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
    printf "\e[32m%*s\e[0m\n" $((($terminal_width + ${#1}) / 2)) "$1"
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
}

splash_banner "AppCataloga Service Stopper"

read -p "All appCataloga services will be stopped. Do you want to continue? [y/N]" -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Operation canceled. Nothing was done."
    exit 1
fi

services=("appCataloga.service" "appCataloga_file_bin_proces.service" "appCataloga_host_check.service" "appCataloga_pub_metadata.service")

for i in "${services[@]}"; do
    echo "  - $i stop requested"
    systemctl stop "$i"
done

# Create a list with all instances of appCataloga_file_bkp@* services
# and stop them
for i in $(systemctl list-units --full --all | grep appCataloga_file_bkp@ | awk '{print $1}'); do
    echo "  - $i stop requested"
    systemctl stop "$i"
done

read -p "Remove log file? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Log file not removed"
else
    rm -f /var/log/appCataloga.log
    echo "  - Log file removed"
fi

echo
echo bye
echo
