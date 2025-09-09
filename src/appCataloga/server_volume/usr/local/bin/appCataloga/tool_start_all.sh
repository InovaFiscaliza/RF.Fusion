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

splash_banner "AppCataloga Service Starter"

read -p "All appCataloga services will be started. Do you want to continue? [y/N]" -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Operation canceled. Nothing was done."
    exit 1
fi

services=("appCataloga.service" "appCataloga_file_bkp@0.service" "appCataloga_file_bin_proces.service" "appCataloga_host_check.service" "appCataloga_pub_metadata.service")

for i in "${services[@]}"; do
    systemctl start "$i"
    echo "  - $i started"
done

echo
echo bye
echo
