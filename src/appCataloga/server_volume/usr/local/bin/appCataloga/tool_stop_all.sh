#!/bin/bash
# This script stops all appCataloga services

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

splash_banner() {
    terminal_width=$(tput cols 2>/dev/null || echo 80)
    echo
    echo -e "\e[31m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
    printf "\e[31m%*s\e[0m\n" $((($terminal_width + ${#1}) / 2)) "$1"
    echo -e "\e[31m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
}

splash_banner "AppCataloga Service Stopper"

read -p "All appCataloga services will be stopped. Do you want to continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Operation canceled. Nothing was done."
    exit 1
fi

# Lista dos serviços (cada um tem um .sh correspondente no mesmo diretório)
services=("appCataloga" "appCataloga_file_bkp" "appCataloga_file_bin_proces" "appCataloga_host_check")
#services=("appCataloga" "appCataloga_file_bkp" "appCataloga_file_bin_proces" "appCataloga_host_check" "appCataloga_pub_metadata")

for i in "${services[@]}"; do
    SCRIPT_PATH="$SCRIPT_DIR/${i}.sh"

    if [ -x "$SCRIPT_PATH" ]; then
        "$SCRIPT_PATH" stop
        if [ $? -eq 0 ]; then
            echo "  - $i stopped"
        else
            echo "  - ERROR: $i failed to stop"
        fi
    else
        echo "  - Script $SCRIPT_PATH not found or not executable"
    fi
done

# Perguntar sobre remoção de log
read -p "Remove all log files in /var/log/appCataloga? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Log files not removed"
else
    rm -f /var/log/appCataloga/*.log
    echo "  - All log files removed"
fi

echo
echo "bye"
