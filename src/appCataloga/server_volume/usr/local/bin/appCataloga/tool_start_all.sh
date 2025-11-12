#!/bin/bash
# This script starts all appCataloga services

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

splash_banner() {
    terminal_width=$(tput cols 2>/dev/null || echo 80)

    echo
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
    printf "\e[32m%*s\e[0m\n" $((($terminal_width + ${#1}) / 2)) "$1"
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $terminal_width))\e[0m"
}

splash_banner "AppCataloga Service Starter"

read -p "All appCataloga services will be started. Do you want to continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  - Operation canceled. Nothing was done."
    exit 1
fi

#services=("appCataloga" "appCataloga_host_check" "appCataloga_file_bkp" "appCataloga_file_bin_proces" "appCataloga_pub_metadata")
# Por enquanto nao vou subir o pub metadata 
services=("appCataloga" "appCataloga_host_check" "appCataloga_file_bkp" "appCataloga_file_bin_proces")

# Uso esta linha caso queira desativar algum servico e depurar via terminal
#services=("appCataloga" "appCataloga_file_bkp" "appCataloga_host_check" "appCataloga_pub_metadata")

for i in "${services[@]}"; do
    SCRIPT_PATH="$SCRIPT_DIR/${i}.sh"

    if [ -x "$SCRIPT_PATH" ]; then
        "$SCRIPT_PATH" start
        if [ $? -eq 0 ]; then
            echo "  - $i started"
        else
            echo "  - ERROR: $i failed to start"
        fi
    else
        echo "  - Script $SCRIPT_PATH not found or not executable"
    fi
done

echo
echo "bye"
