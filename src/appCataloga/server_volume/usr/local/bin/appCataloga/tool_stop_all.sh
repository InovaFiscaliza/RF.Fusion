#!/bin/bash
# =============================================================================
# Script: tool_stop_all.sh
# Purpose: Stop ALL appCataloga services (auxiliaries + CORE)
#
# Usage:
#   ./tool_stop_all.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

banner() {
    local w
    w=$(tput cols 2>/dev/null || echo 80)
    echo
    echo -e "\e[31m$(printf "%0.s~" $(seq 1 $w))\e[0m"
    printf "\e[31m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[31m$(printf "%0.s~" $(seq 1 $w))\e[0m"
}

banner "AppCataloga – STOP ALL SERVICES"

read -p "All appCataloga services will be STOPPED. Continue? [y/N] " -n 1 -r
echo
[[ ! $REPLY =~ ^[Yy]$ ]] && echo "Operation canceled." && exit 1

services=(
  appCataloga_file_bkp
  appCataloga_file_bin_proces
  appCataloga_discovery
  appCataloga_host_check
  appCataloga
)

for svc in "${services[@]}"; do
    script="$SCRIPT_DIR/$svc.sh"
    if [[ -x "$script" ]]; then
        echo
        echo ">>> Stopping $svc"
        "$script" stop
    else
        echo "[ERROR] Script not found or not executable: $script"
    fi
done

# Limpeza opcional de logs
read -p "Remove all log files in /var/log/appCataloga? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f /var/log/appCataloga/*.log
    echo "All log files removed."
else
    echo "Log files preserved."
fi

echo
echo "All appCataloga services stopped."
echo "bye"
