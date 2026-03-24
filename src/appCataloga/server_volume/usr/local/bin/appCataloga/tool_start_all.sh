#!/bin/bash
# =============================================================================
# Script: tool_start_all.sh
# Purpose: Start ALL appCataloga services (CORE + auxiliaries)
#
# Usage:
#   ./tool_start_all.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

banner() {
    local w
    w=$(tput cols 2>/dev/null || echo 80)
    echo
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $w))\e[0m"
    printf "\e[32m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[32m$(printf "%0.s~" $(seq 1 $w))\e[0m"
}

banner "AppCataloga – START ALL SERVICES"

read -p "All appCataloga services will be STARTED. Continue? [y/N] " -n 1 -r
echo
[[ ! $REPLY =~ ^[Yy]$ ]] && echo "Operation canceled." && exit 1

services=(
  appCataloga
  appCataloga_host_check
  appCataloga_host_maintenance
  appCataloga_discovery
  appCataloga_file_bkp
  appCataloga_file_bin_proces_appAnalise
)

for svc in "${services[@]}"; do
    script="$SCRIPT_DIR/$svc.sh"
    if [[ -x "$script" ]]; then
        echo
        echo ">>> Starting $svc"
        "$script" start
    else
        echo "[ERROR] Script not found or not executable: $script"
    fi
done

echo
echo "All appCataloga services started."
echo "bye"
