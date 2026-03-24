#!/bin/bash
# =============================================================================
# Script: tool_status_all.sh
# Purpose: Show consolidated status of ALL appCataloga services
#
# Usage:
#   ./tool_status_all.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

banner() {
    local w
    w=$(tput cols 2>/dev/null || echo 80)
    echo
    echo -e "\e[36m$(printf "%0.s~" $(seq 1 $w))\e[0m"
    printf "\e[36m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[36m$(printf "%0.s~" $(seq 1 $w))\e[0m"
}

banner "AppCataloga – SERVICE STATUS"

services=(
  appCataloga
  appCataloga_host_check
  appCataloga_host_maintenance
  appCataloga_discovery
  appCataloga_file_bin_proces
  appCataloga_file_bin_proces_appAnalise
  appCataloga_file_bkp
)

for svc in "${services[@]}"; do
    script="$SCRIPT_DIR/$svc.sh"
    if [[ -x "$script" ]]; then
        echo
        echo ">>> Status of $svc"
        "$script" status
    else
        echo "[ERROR] Script not found or not executable: $script"
    fi
done

echo
echo "Status check completed."
echo "bye"
