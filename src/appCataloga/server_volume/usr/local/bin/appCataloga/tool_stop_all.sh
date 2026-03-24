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
  appCataloga_file_bin_proces_appAnalise
  appCataloga_discovery
  appCataloga_host_maintenance
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

# -----------------------------------------------------------------------------
# Safe shutdown cleanup (HOST + FILE_TASK)
# -----------------------------------------------------------------------------
echo
echo ">>> Running safe shutdown cleanup (DB state reset)"

SAFE_STOP_SCRIPT="$SCRIPT_DIR/safe_stop.py"

if [[ -f "$SAFE_STOP_SCRIPT" ]]; then
    /opt/conda/envs/appdata/bin/python "$SAFE_STOP_SCRIPT"
    echo ">>> Safe shutdown cleanup completed."
else
    echo "[WARN] safe_stop.py not found. Skipping DB cleanup."
fi

# -----------------------------------------------------------------------------
# Optional log cleanup
# -----------------------------------------------------------------------------
LOG_DIR="/var/log"

read -p "Remove appCataloga log files in ${LOG_DIR} (appCataloga*.log)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    shopt -s nullglob
    log_files=("${LOG_DIR}"/appCataloga*.log)
    shopt -u nullglob

    if [[ ${#log_files[@]} -eq 0 ]]; then
        echo "No appCataloga log files found."
    else
        rm -f "${log_files[@]}"
        echo "Removed ${#log_files[@]} appCataloga log file(s)."
    fi
else
    echo "Log files preserved."
fi

echo
echo "All appCataloga services stopped."
echo "bye"
