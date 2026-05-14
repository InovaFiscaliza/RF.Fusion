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

echo
echo ">>> Disabling legacy RFFUSION_SUMMARY SQL event"
if cd "$SCRIPT_DIR" && /opt/conda/envs/appdata/bin/python - <<'PY'
from bootstrap_paths import bootstrap_app_paths

bootstrap_app_paths("appCataloga_rffusion_summary_worker.py")

from db.dbHandlerSummary import dbHandlerSummary
import config as k


class _SilentLog:
    def entry(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


db = dbHandlerSummary(
    database=k.SUMMARY_DATABASE_NAME,
    log=_SilentLog(),
    reuse_connection=True,
)
try:
    db.disable_sql_event(k.SUMMARY_WORKER_SQL_EVENT_NAME)
finally:
    db._disconnect(force=True)
PY
then
    echo ">>> Legacy SQL event disabled."
else
    echo "[WARN] Failed to disable the legacy SQL event preflight. The summary worker will retry on startup."
fi

services=(
  appCataloga
  appCataloga_host_check
  appCataloga_host_maintenance
  appCataloga_discovery
  appCataloga_backlog_management
  appCataloga_file_bkp
  appCataloga_file_bin_proces_appAnalise
  # Keep the summary consumer last because it performs a full reconcile on cold start.
  appCataloga_rffusion_summary_worker
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
