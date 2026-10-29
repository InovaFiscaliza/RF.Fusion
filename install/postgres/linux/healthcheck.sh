#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
postgres_validate_identity
[[ "$(postgres_state_value phase)" == complete ]] ||
    { postgres_error "Bootstrap is not complete."; exit 1; }
[[ "$(postgres_state_value environment)" == "$RFFUSION_ENV" ]] ||
    { postgres_error "State identity mismatch."; exit 1; }
# Only this process's fresh bootstrap is recognized while A1 remains unresolved.
[[ -s /run/rffusion-postgres/fresh-bootstrap ]] || { postgres_a1_pending; exit 1; }
timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/2828' ||
    { postgres_error "SSH readiness failed."; exit 1; }
export PGCONNECT_TIMEOUT=3 PGOPTIONS="-c statement_timeout=3000 -c default_transaction_read_only=on"
[[ "$(postgres_pg -qAt -d postgres -c 'SELECT 1')" == 1 ]] ||
    { postgres_error "SQL readiness failed."; exit 1; }
