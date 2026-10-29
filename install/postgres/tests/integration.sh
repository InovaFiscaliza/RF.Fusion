#!/usr/bin/env bash
set +x
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/safety.sh"
postgres_test_gate "$@"
if ! bootstrap_report="$(podman logs "$test_container" |
    grep -E '^bootstrap duration_ms=[0-9]+ result=0 stage=complete clock=realtime$' | tail -n 1)"; then
    postgres_error "Successful bootstrap measurement missing or container log unavailable."
    exit 1
fi
printf '%s\n' "$bootstrap_report"
for database in BPDATA RFDATA RFFUSION_SUMMARY WEBFUSION; do
    postgres_test_pg -d "$database" -f - < "$POSTGRES_PACKAGE/tests/sql/test_structure.sql"
    postgres_test_pg -d "$database" -f - < "$POSTGRES_PACKAGE/tests/sql/test_rules.sql"
done
postgres_test_pg -d BPDATA -f - < "$POSTGRES_PACKAGE/tests/sql/test_uniqueness.sql"
bash "$POSTGRES_PACKAGE/tests/concurrency.sh" --container "$test_container" --ack-disposable
printf 'Prepared structural/rule/concurrency fixtures finished; A1 lifecycle acceptance remains mandatory.\n'
postgres_a1_pending
exit 1
