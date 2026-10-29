#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
postgres_validate_identity
postgres_snapshot_check
export PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=60000 -c lock_timeout=5000"
for database in BPDATA RFDATA RFFUSION_SUMMARY WEBFUSION; do
    postgres_pg -q -d "$database" -f "$POSTGRES_PACKAGE/sql/verify${database}.sql"
done
postgres_pg -q -d RFFUSION_SUMMARY <<'SQL'
BEGIN READ ONLY;
DO $view$
DECLARE n integer;
BEGIN
    SELECT count(*) INTO n FROM information_schema.columns
    WHERE table_schema='RFFUSION_SUMMARY' AND table_name='VW_ERROR_EVENT_CANONICAL';
    IF n<>13 THEN RAISE EXCEPTION 'Canonical view must expose 13 columns'; END IF;
    IF (SELECT array_agg(column_name::text ORDER BY ordinal_position) FROM information_schema.columns
        WHERE table_schema='RFFUSION_SUMMARY' AND table_name='VW_ERROR_EVENT_CANONICAL') IS DISTINCT FROM
        ARRAY['NA_SOURCE_TABLE','ID_SOURCE_ROW','NA_ERROR_SCOPE','FK_HOST','NA_HOST_NAME','DT_EVENT_AT',
              'NU_STATUS_VALUE','NA_ERROR_CODE','NA_ERROR_SUMMARY','NA_ERROR_SUMMARY_HASH','NA_ERROR_DETAIL',
              'NU_ERROR_CLASSIFIER_VERSION','NA_RAW_MESSAGE'] THEN RAISE EXCEPTION 'View column order/names'; END IF;
    IF (SELECT relkind FROM pg_class WHERE oid='"RFFUSION_SUMMARY"."VW_ERROR_EVENT_CANONICAL"'::regclass)<>'v'
    THEN RAISE EXCEPTION 'Expected dynamic canonical view'; END IF;
    IF (SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_table_schema='BPDATA')<>4
    THEN RAISE EXCEPTION 'FDW must expose four foreign tables'; END IF;
    IF EXISTS (SELECT 1 FROM pg_user_mappings WHERE srvname='rffusion_bpdata' AND umuser=0)
    THEN RAISE EXCEPTION 'PUBLIC FDW mapping is forbidden'; END IF;
END;
$view$;
SELECT count(*) AS canonical_error_events FROM "RFFUSION_SUMMARY"."VW_ERROR_EVENT_CANONICAL";
COMMIT;
SQL
printf 'Verification complete: catalog and reference values; no writes performed.\n'
