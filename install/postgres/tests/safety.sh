#!/usr/bin/env bash

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../linux/common.sh"

postgres_test_pg() {
    podman exec -i "$test_container" runuser -u postgres -- \
        psql -X -h /var/run/postgresql -p 5432 -U postgres \
        -v ON_ERROR_STOP=1 -v VERBOSITY=verbose "$@"
}

postgres_test_gate() {
    local ack=0 selected=0 inspected all_ids volumes identity database
    test_container=""
    while (( $# )); do
        case "$1" in
            --container)
                (( selected == 0 && $# >= 2 )) && [[ "$2" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] ||
                    { postgres_error "Invalid --container."; return 2; }
                test_container="$2"; selected=1; shift 2 ;;
            --ack-disposable)
                (( ack == 0 )) || { postgres_error "Duplicate acknowledgment."; return 2; }
                ack=1; shift ;;
            *) postgres_error "Unknown integration argument."; return 2 ;;
        esac
    done
    (( selected == 1 && ack == 1 )) ||
        { postgres_error "--container NAME and --ack-disposable are required."; return 2; }
    [[ -z "${CONTAINER_HOST:-}${CONTAINER_CONNECTION:-}${DOCKER_HOST:-}" ]] ||
        { postgres_error "Remote Podman context forbidden."; return 1; }
    podman() { command podman --remote=false "$@"; }
    inspected="$(podman inspect --type container "$test_container")"
    printf '%s' "$inspected" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" target --name "$test_container"
    volumes="$(podman volume inspect rffusion-postgres-development-data rffusion-postgres-development-state)"
    printf '%s' "$volumes" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" volumes --environment development
    all_ids="$(podman ps -a -q)"
    [[ -n "$all_ids" ]] || { postgres_error "Selected test container disappeared."; return 1; }
    local -a ids
    read -r -a ids <<< "${all_ids//$'\n'/ }"
    podman inspect "${ids[@]}" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" exclusive --name "$test_container"
    identity="$(podman exec "$test_container" cat /etc/rffusion-environment)"
    [[ "$identity" == $'RFFUSION_ENV=development\nRFFUSION_COMPONENT=postgres' ]] ||
        { postgres_error "Recorded identity mismatch."; return 1; }
    podman exec "$test_container" /usr/local/bin/healthcheck.sh
    podman exec "$test_container" /usr/local/bin/verify.sh
    for database in BPDATA RFDATA RFFUSION_SUMMARY WEBFUSION; do
        postgres_test_pg -q -d "$database" <<'SQL'
BEGIN READ ONLY;
DO $empty$
DECLARE r record; present boolean;
BEGIN
    FOR r IN SELECT c.relname,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='r' AND n.nspname=current_database() LOOP
        IF current_database()='RFDATA' AND r.relname IN
          ('DIM_EQUIPMENT_TYPE','DIM_SITE_STATE','DIM_SITE_COUNTY','DIM_FILE_TYPE','DIM_SPECTRUM_UNIT')
          THEN CONTINUE; END IF;
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.%I)',r.nspname,r.relname) INTO present;
        IF present THEN RAISE EXCEPTION 'Operational data in %.%; test refused',r.nspname,r.relname; END IF;
    END LOOP;
END;
$empty$;
COMMIT;
SQL
    done
}
