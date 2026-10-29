#!/usr/bin/env bash
set +x
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
postgres_validate_identity
postgres_snapshot_check

mkdir -p "$POSTGRES_STATE_DIR" "$PGDATA"
exec 9>"$POSTGRES_STATE_DIR/.bootstrap.lock"
flock -n 9 || { postgres_error "Bootstrap already locked."; exit 1; }
if [[ -f "$PGDATA/PG_VERSION" || -f "$POSTGRES_STATE_DIR/bootstrap.state" ]]; then
    if [[ -f "$PGDATA/PG_VERSION" && "$(postgres_state_value phase)" == complete ]]; then
        postgres_a1_pending
    else
        postgres_error "Unknown or partial cluster/state; explicit recovery required."
    fi
    exit 1
fi
[[ -z "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]] ||
    { postgres_error "PGDATA is not empty."; exit 1; }
[[ -z "$(find "$POSTGRES_STATE_DIR" -mindepth 1 -maxdepth 1 ! -name .bootstrap.lock -print -quit)" ]] ||
    { postgres_error "State volume is not new."; exit 1; }
postgres_validate_password_secrets /run/secrets

stage=initializing
temporary_started=0
started="$(date +%s%N)"
snapshot_id="$(sha256sum "$POSTGRES_PACKAGE/snapshot.sha256")"
snapshot_id="${snapshot_id%% *}"
write_state() {
    printf 'phase=%s\nstage=%s\nenvironment=%s\ncomponent=postgres\npg_major=18\nbootstrap_version=1\nsnapshot_sha256=%s\n' \
        "$1" "$stage" "$RFFUSION_ENV" "$snapshot_id" > "$POSTGRES_STATE_DIR/.state.$$"
    chmod 600 "$POSTGRES_STATE_DIR/.state.$$"
    mv -T "$POSTGRES_STATE_DIR/.state.$$" "$POSTGRES_STATE_DIR/bootstrap.state"
}
finish() {
    local status=$? elapsed
    trap - EXIT
    set +e
    if (( status != 0 )); then
        write_state failed || printf 'ERROR: Cannot persist failed bootstrap state.\n' >&2
        printf 'ERROR: Bootstrap failed at stage=%s, exit=%s; volumes preserved.\n' "$stage" "$status" >&2
    fi
    if (( temporary_started )); then
        runuser -u postgres -- pg_ctl -D "$PGDATA" -m fast -w -t 30 stop ||
            { printf 'ERROR: Cannot stop temporary PostgreSQL.\n' >&2; (( status != 0 )) || status=1; }
    fi
    elapsed=$(( ($(date +%s%N) - started) / 1000000 ))
    printf 'bootstrap duration_ms=%s result=%s stage=%s clock=realtime\n' "$elapsed" "$status" "$stage"
    exit "$status"
}
trap finish EXIT
write_state initializing
cp "$POSTGRES_PACKAGE/snapshot.sha256" "$POSTGRES_STATE_DIR/bootstrap-snapshot.sha256"
chmod 600 "$POSTGRES_STATE_DIR/bootstrap-snapshot.sha256"
chown postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
install -d -o postgres -g postgres -m 770 /var/run/postgresql
runuser -u postgres -- initdb -D "$PGDATA" --encoding=UTF8 --locale=C.UTF-8 \
    --auth-local=peer --auth-host=scram-sha-256 --no-instructions
stage=temporary-server
temporary_started=1
runuser -u postgres -- pg_ctl -D "$PGDATA" -w -t 60 \
    -o "-c config_file=$POSTGRES_PACKAGE/config/postgresql.conf -c hba_file=$POSTGRES_PACKAGE/config/pg_hba.conf -c ident_file=$POSTGRES_PACKAGE/config/pg_ident.conf -c listen_addresses=''" start
stage=roles
write_state initializing
postgres_pg -q -d postgres -f "$POSTGRES_PACKAGE/sql/createRoles.sql"
for pair in 'BPDATA:createProcessingDB.sql' 'RFDATA:createMeasureDB.sql' \
            'RFFUSION_SUMMARY:createFusionSummaryDB.sql' 'WEBFUSION:createWebFusionDB.sql'; do
    stage="${pair%%:*}"
    write_state initializing
    postgres_pg -q -d "$stage" -f "$POSTGRES_PACKAGE/sql/${pair#*:}"
done
stage=fdw
write_state initializing
postgres_pg -q -d BPDATA <<'SQL'
REVOKE ALL ON DATABASE "BPDATA" FROM PUBLIC;
GRANT CONNECT ON DATABASE "BPDATA" TO rffusion_fdw_reader;
GRANT USAGE ON SCHEMA "BPDATA" TO rffusion_fdw_reader;
GRANT SELECT ON "BPDATA"."HOST", "BPDATA"."HOST_TASK", "BPDATA"."FILE_TASK", "BPDATA"."FILE_TASK_HISTORY" TO rffusion_fdw_reader;
SQL
postgres_pg -q -d RFFUSION_SUMMARY -f "$POSTGRES_PACKAGE/sql/createCrossDatabaseView.sql"
for database in BPDATA RFDATA RFFUSION_SUMMARY WEBFUSION; do
    postgres_pg -q -d "$database" -v database="$database" <<'SQL'
REVOKE ALL ON DATABASE :"database" FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE :"database" TO rffusion_appcataloga,rffusion_webfusion;
GRANT USAGE ON SCHEMA :"database" TO rffusion_appcataloga,rffusion_webfusion;
GRANT SELECT ON ALL TABLES IN SCHEMA :"database" TO rffusion_webfusion;
SQL
    if [[ "$database" == WEBFUSION ]]; then
        postgres_pg -q -d "$database" <<'SQL'
REVOKE CONNECT ON DATABASE "WEBFUSION" FROM rffusion_appcataloga;
REVOKE ALL ON SCHEMA "WEBFUSION" FROM rffusion_appcataloga;
GRANT INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "WEBFUSION" TO rffusion_webfusion;
ALTER ROLE rffusion_webfusion IN DATABASE "WEBFUSION" SET search_path = "WEBFUSION",public;
SQL
    else
        postgres_pg -q -d "$database" -v database="$database" <<'SQL'
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA :"database" TO rffusion_appcataloga;
ALTER ROLE rffusion_appcataloga IN DATABASE :"database" SET search_path = :"database",public;
ALTER ROLE rffusion_webfusion IN DATABASE :"database" SET search_path = :"database",public;
SQL
    fi
done
stage=verification
write_state initializing
"$POSTGRES_PACKAGE/linux/verify.sh"
stage=stop-temporary-server
runuser -u postgres -- pg_ctl -D "$PGDATA" -m fast -w -t 30 stop
temporary_started=0
stage=complete
write_state complete
