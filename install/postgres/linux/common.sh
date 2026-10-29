#!/usr/bin/env bash

POSTGRES_PACKAGE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$POSTGRES_PACKAGE/linux/environment.sh"
export PATH="/usr/lib/postgresql/18/bin:$PATH"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
POSTGRES_STATE_DIR="${POSTGRES_STATE_DIR:-/srv/app_volume}"

postgres_pg() {
    runuser -u postgres -- psql -X -h /var/run/postgresql -p 5432 -U postgres -v ON_ERROR_STOP=1 "$@"
}

postgres_snapshot_check() {
    [[ -s "$POSTGRES_PACKAGE/snapshot.sha256" ]] ||
        { postgres_error "Snapshot manifest missing."; return 1; }
    (cd "$POSTGRES_PACKAGE" && sha256sum --strict --check snapshot.sha256)
}

postgres_state_value() {
    local key="$1"
    [[ -r "$POSTGRES_STATE_DIR/bootstrap.state" ]] || return 1
    sed -n "s/^${key}=//p" "$POSTGRES_STATE_DIR/bootstrap.state"
}

postgres_a1_pending() {
    postgres_error "A1 PENDING: compatibility/reuse policy not approved; preserve volumes and request an explicit decision."
}

postgres_validate_password_secrets() {
    local directory="$1" secret value first=""
    for secret in rffusion-postgres-appcataloga rffusion-postgres-webfusion; do
        [[ -s "$directory/$secret" && -r "$directory/$secret" ]] ||
            { postgres_error "Required password secret missing: $secret."; return 1; }
        [[ "$(LC_ALL=C tr -cd '\000' < "$directory/$secret" | wc -c)" -eq 0 ]] ||
            { postgres_error "NUL in password secret."; return 1; }
        value="$(cat "$directory/$secret")"
        [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* &&
           "$(wc -l < "$directory/$secret")" -le 1 ]] ||
            { postgres_error "Password secret must be one nonempty line."; return 1; }
        if [[ -z "$first" ]]; then
            first="$value"
        elif [[ "$first" == "$value" ]]; then
            postgres_error "Application passwords must be distinct."
            return 1
        fi
    done
}
