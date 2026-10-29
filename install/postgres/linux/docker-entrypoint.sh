#!/usr/bin/env bash
set +x
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
postgres_install_identity
install -d -m 700 /run/rffusion-postgres
if [[ -e /run/rffusion-postgres/fresh-bootstrap ]]; then
    rm /run/rffusion-postgres/fresh-bootstrap
fi
[[ -s /run/secrets/rffusion-postgres-authorized-keys ]] ||
    { postgres_error "SSH authorized_keys secret missing."; exit 1; }
ssh-keygen -lf /run/secrets/rffusion-postgres-authorized-keys >/dev/null
"$POSTGRES_PACKAGE/linux/bootstrap.sh"
install -d -m 700 "$POSTGRES_STATE_DIR/ssh" /root/.ssh
install -m 600 /run/secrets/rffusion-postgres-authorized-keys /root/.ssh/authorized_keys
for type in ed25519 rsa; do
    key="$POSTGRES_STATE_DIR/ssh/ssh_host_${type}_key"
    if [[ ! -e "$key" ]]; then
        ssh-keygen -q -t "$type" -N '' -f "$key"
    fi
done
install -d -m 755 /run/sshd
/usr/sbin/sshd -t -f "$POSTGRES_PACKAGE/config/sshd_config"
pg_pid="" ssh_pid=""
shutdown() {
    local status="${1:-0}"
    trap - TERM INT EXIT
    if [[ -n "$ssh_pid" ]] && kill -0 "$ssh_pid" 2>/dev/null; then
        kill -TERM "$ssh_pid"
        wait "$ssh_pid" || printf 'SSH stopped.\n' >&2
    fi
    if [[ -n "$pg_pid" ]] && kill -0 "$pg_pid" 2>/dev/null; then
        runuser -u postgres -- pg_ctl -D "$PGDATA" -m fast -w -t 30 stop ||
            { printf 'ERROR: PostgreSQL shutdown failed.\n' >&2; status=1; }
        wait "$pg_pid" || printf 'PostgreSQL process stopped.\n' >&2
    fi
    exit "$status"
}
trap 'shutdown 0' TERM INT
trap 'shutdown $?' EXIT
runuser -u postgres -- postgres -D "$PGDATA" \
    -c "config_file=$POSTGRES_PACKAGE/config/postgresql.conf" \
    -c "hba_file=$POSTGRES_PACKAGE/config/pg_hba.conf" \
    -c "ident_file=$POSTGRES_PACKAGE/config/pg_ident.conf" &
pg_pid=$!
/usr/sbin/sshd -D -e -f "$POSTGRES_PACKAGE/config/sshd_config" &
ssh_pid=$!
printf '%s\n' "$pg_pid" > /run/rffusion-postgres/fresh-bootstrap
set +e
wait -n "$pg_pid" "$ssh_pid"
status=$?
set -e
printf 'ERROR: Essential service exited, status=%s.\n' "$status" >&2
shutdown 1
