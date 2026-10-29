#!/usr/bin/env bash
set -Eeuo pipefail

# Separate host keys avoid sharing the PostgreSQL container's SSH identity.
install -d -m 700 /var/lib/rffusion-pgadmin-ssh/etc/ssh
ssh-keygen -A -f /var/lib/rffusion-pgadmin-ssh
for key in /var/lib/rffusion-pgadmin-ssh/etc/ssh/ssh_host_*_key; do
    ln -sf "$key" "/etc/ssh/${key##*/}"
    ln -sf "${key}.pub" "/etc/ssh/${key##*/}.pub"
done
if [[ ! -e /etc/ssh/rffusion-password-initialized ]]; then
    printf 'root:%s\n' "${PGADMIN_SSH_PASSWORD:?PGADMIN_SSH_PASSWORD is required}" | chpasswd
    touch /etc/ssh/rffusion-password-initialized
fi
/usr/sbin/sshd -o Port=2222 -o PermitRootLogin=yes -o PasswordAuthentication=yes

# The upstream entrypoint initializes pgAdmin and drops privileges to its user.
exec /entrypoint.sh "$@"
