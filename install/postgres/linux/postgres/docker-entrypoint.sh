#!/usr/bin/env bash
set -Eeuo pipefail

bash /RFFusion/install/common/container-identity.sh

# Host keys persist with the database so SSH identity survives container replacement.
install -d -m 700 /var/lib/rffusion-ssh/etc/ssh
ssh-keygen -A -f /var/lib/rffusion-ssh
for key in /var/lib/rffusion-ssh/etc/ssh/ssh_host_*_key; do
    ln -sf "$key" "/etc/ssh/${key##*/}"
    ln -sf "${key}.pub" "/etc/ssh/${key##*/}.pub"
done
# Preserve password changes across restarts of this container.
if [[ ! -e /etc/ssh/rffusion-password-initialized ]]; then
    printf 'root:%s\n' "${SSH_PASSWORD:?SSH_PASSWORD is required}" | chpasswd
    touch /etc/ssh/rffusion-password-initialized
fi
/usr/sbin/sshd -o Port=22 -o PermitRootLogin=yes \
    -o PasswordAuthentication=yes

exec /usr/local/bin/docker-entrypoint.sh "$@"
