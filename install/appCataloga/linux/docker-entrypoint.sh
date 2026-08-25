#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] init ==="

# 1) Runtime dir do SSHD (precisa existir a cada start)
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

# 2) Gera host keys se não existirem (primeiro start de uma imagem nova)
if ! ls /etc/ssh/ssh_host_*key >/dev/null 2>&1; then
  echo "[entrypoint] Generating SSH host keys..."
  ssh-keygen -A
fi

# 3) Pam loginuid: evita encerramento em ambientes sem systemd
if [ -f /etc/pam.d/sshd ]; then
  sed -i 's/^\s*session\s\+required\s\+pam_loginuid\.so/session optional pam_loginuid.so/' /etc/pam.d/sshd || true
fi

# 4) Configura root + senha
echo "root:${SSH_PASSWORD:-changeme}" | chpasswd
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# A dedicated WebFusion key can run only the read-only runtime health script.
# The key is optional so existing deployments remain unchanged until configured.
if [ -n "${RUNTIME_HEALTH_SSH_PUBLIC_KEY:-}" ]; then
  install -d -m 700 /root/.ssh
  health_key="restrict,command=\"/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/appCataloga_runtime_health.sh\" ${RUNTIME_HEALTH_SSH_PUBLIC_KEY}"
  touch /root/.ssh/authorized_keys
  grep -Fqx "${health_key}" /root/.ssh/authorized_keys || printf '%s\n' "${health_key}" >> /root/.ssh/authorized_keys
  chmod 600 /root/.ssh/authorized_keys
fi

# 5) Log útil e validação
ssh -V || true
/usr/sbin/sshd -t || { echo "[entrypoint] sshd -t FAILED"; exit 1; }

echo "=== [entrypoint] handoff ==="
exec "$@"
