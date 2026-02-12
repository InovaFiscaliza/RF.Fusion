#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] RF.Fusion Web container starting ==="

# -------------------------------------------------
# SSH
# -------------------------------------------------
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "[entrypoint] Generating SSH host keys..."
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# -------------------------------------------------
# Ambiente RF.Fusion
# -------------------------------------------------
export PATH="/opt/rffusion/bin:${PATH}"

# -------------------------------------------------
# Garantir permissões no volume
# -------------------------------------------------
if [ -d /RF.Fusion ]; then
    chmod -R 755 /RF.Fusion || true
fi

# -------------------------------------------------
# Log diagnóstico
# -------------------------------------------------
echo "[entrypoint] PHP: $(php -v | head -n1)"
echo "[entrypoint] Python: $(python3 --version)"
echo "[entrypoint] RF.Fusion mount:"
ls -la /RF.Fusion || true
ip a || true

# -------------------------------------------------
# Subir stack base (nginx + php-fpm via supervisord)
# -------------------------------------------------
echo "[entrypoint] Starting nginx + php-fpm..."
/entrypoint supervisord &

# -------------------------------------------------
# Subir SSH (processo âncora)
# -------------------------------------------------
echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e
