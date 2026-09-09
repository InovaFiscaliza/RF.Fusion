#!/usr/bin/env bash
set -Eeuo pipefail

# Default password values
DEFAULT_ROOT_SSH_PASSWORD="changeme"
DEFAULT_APP_SSH_PASSWORD="changeme"
DEFAULT_MARIADB_ROOT_PASSWORD="changeme"

echo "=== [entrypoint] init MariaDB + SSH container ==="

# Function to prompt for password if not set
prompt_for_password() {
    local var_name=$1
    local prompt_text=$2
    local default_value=$3
    
    if [ -z "${!var_name:-}" ]; then
        read -p "${prompt_text}" -s input_password
        echo
        if [ -n "$input_password" ]; then
            export $var_name="$input_password"
        elif [ -n "$default_value" ]; then
            export $var_name="$default_value"
        else
            echo "Error: Password is required"
            exit 1
        fi
    fi
}

# -------------------------------------------------------------------
# 1) SSH
# -------------------------------------------------------------------
echo "[entrypoint] Configuring SSH..."
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

# Prompt for SSH password if not provided
prompt_for_password "SSH_PASSWORD" "Enter root SSH password (default: ${DEFAULT_ROOT_SSH_PASSWORD}): " "${DEFAULT_ROOT_SSH_PASSWORD}"

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "[entrypoint] Generating SSH host keys..."
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# A dedicated WebFusion key can run only the read-only runtime health script.
# The key is optional so existing deployments remain unchanged until configured.
if [ -n "${RUNTIME_HEALTH_SSH_PUBLIC_KEY:-}" ]; then
    install -d -m 700 /root/.ssh
    health_key="restrict,command=\"/RFFusion/src/mariadb/scripts/mariadb_runtime_health.sh\" ${RUNTIME_HEALTH_SSH_PUBLIC_KEY}"
    touch /root/.ssh/authorized_keys
    grep -Fqx "${health_key}" /root/.ssh/authorized_keys || printf '%s\n' "${health_key}" >> /root/.ssh/authorized_keys
    chmod 600 /root/.ssh/authorized_keys
fi

default_ssh_app_public_key='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICOV2QzbKI1es3i5dc93j9zNtyfQAVPdrtQCpFjrdcWF rffusion-service'

# Prompt for SSH app user password if not provided
prompt_for_password "SSH_APP_PASSWORD" "Enter SSH app user password (default: ${DEFAULT_APP_SSH_PASSWORD}): " "${DEFAULT_APP_SSH_PASSWORD}"

ssh_user="${SSH_APP_USER:-rffusion}"
ssh_user_password="${SSH_APP_PASSWORD:-changeme}"
ssh_app_public_key="${SSH_APP_PUBLIC_KEY:-${default_ssh_app_public_key}}"
ssh_user_home="/home/${ssh_user}"

if ! id -u "${ssh_user}" >/dev/null 2>&1; then
    echo "[entrypoint] Creating SSH user ${ssh_user}..."
    useradd -m -s /bin/bash "${ssh_user}"
fi

echo "${ssh_user}:${ssh_user_password}" | chpasswd
mkdir -p "${ssh_user_home}/.ssh"
chmod 700 "${ssh_user_home}/.ssh"

if [ -n "${ssh_app_public_key}" ]; then
    printf '%s\n' "${ssh_app_public_key}" > "${ssh_user_home}/.ssh/authorized_keys"
    chmod 600 "${ssh_user_home}/.ssh/authorized_keys"
fi

chown -R "${ssh_user}:${ssh_user}" "${ssh_user_home}/.ssh"

# -------------------------------------------------------------------
# 2) MariaDB
# -------------------------------------------------------------------
echo "[entrypoint] Configuring MariaDB..."
mkdir -p /var/run/mysqld
chown -R mysql:mysql /var/run/mysqld
chmod 775 /var/run/mysqld

# Prompt for MariaDB root password if not provided
prompt_for_password "MARIADB_ROOT_PASSWORD" "Enter MariaDB root password (default: ${DEFAULT_MARIADB_ROOT_PASSWORD}): " "${DEFAULT_MARIADB_ROOT_PASSWORD}"

if [ ! -d /var/lib/mysql/mysql ]; then
    echo "[entrypoint] Initializing database..."
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql > /dev/null
fi

echo "[entrypoint] Starting temporary MariaDB..."
mysqld_safe --skip-networking --datadir=/var/lib/mysql &
pid="$!"

for i in {30..0}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting for MariaDB..."
    sleep 1
done

if [ "$i" = 0 ]; then
    echo >&2 "[entrypoint] MariaDB init process failed."
    exit 1
fi

echo "[entrypoint] Running initialization SQL..."
mariadb --protocol=socket <<-EOSQL
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD:-changeme}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

    CREATE DATABASE IF NOT EXISTS appdb;
    CREATE USER IF NOT EXISTS 'appdb'@'%' IDENTIFIED BY 'changeme';
    GRANT ALL PRIVILEGES ON appdb.* TO 'appdb'@'%';
    FLUSH PRIVILEGES;
EOSQL

echo "[entrypoint] Shutting down temporary MariaDB..."
mysqladmin --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD:-changeme}" shutdown

# -------------------------------------------------------------------
# 3) Subir serviços finais
# -------------------------------------------------------------------
echo "[entrypoint] Starting MariaDB..."
mysqld_safe --datadir=/var/lib/mysql --bind-address=0.0.0.0 &

echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e
