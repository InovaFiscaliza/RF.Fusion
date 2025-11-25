#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] init MariaDB + SSH container ==="

# -------------------------------------------------------------------
# 1) SSH
# -------------------------------------------------------------------
echo "[entrypoint] Configuring SSH..."
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "[entrypoint] Generating SSH host keys..."
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# -------------------------------------------------------------------
# 2) MariaDB
# -------------------------------------------------------------------
echo "[entrypoint] Configuring MariaDB..."

mkdir -p /var/run/mysqld
chmod 775 /var/run/mysqld

# ⚠ IMPORTANTE:
# NÃO usar chown em /var/lib/mysql → volume rootless resolve permissões automaticamente
# e chown quebra o container em qualquer FS que impeça alteracao de UID/GID

if [ ! -d /var/lib/mysql/mysql ]; then
    echo "[entrypoint] First run: initializing MariaDB data directory..."
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql > /dev/null
fi

echo "[entrypoint] Starting temporary MariaDB..."
mysqld_safe --skip-networking --datadir=/var/lib/mysql &
pid="$!"

# Aguardar MariaDB responder
for i in {30..0}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting for MariaDB..."
    sleep 1
done

if [[ "$i" == "0" ]]; then
    echo "[entrypoint] MariaDB did not start during initialization"
    exit 1
fi

echo "[entrypoint] Running initialization SQL..."
mariadb --protocol=socket <<-EOSQL
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD:-changeme}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;
EOSQL

echo "[entrypoint] Shutdown temporary MariaDB..."
mysqladmin --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD:-changeme}" shutdown

# -------------------------------------------------------------------
# 3) Running services
# -------------------------------------------------------------------
echo "[entrypoint] Starting MariaDB in normal mode..."
mysqld_safe --datadir=/var/lib/mysql --bind-address=0.0.0.0 &

echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e
