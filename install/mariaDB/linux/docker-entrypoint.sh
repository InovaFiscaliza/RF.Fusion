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

# Diretórios essenciais
mkdir -p /var/lib/mysql /var/run/mysqld
chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
chmod 775 /var/run/mysqld

# Inicialização ONLY first run
if [ ! -d /var/lib/mysql/mysql ]; then
    echo "[entrypoint] First run: initializing MariaDB data directory..."
    mariadb-install-db \
        --user=mysql \
        --basedir=/usr \
        --datadir=/var/lib/mysql \
        > /dev/null
fi

echo "[entrypoint] Starting temporary MariaDB..."
mysqld_safe --skip-networking --datadir=/var/lib/mysql &
pid="$!"

# Esperar subida
for i in {30..0}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting for MariaDB..."
    sleep 1
done

if [[ "$i" = "0" ]]; then
    echo "[entrypoint] MariaDB did not start during initialization"
    echo "----- MariaDB LOG -----"
    cat /var/lib/mysql/*.err 2>/dev/null || echo "(no log file yet)"
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
mysqld_safe \
    --datadir=/var/lib/mysql \
    --basedir=/usr \
    --bind-address=0.0.0.0 &

echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e
