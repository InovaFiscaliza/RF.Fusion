#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] init MariaDB + SSH container ==="

# -------------------------------------------------------------------
# SSH
# -------------------------------------------------------------------
echo "[entrypoint] Configuring SSH..."
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# -------------------------------------------------------------------
# MariaDB
# -------------------------------------------------------------------
echo "[entrypoint] Configuring MariaDB..."
mkdir -p /var/run/mysqld
chown -R mysql:mysql /var/run/mysqld
chmod 775 /var/run/mysqld

if [ ! -d /var/lib/mysql/mysql ]; then
    echo "[entrypoint] Initializing database..."
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql
fi

echo "[entrypoint] Starting temporary MariaDB..."
mariadbd --skip-networking --datadir=/var/lib/mysql &
pid_tmp=$!

# Esperar DB temporário
for i in {1..30}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting for temporary MariaDB..."
    sleep 1
done

echo "[entrypoint] Running initialization SQL..."
mariadb --protocol=socket <<-EOSQL
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD:-changeme}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;
EOSQL

echo "[entrypoint] Shutting down temporary MariaDB..."
mysqladmin --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD:-changeme}" shutdown || true

# Aguarda matar tudo
sleep 2
while pgrep -x mariadbd >/dev/null; do
    echo "[entrypoint] Waiting mariadbd to exit..."
    sleep 1
done

# Agora inicia o MariaDB FINAL
echo "[entrypoint] Starting MariaDB..."
exec mariadbd --datadir=/var/lib/mysql --bind-address=0.0.0.0
