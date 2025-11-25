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

# /var/run/mysqld precisa de permissão – permitido
mkdir -p /var/run/mysqld
chown mysql:mysql /var/run/mysqld || true

DATADIR="/var/lib/mysql"

# IMPORTANTE:
# nunca faça chown/chmod em /var/lib/mysql (bind mount)
# apenas inicialize se ainda não existir "mysql/" dentro dele
if [ ! -d "$DATADIR/mysql" ]; then
    echo "[entrypoint] First run: initializing MariaDB data directory..."
    mariadb-install-db --user=mysql --datadir="$DATADIR" --skip-test-db
else
    echo "[entrypoint] Existing MariaDB directory detected, skipping initialization."
fi

# -------------------------------------------------------------------
# 3) Criar usuário root remoto (após subir MariaDB)
# -------------------------------------------------------------------
echo "[entrypoint] Starting MariaDB (bootstrap)..."
mysqld --datadir="$DATADIR" --skip-networking &
MYSQLPID=$!

# Esperar MariaDB subir
for i in {30..0}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting MariaDB to become ready..."
    sleep 1
done

if [ "$i" = 0 ]; then
    echo "[entrypoint] ERROR: MariaDB did not start."
    exit 1
fi

echo "[entrypoint] Running initialization SQL..."
mariadb --protocol=socket <<-EOSQL
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD:-changeme}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;
EOSQL

echo "[entrypoint] Stopping bootstrap MariaDB..."
mysqladmin --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD:-changeme}" shutdown

# -------------------------------------------------------------------
# 4) Subir serviços finais
# -------------------------------------------------------------------
echo "[entrypoint] Starting MariaDB (production mode)..."
mysqld --datadir="$DATADIR" --bind-address=0.0.0.0 &

echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e
