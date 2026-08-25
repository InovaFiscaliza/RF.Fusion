#!/usr/bin/env bash
# Read-only runtime report consumed by the WebFusion server dashboard.

set -u

checked_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
database_pid=$(pgrep -x mariadbd | head -n 1 || true)

if [[ -z "$database_pid" ]]; then
    database_pid=$(pgrep -x mysqld | head -n 1 || true)
fi

if [[ -n "$database_pid" ]]; then
    process_status="healthy"
    process_detail="Processo MariaDB em execução"
else
    process_status="unavailable"
    process_detail="Processo MariaDB não encontrado"
fi

if mariadb --protocol=socket --connect-timeout=2 -uroot -Nse "SELECT 1" >/dev/null 2>&1; then
    query_status="healthy"
    query_detail="Consulta local respondida"
else
    query_status="unavailable"
    query_detail="Socket ou consulta local indisponível"
fi

if [[ "$process_status" == "healthy" && "$query_status" == "healthy" ]]; then
    overall_status="healthy"
elif [[ "$process_status" == "unavailable" && "$query_status" == "unavailable" ]]; then
    overall_status="unavailable"
else
    overall_status="degraded"
fi

printf '{"component":"mariadb","checked_at":"%s","status":"%s","checks":[' \
    "$checked_at" "$overall_status"
printf '{"name":"Processo MariaDB","status":"%s","detail":"%s"},' \
    "$process_status" "$process_detail"
printf '{"name":"Resposta local","status":"%s","detail":"%s"}]}' \
    "$query_status" "$query_detail"
printf '\n'
