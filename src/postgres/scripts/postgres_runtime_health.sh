#!/usr/bin/env bash
# Report local PostgreSQL readiness without disclosing credentials.
set -u
checked_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
if ps -eo comm | grep -qx postgres; then
    process_status=healthy
    process_detail="Processo PostgreSQL em execução"
else
    process_status=unavailable
    process_detail="Processo PostgreSQL não encontrado"
fi
if su postgres -s /bin/sh -c 'PGCONNECT_TIMEOUT=2 psql -X -U postgres -d postgres -Atqc "SELECT 1"' >/dev/null 2>&1; then
    query_status=healthy
    query_detail="Consulta local respondida"
else
    query_status=unavailable
    query_detail="Socket ou consulta local indisponível"
fi
if [[ "$process_status" == healthy && "$query_status" == healthy ]]; then
    overall_status=healthy
elif [[ "$process_status" == unavailable && "$query_status" == unavailable ]]; then
    overall_status=unavailable
else
    overall_status=degraded
fi
printf '{"component":"postgres","checked_at":"%s","status":"%s","checks":[' "$checked_at" "$overall_status"
printf '{"name":"Processo PostgreSQL","status":"%s","detail":"%s"},' "$process_status" "$process_detail"
printf '{"name":"Resposta local","status":"%s","detail":"%s"}]}\n' "$query_status" "$query_detail"
