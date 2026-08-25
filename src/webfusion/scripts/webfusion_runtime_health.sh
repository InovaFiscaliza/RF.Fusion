#!/usr/bin/env bash
# Read-only runtime report for the local WebFusion container.

set -u

checked_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
nginx_pids=$(pgrep -x nginx | paste -sd ',' - || true)
waitress_pids=$(pgrep -f -- "python3 /RF.Fusion/src/webfusion/app.py" | paste -sd ',' - || true)

if [[ -n "$nginx_pids" ]]; then
    nginx_status="healthy"
    nginx_detail="Nginx em execução"
else
    nginx_status="unavailable"
    nginx_detail="Processo Nginx não encontrado"
fi

if [[ -n "$waitress_pids" ]]; then
    waitress_status="healthy"
    waitress_detail="Waitress em execução"
else
    waitress_status="unavailable"
    waitress_detail="Processo Waitress não encontrado"
fi

if curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8000/health >/dev/null 2>&1; then
    health_status="healthy"
    health_detail="Endpoint interno respondeu"
else
    health_status="unavailable"
    health_detail="Endpoint interno não respondeu"
fi

if [[ "$nginx_status" == "healthy" && "$waitress_status" == "healthy" && "$health_status" == "healthy" ]]; then
    overall_status="healthy"
elif [[ "$nginx_status" == "unavailable" && "$waitress_status" == "unavailable" ]]; then
    overall_status="unavailable"
else
    overall_status="degraded"
fi

printf '{"component":"webfusion","checked_at":"%s","status":"%s","checks":[' \
    "$checked_at" "$overall_status"
printf '{"name":"Nginx","status":"%s","detail":"%s"},' \
    "$nginx_status" "$nginx_detail"
printf '{"name":"Waitress","status":"%s","detail":"%s"},' \
    "$waitress_status" "$waitress_detail"
printf '{"name":"Endpoint interno","status":"%s","detail":"%s"}]}' \
    "$health_status" "$health_detail"
printf '\n'
