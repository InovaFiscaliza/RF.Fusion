#!/usr/bin/env bash
# Read-only runtime report consumed by the WebFusion server dashboard.

set -u

workers=(
  "Gateway|appCataloga.py"
  "Verificação de conectividade|appCataloga_host_check.py"
  "Descoberta de arquivos|appCataloga_discovery.py"
  "Gerenciamento de backlog|appCataloga_backlog_management.py"
  "Backup de arquivos|appCataloga_file_bkp.py"
  "Processamento analítico|appCataloga_file_bin_process_appAnalise.py"
  "Manutenção de hosts|appCataloga_host_maintenance.py"
  "Resumo incremental|appCataloga_summary_database.py"
  "Limpeza de artefatos|appCataloga_garbage_collector.py"
)

checked_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
healthy_count=0
first_check=1

printf '{"component":"appcataloga","checked_at":"%s","checks":[' "$checked_at"

for worker in "${workers[@]}"; do
    IFS='|' read -r label process_pattern <<< "$worker"
    pids=$(pgrep -f -- "$process_pattern" | paste -sd ',' - || true)

    if [[ -n "$pids" ]]; then
        IFS=',' read -r -a pid_list <<< "$pids"
        worker_count=${#pid_list[@]}
        check_status="healthy"
        check_detail="${worker_count} worker"
        if [[ "$worker_count" -ne 1 ]]; then
            check_detail+="s"
        fi
        check_detail+=" em execução"
        healthy_count=$((healthy_count + 1))
    else
        worker_count=0
        check_status="unavailable"
        check_detail="Processo não encontrado"
    fi

    if [[ "$first_check" -eq 0 ]]; then
        printf ','
    fi
    first_check=0

    printf '{"name":"%s","script":"%s","worker_count":%s,"status":"%s","detail":"%s"}' \
        "$label" "$process_pattern" "$worker_count" "$check_status" "$check_detail"
done

if [[ "$healthy_count" -eq "${#workers[@]}" ]]; then
    overall_status="healthy"
elif [[ "$healthy_count" -eq 0 ]]; then
    overall_status="unavailable"
else
    overall_status="degraded"
fi

printf '],"status":"%s"}\n' "$overall_status"
