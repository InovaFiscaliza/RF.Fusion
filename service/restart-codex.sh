#!/usr/bin/env bash
set -uo pipefail

# Reinicia o processo Codex app-server do VS Code Remote.
# Execute no mesmo ambiente em que o Codex está rodando.

mapfile -t pids < <(ps -eo pid=,args= | awk '$0 ~ /\/bin\/linux-x86_64\/codex/ && $0 ~ /app-server/ && $0 !~ /awk/ {print $1}')

if ((${#pids[@]} == 0)); then
  echo 'Nenhum processo Codex app-server encontrado.'
  echo 'Abra o painel do Codex no VS Code para iniciá-lo.'
  exit 0
fi

printf 'Processos Codex encontrados: %s\n' "${pids[*]}"
for pid in "${pids[@]}"; do
  echo "Enviando SIGTERM para PID $pid..."
  kill -TERM "$pid" 2>/dev/null || true
done

for _ in {1..10}; do
  remaining=()
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then remaining+=("$pid"); fi
  done
  ((${#remaining[@]} == 0)) && break
  sleep 1
done

for pid in "${remaining[@]}"; do
  echo "PID $pid não encerrou; enviando SIGKILL..."
  kill -KILL "$pid" 2>/dev/null || true
done

echo 'Finalizado. O VS Code pode iniciar uma nova instância automaticamente.'
echo 'Se não iniciar, abra o painel Codex ou execute Developer: Reload Window no VS Code.'
