# Métricas do Servidor RF.Fusion

O WebFusion oferece um endpoint de leitura para o template Zabbix
`rffusion_server`:

```text
http://172.16.18.11:9082/rffusion/server/zabbix_metrics
```

Ele retorna um objeto JSON plano, sem HTML e sem carregar listas de hosts ou
histórico de arquivos. A consulta usa os dados materializados do painel
`/server`, as sondagens de runtime em cache e os contadores já persistidos de
uso do WebFusion.

Exemplo resumido:

```json
{
  "status": "ok",
  "reference_month": "2026-08",
  "host_total": 410,
  "host_online": 380,
  "host_offline": 30,
  "memory_used_percent": 54.2,
  "reposfi_used_percent": 71.4,
  "appanalise_online": 1,
  "discovered_files_total": 123456,
  "backup_pending_files_total": 456,
  "processing_error_files_total": 12,
  "webfusion_nginx_download_count_total": 5607
}
```

## Item mestre

No template `rffusion_server`, crie o item:

| Campo | Valor |
| --- | --- |
| Nome | RF.Fusion: métricas do servidor (bruto) |
| Tipo | HTTP agent |
| Chave | `rffusion.server.metrics.raw` |
| Tipo de informação | Texto |
| URL | `http://172.16.18.11:9082/rffusion/server/zabbix_metrics` |
| Método | GET |
| Código de status obrigatório | `200` |
| Intervalo de atualização | `2m` |

O intervalo de dois minutos acompanha o cache de runtime do painel e evita
sondagens desnecessárias ao `appAnalise`.

## Itens dependentes

Crie itens do tipo `Dependent item`, usando o item mestre acima. Em cada item,
adicione uma etapa de pré-processamento `JSONPath`.

| Nome | Chave | Tipo de informação | JSONPath |
| --- | --- | --- | --- |
| Hosts totais | `rffusion.host.total` | Numérico sem sinal | `$.host_total` |
| Hosts offline | `rffusion.host.offline` | Numérico sem sinal | `$.host_offline` |
| Memória utilizada | `rffusion.memory.used.percent` | Numérico (float) | `$.memory_used_percent` |
| Repositório utilizado | `rffusion.reposfi.used.percent` | Numérico (float) | `$.reposfi_used_percent` |
| appAnalise disponível | `rffusion.appanalise.online` | Numérico sem sinal | `$.appanalise_online` |
| Arquivos aguardando backup | `rffusion.backup.pending.files` | Numérico sem sinal | `$.backup_pending_files_total` |
| Arquivos com erro de processamento | `rffusion.processing.error.files` | Numérico sem sinal | `$.processing_error_files_total` |
| Downloads NGINX | `rffusion.webfusion.downloads.total` | Numérico sem sinal | `$.webfusion_nginx_download_count_total` |

Os demais cartões de pipeline do painel `/server` seguem a mesma convenção:
o nome da chave JSON é a versão em minúsculas de seu indicador, por exemplo
`BACKUP_DONE_FILES_TOTAL` torna-se `backup_done_files_total`.

## Observações

- A chamada de monitoramento não incrementa `page_view_count`.
- `appanalise_online` e `reposfi_mounted` retornam `1` para disponível e `0`
  para indisponível.
- Quando não houver latência do `appAnalise`, `appanalise_latency_ms` retorna
  `0`; use `appanalise_online` para disparar indisponibilidade.
