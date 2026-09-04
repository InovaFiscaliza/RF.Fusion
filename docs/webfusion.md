# Referência de Rotas e Acesso do WebFusion

Este documento registra somente rotas e comportamentos implementados no código atual do WebFusion.

## Identidade e Autorização

O proxy fornece identidade pelos cabeçalhos `X-User-*`. O método `AuthService.request_identity()` usa `X-User-Email` como identificador, valida
o formato e normaliza o e-mail com `casefold()`. Também recebe `X-User-Name`, `X-User-Job-Title`, `X-User-Department` e `X-User-Location`.

O perfil observado é mantido em `WEBFUSION.USERS`. A role não vem dos cabeçalhos: `AuthService.load_access_role()` consulta, pelo e-mail normalizado:

```sql
SELECT 'admin' AS NA_ROLE FROM ADMINS
WHERE NA_USER_EMAIL = %s AND IS_ACTIVE = 1 LIMIT 1;

SELECT 'developer' AS NA_ROLE FROM DEVELOPERS
WHERE NA_USER_EMAIL = %s AND IS_ACTIVE = 1 LIMIT 1;
```

`admin` tem prioridade quando as duas associações estão ativas. Sem associação ativa, a role é `None`. A aplicação retorna HTTP 403 para os blueprints `maintenance`, `task`, `users`, `zabbix_configuration` e `alarms` quando a role é ausente.

`/users/` é uma página HTML administrativa.

## Rotas Gerais

| Método | Caminho | Parâmetros | Retorno |
| --- | --- | --- | --- |
| GET | `/` | nenhum | HTML |
| GET | `/api/map/stations` | `start_date`, `end_date` opcionais | JSON com `points` |
| GET | `/api/map/stations/<int:site_id>` | `site_id`; `start_date`, `end_date` opcionais | JSON |
| GET | `/health` | nenhum | JSON `{"status": "ok"}` |
| GET | `/debug/headers` | nenhum | JSON dos cabeçalhos `X-User-*` |

Exemplo: `https://fiscalizacao.anatel.gov.br/rffusion/api/map/stations?start_date=2026-09-01&end_date=2026-09-30`.

## Espectro

Rotas do blueprint `spectrum`, sem guarda de role local:

| Método | Caminho | Parâmetros | Retorno |
| --- | --- | --- | --- |
| GET | `/spectrum` | `equipment_id`, `state_id`, `district_id`, `site_id`, `start_date`, `end_date`, `freq_start`, `freq_end`, `description`, `sort_by`, `sort_order`, `page` | HTML |
| GET | `/api/spectrum/filters` | filtros anteriores, exceto ordenação/página; `bootstrap=1` opcional | JSON |
| GET | `/api/spectrum/localities` | `equipment_id`; `state_id`, `district_id`, `site_id`, `start_date`, `end_date`, `freq_start`, `freq_end`, `description` | JSON com `rows` |
| GET | `/spectrum/download/<int:spectrum_id>` | `spectrum_id` | download ou 404 |
| GET | `/spectrum/download-file/<int:file_id>` | `file_id` | download ou 404 |
| GET | `/api/spectrum/file/<int:file_id>/spectra` | `file_id` e filtros de espectro | JSON com `rows` |

`freq_start` e `freq_end` são convertidos em valores numéricos e validados.

## Hosts e Servidor

| Método | Caminho | Parâmetros | Retorno |
| --- | --- | --- | --- |
| GET | `/host` | `host_id`, `search`, `online_only=1` opcionais | HTML |
| GET | `/api/host/<int:host_id>/zabbix_metrics` | `host_id` | JSON |
| GET | `/api/host/<int:host_id>/processing-errors` | `host_id` | JSON |
| GET | `/api/host/<int:host_id>/backup-errors` | `host_id` | JSON |
| GET | `/api/host/<int:host_id>/locations` | `host_id` | JSON |
| GET | `/api/host/<int:host_id>/activity` | `host_id` | JSON |
| GET | `/api/host/<int:host_id>/activity/<string:source>/<int:task_id>` | `host_id`, `source`, `task_id`; `file_path`, `file_name` opcionais | JSON |
| GET | `/api/host/<int:host_id>/processed-spectrum-metadata` | `host_id`; `file_name` obrigatório | JSON |
| POST | `/api/host/<int:host_id>/connectivity-test` | `host_id`; credencial Basic | JSON, normalmente 202 |
| GET | `/api/host/<int:host_id>/connectivity-test/<int:task_id>` | `host_id`, `task_id`; credencial Basic | JSON |
| GET | `/server` | `search`, `online_only=1` opcionais | HTML |
| GET | `/server/zabbix_metrics` | nenhum | JSON |
| GET | `/api/server/processing-errors` | nenhum | JSON |
| GET | `/api/server/backup-errors` | nenhum | JSON |
| GET | `/api/server/summary-metrics` | nenhum | JSON |
| GET | `/api/server/usage-metrics` | nenhum | JSON |
| GET | `/server/runtime-health` | nenhum | JSON |
| POST | `/api/server/usage-metrics/download-action` | nenhum | JSON, normalmente 202 |
| GET | `/api/server/hosts` | `search`, `online_only=1` opcionais | JSON com `rows` e `count` |

## Rotas Restritas

As rotas abaixo exigem role ativa em `WEBFUSION.ADMINS` ou
`WEBFUSION.DEVELOPERS`.

| Método | Caminho | Parâmetros | Retorno |
| --- | --- | --- | --- |
| GET, POST | `/maintenance/` | GET: `host_task_load`, `file_task_load`, `history_load` e filtros; POST: `maintenance_form`, `action` e campos da ação | HTML ou redirecionamento |
| GET | `/maintenance/file-task-hosts` | nenhum | JSON com `hosts` |
| GET, POST | `/task/` | GET: `host_id`, `task_type`, `mode`, `action`, `execution_type`, `online_only`, `start_date`, `end_date`, `last_n_files`, `extension`, `file_path`, `file_name`, `max_total_gb`, `sort_order`, `collective_host_ids`, `collective_host_search`; POST: dados de tarefa e filtros | HTML ou redirecionamento |
| GET | `/task/api/host/<int:host_id>/backup-defaults` | `host_id` | JSON |
| GET | `/task/api/hosts/backup-defaults` | um ou mais `host_id` | JSON |
| GET | `/task/list` | `queued_count`, `skipped_count`, `created_host_id` opcionais | HTML |
| GET | `/users/` | `email`, `job_title`, `department`, `role`, `error`, `notice` opcionais | HTML |
| POST | `/users/` | `user_name`, `user_email`, `job_title`, `department`, `location`, `is_admin`, `is_developer` | redirecionamento |
| POST | `/users/privileges` | `user_email`, `is_admin`, `is_developer` | redirecionamento |
| POST | `/users/delete` | `user_email` | redirecionamento |
| GET | `/host-configuration/` | `target_kind`, `target_id`, `error`, `notice` opcionais | HTML |
| POST | `/host-configuration/macro` | `target_kind`, `target_id`, `macro_name`, `action`, `value` | redirecionamento |
| GET | `/alarms/` | nenhum | HTML |

## Fontes de Implementação

- `src/webfusion/app.py`: rotas gerais e mapa.
- `src/webfusion/auth/service.py` e `src/webfusion/auth/db_users.py`: identidade e roles.
- `src/webfusion/modules/*/routes.py`: rotas dos blueprints.