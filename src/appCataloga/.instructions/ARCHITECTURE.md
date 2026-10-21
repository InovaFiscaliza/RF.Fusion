# Arquitetura do appCataloga

## 1. Escopo e autoridade

Este documento define os contratos do runtime em
`src/appCataloga/server_volume/usr/local/bin/appCataloga/`. Os caminhos de
módulos abaixo são relativos a esse diretório, salvo indicação contrária.

Contratos arquiteturais prevalecem sobre regras de manutenção e código
existente. Não implementar um caso sem cobertura arquitetural: atualizar o
contrato primeiro.

Regras gerais estão em [INSTRUCTIONS.md](../../../docs/agents/INSTRUCTIONS.md),
manutenção local em [INSTRUCTIONS.md](INSTRUCTIONS.md) e execução em
[ENVIRONMENT.md](ENVIRONMENT.md). Contratos públicos do summary estão em
[SUMMARY.md](../../../docs/architecture/SUMMARY.md), e métricas em
[METRICAS_OPERACIONAIS.md](../../../docs/architecture/METRICAS_OPERACIONAIS.md).
Identidade, avatar e upload pertencem à
[arquitetura do WebFusion](../../webfusion/.instructions/ARCHITECTURE.md).

## 2. Camadas e responsabilidade

O appCataloga descobre arquivos nas estações via SSH/SFTP, organiza filas,
transfere e processa artefatos, persiste espectros e mantém dados derivados.

| Camada | Responsabilidade | Dependências permitidas |
|---|---|---|
| Entrypoints `appCataloga*.py` | Loop, sinais, orquestração e ciclo de vida das filas | Domínio, runtime, shared e handlers de banco |
| Domínio `host_handler/`, `appAnalise/`, `gc_handler/`, `summary_handler/` | Regras e trabalho de domínio | Infraestrutura, shared e métodos públicos de banco |
| Runtime `server_handler/` | Sinais, processos, pool, transporte e temporização | Infraestrutura compartilhada, sem assumir resolução de tasks |
| Shared `shared/` | Utilitários, erros, logs e objetos de valor | Nunca entrypoints, domínio ou banco |
| Banco `db/` | SQL, conexões e transações | Shared; nunca domínio ou entrypoints |

Uma camada inferior não importa uma superior. Entrypoints não contêm SQL;
handlers de banco não contêm SSH, operações de arquivos, geocoding ou decisões
de negócio. Domínio não controla o loop do chamador.

### 2.1 Propriedade dos dados

| Banco | Constante em `config.py` | Handler | Papel |
|---|---|---|---|
| BPDATA | `BKP_DATABASE_NAME` | `dbHandlerBKP` | Hosts, filas e histórico |
| RFDATA | `RFM_DATABASE_NAME` | `dbHandlerRFM` | Espectros e dimensões analíticas |
| RFFUSION_SUMMARY | `SUMMARY_DATABASE_NAME` | `dbHandlerSummary` | Modelos de leitura derivados |

O entrypoint é dono das transições de `HOST_TASK`, `FILE_TASK` e
`FILE_TASK_HISTORY`: claim, DONE, ERROR, FROZEN, exclusão e promoção. Domínio
não finaliza essas linhas em nome do worker. Escritas analíticas em RFDATA
podem ser orquestradas pelo domínio quando integram o processamento do artefato.

`HOST_TASK_UPDATE_STATISTICS_TYPE` continua atendido pelo host-check, com a
prioridade existente em relação às verificações de conectividade. Essas tasks
legadas solicitam refresh de escopo do summary; não reintroduzir contadores
operacionais em `BPDATA.HOST`.

O fluxo convencional de sites fixos usa `k.SITE_TYPE_FIXED` em
`DIM_SPECTRUM_SITE.FK_TYPE`. `backfillFixedSiteType.sql` preenche apenas sites
legados sem classificação. Não aplicar o insert de sites fixos a dados móveis.

### 2.2 Acesso e transações

- Somente handlers de banco importam `mysql.connector` e abrem conexões
  MariaDB/MySQL com `mysql.connector.connect(...)`. Não introduzir outro driver.
- Chamadores usam métodos públicos de handlers concretamente tipados.
  `_select_rows`, `_insert_row`, `_select_raw`, `_connect` e similares são internos.
  Uma consulta nova exige método público com nome da operação de domínio.
- Escritas relacionadas em duas ou mais tabelas usam `begin_transaction()`,
  `commit()` e `rollback()` em falha, preservando o resultado atômico.
- Atualização/exclusão de `FILE_TASK` e atualização de `FILE_TASK_HISTORY`,
  inclusive FROZEN, são sempre atômicas. Não deixar fila e histórico divergentes.
- Locks MariaDB são vinculados à conexão. Um lock mantido durante o processo
  exige preservar a conexão proprietária até a liberação.

## 3. Categorias de processo

| Categoria | Arquivos | Controle |
|---|---|---|
| Gateway | `appCataloga.py` | Selector TCP; despacha requisições para filas, sem claim |
| Task worker | `appCataloga_host_check.py`, `appCataloga_discovery.py`, `appCataloga_backlog_management.py`, `appCataloga_file_bkp.py`, `appCataloga_file_bin_process_appAnalise.py` | Lê, faz claim e finaliza uma task |
| Manutenção | `appCataloga_host_maintenance.py`, `appCataloga_garbage_collector.py`, `appCataloga_summary_database.py` | Temporizador ou cursor/checkpoint, sem claim de task |
| Utilitários | `utils/bootstrap_paths.py`, `utils/safe_stop.py` e ferramentas de reparo | Execução pontual; não são daemons |

A anatomia e as assinaturas de fila abaixo valem apenas para task workers.
Gateway e manutenção preservam os contratos gerais de início, sinais e parada,
mas não recebem claim ou finalização artificial de tasks.

## 4. Anatomia e loop dos task workers

Ordem canônica do arquivo:

1. Shebang, encoding e docstring do módulo: finalidade, fila e restrição operacional.
2. Imports da biblioteca padrão.
3. Bootstrap de caminhos.
4. Imports internos.
5. Constantes, incluindo `SERVICE_NAME`.
6. Estado global: `log` e `process_status = {"running": True}`.
7. `_shutdown_cleanup` e `signal_runtime.install_shutdown_handlers(...)`
   no nível do módulo.
8. Leitura e claim: `_read_next_task`, `_claim_task`.
9. Trabalho: `_do_work`.
10. Resolução e limpeza: `_finalize_success`, `_finalize_error`, `_cleanup`.
11. `main()` e guarda `if __name__ == "__main__":`.

Imports ficam no início; não criar imports tardios ou funções aninhadas nas
seções de trabalho e resolução. Separadores de fase usam `# --- read ---`,
`# --- claim ---`, `# --- work ---` e `# --- finalize ---`, sem `ACT` ou `Phase N`.

### 4.1 Assinaturas e resultados

| Função | Contrato |
|---|---|
| `_read_next_task(db: dbHandlerBKP) -> dict \| None` | `None` significa fila vazia; não é sinal de falha |
| `_claim_task(db: dbHandlerBKP, task: dict) -> bool` | Claim atômico; `False` significa disputa perdida |
| `_do_work(db: dbHandlerBKP, task: dict) -> dict` | Delega trabalho ao domínio; falhas levantam exceção; resultado contém `elapsed_sec: float` |
| `_finalize_success(db: dbHandlerBKP, task: dict, result: dict) -> None` | Persiste sucesso e emite `task_done`; trata suas falhas sem propagar |
| `_finalize_error(db: dbHandlerBKP, task: dict \| None, err: errors.ErrorHandler) -> None` | Persiste a resolução de erro e emite seu evento; segura com `task=None`; não propaga |
| `_cleanup(task: dict \| None, *, sftp=None) -> None` | Libera recursos no `finally`; não propaga nem muda estado da fila |

Handlers de banco vêm primeiro, com tipos concretos. Quando há dois,
`db_bp: dbHandlerBKP` e `db_rfm: dbHandlerRFM` precedem `task`.
Helpers de domínio recebem `err` e `logger`, quando necessários, como argumentos
nomeados; a assinatura canônica de `_finalize_error` acima é a referência para
o entrypoint. Helpers locais podem usar o `log` do próprio módulo.

Erros de leitura/claim propagam ao tratamento externo. `None` indica somente
fila vazia e `False` indica somente disputa perdida.

### 4.2 Sequência canônica

1. `main()` emite `service_start` e inicializa os handlers.
2. A cada iteração, recria `errors.ErrorHandler(log)` e inicia `task = None`.
3. No `try`, lê a próxima task; fila vazia faz jitter sleep e `continue`.
4. Faz claim; disputa perdida faz jitter sleep e `continue`.
5. Executa `_do_work()` e chama `_finalize_success()`.
6. No `except` externo, captura a falha com `err.capture(...)`, no máximo uma
   vez, e chama `_finalize_error()` uma vez.
7. No `finally`, chama `_cleanup()` exclusivamente para liberar recursos.
8. Executa `runtime_sleep.random_jitter_sleep()` ao final da passagem.
9. Ao encerrar, emite `service_stop`.

Invariantes:

- `continue` atende fila vazia e disputa perdida; não é resolução de erro.
- DONE, ERROR e FROZEN são resolvidos nas funções de finalização.
- `finally` não escreve resolução da fila.
- Não usar `skip_to_next_iteration` nem guardas intermediárias `err.triggered`
  para decidir se uma etapa seguinte pode executar.
- Helpers de domínio retornam resultado válido ou levantam exceção; não chamam
  `err.capture()` e não devolvem sentinela de falha.
- `err.capture(reason=..., stage=k.STAGE_*, exc=...)` é a única API de captura;
  `err.set(...)` não é permitido.
- Bootstrap retryável preserva a política de requeue/cooldown e a resolução
  no entrypoint.

## 5. Mapa de módulos

| Responsabilidade | Local atual |
|---|---|
| Leitura, claim e resolução de filas | Entrypoints `appCataloga_*.py` |
| Máquina de conectividade | `host_handler/host_connectivity.py` |
| Contexto e bootstrap de sessão SSH | `host_handler/host_context.py` |
| Lock BUSY e estado operacional | `host_handler/host_runtime.py` |
| SSH/SFTP e transferência | `host_handler/host_ssh_utils.py` |
| Probes de manutenção | `host_handler/host_maintenance.py` |
| Processamento e persistência analítica appAnalise | `appAnalise/processing_bin.py` |
| Parsing de payload | `appAnalise/payload_parser.py` |
| Conexão com serviço appAnalise | `appAnalise/appAnalise_connection.py` |
| Artefatos, caminhos e nome de arquivo no servidor | `shared/file_utils.py` |
| Filesystem da coleta de lixo | `gc_handler/gc_maintenance.py` |
| Materialização do summary | `summary_handler/refresh_engine.py` |
| Infraestrutura de processos, sinais, pool, socket e sleep | `server_handler/` |
| Erros, logs, metadados, filtros e geocoding | `shared/errors.py`, `logging_utils.py`, `file_metadata.py`, `filter.py`, `geolocation_utils.py` |
| Constantes compartilhadas sem configuração | `shared/constants.py` |
| Configuração e constantes do aplicativo | `config.py` |
| SQL por banco | `db/dbHandlerBKP.py`, `dbHandlerRFM.py`, `dbHandlerSummary.py` |
| Conexão e SQL genérico | `db/dbHandlerBase.py` |

Uma nova extração de módulo exige decisão arquitetural explícita.

## 6. Tipos e configuração

Tipar handlers com `dbHandlerBKP`, `dbHandlerRFM` ou `dbHandlerSummary`, nunca
`Any`. Distinguir o dicionário bruto retornado pelo banco do contexto normalizado
pelo worker; os tipos devem refletir as chaves efetivamente disponíveis.

Importar configuração com `import config as k`.

| Categoria | Convenção |
|---|---|
| Tipos de task | `HOST_TASK_*`, `FILE_TASK_*` |
| Estados | `TASK_*` |
| Estágios de erro | `STAGE_*` |
| Caminhos | `*_FOLDER`, `*_SUBDIR` |
| Sentinelas legítimas | `*_UNLOCKED_*`, `NONE_*` |
| Limites, tempos e conexão | Nomes descritivos existentes em `config.py` |

Identificadores de estágio em captura e comparação usam as mesmas constantes,
sem strings literais. Valores numéricos sem semântica própria podem ser locais;
estados, limites e valores repetidos com significado exigem constante nomeada.

## 7. Logs

### 7.1 Eventos canônicos dos workers

Usar a API pública de `shared/logging_utils.py`; não montar strings
pseudoestruturadas `event=...` nem criar aliases para eventos canônicos.

| Método | Momento e campos |
|---|---|
| `log.service_start(...)` / `service_stop(...)` | Uma vez no início/parada do processo |
| `log.task_claimed(...)` | Após claim confirmado; `service`, `host_id`, `task_id`, `task_type` |
| `log.task_phase(...)` | Ao concluir uma fase; contexto da task, `phase`, `elapsed_sec`; `since_start_sec` quando disponível |
| `log.task_done(...)` | Após persistir sucesso; contexto da task e `elapsed_sec` |
| `log.task_error(...)` | Após persistir ERROR; contexto, `stage`, `reason`, `error` |
| `log.task_frozen(...)` | Após persistir FROZEN; contexto e estágio/detalhe da retenção |
| `log.warning_event(...)` / `error_event(...)` | Anomalias auxiliares sem evento canônico correspondente |

Incluir `worker_id` e `file` quando houver. `task_phase` mede apenas a fase;
`since_start_sec` é o tempo desde o início da task; `task_done.elapsed_sec`
mede o total. Emitir no instante do fato, sem reconstruir uma sequência de
timestamps ao final. Não emitir todas as resoluções em toda iteração: DONE,
ERROR e FROZEN são desfechos alternativos.

Usar `task_phase` para fases concluídas, sem pares artificiais de início/fim.
Não duplicar uma fase
no domínio e no entrypoint. Métricas de domínio são contexto adicional, não
substituem os campos canônicos.

### 7.2 Domínio e infraestrutura

Somente o entrypoint emite `task_claimed`, `task_done`, `task_error` e
`task_frozen`. Orquestradores de domínio podem emitir `task_phase`, recebendo
explicitamente `service`, `host_id`, `task_id`, `task_type` e, quando existirem,
`worker_id` e `file`. Não reconstruir essa identidade por globals ou consultas
ocultas. Helpers puros retornam dados ou levantam exceções, sem narrar tasks.

| Área | Métodos permitidos | Restrições |
|---|---|---|
| Orquestração de domínio | `task_phase`, `event`, `warning_event`, `error_event` | Sem eventos de claim ou resolução |
| SSH/SFTP e runtime de host | `event`, `warning_event`, `error_event` | Sem métodos `task_*` em helpers de infraestrutura |
| `server_handler/` | `event`, `warning_event`, `error_event`, `signal_received` quando aplicável | Sem métodos `task_*` |
| `db/` | `event`, `warning_event`, `error_event` | Sem métodos `task_*` nem narrativa de CRUD bem-sucedido |

Em `host_handler/`, fases estáveis são `probe`, `persist`, `queue_followup`
em `host_connectivity.run_check`, `connect` no contexto de host e `transfer`
na orquestração de backup. Helpers inferiores não inventam fases novas.

Padronizar cada subsistema em conjunto, sem introduzir novos logs livres
`log.entry`, `log.warning`, `log.error` com tags `[SSH]`, `[META]` ou nomes de
funções. Legado pode existir, mas não é o padrão para novos logs.

### 7.3 Vocabulário e payload

Nomes de evento em `snake_case` descrevem o fato de domínio/infraestrutura,
sem espelhar apenas o nome da função Python.

| Subsistema | Exemplos de eventos | Campos estáveis |
|---|---|---|
| Host | `ssh_connected`, `ssh_connect_failed`, `metadata_iteration_started`, `metadata_iteration_completed`, `metadata_mode_file_skip_dedup`, `backup_transfer_progress`, `backup_transfer_abort`, `backup_transfer_skipped`, `host_state_transition`, `host_operational_recovery`, `host_check_all_batch_done`, `cleanup_busy_hosts`, `cleanup_busy_hosts_failed`, `host_release_failed` | `component`, `operation`, `host`, `host_id`, `address`, `connect_addr`, `port`, `user`, `file`, `remote_file`, `local_file`, `remote_path`, `server_file`, `reason`, `error` |
| Runtime | `worker_pool_scan`, `worker_spawned`, `worker_spawn_failed`, `worker_pool_shutdown_broadcast`, `worker_pool_shutdown_broadcast_failed`, `worker_seed_missing`, `worker_pool_scale_out_failed`, `service_stop_scan_empty`, `service_stop_signal_sent`, `service_stop_signal_failed`, `service_stop_failed`, `client_connected`, `response_sent`, `response_send_failed`, `socket_message_parse_failed` | `component`, `operation`, `script_name`, `signal`, `pid`, `worker_id`, `active_workers`, `targets`, `client_address`, `peer_ip`, `reason`, `error` |
| Banco | `db_connection_reconnected`, `db_connect_failed`, `db_reconnect_failed`, `db_disconnect_failed`, `db_insert_failed`, `db_update_failed`, `db_delete_failed`, `db_select_failed`, `db_execute_failed`, `db_executemany_failed`, `db_invalid_input`, `summary_enqueue_failed`, `summary_scope_publish_failed` | `db_handler`, `database`, `operation`, `table` em falhas SQL, `reason` em publicação, `error` |

`host` é o nome lógico/UID; `address` é o endereço configurado e `connect_addr`
o endereço resolvido. Usar `remote_file` e `local_file` se a direção importa.
`reason` classifica de forma estável; `error` explica a exceção. Não alternar
sinônimos como `addr`/`address` ou `hostname`/`host` nos logs. Para localização,
preferir `component` e `operation` a um campo com nome da função.

Logs de banco cobrem conexão, SQL, transação, entradas inválidas e publicação
auxiliar. Não registrar cada sucesso normal de insert/update/delete. Devolver
detalhes estruturados ao worker, que decide como resumir a operação. Contexto
como `rows_affected`, `where_mode`, `commit` e `full_reconcile` é opcional.

### 7.4 Resumos e timestamps

Para inserts sequenciais, usar `<entity>_count`, `<entity>_id_start` e
`<entity>_id_end`, por exemplo `file_task_*`, `file_history_*` e `spectrum_*`.
Esses dados devem vir de método público de banco: nunca inferir intervalos
por auto-incremento ou logar listas de IDs. Discovery inclui os resumos na
fase de persistência; processamento inclui `spectrum_*` na fase `db`.
`task_done` pode repetir o resumo compacto relevante.

Logs usam timestamps UTC com indicação de zona. Valores de colunas de banco
mantêm a convenção existente; padronizar logs não autoriza alterar datas SQL.
Anomalias de baixo nível não duplicam toda a mensagem final persistida pelo
entrypoint e não geram logs por linha quando um resumo basta.

## 8. Concorrência e manutenção

Workers são processos sequenciais. Não introduzir `threading`, `asyncio` ou
`multiprocessing` dentro deles. Escala normal ocorre por processos supervisionados
pelo gateway/pool e claim atômico.

Exceção restrita: `appCataloga_host_maintenance.py` pode usar pool limitado
para probes exclusivamente de rede em `host_handler/host_maintenance.py`.
ICMP e probes SSH curtos podem ser paralelos; escrita de banco, transições de
fila e estado persistido continuam sequenciais na thread principal. O executor
não sai da varredura de manutenção nem vira mecanismo genérico de concorrência.

O summary é daemon de manutenção por cursor `SUMMARY_OUTBOX`, com reconcile
completo diário às 02:00 BRT e na inicialização quando o último sucesso estiver
defasado. Manter conexões separadas para trabalho e lock singleton, usar
`acquire_worker_lock`/`release_worker_lock` públicos e liberar recursos na saída.
`_run_full_reconcile` chama `refresh_all` e `reset_after_reconcile`;
`_run_incremental_batch` avança o checkpoint somente após sucesso e drena os
eventos consumidos.

## 9. Testes e evolução

Testes ficam em `test/tests/`, conforme a organização real da suíte:
`workers/`, `db/`, `shared/`, `stations/` e `webfusion/`.
Consultar [test/AGENTS.md](../../../test/AGENTS.md) e
[test/README.md](../../../test/README.md) para ambiente e comandos.

Proteger leitura vazia, disputa de claim, finalização e atomicidade,
`task=None`, falha no trabalho, limpeza, retries e contratos de logs. Doubles
de handlers devem respeitar a interface real, preferencialmente com `spec=`.
SQL, locks e transações específicos do MariaDB precisam de integração em
banco isolado; SQLite não comprova equivalência. Não iniciar daemons reais
ou acessar diretórios de produção em testes unitários.

Ao adicionar worker: escolher a categoria, seguir a anatomia aplicável,
usar handlers públicos e tipados, constantes e logs canônicos, atualizar o
cadastro do supervisor/pool, launcher em `shell/`, scripts de operação e testes.
Ao adicionar método de banco: escolher o handler dono da tabela, manter apenas
persistência, nomear a operação de domínio e proteger escritas relacionadas
com transação. Atualizar este documento se houver responsabilidade nova.

Launchers ficam em `shell/`. Preservar o padrão real dos scripts, interpreter
e caminhos do [ambiente](ENVIRONMENT.md), diretórios de PID/log e distinção
entre singleton e múltiplas instâncias. Usar SIGTERM para parada limpa antes
de eventual SIGKILL.
