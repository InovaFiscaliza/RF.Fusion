# appCataloga

O `appCataloga` e o runtime operacional do RF.Fusion.

Ele e responsavel por coordenar o ciclo de vida dos arquivos de medicao
recebidos das estacoes remotas, desde a descoberta ate o processamento e a
atualizacao dos resumos operacionais consumidos pela interface web.

## Papel No Sistema

De forma resumida, o `appCataloga` faz:

- cadastro e atualizacao de hosts
- descoberta de arquivos remotos
- promocao e controle de filas
- backup para o repositorio `/mnt/reposfi`
- processamento e catalogacao de espectros
- manutencao operacional e limpeza de artefatos
- atualizacao do `RFFUSION_SUMMARY`

Se o [README raiz](/RFFusion/README.md) apresenta a plataforma, este documento
explica o modulo que move o trabalho operacional do sistema.

## Onde Fica O Runtime

O codigo ativo do runtime fica em:

- [src/appCataloga/server_volume/usr/local/bin/appCataloga](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga)

Esse diretorio contem:

- entrypoints Python
- handlers de dominio
- handlers de banco
- utilitarios compartilhados
- scripts shell de operacao

## Arquitetura Resumida

O `appCataloga` segue uma separacao arquitetural clara:

- entrypoints: controlam loop, fila, sinais e orquestracao
- handlers de dominio: concentram as regras operacionais
- `shared/`: utilitarios comuns
- `db/`: acesso a banco e SQL

Essa separacao nao e apenas organizacional. Ela define a direcao das chamadas:

- entrypoints podem chamar handlers, `shared/` e `db/`
- handlers podem chamar `shared/` e `db/`
- `shared/` nao conhece workers nem schema
- `db/` nao contem regra de negocio

### Diagrama Simplificado

```text
┌──────────────────────────────────────────────────────────────┐
│ ENTRYPOINTS                                                  │
│ appCataloga.py                                               │
│ appCataloga_discovery.py                                     │
│ appCataloga_file_bkp.py                                      │
│ appCataloga_file_bin_process_appAnalise.py                   │
│ appCataloga_host_check.py                                    │
│ appCataloga_host_maintenance.py                              │
│ appCataloga_garbage_collector.py                             │
│ appCataloga_summary_database.py                              │
└─────────────────────────────┬────────────────────────────────┘
                              │
                              v
┌──────────────────────────────────────────────────────────────┐
│ DOMAIN HANDLERS                                              │
│ host_handler/   appAnalise/   summary_handler/   gc_handler/ │
└─────────────────────────────┬────────────────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               v                             v
┌──────────────────────────────┐   ┌───────────────────────────┐
│ SHARED                       │   │ DB HANDLERS               │
│ shared/                      │   │ db/                       │
│ erros, log, filtros, utils   │   │ dbHandlerBKP              │
└──────────────────────────────┘   │ dbHandlerRFM              │
                                   │ dbHandlerSummary          │
                                   └─────────────┬──────────── ┘
                                                 │
                                                 v
                                   ┌───────────────────────────┐
                                   │ DATABASES                 │
                                   │ BPDATA                    │
                                   │ RFDATA                    │
                                   │ RFFUSION_SUMMARY          │
                                   └───────────────────────────┘
```

Leitura pratica do diagrama:

- os entrypoints sao o ponto de orquestracao
- os handlers executam o trabalho de dominio
- `shared/` concentra funcoes reutilizaveis
- `db/` isola SQL e conexoes
- os schemas ficam abaixo dessa pilha e nao definem o fluxo por conta propria

Na pratica, o runtime opera sobre tres camadas de dados:

- `BPDATA`: estado operacional, filas e historico
- `RFDATA`: persistencia analitica de espectros, sites, arquivos e equipamentos
- `RFFUSION_SUMMARY`: read models publicos para consultas rapidas

Documentacao de referencia:

- [ARCHITECTURE.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/.instructions/ARCHITECTURE.md)
- [INSTRUCTIONS.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/.instructions/INSTRUCTIONS.md)

## Modelo Operacional

O fluxo principal do `appCataloga` e orientado por estado:

1. um host existe em `BPDATA.HOST`
2. um `HOST_TASK` representa o trabalho do host
3. a descoberta cria ou atualiza `FILE_TASK` e `FILE_TASK_HISTORY`
4. o backlog decide o que entra em backup
5. o backup copia o arquivo para `/mnt/reposfi`
6. o processamento valida e persiste os dados analiticos
7. `FILE_TASK_HISTORY` permanece como registro autoritativo do ciclo do arquivo
8. manutencao, limpeza e resumo operam sobre esse estado consolidado

## Categorias De Componentes

### 1. Interface Do appCataloga

O ponto de entrada de interface e:

- [appCataloga.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga.py)

Esse entrypoint recebe requisicoes do host e inicia o fluxo operacional
correspondente. Em termos praticos, ele e a porta de entrada do runtime.

Responsabilidades principais:

- receber requisicoes externas via socket TCP
- validar o comando recebido
- garantir a existencia e a atualizacao do `HOST`
- criar ou atualizar o `HOST_TASK` correspondente
- devolver a resposta imediata ao chamador, sem executar o pipeline pesado ali

Em outras palavras, `appCataloga.py` nao e um worker de fila. Ele e a
interface externa do runtime.

### 2. Workers De Fila

Sao os processos que consomem `HOST_TASK` e `FILE_TASK` e executam o pipeline
principal:

- [appCataloga_host_check.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_host_check.py)
- [appCataloga_discovery.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_discovery.py)
- [appCataloga_backlog_management.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_backlog_management.py)
- [appCataloga_file_bkp.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py)
- [appCataloga_file_bin_process_appAnalise.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bin_process_appAnalise.py)

Esses workers fazem, respectivamente:

- verificacao de conectividade e estado do host
- descoberta de arquivos candidatos
- promocao e rollback de backlog
- transferencia para o repositorio
- processamento e persistencia analitica

### Estilo Canonico Dos Entrypoints De Fila

Os workers de fila seguem um estilo de entrypoint recorrente, alinhado ao
`ARCHITECTURE.md`. Um exemplo simplificado desse padrao e:

```python
def main() -> None:
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None
        sftp = None

        try:
            task = _read_next_task(db)
            if task is None:
                runtime_sleep.random_jitter_sleep()
                continue

            if not _claim_task(db, task):
                continue

            result = _do_work(db, sftp, task)
            _finalize_success(db, task, result)
        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_work_failure(e, task=task, sftp=sftp)
                err.capture(reason=reason, stage=stage, exc=e)
            _finalize_error(db, task, err)
        finally:
            _cleanup(sftp, db, task)
```

Na pratica, cada worker adapta esse esqueleto ao seu contexto:

- alguns usam apenas `db`
- outros usam `db_bp` e `db_rfm`
- alguns possuem `sftp`, outros nao
- workers recorrentes podem operar por ciclo, e nao por `task`

Mesmo assim, o padrao estrutural e o mesmo:

- inicializar dependencias
- ler a proxima unidade de trabalho
- tentar o claim
- executar o trabalho
- classificar falhas
- finalizar sucesso ou erro
- liberar recursos no fim do ciclo

Isso e o que mantem o runtime previsivel entre discovery, backup, host check e
processamento.

### 3. Workers Recorrentes E De Suporte

Sao processos que mantem o ambiente coerente ao longo do tempo:

- [appCataloga_host_maintenance.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_host_maintenance.py)
- [appCataloga_garbage_collector.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_garbage_collector.py)
- [appCataloga_summary_database.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_summary_database.py)

Esses componentes cuidam de:

- manutencao de estado e saude operacional dos hosts
- limpeza de artefatos conforme politica de retencao
- refresh incremental e reconciliacao do `RFFUSION_SUMMARY`

Eles diferem dos workers de fila porque nao dependem de um `HOST_TASK` ou
`FILE_TASK` por iteracao. Em vez disso, executam ciclos continuos de
manutencao:

- `appCataloga_host_maintenance.py`
  - limpeza de locks e tarefas operacionais stale
  - sweep recorrente de conectividade

- `appCataloga_garbage_collector.py`
  - remocao de artefatos em quarentena
  - tratamento separado de `trash/` e `trash/resolved_files/`

- `appCataloga_summary_database.py`
  - reconcile completo em janelas programadas
  - refresh incremental orientado por `SUMMARY_OUTBOX`

### 4. Scripts De Operacao

Os scripts shell usados no runtime ficam em:

- [shell/](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell)

Os principais sao:

- `tool_start_all.sh`
- `tool_status_all.sh`
- `tool_stop_all.sh`

Eles iniciam, consultam e encerram o conjunto normal de servicos do
`appCataloga`.

## Estrutura Interna Relevante

Os subdiretorios mais importantes do runtime sao:

- `appAnalise/`: integracao e processamento orientado ao `appAnalise`
- `db/`: handlers de banco e SQL
- `gc_handler/`: manutencao e limpeza de artefatos
- `host_handler/`: regras de conectividade, contexto e manutencao de hosts
- `server_handler/`: infraestrutura do gateway e controle de processo
- `summary_handler/`: refresh do `RFFUSION_SUMMARY`
- `shared/`: utilitarios comuns
- `shell/`: scripts operacionais
- `utils/`: utilitarios administrativos pontuais
- `.instructions/`: arquitetura, regras e documentos normativos do modulo

### Pastas Do Runtime

No nivel superior de `/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga`,
as pastas mais relevantes sao:

- `appAnalise/`
  - adaptadores e fluxo de processamento orientado ao `appAnalise`

- `db/`
  - classes `dbHandler*` e consultas SQL

- `gc_handler/`
  - rotinas de coleta e remocao de artefatos em quarentena

- `host_handler/`
  - regras de conectividade, manutencao e contexto de host

- `server_handler/`
  - suporte ao gateway TCP, sinais, controle de processo e temporizacao

- `shared/`
  - funcoes utilitarias compartilhadas entre entrypoints e handlers

- `shell/`
  - scripts operacionais para start, stop e status dos servicos

- `summary_handler/`
  - logica de reconciliacao e refresh incremental do `RFFUSION_SUMMARY`

- `utils/`
  - utilitarios administrativos e scripts auxiliares

- `.instructions/`
  - documentos normativos do modulo, incluindo arquitetura e regras de refatoracao

- `__pycache__/`
  - artefatos de bytecode gerados em runtime; nao fazem parte da arquitetura funcional

## Formato Do Filtro

O contrato de filtro operacional e centralizado em:

- [shared/filter.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/filter.py)

Esse filtro e reutilizado em discovery, backlog e criacao de tarefas pela
interface.

Campos canonicos:

- `mode`
- `file_path`
- `extension`
- `start_date`
- `end_date`
- `last_n_files`
- `file_name`
- `max_total_gb`
- `sort_order`

Modos principais:

- `NONE`: discovery incremental padrao
- `ALL`: promove todos os candidatos elegiveis
- `RANGE`: filtra por janela de data
- `LAST`: seleciona os arquivos mais recentes
- `FILE`: seleciona arquivo ou padrao explicito
- `REDISCOVERY`: forca novo scan sem cutoff incremental

Exemplo:

```json
{
  "mode": "RANGE",
  "start_date": "2026-04-01T00:00:00",
  "end_date": "2026-04-07T23:59:59",
  "extension": ".zip",
  "file_path": "C:/CelPlan/CellWireless RU/Spectrum/Completed",
  "max_total_gb": 50,
  "sort_order": "newest_first"
}
```

O ponto importante e que o filtro e normalizado antes de uso. Cada modo
mantem apenas os campos semanticamente validos para aquela operacao.

## Operacao Basica

Dentro do ambiente onde o runtime esta montado:

```bash
cd /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell
./tool_start_all.sh
./tool_status_all.sh
./tool_stop_all.sh
```

## Observacoes Importantes

- O `appCataloga` e orientado por filas e estado persistido em banco.
- `FILE_TASK_HISTORY` e o registro autoritativo do ciclo de vida do arquivo.
- O runtime toca `BPDATA`, `RFDATA` e `RFFUSION_SUMMARY`, mas com papeis diferentes.
- A documentacao arquitetural em `.instructions/` deve prevalecer sobre README quando houver conflito.
