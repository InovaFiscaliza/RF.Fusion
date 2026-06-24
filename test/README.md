# Testes

Este diretório contém a suíte ativa de validação do RF.Fusion. O objetivo aqui
não é maximizar cobertura por si só, mas proteger contratos que quebram com
facilidade quando workers, handlers, services e rotas evoluem.

A suíte atual é focada no runtime que ainda está em uso no produto.

## O Que a Suíte Valida Melhor

Hoje os testes são mais fortes em:

- contratos de helpers compartilhados
- parsing e normalização de payloads
- regras de decisão dos workers
- semântica de filas e histórico
- comportamento de handlers de banco
- regras de serviço e parte das rotas do `webfusion`
- fluxo incremental do summary database

O viés é para efeitos observáveis e contratos de workflow, e não para testes
altamente mockados só para subir percentual de cobertura.

## Estrutura

### `tests/shared/`

Testes determinísticos para utilitários compartilhados, como erros, filtros,
geolocalização, logging, SSH e helpers gerais.

### `tests/stations/`

Testes de adaptação e protocolo, hoje concentrados em `appAnalise`.

### `tests/workers/`

Testes dos entrypoints e regras de worker, incluindo discovery, backup,
host-check, garbage collector, stop seguro, processamento `appAnalise` e
refresh do summary.

### `tests/db/`

Testes de comportamento dos handlers de banco:

- `dbHandlerBKP`
- `dbHandlerRFM`
- `dbHandlerSummary`

### `tests/webfusion/`

Testes direcionados para services e partes das rotas do `webfusion`.

### `tests/_support.py`

Infraestrutura compartilhada da própria suíte, usada para carregar módulos,
montar paths e criar doubles leves sem depender do runtime completo.

### `fixtures/`

Espaço reservado para fixtures persistentes da suíte.

### `tools/`

Helpers manuais próximos da suíte, quando ajudam a emular contratos de
produção, mas não fazem parte da superfície automática do `pytest`.

## Cobertura Atual

### Shared

Principais áreas cobertas:

- `shared.errors`
- `shared.filter`
- `shared.geolocation_utils`
- `shared.logging_utils`
- `shared.tools`
- `host_handler.ssh_utils`

Contratos típicos:

- retenção do primeiro erro
- formatação canônica de mensagens
- comportamento de filtros
- classificação de falhas transitórias
- utilitários de geolocalização
- rotação e formatação de logs

### Stations

Principal arquivo coberto:

- `appAnalise/appAnalise_connection.py`

Contratos típicos:

- validação de payload
- distinção entre falha definitiva e retryável
- normalização por espectro
- tratamento de respostas malformadas

### Workers

Principais áreas cobertas:

- `appCataloga.py`
- `appCataloga_backlog_management.py`
- `appCataloga_file_bin_process_appAnalise.py`
- `appCataloga_file_bkp.py`
- `appCataloga_discovery.py`
- `appCataloga_garbage_collector.py`
- `appCataloga_host_check.py`
- `appCataloga_summary_database.py`
- `summary_handler/refresh_engine.py`

Contratos típicos:

- roteamento do entrypoint principal
- promoção e rollback de backlog
- exportação e finalização de artefatos do `appAnalise`
- diferenciação entre erro transitório e erro definitivo
- cooldown, reconnect e conectividade de host
- retenção e descarte no garbage collector
- reconcile incremental e ciclo de refresh do summary
- parada segura do pool

### Banco

Principais áreas cobertas:

- `dbHandlerBKP.py`
- `dbHandlerRFM.py`
- `dbHandlerSummary.py`

Contratos típicos:

- lifecycle de `HOST_TASK` e `FILE_TASK`
- preservação e liberação de cooldown
- seleção e atualização de histórico
- regras de site, arquivo e dimensões em `RFDATA`
- idempotência de inserts
- publicação e consumo de metadados do summary

### webfusion

Principais áreas cobertas:

- `modules/host/service.py`
- `modules/map/service.py`
- `modules/spectrum/service.py`
- `modules/spectrum/routes.py`
- `modules/task/service.py`
- `modules/task/routes.py`

Contratos típicos:

- regras de resumo e conectividade por host
- reconciliação de nomes no mapa
- normalização de filtros e ordenação em spectrum
- exposição apenas das ações públicas de task
- reaproveitamento de linhas duráveis de `HOST_TASK`

## Gaps Relevantes

As maiores lacunas hoje são:

- testes de integração real com MariaDB
- integração real com filesystem do repositório
- cobertura de rotas e app-level fora das áreas já tratadas no `webfusion`
- parte do comportamento operacional de publicação de metadados
- validação ponta a ponta com `nginx` e `waitress`

Em outras palavras, a suíte já protege bem contratos locais e fluxos de regra,
mas ainda não substitui testes reais de ambiente.

## Como Executar

O helper padrão é [test_all.sh](/RFFusion/test/test_all.sh). Sem argumentos,
ele executa:

```bash
cd /RFFusion/test
./test_all.sh
```

Internamente, isso equivale a executar `pytest tests -q` com o Python do
ambiente `appdata`.

Também é possível passar argumentos normais do `pytest`:

```bash
cd /RFFusion/test
./test_all.sh tests/workers -q
./test_all.sh tests/db/test_dbhandler_rfm.py -q
./test_all.sh tests -k garbage -q
```

Execução direta por área:

```bash
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/stations -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion -q
```

Execução de um arquivo específico:

```bash
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appCataloga_summary_database.py -q
```

## Convenções da Suíte

As convenções principais são:

- nomes de testes descrevem contrato, não detalhe de implementação
- doubles locais modelam apenas o comportamento necessário
- comentários explicam por que o cenário importa
- `_support.py` concentra bootstraps e loaders usados por vários testes

O arquivo [pytest.ini](/RFFusion/test/pytest.ini) define:

- `testpaths = tests`
- `python_files = test_*.py`
- `python_classes = Test*`
- `python_functions = test_*`

## Regra de Uso da Pasta

`tests/` deve conter apenas artefatos automatizados de validação.

Exceções intencionais:

- `tools/`, para helpers manuais de emulação
- `fixtures/`, para insumos compartilhados da suíte

Rascunhos operacionais, dumps ad hoc, notebooks e experimentos temporários não
devem ficar aqui.

## Referências Relacionadas

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/src/appCataloga/README.md](/RFFusion/src/appCataloga/README.md)
- [/RFFusion/src/webfusion/README.MD](/RFFusion/src/webfusion/README.MD)
