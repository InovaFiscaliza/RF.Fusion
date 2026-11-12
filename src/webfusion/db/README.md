# Persistência do WebFusion

Este pacote concentra as conexões, os handlers e a composição SQL compartilhada
do WebFusion. Os consumidores importam o módulo do domínio explicitamente.
`__init__.py` não carrega handlers, credenciais ou blueprints.

| Arquivo | Responsabilidade |
|---|---|
| [connection.py](connection.py) | Fábricas PostgreSQL por schema e tipo `DbRow` |
| [summary.py](summary.py) | Construtores das projeções compartilhadas do Summary |
| [host.py](host.py) | Estações, atividade, localidades, diagnósticos e métricas do servidor |
| [spectrum.py](spectrum.py) | Arquivos, espectros, filtros e tabelas temporárias da interface |
| [tasks.py](tasks.py) | Criação de tarefas, filas, histórico, transições e outbox |
| [usage_metrics.py](usage_metrics.py) | Contadores mensais de uso e checkpoints do NGINX |
| [map.py](map.py) | Consultas de sites e estações do mapa compartilhado |
| [users.py](users.py) | Identidades e privilégios do WebFusion |
| [appanalise.py](appanalise.py) | Classe `AppAnaliseDB` e resultado `TableResult` do contrato MATLAB |
| [config.py](config.py) | Identificadores SQL, campos de ordenação permitidos e contratos de resultados |

## Conexões e transações

As fábricas de `connection.py` abrem sessões independentes, com linhas em
dicionário, timezone UTC e `search_path` explícito. Importar um módulo não abre
conexão. Cada operação de leitura fecha a conexão que abriu, inclusive em falhas.

Operações de fila e checkpoint podem receber uma conexão ou um cursor tipado
do chamador. A mudança de diretório conserva os limites de commit, rollback e
locks: fila e histórico são atualizados juntos; contadores do NGINX e seu offset
são gravados na mesma transação. A publicação de manutenção no outbox conserva
seu commit independente.

O SQL parametrizado e a adaptação dos resultados pertencem aos handlers.
Serviços mantêm regras, caches e montagem das respostas; rotas mantêm HTTP.
Os handlers não importam rotas ou serviços. Configurações compartilhadas com as
funcionalidades permanecem em seus respectivos módulos de configuração. Listas
de colunas permitidas definidas em `db/config.py`, como ordenação e filtros de
data, são importadas diretamente pelos serviços que validam essas chaves, sem
reexportação nos `config.py` das funcionalidades.

O handler appAnalise referencia os filtros da API apenas para tipagem. Suas
constantes de persistência ficam em `db/config.py`, permitindo importar o handler
diretamente sem carregar o blueprint nem criar dependência circular.

## Projeções do Summary

Cada projeção de `summary.py` possui um
construtor tipado: `site_observations_query`, `host_locations_query`,
`map_stations_query`, `map_sites_query`, `host_operational_query`,
`server_totals_query` e `server_errors_query`. A composição geográfica comum
é interna ao módulo.

Essas funções retornam SQL fixo do servidor e não recebem entrada do cliente.
Os handlers consumidores compõem suas consultas e vinculam os valores por
parâmetros do driver. A composição ocorre quando solicitada, sem materializar
todas as projeções no import. Não são criadas tabelas ou views de compatibilidade.
Filtros e formatos de resposta da interface e do appAnalise continuam próprios
de cada consumidor.

## Validação

Siga o [ambiente autorizado](../.instructions/ENVIRONMENT.md). No webserver,
com raiz `/RF.Fusion`, execute os unitários afetados em processos separados:

```bash
for suite in test_host_service test_spectrum_service test_maintenance_service test_task_service test_map_service test_appanalise_api test_app_headers test_server_routes; do
    PYTHONPATH=/RF.Fusion/src/webfusion /usr/local/bin/python -m unittest "test.tests.webfusion.$suite" -q || break
done
```

A [integração PostgreSQL](../../../test/tests/webfusion/test_postgres_integration.py)
exige `RFFUSION_TEST_PG_CONNINFO` selecionando explicitamente um banco descartável
`rffusion_test_*`. Ela valida os quatro schemas, os contratos HTTP, os resultados
das projeções e o rollback de fila/histórico e contadores/checkpoint.

```bash
PYTHONPATH=/RF.Fusion/src/webfusion /usr/local/bin/python -m unittest test.tests.webfusion.test_postgres_integration -v
```

Os contratos normativos estão na [arquitetura](../.instructions/ARCHITECTURE.md)
e no [Summary PostgreSQL](../../../docs/architecture/SUMMARY_POSTGRESQL.md).
