# Camada de banco PostgreSQL

`dbHandlerBase.py` contém `DbHandler`, base dos handlers PostgreSQL.
`dbHandlerBKP.py` implementa o domínio operacional de hosts, filas e histórico.
O código MariaDB movido para `backup/` permanece como referência, sem alteração.
`dbHandlerRFM.py` implementa sites, dimensões, arquivos, espectros e linhagem.
Sites usam PostGIS; a integração espacial precisa ser validada em uma instância
que tenha a extensão. `dbHandlerSummary.py` implementa os agregados canônicos, checkpoints, outbox e
auditoria. Os entrypoints passam `k.PG_CONNINFO` aos handlers PostgreSQL;
estações, appAnalise e consumidores WebFusion ainda exigem validação do fluxo
completo antes da ativação.
O DDL analítico não impõe unicidade aos nomes das dimensões nem à identidade de
arquivo por volume/caminho/nome. A busca antes do insert preserva o comportamento
legado, mas inserções simultâneas ainda podem criar duplicatas. Revise esses dados
e as restrições antes de ativar ingestão concorrente.

O contrato está na [arquitetura do appCataloga](../../../../../../.instructions/ARCHITECTURE.md#23-base-postgresql-e-migração-por-lotes).
Os métodos internos da base concentram composição de SQL, validação estrutural,
filtros e ordenação, sem um módulo separado de SQL genérico.
As constantes de persistência e filas ficam em [constants.json](constants.json).
O pacote `db` lê esse arquivo uma vez, em `__init__.py`, e disponibiliza o
dicionário para acesso direto:

```python
from db import constants as db_constants

batch_size = db_constants["persistence"]["batch_size"]
```

O arquivo tem cinco seções:

| Seção | Conteúdo |
|---|---|
| `persistence` | Tamanho de lote e unidade de volume. |
| `tasks` | Estados, tipos de host/arquivo, tipos de conectividade, filtro padrão e proteção de filas. |
| `hosts` | Sentinelas de reserva e campos dos sinais operacionais. |
| `rfm` | Regras de sites, códigos geográficos e identidade de espectros. |
| `summary` | Limites de consultas, estados do worker, escopos, fontes de erro e métricas. |

Os valores são acessados diretamente pelo caminho do grupo, sem aliases para
os nomes antigos, classe de configuração, validadores paralelos ou redeclaração
de cada chave em Python. Referências a tipos/estados usam nomes relativos a
`tasks.host_types`, `tasks.file_types` ou `tasks.states`, resolvidos pela operação
consumidora. O arquivo não contém credenciais nem cópias do schema SQL.
Alterações exigem reinício dos processos; falhas de leitura/JSON propagam.

`summary.host_metrics` agrupa as métricas pela fonte (`queue`/`history`) e
pela fase (`discovery`, `backup`, `processing`). A fase declara `status_column`
e, nas filas, `task_type`. Sua lista `metrics` contém o estado e os nomes
explícitos de contagem/volume; `month_column`, quando presente, recebe o
intervalo mensal passado ao handler. O fallback de tamanho dos payloads
removidos permanece no cálculo do handler.

Destinos, schemas e credenciais de conexão continuam na
[configuração central](../../../../../etc/appCataloga/config.py), importada
como `config as k`, junto das configurações dos demais componentes do aplicativo.

## Uso pelos handlers

| Escopo | Métodos | Uso |
|---|---|---|
| Público: sessão e transações | `connect`, `close`, `begin_transaction`, `commit`, `rollback`, `in_transaction` (propriedade) | Entrypoints e serviços podem controlar o ciclo de vida pelo handler concreto. |
| Protegido: persistência | Pares `row`/`batch` de SELECT, INSERT, UPDATE, UPSERT e DELETE; `_select_raw`, `_execute`, `_execute_batch` | Implementação dos métodos de domínio nas subclasses. |
| Interno: implementação | Definições de tabela/chave, validações, construção de SQL, alinhamento de lotes, cursor e contexto de falhas | Apoio à própria base; não constitui API para chamadores. |

O prefixo `_` sinaliza uso não público em Python; a distinção entre operações
protegidas para subclasses e detalhes internos está no contrato acima.
Os métodos internos confiam nos tipos declarados, recebidos de chamadores
controlados. A validação de tipos de dados externos pertence às entradas;
internamente permanecem as regras semânticas de SQL, campos e transações.
Filtros recebem valores `object`, cujo formato é validado por operador.
O construtor apenas configura o handler. `connect()` permite validar a conexão
explicitamente e retorna a sessão administrada pela base, sem transferir sua
propriedade. Chamadores não executam SQL nem controlam transações diretamente
nessa sessão. O CRUD também abre a conexão sob demanda.

Uma subclasse recebe schema, logger e `conninfo` explícitos; não mantém uma
lista de tabelas ou colunas que duplique o DDL. O banco de conexão é separado do schema;
a instalação do projeto usa `dbname=postgres`. A base não usa as credenciais
MariaDB da configuração central para conectar, não abre conexão no construtor
e não grava arquivos de credenciais. O carregamento atual da configuração
central conserva seu mecanismo de leitura de `.secret`; a revisão da
autenticação permanece prevista para o final da Fase 1.

CRUD usa nomes de tabelas sem qualificação. SELECT aceita um JOIN simples.
A base qualifica pelo
schema e delimita identificadores, preservando maiúsculas. Valores são passados
separadamente, por parâmetros. Tabelas e colunas inexistentes geram erros do PostgreSQL, que propagam
sem descarte silencioso. Permissões por operação pertencem aos métodos públicos e ao banco.
A delimitação de nomes usa `sql.Identifier`, sem regex própria. Cada nome é
tratado como um identificador literal, inclusive quando contém pontos,
aspas ou texto que pareça uma expressão. O banco verifica sua existência.
Validações locais cobrem a estrutura da operação: filtros obrigatórios,
colunas vazias ou repetidas, operadores, ordenação, limites e transações.
Não há descoberta nem cache do catálogo.

| Operação | Individual | Em lote |
|---|---|---|
| SELECT | `_select_row(table, where)`: dicionário ou `None`, pelo filtro de identidade do domínio. | `_select_batch(table, where)`: lista dos registros filtrados. |
| INSERT | `_insert_row(table, data)`: dicionário pedido por `returning`, ou `None`. | `_insert_batch(table, data)`: contagem de inserções. |
| UPDATE | `_update_row(table, data, where)`: altera o registro identificado pelo domínio. | `_update_batch(table, data, where)`: aplica os mesmos valores a todos os registros filtrados. |
| UPSERT | `_upsert_row(table, data, conflict_columns=...)`: contagem de inserção/atualização. | `_upsert_batch(table, data, conflict_columns=...)`: contagem total afetada. |
| DELETE | `_delete_row(table, where)`: exclui o registro identificado pelo domínio. | `_delete_batch(table, where)`: exclui o conjunto filtrado. |

Os argumentos seguem a ordem `table`, `data`, `where`, quando aplicáveis;
opções ficam após `*` e são passadas por nome. `data` é um dicionário na
operação individual e uma sequência de dicionários em INSERT/UPSERT em lote.
Em UPDATE em lote, é um dicionário de valores aplicado ao conjunto filtrado.
Nos handlers, passar os argumentos por nome e declarar dados e filtros antes
da chamada, por exemplo:

```python
data = {"IS_BUSY": 0, "NU_PID": k.HOST_UNLOCKED_PID}
where_conditions = {"NU_PID": pid}
self._update_batch(table="HOST", data=data, where=where_conditions)
```

| Opção | Significado |
|---|---|
| `columns` | Campos retornados por SELECT. |
| `returning` | Campos retornados pelo banco após INSERT individual. |
| `skip_duplicates` | Em INSERT, usar `ON CONFLICT DO NOTHING` para conflitos de unicidade; demais erros continuam propagando. |
| `conflict_columns` | Em UPSERT, colunas da chave ou índice único que identifica o conflito. |
| `batch_size` | Número de linhas por bloco enviado em INSERT/UPSERT em lote; não cria commits intermediários. |
| `limit` | Máximo de registros retornados pelo SELECT. |
| `increments` | Deltas numéricos aplicados atomicamente no UPDATE junto das substituições. |
| `maximums` | Valores aplicados com `GREATEST` no UPDATE: a coluna só avança; coluna nula recebe o valor. |
| `for_update` | Em SELECT em lote, bloqueia as linhas selecionadas até o fim da transação explícita do chamador; rejeita uso sem essa transação. |

Leitura, atualização e exclusão individuais recebem filtros explícitos do
método de domínio, responsável por identificar no máximo um registro por ID,
chave composta ou outra identidade única. A base não mantém declarações de
chaves nem consulta o catálogo; aceita condições adicionais, como o estado
esperado. SELECT individual usa `LIMIT 1`, sem verificar unicidade. UPDATE e
DELETE aplicam o filtro fornecido, sem limite de linhas ou validação de
cardinalidade. Filtros vazios são rejeitados. Constraints e erros de integridade
continuam sob responsabilidade do PostgreSQL.

UPDATE e DELETE em lote exigem filtros não vazios. `batch` descreve uma operação
sobre um conjunto; essas duas variantes usam uma instrução SQL para o conjunto.
INSERT e UPSERT em lote aceitam linhas homogêneas e alinham os valores pelas
colunas da primeira linha, independentemente da ordem dos demais dicionários.
Todos os blocos participam da mesma transação. INSERT em lote retorna contagem;
obtenha IDs gerados por `RETURNING` nas inserções individuais dentro de uma
transação explícita quando necessário.

UPSERT usa `ON CONFLICT` com chaves explícitas, sem a convenção MariaDB de duas
linhas afetadas por atualização. `columns` define a projeção ordenada de SELECT;
sua ausência usa `SELECT *` da tabela principal. A ordem das chaves acompanha
o resultado SQL; projeções explícitas conservam a ordem solicitada.

- `_select_raw`, `_execute` e `_execute_batch`: executam objetos
  `psycopg.sql.Composable` de confiança e valores separados. Atendem apenas
  as consultas que o CRUD não representa, listadas no fim deste documento.

`_execute` centraliza execução de uma instrução; `_execute_batch` centraliza
execução de lotes atômicos. `_build_insert_query` compartilha o INSERT entre
as variantes individuais, em lote e UPSERT. `_upsert_row` delega ao método
`_upsert_batch` com uma linha; a cláusula de conflito é construída nesse método,
sem construtor separado de UPSERT. A operação individual também usa a transação
do executor de lotes quando não há transação do chamador. Esses mecanismos
não são operações de domínio para uso direto por workers.

Cada operação de INSERT/UPSERT resolve a tabela e valida a estrutura das colunas.
`_build_insert_query` recebe o identificador qualificado e a sequência de colunas já validadas;
concentra apenas a composição do INSERT. Por exemplo, com
`sql.Identifier("BPDATA", "HOST")` e `("NA_HOST_NAME",)`, produz:

```sql
INSERT INTO "BPDATA"."HOST" ("NA_HOST_NAME") VALUES (%s)
```

Para o dado `{"NA_HOST_NAME": "station-01"}`, a operação alinha separadamente
a tupla de parâmetros `("station-01",)`. `_insert_row`, `_insert_batch` e
`_upsert_batch` decidem como executar ou complementar a instrução.
WHERE e JOIN não fazem parte desse INSERT.

Filtros usam `COLUNA` ou `COLUNA__operador`: `eq`, `ne`, `lt`, `gt`, `lte`,
`gte`, `like`, `ilike`, `trim_ne`, `between`, `in`, `not_in`.
`ilike` compara padrões sem distinguir maiúsculas; `trim_ne` compara
`TRIM(coluna)` com um texto, mantendo o valor como parâmetro.
`None` com igualdade/desigualdade usa
`IS NULL`/`IS NOT NULL`. Listas vazias em `in` e `not_in` representam falso e
verdadeiro, respectivamente. Os filtros são unidos por AND. Ordenação recebe
pares `(coluna, "ASC" ou "DESC")`, com `NULLS FIRST` ou `NULLS LAST` opcional,
sempre por uma lista fechada de opções. Não há interpretação de `#CUSTOM#`
ou trechos SQL; nomes são identificadores literais e `__expr` não é operador.

## SELECTs relacionais

`_select_batch` aceita um JOIN simples por `join=(tabela, coluna_da_tabela_principal,
coluna_da_tabela_relacionada)`. Uma projeção nomeada associa cada resultado a
`(tabela, coluna)`, enquanto `where` e `joined_where` filtram as respectivas
tabelas. Por exemplo:

```python
columns = {"task_id": ("HOST_TASK", "ID_HOST_TASK"), "host_name": ("HOST", "NA_HOST_NAME")}
join = ("HOST", "FK_HOST", "ID_HOST")
rows = self._select_batch(
    table="HOST_TASK", columns=columns, join=join,
    where={"NU_STATUS": status}, joined_where={"IS_BUSY": 0},
)
```

O PostgreSQL valida a existência de tabelas e campos; identificadores são
delimitados e valores são parametrizados. Com `prefix_tables=True` e sem `columns`, o JOIN retorna `*` das duas
tabelas. A base usa nomes e origens das colunas do resultado para formar
`TABELA__COLUNA`, sem outra consulta e preservando os tipos nativos.

Cada UPDATE atribui uma coluna uma única vez, seja por valor,
`increments`, `maximums` ou `touch_field`.

## Transações e sessão

Operações isoladas usam autocommit. Para operações relacionadas, chamar
`begin_transaction()`, executar operações e chamar `commit()`; em falha,
`rollback()`. Helpers participam da transação aberta e não a encerram.
CRUD e executores não recebem argumento `commit`; a transação é controlada
pelos métodos públicos de ciclo de vida. Sem transação explícita, lotes criam
uma transação própria: todos os seus blocos são atômicos.

Cada operação fecha seu cursor. `reuse_connection=True` (padrão) conserva a sessão
entre operações, inclusive nos loops dos workers, até o encerramento explícito.
`reuse_connection=False` fecha sessões ociosas,
mas retém a conexão durante transação explícita. `close()` descarta a conexão
e qualquer transação não concluída. Uma sessão perdida exige encerramento
explícito: não há reconexão ou repetição automática de SQL, para preservar a
avaliação de locks perdidos e commits com resultado desconhecido.

O logger recebe apenas operação, classe de erro e SQLSTATE; não recebe SQL,
parâmetros ou conninfo. Exceções originais propagam e também não devem ser
exibidas indiscriminadamente pelos futuros chamadores.
O tipo do logger é o já definido em `shared.logging_utils.log`, fornecido pelo
chamador. A base não define interface própria de logging; formatação, destinos
e rotação permanecem no módulo compartilhado.

## Handler operacional BKP

O construtor preserva `database` como nome do argumento do schema e exige
`conninfo` explícito. Por exemplo, `dbHandlerBKP(database="BPDATA", log=log,
conninfo="dbname=postgres")` configura o handler sem abrir uma conexão nem
escolher credenciais MariaDB. A autenticação é resolvida pelo libpq no ambiente.
Os processos recebem a conexão composta no `config.py`; `RFFUSION_PG_CONNINFO`
não vazio pode substituí-la. A autenticação é fornecida ao libpq, sem usar as
credenciais MariaDB legadas.

O `.secret` existente pode fornecer `DB_USER_NAME_PG` e `DB_PASSWORD_PG` para a
conexão composta. `PGUSER`/`PGPASSWORD` têm prioridade sobre esses campos;
uma sobrescrita completa por `RFFUSION_PG_CONNINFO` não os acrescenta à conexão.

- Métodos públicos seguem `entidade_ação_qualificação`, organizados no arquivo
  em blocos HOST, HOST_TASK, FILE_TASK e FILE_TASK_HISTORY. Helpers exclusivos
  acompanham a entidade; persistência compartilhada e publicação ficam no final.
  Os chamadores usam os mesmos nomes, sem aliases para a nomenclatura anterior.
  Leituras de tasks com JOIN
  preservam as chaves `HOST__*`, `HOST_TASK__*` e `FILE_TASK__*`; a seleção de
  arquivos retorna `(row, host_id, task_id)`. `host_task_read` retorna o dicionário
  e `host_filter` decodificado, incluindo JSON nativo do PostgreSQL.
- Os métodos de domínio montam os filtros de identidade; chaves primárias
  pertencem ao DDL. Flags são gravadas como 0/1, conforme as colunas `smallint`.
- INSERT/UPSERT de arquivos usa os índices únicos por host e hashes de caminho
  e nome. O conflito atualiza os valores enviados, incluindo timestamps explícitos.
  Não há inferência de IDs ou contagem MariaDB de duas linhas por atualização.
- Atualizações condicionais usam `expected_status` para claims. Reservas de host
  usam UPDATE condicional; liberações seguras conferem PID e timestamp observados
  para preservar uma reserva adquirida após a leitura.
- Operações compostas abrem transação apenas quando o chamador não possui uma.
  Falhas propagam e transações externas continuam sob responsabilidade do chamador.
  Campos inexistentes geram erro no PostgreSQL. Uma descoberta ausente retorna `None`;
  falha na consulta de descoberta propaga, sem parecer uma marca d'água ausente.
- Histórico conserva `None` como ausência de alteração e `constants.SET_NULL`
  como limpeza explícita. Atualizações de fase não inventam `DT_BACKUP`,
  `DT_PROCESSED` ou `DT_FILE_TASK`. Marcos do host avançam com a mesma transação.
- Os janitors de `host_handler/host_maintenance.py` decidem recuperação por
  liveness, TTL e cooldown. O BKP fornece snapshots de host/tasks e
  `host_release_reservation`, que compara o PID e o timestamp observados.
- O worker de backlog interpreta filtros, calcula o orçamento por prefixo e
  controla a transação. O BKP fornece `file_task_list_for_backlog` com lock e
  `file_task_update_backlog` com dados e filtros preparados. Não executa
  `extra_sql` nem `NA_MESSAGE__expr` recebidos. A publicação ocorre uma vez,
  dentro da mesma transação, por solicitação do worker.
- O BKP escolhe as invalidações de host/mês e as grava por
  `_publish_summary_scopes` da base, compartilhada com o RFM. Em conflitos, renova
  `ID_OUTBOX` para manter a leitura por checkpoint. Savepoints isolam falhas dessa
  publicação; um rollback operacional também descarta suas invalidações.
  A reconciliação periódica continua cobrindo publicações perdidas.

| Entidade | Exemplos da API |
|---|---|
| HOST | `host_upsert`, `host_read_status`, `host_read_last_discovery`, `host_request_summary_refresh` |
| HOST_TASK | `host_task_list`, `host_task_create`, `host_task_read`, `host_task_update`, `host_task_enqueue` |
| FILE_TASK | `file_task_read`, `file_task_create`, `file_task_update`, `file_task_delete`, `file_task_list_for_backlog`, `file_task_update_backlog` |
| FILE_TASK_HISTORY | `file_task_history_create`, `file_task_history_update`, `file_task_history_delete`, `file_task_history_filter_existing_batch`, `file_task_history_list_gc_candidates` |

`list` identifica consultas de coleção; `read` identifica seleção individual.
Retomadas de HOST_TASK e FILE_TASK mantêm seus próprios critérios no respectivo
método. `tasks.connectivity_host_types` centraliza os tipos que dependem do host.
Seleções compõem apenas templates controlados; nomes usam `sql.Identifier` e
valores seguem por binding. Opções de prioridade não recebem fragmentos SQL.
Operações representáveis pelo CRUD usam a base. SQL parametrizado permanece
no BKP apenas em:

- seleção de backup com rodízio por host e reserva de discovery
  (`_select_prioritized_file_task`), que exige subconsultas correlacionadas
  num único snapshot;
- mensagem por linha do backlog (`file_task_update_backlog`), composta a partir
  das colunas da própria linha;
- UPSERT de arquivos (`_file_upsert_batch`), cujo conflito usa o índice por
  expressão de hash.

A publicação no outbox do summary, em outro schema e sob savepoint, fica na
base (`_publish_summary_scopes`); a transação composta de domínio usa
`_unit_of_work`, também da base.

Transições por host (suspensão e retomada de HOST_TASK, FILE_TASK e histórico)
compartilham `_task_update_by_host`: os UPDATEs de cada chamada são atômicos e
o host é invalidado uma vez. O snapshot do janitor lê os hosts ocupados e,
depois, as tasks em execução desses hosts; a liberação continua condicionada
ao dono e ao `DT_BUSY` observados.

A reserva de host (`lock_host=True`) trava a linha do host numa transação,
verifica se há HOST_TASK de discovery (processing) ou FILE_TASK de
discovery/backup em RUNNING com PID vivo de outro processo e só então ocupa o
host. Verificações de conectividade (host_check) são leves: não ocupam o host
nem bloqueiam a reserva. O próprio PID
pode reservar de novo, o que torna a repetição idempotente. Uma task RUNNING só
é retomada por idade quando seu PID dono não existe mais, a liberação por
timeout compara o dono e o `DT_BUSY` observados, e a redescoberta não altera
linhas de fila ou histórico em RUNNING.

## Handler analítico RFM

`dbHandlerRFM` usa o CRUD e `_select_nearest_point` da base. `get_site_id`
aplica o limite estrito de `rfm.sites.match_distance_meters` à distância
esférica, com desempate pelo menor ID. A leitura PostGIS de
`get_site_geography` mantém SQL específico. Dimensões, equipamentos e arquivos usam
`_get_or_insert`: procura pela identidade, insere com `ON CONFLICT DO NOTHING` e,
se perder a disputa, lê o vencedor. Isso depende dos índices únicos de
`createMeasureDB.sql`: rótulos de dimensão, `NA_EQUIPMENT` e a identidade SHA-256
de volume, caminho e nome de `DIM_SPECTRUM_FILE`. As buscas de arquivo usam o
índice de `NA_FILE`. `insert_file` só atualiza os campos informados.

`insert_site` exige o tipo `1 = Fixed` em `DIM_SITE_TYPE`, cadastrado pelo
bootstrap PostgreSQL. Instalações anteriores usam o
[reparo do catálogo](../../../../../../../../src/postgres/scripts/ensureSiteTypes.sql),
conforme o [procedimento](../../../../../../../../src/postgres/scripts/README.md).
O handler não cria nem substitui esse catálogo durante a ingestão. Erros do
Psycopg na resolução SITE propagam ao worker, inclusive violações de chave
estrangeira; não são convertidos em descarte por localidade.

`FACT_SPECTRUM` não tem índice único, porque dados legados podem repetir a
identidade lógica (`rfm.spectrum_identity_fields`) e `reconcile_reprocessed_file_lineage`
existe para consolidá-los. `insert_spectrum` trava a linha do site com
`FOR UPDATE` antes de procurar, ampliar ou inserir, serializando escritores do
mesmo site. `update_site` lê e trava o site numa única consulta. A remoção de
linhagem descobre órfãos por diferença entre os candidatos e os vínculos que
restaram, sem SQL específico.

## Validação

A dependência Psycopg está fixada no
[ambiente do container](../../../../../../../../install/appCataloga/linux/environment.yml).
Executar as suítes PostgreSQL da base, BKP, RFM e Summary; a suíte MariaDB
legada não valida as implementações novas.

```bash
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/db -p 'test_dbhandler_*postgres*.py' -v
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/workers -p 'test_backlog_management_worker.py' -v
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/workers -p 'test_host_maintenance_postgres.py' -v
```

Os testes unitários usam o driver real para composição de SQL e doubles de
conexão. A integração exige `RFFUSION_TEST_PG_CONNINFO` no ambiente, apontando
explicitamente para um banco descartável cujo nome comece com `rffusion_test_`.
Sem essa configuração a integração é ignorada. Não usar o banco `postgres`
compartilhado pelo projeto. A suíte cria e remove apenas um schema aleatório
nesse banco de testes; não inicia um servidor nem lê credenciais do runtime.
O BKP também verifica o binding pelo Psycopg instalado. Sua integração cria
schemas aleatórios de BKP e outbox; as tabelas
operacionais e índices vêm do DDL PostgreSQL do repositório. Integração real,
concorrência entre processos e autenticação operacional dependem de validação
no ambiente apropriado; testes com doubles não comprovam esses comportamentos.

## Handler Summary

A estrutura segue o [modelo PostgreSQL](../../../../../../../../docs/architecture/SUMMARY_POSTGRESQL.md).
São seis agregados reconstruíveis, três tabelas de controle e as duas tabelas
históricas de uso. Mapas, localidades e totais de servidor não são tabelas
físicas independentes. Nomes, geografia, conectividade e sinais vêm dos
cadastros autoritativos; não são replicados nos agregados.

O construtor recebe schema em `database` e exige `conninfo`. Métodos públicos
seguem `entidade_ação_qualificação`, organizados por entidade como no BKP:
`site_equipment_list_sources`/`site_equipment_replace`,
`host_metric_list_sources`/`host_metric_replace`, `host_visit_*`,
`host_monthly_*`, `host_error_*`, `worker_state_*`, `outbox_*` e `refresh_log_record`.
Não existe API pública de escrita genérica, SQL em texto ou aliases legados.
A base administra sessões, cursores e transações.

As fontes de BPDATA e RFDATA usam `SCHEMA_<TABELA>` para roteamento e
projeções explícitas nas consultas pelo CRUD da base: hosts, equipamentos,
owners de sites afetados, hosts com visitas afetadas e as
cinco fontes de erros (o mapeamento de fase, estado e data fica no Python).
O handler não contém SQL. Agregados por site, host e mês usam
`_select_aggregate` da base (COUNT/SUM/MIN/MAX com filtro por agregado em
mapeamento e agrupamento por mês); a conversão KB→GB e a junção por host ficam no
Python. O filtro mensal usa um único intervalo, o que mantém utilizável o índice
`(FK_HOST, DT_FILE_CREATED_HOST)`; meses não pedidos dentro dele são descartados.
As visitas são montadas no Python a partir dos espectros do host em ordem
cronológica, sob `statement_timeout` local; a memória cresce com o número de
espectros do host. `host_read_operational` faz leituras simples e decodifica
as coordenadas do EWKB devolvido pelo PostgreSQL. Lock singleton, `lock_timeout`
e o timeout local usam helpers da base.

Substituições por escopo usam DELETE + INSERT na mesma transação, por
`_replace_scope(tabela, linhas, COLUNA=valores)`. `None` deixa a coluna sem
restrição; sem nenhuma restrição a tabela inteira é reconstruída por
`_delete_all`. Coleções vazias representam escopo vazio.
Linhas fora do escopo são rejeitadas. O motor publica os agregados relacionados
na mesma transação. Falhas da retenção do log são isoladas por savepoint.

Sinais de discovery/SSH/GNSS usam `dbHandlerBKP.host_update_signals` em
`BPDATA.HOST`. Os leitores enxergam essa fonte diretamente, sem lock curto ou
cópia de snapshot. `HOST_EQUIPMENT_LINK` concentra somente o cruzamento
automático dos cadastros de hosts e equipamentos, reconstruído a cada refresh.
Não há tabela nem métodos de manipulação de overrides manuais no PostgreSQL.
O singleton continua sendo advisory lock de sessão reutilizável. O worker
captura o watermark antes do reconcile e remove somente eventos cobertos por
ele, preservando invalidações recebidas durante a reconstrução.

O worker e os escritores diretos recebem `PG_CONNINFO` da configuração, com
sobrescrita opcional por `RFFUSION_PG_CONNINFO`. Não há fallback MariaDB nem arquivo
novo de credenciais. Configurar esse valor não converte os consumidores legados;
validar a instalação nova antes de iniciar processos.

Somente `WEBFUSION_USAGE_LOG_CHECKPOINT` e `WEBFUSION_USAGE_METRIC_MONTHLY`
precisam de restore do Summary, sempre juntas. Nenhuma operação de rebuild as
escreve. Full reconcile não adapta SQL do WebFusion: seus consumidores precisam
usar o novo modelo antes da troca de ambiente.

```bash
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/db -p 'test_dbhandler_summary_postgres*.py' -v
/opt/conda/envs/appdata/bin/python -m unittest discover -s test/tests/workers -p 'test_*summary*.py' -v
```
