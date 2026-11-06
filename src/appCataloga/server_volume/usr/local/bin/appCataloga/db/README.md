# Camada de banco PostgreSQL

`dbHandlerBase.py` contém `DbHandler`, base dos handlers PostgreSQL.
`dbHandlerBKP.py` implementa o domínio operacional de hosts, filas e histórico.
O código MariaDB movido para `backup/` permanece como referência, sem alteração.
Os handlers RFM/Summary e as conexões dos workers ainda precisam ser migrados;
as classes novas não são substitutas diretas da interface de conexão MariaDB.

O contrato está na [arquitetura do appCataloga](../../../../../../.instructions/ARCHITECTURE.md#23-base-postgresql-e-migração-por-lotes).
Os métodos internos da base concentram composição de SQL, validação de campos,
filtros e ordenação, sem um módulo separado de SQL genérico.
O tamanho padrão dos lotes usa `DB_BATCH_SIZE` na
[configuração central do appCataloga](../../../../../etc/appCataloga/config.py),
importada como `config as k`. Não há arquivo de configuração próprio em `db/`.

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

Uma subclasse declara `VALID_FIELDS_<TABELA>` como um conjunto (`set[str]`) e recebe
schema, logger e `conninfo` explícitos. O banco de conexão é separado do schema;
a instalação do projeto usa `dbname=postgres`. A base não usa as credenciais
MariaDB da configuração central para conectar, não abre conexão no construtor
e não grava arquivos de credenciais. O carregamento atual da configuração
central conserva seu mecanismo de leitura de `.secret`; a revisão da
autenticação permanece prevista para o final da Fase 1.

CRUD usa nomes de tabelas sem qualificação e sem aliases. A base qualifica pelo
schema e delimita identificadores, preservando maiúsculas. Valores são passados
separadamente, por parâmetros. Colunas desconhecidas são rejeitadas, sem descarte
silencioso. Permissões por operação pertencem aos métodos públicos e ao banco.
A delimitação de nomes usa `sql.Identifier`, sem regex própria. Cada nome é
tratado como um identificador literal; as declarações de campos definem o que
o handler aceita.

As declarações de campos representam o contrato suportado pelo handler e devem
ser atualizadas junto das migrações das tabelas. A base aceita conjuntos simples
e imutáveis, sem exigir `frozenset` e sem armazenar um cache dessas declarações.
Não há descoberta automática para liberar acesso a novas colunas do banco.

| Operação | Individual | Em lote |
|---|---|---|
| SELECT | `_select_row(table, where)`: dicionário ou `None`, por chave primária completa. | `_select_batch(table, where)`: lista dos registros filtrados. |
| INSERT | `_insert_row(table, data)`: dicionário pedido por `returning`, ou `None`. | `_insert_batch(table, data)`: contagem de inserções. |
| UPDATE | `_update_row(table, data, where)`: altera o registro identificado pela chave primária. | `_update_batch(table, data, where)`: aplica os mesmos valores a todos os registros filtrados. |
| UPSERT | `_upsert_row(table, data, conflict_columns=...)`: contagem de inserção/atualização. | `_upsert_batch(table, data, conflict_columns=...)`: contagem total afetada. |
| DELETE | `_delete_row(table, where)`: exclui o registro identificado pela chave primária. | `_delete_batch(table, where)`: exclui o conjunto filtrado. |

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
| `for_update` | Em SELECT em lote, bloqueia as linhas selecionadas até o fim da transação explícita do chamador; rejeita uso sem essa transação. |

Leitura, atualização e exclusão individuais exigem `PRIMARY_KEY_<TABELA>`
declarada no handler como tupla ordenada de nomes, correspondente à chave primária
real do banco. O argumento `where` deve conter exatamente esses campos com valores
não nulos, sem operadores ou filtros adicionais. A declaração pode representar
uma chave composta. Por exemplo:

```python
VALID_FIELDS_HOST = {"ID_HOST", "NA_HOST_NAME"}
PRIMARY_KEY_HOST = ("ID_HOST",)
```

UPDATE e DELETE em lote exigem filtros não vazios. `batch` descreve uma operação
sobre um conjunto; essas duas variantes usam uma instrução SQL para o conjunto.
INSERT e UPSERT em lote aceitam linhas homogêneas e alinham os valores pelas
colunas da primeira linha, independentemente da ordem dos demais dicionários.
Todos os blocos participam da mesma transação. INSERT em lote retorna contagem;
obtenha IDs gerados por `RETURNING` nas inserções individuais dentro de uma
transação explícita quando necessário.

UPSERT usa `ON CONFLICT` com chaves explícitas, sem a convenção MariaDB de duas
linhas afetadas por atualização. `columns` define a projeção ordenada de SELECT;
sua ausência usa os campos declarados em ordem alfabética.

- `_select_raw`, `_execute` e `_execute_batch`: aceitam objetos
  `psycopg.sql.Composable` de confiança e valores separados. JOINs, expressões
  e conflitos baseados em expressões de índice ficam no SQL do handler concreto.

`_execute` centraliza execução de uma instrução; `_execute_batch` centraliza
execução de lotes atômicos. `_build_insert_query` compartilha o INSERT entre
as variantes individuais, em lote e UPSERT. `_upsert_row` delega ao método
`_upsert_batch` com uma linha; a cláusula de conflito é construída nesse método,
sem construtor separado de UPSERT. A operação individual também usa a transação
do executor de lotes quando não há transação do chamador. Esses mecanismos
não são operações de domínio para uso direto por workers.

Cada operação de INSERT/UPSERT resolve a tabela e valida as colunas uma vez.
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
sempre por uma lista fechada de opções. A base não aceita
`#CUSTOM#`, `__expr`, aliases ou trechos SQL na estrutura de filtros.

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
A configuração dos processos continua pendente; não iniciar workers com a
interface antiga esperando conversão automática.

- Métodos públicos seguem `entidade_ação_qualificação`, organizados no arquivo
  em blocos HOST, HOST_TASK, FILE_TASK e FILE_TASK_HISTORY. Helpers exclusivos
  acompanham a entidade; persistência compartilhada e publicação ficam no final.
  Os chamadores usam os mesmos nomes, sem aliases para a nomenclatura anterior.
  Leituras de tasks com JOIN
  preservam as chaves `HOST__*`, `HOST_TASK__*` e `FILE_TASK__*`; a seleção de
  arquivos retorna `(row, host_id, task_id)`. `host_task_read` retorna o dicionário
  e `host_filter` decodificado, incluindo JSON nativo do PostgreSQL.
- `VALID_FIELDS_*` e `PRIMARY_KEY_*` declaram as quatro tabelas operacionais.
  Flags são gravadas como 0/1, conforme as colunas `smallint` existentes.
- INSERT/UPSERT de arquivos usa os índices únicos por host e hashes de caminho
  e nome. O conflito atualiza os valores enviados, incluindo timestamps explícitos.
  Não há inferência de IDs ou contagem MariaDB de duas linhas por atualização.
- Atualizações condicionais usam `expected_status` para claims. Reservas de host
  usam UPDATE condicional; liberações seguras conferem PID e timestamp observados
  para preservar uma reserva adquirida após a leitura.
- Operações compostas abrem transação apenas quando o chamador não possui uma.
  Falhas propagam e transações externas continuam sob responsabilidade do chamador.
  Campos desconhecidos são rejeitados. Uma descoberta ausente retorna `None`;
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
- O próprio BKP publica invalidações de host/mês no outbox. Em conflitos, renova
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
método. `HOST_TASK_CONNECTIVITY_TYPES` centraliza os tipos que dependem do host.
Seleções compõem apenas templates controlados; nomes usam `sql.Identifier` e
valores seguem por binding. Opções de prioridade não recebem fragmentos SQL.
Operações representáveis pelo CRUD usam a base, inclusive a listagem de
conectividade e a leitura com lock do backlog. SQL específico permanece nos
JOINs, agregações, expressões de atualização, relógio do banco e conflitos
baseados em hashes, com parâmetros separados dos identificadores.

## Validação

A dependência Psycopg está fixada no
[ambiente do container](../../../../../../../../install/appCataloga/linux/environment.yml).
Executar as suítes PostgreSQL da base e do BKP enquanto RFM/Summary permanecerem
em `backup/`; a suíte MariaDB legada não valida a implementação nova.

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
