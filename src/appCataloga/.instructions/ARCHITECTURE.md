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
| Shared `shared/` | Utilitários, erros, logs e objetos de valor | Nunca entrypoints, domínio ou handlers; pode importar os dados `constants` do pacote `db` |
| Banco `db/` | SQL, conexões e transações | Shared; nunca domínio ou entrypoints |

Uma camada inferior não importa uma superior. A exceção dos dados `db.constants`
permite compartilhar códigos persistidos de tasks sem importar handlers ou
drivers: o pacote apenas carrega o dicionário de `db/constants.json`.
Entrypoints não contêm SQL;
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

O fluxo convencional de sites fixos usa `db.constants["rfm"]["sites"]["fixed_type"]`
em `DIM_SPECTRUM_SITE.FK_TYPE`. O bootstrap cadastra `DIM_SITE_TYPE.ID_TYPE = 1`
como `Fixed` e `ID_TYPE = 2` como `Mobile`; instalações anteriores usam
`ensureSiteTypes.sql`, que rejeita um significado diferente para esses IDs,
sem reclassificar sites. Rótulos legados com aspas simples externas são aceitos
pelo reparo; novas linhas usam os rótulos sem aspas no conteúdo.
`backfillFixedSiteType.sql` preenche apenas sites
legados sem classificação. Não aplicar o insert de sites fixos a dados móveis.

### 2.2 Acesso e transações

- A camada PostgreSQL usa Psycopg 3, conforme a seção 2.3. O código MariaDB
  movido para `db/backup/` é referência da migração, sem integração automática
  com a nova base. Não misturar drivers em uma mesma operação.
- Chamadores usam métodos públicos de handlers concretamente tipados.
  `_select_row`, `_select_batch`, `_insert_row`, `_select_raw` e similares são protegidos para
  implementação dos handlers específicos, sem uso direto pelo domínio.
  Uma consulta nova exige método público com nome da operação de domínio.
- Escritas relacionadas em duas ou mais tabelas usam `begin_transaction()`,
  `commit()` e `rollback()` em falha, preservando o resultado atômico.
- Atualização/exclusão de `FILE_TASK` e atualização de `FILE_TASK_HISTORY`,
  inclusive FROZEN, são sempre atômicas. Não deixar fila e histórico divergentes.
- Locks MariaDB são vinculados à conexão. Um lock mantido durante o processo
  exige preservar a conexão proprietária até a liberação.

### 2.3 Base PostgreSQL e migração por lotes

`db/dbHandlerBase.py` define `DbHandler`, com Psycopg 3.3.8. A migração começa
pela base; os handlers de domínio e seus chamadores serão convertidos em lotes
próprios. A existência da base não torna os workers executáveis em PostgreSQL.

A API pública de ciclo de vida é `connect()`, `close()`, `begin_transaction()`,
`commit()`, `rollback()` e a propriedade `in_transaction`. `connect()` pode ser
chamado no início do processo para validar a sessão; operações de banco também
o chamam sob demanda. Retorna a sessão administrada pelo handler, sem transferir
sua propriedade nem autorizar SQL ou controle transacional direto nos chamadores.
Os métodos de CRUD e execução com `_` são protegidos para subclasses. Construtores
SQL, validações, gerenciamento de cursor e relato de falhas são detalhes internos
da base, sem contrato de uso externo. No arquivo, manter a API pública primeiro,
seguida das operações de persistência e dos detalhes de implementação.

A validação de tipos ocorre nas fronteiras que recebem dados externos.
Métodos internos e protegidos confiam nas assinaturas tipadas, sem repetir
`isinstance` para cada parâmetro. Mantêm verificações semânticas de nomes,
duplicação, operadores, limites e estado transacional.
Valores de filtros tipados como `object` exigem validação de formato conforme
o operador, sem enfraquecer os demais contratos para `Any`.

A interface protegida de persistência tem pares `_select_row`/`_select_batch`,
`_insert_row`/`_insert_batch`, `_update_row`/`_update_batch`,
`_upsert_row`/`_upsert_batch` e `_delete_row`/`_delete_batch`.
As assinaturas usam `table` primeiro, depois `data` nas escritas e `where`
quando houver seleção de registros. INSERT/UPSERT individuais recebem um
mapeamento em `data`; suas variantes em lote recebem uma sequência de
mapeamentos com o mesmo nome. UPDATE em lote recebe um único mapeamento de
atribuições aplicado ao conjunto filtrado. Opções são nomeadas após `*`:
`columns` para projeção, `skip_duplicates` para INSERT com `ON CONFLICT DO NOTHING`
e `conflict_columns` para o alvo de conflito do UPSERT.
`batch_size` controla apenas os blocos de INSERT/UPSERT em lote. SELECT usa
`limit`; UPDATE/DELETE em lote executam uma instrução para o conjunto filtrado.
Leitura, atualização e exclusão `row` recebem filtros explícitos preparados
pelo método de domínio, que deve identificar no máximo um registro, por ID,
chave composta ou outra identidade única. A base não declara nem descobre
chaves primárias e não valida se o filtro corresponde a uma constraint.
SELECT individual usa `LIMIT 1`; UPDATE e DELETE executam o filtro fornecido,
sem limitador ou verificação de cardinalidade. Filtros vazios são rejeitados.
A identidade e as condições adicionais da operação pertencem ao domínio;
constraints e erros de integridade pertencem exclusivamente ao PostgreSQL.

SELECT em lote aceita filtros e retorna uma lista. INSERT e UPSERT em lote
recebem linhas com o mesmo conjunto de colunas e preservam atomicidade entre
todos os blocos. UPDATE em lote aplica os mesmos valores ao conjunto filtrado;
DELETE em lote remove o conjunto filtrado. Ambos exigem filtros explícitos.
INSERT em lote retorna contagem; IDs gerados são obtidos por `RETURNING` na
operação individual, sem inferência por sequência.

`_execute` e `_execute_batch` centralizam execução parametrizada, cursores e
atomicidade dos lotes, incluindo SQL específico ainda não convertido. Para
SELECTs relacionais simples, `_select_batch` aceita um INNER JOIN por colunas
declaradas e `distinct=True`. Projeções e ordenação mantêm validações estruturais; valores permanecem
parametrizados. A existência de tabelas e colunas é validada pelo PostgreSQL.
Um handler pode ler tabelas de outro schema declarando `SCHEMA_<TABELA>`;
o Summary usa isso para as fontes de BPDATA e RFDATA. SELECT sem projeção usa
`*` da tabela principal. Leituras de filas podem projetar `*` das duas tabelas
e prefixar os nomes pelos metadados do resultado, preservando tipos nativos
e o snapshot único da consulta, sem consultar ou armazenar o catálogo.
`_delete_all` é a única exclusão sem filtro, reservada à reconstrução global.
`_select_aggregate` aceita apenas COUNT, SUM, MIN e MAX, cada um com filtro opcional
na mesma sintaxe de mapeamento, agrupamento por colunas explícitas e por mês de
uma coluna temporal; não é linguagem de expressões. Parâmetros de sessão
(`_setting_read`, `_setting_write`, `_local_setting`) e advisory locks
(`_advisory_lock_try`, `_advisory_unlock`) também pertencem à base.
O contrato usa sequências e mapeamentos, sem classes ou linguagem geral de
expressões. Consultas que exigem prioridades e subconsultas correlacionadas
conservam SQL específico no handler, com identificadores delimitados e valores
separados. `_select_raw` executa essas consultas internas.
`_select_nearest_point` concentra na base a seleção espacial do ponto mais
próximo: recebe tabela, coluna espacial, coluna de identidade e coordenadas;
retorna identidade e distância esférica em metros, ou `None` sem registros.
Preserva o SRID do ponto armazenado e desempata pela identidade crescente.
O RFM aplica o limite de distância de `db.constants`, sem compor SQL nessa busca.
`_build_insert_query` compartilha a construção do
INSERT entre as operações individuais, em lote e UPSERT. `_upsert_row` delega
a `_upsert_batch` com uma linha; a cláusula de conflito é construída no próprio
método de lote. A execução isolada de UPSERT individual também usa a transação
do executor de lotes; uma transação já existente permanece sob controle do
chamador. `_prepare_batch_values` valida a homogeneidade e alinha valores às colunas.
Cada operação resolve a tabela e valida a estrutura das colunas antes do INSERT.
O construtor recebe o identificador qualificado da tabela e a sequência de colunas
já validadas e retorna apenas a instrução com placeholders. Os valores são
alinhados separadamente, sem executar SQL, abrir cursores ou resolver transações
durante a construção. Métodos individuais reutilizam os construtores de
filtros e a execução das operações em lote, sem contratos de schema paralelos.

- A conexão recebe `conninfo` explícito, incluindo `dbname`, sem usar as
  credenciais MariaDB da configuração central para conectar. Destinos e
  credenciais de banco pertencem ao `config.py`, importado como `k`. Constantes
  de persistência, como `persistence.batch_size`, pertencem a `db/constants.json`,
  carregado uma vez em `db/__init__.py`, sem conexão, fallback ou SQL.
  O mecanismo atual de leitura de `.secret` permanece até a revisão prevista
  de autenticação. Não registrar conninfo, SQL, parâmetros nem
  mensagens de erro que possam conter dados sensíveis. Não criar arquivos de
  credenciais. A avaliação de `peer` permanece no final da Fase 1.
- O schema é explícito e separado do banco de conexão; identificadores usam
  `psycopg.sql.Identifier`. Resultados são dicionários com a caixa preservada.
  A existência de tabelas e campos pertence ao DDL e é verificada pelo
  PostgreSQL; não há lista de colunas nem regex adicional para nomes. Cada nome é delimitado como um identificador literal,
  sem interpretar aliases, expressões ou pontos como fragmentos SQL.
- O logger recebido tem o tipo `shared.logging_utils.log`; a camada de banco
  não redefine sua interface. Ela informa apenas contexto e classificação de
  falhas SQL, sem duplicar formatação, destinos ou políticas do logger.
- A própria base concentra os métodos internos de validação de nomes e campos,
  construção de filtros, ordenação e UPSERT. Não há módulo separado de SQL genérico.
- Handlers não duplicam o DDL em listas `VALID_FIELDS_*`. Colunas e tabelas
  inexistentes geram os erros originais do PostgreSQL, que propagam ao chamador.
  Projeções explícitas descrevem apenas o resultado necessário à operação;
  não autorizam campos de outras operações. Permissões de escrita pertencem
  aos métodos públicos do domínio e aos privilégios SQL. Não manter declarações
  `PRIMARY_KEY_*` nos handlers: a chave primária é definida apenas no DDL.
- CRUD genérico é interno. UPDATE aceita, por coluna, substituição, delta
  numérico (`increments`) ou avanço monotônico (`maximums`, via `GREATEST`),
  aplicados atomicamente na mesma instrução. JOINs simples são compostos pela base;
  não portar parser de SQL, linguagem de expressões ou `#CUSTOM#` legado.
  SQL específico necessário para prioridades correlacionadas e escritas mantém
  valores parametrizados; não o usar quando o CRUD representar a operação.
- Cada operação isolada usa autocommit; operações em lote são atômicas.
  `begin_transaction`, `commit` e `rollback` controlam transações explícitas.
  Operações internas nunca encerram a transação do chamador. CRUD e executores
  não recebem argumento `commit`: o estado estabelecido por `begin_transaction()`
  define a participação na transação. Sem transação explícita, uma escrita
  individual usa autocommit e um lote cria sua própria transação atômica.
- Falhas SQL propagam. Uma transação abortada exige rollback; não recuperar
  silenciosamente a sessão. Não reconectar nem repetir SQL automaticamente:
  perda de sessão pode invalidar locks e tornar o resultado do commit incerto.
- A base não decide invalidações: `_publish_summary_scopes` apenas grava no
  outbox, sob savepoint e sem encerrar a transação do chamador, os pares
  `(tipo, valor)` escolhidos pelo handler. O BKP publica host/mês (seção 2.4) e o
  RFM publica site e equipamento. A reconciliação periódica continua sendo a
  recuperação de falhas de publicação. `_unit_of_work` também pertence à base:
  abre transação apenas quando o chamador não tem uma. `_savepoint` isola
  escritas opcionais (outbox, retenção do log do Summary): dentro de transação
  externa usa savepoint; fora dela, uma transação curta própria.
- IDs são obtidos por `RETURNING`, sem inferir intervalos de sequências.
  Contagens de UPSERT seguem PostgreSQL, sem o código MariaDB de duas linhas
  afetadas para uma atualização.

Conservar o conteúdo de `db/backup/` nesta etapa, conforme solicitado para a
migração. Ele não é uma segunda implementação suportada pelo runtime novo.

O schema RFDATA usa `public.geometry(Point)` do PostGIS para manter o tipo
espacial e os SRIDs de origem. A conversão de localização deve preservar a
distância esférica em metros e parametrizar coordenadas. O restore futuro deve
verificar os SRIDs e a ordem de coordenadas antes de qualquer conversão. Os
métodos RFM preservam os contratos de sites, dimensões, espectros, arquivos e
linhagem. Usam o CRUD e a seleção de proximidade da base quando aplicáveis;
as demais consultas espaciais e de integridade que não cabem no CRUD são
internas, com parâmetros separados e
identificadores delimitados. Escritas relacionadas e invalidações do Summary
compartilham a transação, com falha do outbox isolada por savepoint. Os workers
recebem a conexão explícita conforme a seção 6; o fluxo com serviços externos
continua exigindo validação funcional.

Na resolução SITE por espectro, erros do Psycopg propagam como falha do arquivo,
inclusive violações de integridade. Não podem ser convertidos em descarte de
espectros por defeito de localidade nem no erro de filtragem vazia.

A interface pública do pacote `appAnalise/` é o que o worker e os outros
módulos importam: `processing_bin.should_export` e `run_processing_flow`;
`AppAnaliseConnection` (`check_connection_with_log`, `check_connection`,
`process` e os diagnósticos `last_*`); e, no parser,
`canonicalize_equipment_identifier` e os métodos `validate_source_file`,
`detect_protocol_error`, `resolve_output_file` e `normalize_payload`. O restante
é privado, com prefixo `_`, e cada módulo apresenta a interface antes dos helpers.

`appAnalise/processing_bin.py` mantém o cache de pontos e as amostras GNSS
locais a cada chamada de `_resolve_spectrum_sites`. A resolução geográfica recebe
explicitamente o acumulador de amostras; não depende de estado global nem muda
de comportamento conforme o chamador. Pontos repetidos no arquivo são resolvidos
uma vez. Pontos distintos associados ao mesmo site contribuem para uma única
atualização de centroide, após a resolução dos espectros. Um site novo já recebe
suas amostras no insert, sem reaplicá-las na atualização.

Somente `ValueError` de resolução de localidade permite descarte do espectro.
Falhas de serviço, persistência e programação propagam pelo tipo da exceção,
sem classificação pelo texto da mensagem. O descarte mantém a ordem dos
espectros válidos; um arquivo sem espectros válidos falha.

Essa política resolve sites fixos do fluxo atual de coleta. A associação
continua por espectro, sem impor um único site ao arquivo. O cadastro no Zabbix
e o texto de `method` não determinam mobilidade. Dados de drive test exigem
política própria de localização; não recebem automaticamente
o tipo fixo nem a atualização de centroide dessa política.

### 2.4 Handler BKP PostgreSQL

`db/dbHandlerBKP.py` implementa o domínio operacional sobre `DbHandler`.
O argumento `database` do construtor identifica o schema, preservando o nome
usado pelos chamadores; `conninfo` é obrigatório e seleciona o banco PostgreSQL.
Não usar configuração ou credenciais MariaDB como fallback. A conexão dos
workers segue a seção 6; a criação deste handler não os ativa.

- Métodos públicos usam entidade, ação e qualificação: `host_*`, `host_task_*`,
  `file_task_*` e `file_task_history_*`. `list` retorna coleções; `read` seleciona
  um registro. Exemplos: `host_task_list`, `host_task_enqueue`, `file_task_read`,
  `file_task_list_for_backlog` e `file_task_history_filter_existing_batch`.
  Renomear os chamadores junto da implementação, sem aliases concorrentes.
  Refresh operacional usa `host_request_summary_refresh`, sem criar tarefas
  para recalcular estatísticas no cadastro de hosts.
- Organizar o handler em blocos HOST, HOST_TASK, FILE_TASK e FILE_TASK_HISTORY.
  Cada bloco reúne métodos públicos e helpers específicos da entidade; suporte
  compartilhado entre entidades e publicação do Summary ficam no final. Essa
  organização é própria do BKP; a ordem da base continua definida na seção 2.3.
- Janitors pertencem a `host_handler/host_maintenance.py`: esse domínio verifica
  PIDs, aplica TTLs, decide recuperação e solicita novas checagens. O BKP fornece
  snapshots e a liberação condicional por PID/timestamp observado, sem executar
  o ciclo de manutenção. Tipos e estados consultados são argumentos explícitos.
- O worker `appCataloga_backlog_management.py` interpreta os filtros, define a
  ordenação e escolhe o prefixo que cabe no orçamento. Ele controla a transação
  entre leitura com lock, escrita e publicação. O BKP recebe filtros, dados e
  opções tipadas já preparados; não instancia `Filter` nem decide volume ou
  transições de backlog. Não repetir a publicação depois do commit.
- Usar o CRUD da base nas operações que ele representa. Passar seus argumentos
  por nome e preparar dados e filtros em variáveis antes da chamada. SQL
  específico necessário para escritas, agregações, conflitos por expressão e
  seleção justa/reservas de discovery mantém valores parametrizados. Os JOINs
  simples de tasks usam a base; o snapshot do janitor usa SELECTs separados.
  A base oferece `trim_ne` (TRIM seguido de desigualdade), `ilike`, ordenação
  com `NULLS FIRST`/`NULLS LAST` e `for_update=True` em SELECT de conjunto.
  O lock de leitura exige transação explícita e dura até seu commit ou rollback.
- Preservar as chaves `HOST__*`, `HOST_TASK__*` e `FILE_TASK__*` dos selects
  com JOIN, a tupla de seleção de arquivos e os dicionários de atualização.
  `FILTER` usa JSON nativo na persistência; a seleção de host tasks fornece
  também `host_filter` normalizado.
- A identificação de registros nas operações individuais vem dos métodos
  de domínio; chaves primárias e existência de colunas pertencem ao PostgreSQL.
  Flags continuam `smallint` conforme o schema existente; gravar 0/1 e
  comparar inteiros no SQL.
- A identidade natural de arquivos corresponde aos índices únicos existentes:
  host e SHA-256 do caminho/nome. O UPSERT específico usa essas expressões em
  `ON CONFLICT`; a base genérica continua limitada a colunas de conflito.
  Timestamps fornecidos pelo chamador são preservados no conflito.
- Operações compostas abrem uma transação quando isoladas e participam daquela
  já aberta pelo chamador. Nunca finalizam uma transação externa. Erros propagam;
  os janitors do domínio podem registrar falhas individuais e continuar os demais itens.
- Seleção seguida de reserva usa atualização condicional de `HOST.IS_BUSY`,
  respeitando PID, cooldown e elegibilidade. Claims de tasks continuam usando
  estado esperado; a seleção não finaliza a task em nome do entrypoint.
  A composição das seleções só aceita templates SQL controlados e identificadores
  delimitados. Valores de filtro, inclusive extensão, seguem separados por binding.
  Opções de prioridade nunca são tratadas como fragmentos SQL recebidos do chamador.
- Retomadas de HOST_TASK e FILE_TASK mantêm seus filtros visíveis nos respectivos
  métodos, sem um dispatcher que escolha regras de negócio pelo nome da tabela.
  Tipos de host tasks dependentes de conectividade usam
  `db.constants["tasks"]["connectivity_host_types"]`.
- O BKP escolhe escopos host/mês e os grava pela base no outbox. O UPSERT
  renova também `ID_OUTBOX` em conflitos para preservar a leitura por checkpoint.
  Em transação externa, a publicação usa savepoint, participa do mesmo commit e
  pode falhar sem abortar as escritas operacionais. Falhas de publicação geram
  aviso sanitizado; a reconciliação periódica permanece a recuperação prevista.
- Filtros de backlog são traduzidos para predicados parametrizados; não executar
  `extra_sql` ou expressões SQL recebidas por kwargs. Respeitar os filtros
  originais também na gravação do prefixo selecionado pelo worker.
- Imports de compatibilidade de `shared/__init__.py` são resolvidos sob demanda.
  Importar utilitários de shared não carrega domínio, entrypoints ou handlers
  analíticos ainda não migrados. Os nomes de compatibilidade são preservados.

### 2.5 Handler Summary PostgreSQL

A estrutura física e a reconstrução seguem o
[modelo PostgreSQL do Summary](../../../docs/architecture/SUMMARY_POSTGRESQL.md).
O handler usa métodos por entidade, como o BKP, e `DbHandler` para sessões,
transações e CRUD. SQL protegido não é API do domínio. Não manter os métodos
genéricos públicos de escrita da versão MariaDB.
Sinais operacionais pertencem ao BKP. Não copiá-los ao Summary.
O cruzamento host/equipamento é exclusivamente automático e fica em
`HOST_EQUIPMENT_LINK`, reconstruído a partir dos cadastros de hosts e equipamentos,
sem tabela ou API de overrides manuais, conforme o modelo PostgreSQL.
Seu contexto é exclusivo das consultas do WebFusion. O worker do Summary apenas
materializa esse modelo de leitura; coleta, filas e processamento do appCataloga
não dependem do vínculo, conforme o [contrato compartilhado](../../../docs/architecture/SUMMARY_POSTGRESQL.md#cruzamento-para-o-webfusion).
O motor de refresh e seu worker usam a API nova; os leitores e escritores
diretos também foram ajustados. `RFFUSION_PG_CONNINFO` fornece conninfo explícito.
A validação do fluxo com estações e appAnalise e a integração dos consumidores
WebFusion continuam sendo lotes próprios; esta implementação não os ativa.

## 3. Categorias de processo

| Categoria | Arquivos | Controle |
|---|---|---|
| Gateway | `appCataloga.py` | Selector TCP; despacha requisições para filas, sem claim |
| Task worker | `appCataloga_host_check.py`, `appCataloga_discovery.py`, `appCataloga_backlog_management.py`, `appCataloga_file_bkp.py`, `appCataloga_file_bin_process_appAnalise.py` | Lê, faz claim e finaliza uma task |
| Manutenção | `appCataloga_host_maintenance.py`, `appCataloga_garbage_collector.py`, `appCataloga_summary_database.py` | Temporizador ou cursor/checkpoint, sem claim de task |
| Utilitários | `utils/bootstrap_paths.py`, `utils/safe_stop.py` | Suporte aos entrypoints e parada segura; não são daemons |

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
| Probes e políticas dos janitors | `host_handler/host_maintenance.py` |
| Filtros, orçamento e transições de backlog | `appCataloga_backlog_management.py` |
| Processamento e persistência analítica appAnalise | `appAnalise/processing_bin.py` |
| Parsing de payload | `appAnalise/payload_parser.py` |
| Conexão com serviço appAnalise | `appAnalise/appAnalise_connection.py` |
| Artefatos, caminhos e nome de arquivo no servidor | `shared/file_utils.py` |
| Filesystem da coleta de lixo | `gc_handler/gc_maintenance.py` |
| Materialização do summary | `summary_handler/refresh_engine.py` |
| Infraestrutura de processos, sinais, pool, socket e sleep | `server_handler/` |
| Erros, logs, metadados, filtros e geocoding | `shared/errors.py`, `logging_utils.py`, `file_metadata.py`, `filter.py`, `geolocation_utils.py` |
| Constantes compartilhadas e sentinelas | `shared/constants.py` |
| Constantes de persistência, filas e consultas | `db/constants.json`, lido em `db/__init__.py` |
| Configuração e constantes do aplicativo | `config.py` |
| SQL operacional PostgreSQL | `db/dbHandlerBKP.py` |
| SQL analítico e Summary PostgreSQL | `db/dbHandlerRFM.py`, `db/dbHandlerSummary.py` |
| Conexão e SQL genérico | `db/dbHandlerBase.py` |

Uma nova extração de módulo exige decisão arquitetural explícita.

## 6. Tipos e configuração

Os entrypoints e `utils/safe_stop.py` passam `k.PG_CONNINFO` aos handlers
PostgreSQL. O `config.py` compõe a conexão com `SERVER_NAME`, `DB_PORT`,
`PG_DATABASE_NAME` e `PG_USER_NAME`; `RFFUSION_PG_CONNINFO`, quando não vazio,
sobrescreve essa conexão completa. O destino padrão explícito é o banco
`RFFUSION` em `10.88.0.35:5432`, conforme o contrato PostgreSQL compartilhado.
O DEV existente seleciona `rffusion_test_migracao` por conexão explícita,
sem inferência por branch ou troca
automática de backend. O `.secret` existente pode fornecer `DB_USER_NAME_PG` e
`DB_PASSWORD_PG`, específicos do PostgreSQL. `PGUSER` e `PGPASSWORD` têm prioridade
sobre esses campos na conexão composta; a sobrescrita completa por
`RFFUSION_PG_CONNINFO` não recebe credenciais adicionais do `.secret`. Não
reutilizar `DB_USER_NAME`/`DB_PASSWORD` legados nem persistir credenciais na
configuração de debug. A limpeza de shutdown também
usa conexão explícita e fecha sua sessão em sucesso ou falha. A existência
dessas conexões não inicia serviços nem valida integrações externas.
O stop seguro lê filas RUNNING e hosts BUSY/OFFLINE por métodos públicos do BKP;
não usa CRUD protegido nem SQL no utilitário. Ao retornar uma task a PENDING,
confirma o estado RUNNING esperado e limpa seu PID. A limpeza de arquivos TMP
continua restrita à execução após a parada de todos os workers.

Tipar handlers com `dbHandlerBKP`, `dbHandlerRFM` ou `dbHandlerSummary`, nunca
`Any`. Distinguir o dicionário bruto retornado pelo banco do contexto normalizado
pelo worker; os tipos devem refletir as chaves efetivamente disponíveis.

Importar configuração de conexão e de execução com `import config as k`.
Constantes de banco usam `from db import constants as db_constants` nos
consumidores e `from . import constants as db_constants` nos handlers. Valores
persistidos de tipo/estado de task, escopos e estados do Summary, sentinelas de
reserva, filtro neutro, limites de consultas e definições estáveis dos handlers
ficam apenas em `db/constants.json`; não os reexportar pelo `config.py`.
O JSON é organizado por responsabilidade: `persistence` (lotes e unidades),
`tasks` (estados, tipos, filtro padrão e proteção de filas), `hosts` (reservas e
sinais), `rfm` (sites e identidade de espectros) e `summary` (limites, estados,
escopos, erros e métricas). Dentro desses grupos, usar nomes curtos em
`snake_case`; nomes SQL de tabelas e colunas conservam sua caixa.
Os consumidores acessam diretamente o dicionário, por exemplo
`db_constants["tasks"]["states"]["pending"]`. Não redeclarar cada chave em
Python, criar classes de configuração, aliases antigos ou validadores paralelos.
Referências a estados e tipos são nomes relativos aos grupos `tasks.states`,
`tasks.host_types` ou `tasks.file_types`, resolvidos pela operação consumidora.
Falhas de leitura e de JSON propagam na importação; não usar valores padrão
silenciosos. Não há recarga dinâmica: alterações exigem reinício dos processos.
As métricas operacionais ficam em `summary.host_metrics`, agrupadas por fonte
(`queue`/`history`) e fase. Cada fase declara sua coluna de estado, o tipo de
task quando necessário e a lista de pares de contagem/volume. Cada par declara
o estado e, opcionalmente, a coluna do intervalo mensal. Os nomes de saída são
explícitos; não gerar nomes combinando fases e sufixos. Essas definições
descrevem consultas, sem espelhar ou autorizar o schema SQL.

O arquivo JSON não contém credenciais, descrições completas das tabelas nem
validações de existência de colunas. Nomes de campos nas definições de consulta
especificam apenas a operação que os utiliza. SQL e seu vocabulário fechado
permanecem no Python. Configurações de workers, SSH, caminhos e appAnalise
continuam no `config.py`; destinos/schemas também são configuração de conexão.

| Categoria | Convenção |
|---|---|
| Tipos de task | `tasks.host_types`, `tasks.file_types` em `db/constants.json` |
| Estados persistidos | `tasks.states` em `db/constants.json` |
| Estágios de erro | `STAGE_*` |
| Caminhos | `*_FOLDER`, `*_SUBDIR` |
| Sentinelas legítimas | `*_UNLOCKED_*`, `NONE_*` |
| Limites de consultas e persistência | `db/constants.json` |
| Tempos operacionais e conexão | Nomes descritivos existentes em `config.py` |

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
`worker_lock_acquire`/`worker_lock_release` públicos e liberar recursos na saída.
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
