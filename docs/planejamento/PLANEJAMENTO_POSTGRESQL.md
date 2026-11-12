# Planejamento da migração para PostgreSQL

Situação: validação do fluxo completo com estações e appAnalise, adaptação dos
consumidores WebFusion e carga completa dos dados permanecem pendentes. O modelo do Summary
segue o [contrato PostgreSQL](../architecture/SUMMARY_POSTGRESQL.md).
A execução dos demais lotes não está autorizada por este documento.
O desenvolvimento ocorre no branch `postgres-sql`; alterações
em `main` e a entrada em produção exigem escopo próprio.

## Objetivo e premissas

Migrar o acesso a dados para PostgreSQL em duas fases: primeiro adaptar e
organizar o código preservando o modelo funcional; depois transferir os dados,
aplicar a taxonomia e revisar os modelos de leitura e seus consumidores.

O ponto de partida são os [scripts PostgreSQL](../../src/postgres/scripts/README.md),
que adaptam os dados de origem e usam o modelo canônico revisado para Summary.
Isso não comprova equivalência de consultas, concorrência ou desempenho.

O [contrato inicial PostgreSQL](../architecture/POSTGRESQL.md) define um único
banco de conexão, `RFFUSION`, com quatro schemas: `"BPDATA"`, `"RFDATA"`,
`"RFFUSION_SUMMARY"` e `"WEBFUSION"`. Identificadores preservam maiúsculas
com aspas duplas. `RFFUSION_SUMMARY` é um schema com vários modelos de leitura,
não uma única tabela.

Os contratos atuais ainda descrevem drivers e locks MariaDB. Antes de cada
implementação, registrar a decisão correspondente nos contratos afetados;
este plano não os substitui. Referências principais:

- [Arquitetura do appCataloga](../../src/appCataloga/.instructions/ARCHITECTURE.md).
- [Arquitetura do WebFusion](../../src/webfusion/.instructions/ARCHITECTURE.md).
- [Contratos públicos do SUMMARY](../architecture/SUMMARY.md).
- [Métricas operacionais e Zabbix](../architecture/METRICAS_OPERACIONAIS.md).
- [Regras de manutenção](../agents/INSTRUCTIONS.md) e [testes](../../test/AGENTS.md).

## Fase 1 — código compatível e organização da camada de dados

### 1.1 Inventário e decisões de compatibilidade

Levantar métodos públicos, chamadores, SQL e fronteiras de transação em
`appCataloga/db`, workers, handlers de domínio, WebFusion, API appAnalise,
ferramentas operacionais e acesso de debug pelo DBeaver. Registrar uma matriz com operação,
origem, consumidor, adaptação necessária e teste de aceitação.

Verificar os DDLs existentes contra as operações reais, especialmente:

- identidades, sequências, IDs retornados e inserções idempotentes;
- `INSERT IGNORE`, `REPLACE`, UPSERT e alvos de conflito;
- flags, JSON, binários, datas sem fuso, valores nulos e precisão numérica;
- caixa, acentos, collations, buscas textuais e unicidade de caminhos;
- consultas entre schemas, introspecção e tabelas temporárias;
- geografia: `point` nativo não reproduz automaticamente a semântica espacial
  do MariaDB;
- autocommit, transação abortada, isolamento, timeouts, deadlocks e locks de sessão.

O driver da base appCataloga é Psycopg 3.3.8, registrado na arquitetura e no
ambiente do container. Definir o driver e a instalação do WebFusion no seu
próprio lote. Completar a configuração de conexão dos consumidores, seleção de
schema, tipos de linhas e tradução de erros. A seleção do destino deve ser
explícita, sem inferência por branch ou fallback silencioso para outro banco.
Não assumir necessidade de manter dois backends dentro da aplicação: essa
decisão depende do ensaio e da estratégia de retorno.

Entrega: inventário de dependências, lacunas do DDL e decisões arquiteturais
necessárias à implementação, sem renomear o modelo nesta fase.

### 1.2 Classe base e handlers específicos

Escopo inicial:
[camada de banco do appCataloga](../../src/appCataloga/server_volume/usr/local/bin/appCataloga/db/README.md).

Proposta de responsabilidades:

| Componente | Responsabilidade proposta |
|---|---|
| `dbHandlerBase.py`, classe base `DbHandler` | Conexão PostgreSQL, cursores, transações, binding de valores, composição segura de identificadores e CRUD genérico interno. |
| Handler BKP | Hosts, filas, histórico e persistência das operações do domínio operacional. |
| Handler RFM | Dimensões, arquivos, sites, espectros e relações analíticas. |
| Handler Summary | SQL de projeções, consumo da outbox, checkpoints e persistência do refresh. |

A nova base é `DbHandler`; `DBHandlerBase` permanece na referência em `backup/`.
Atualizar imports, tipos e testes dos handlers específicos em seus lotes.
Workers e serviços continuam chamando métodos públicos dos handlers específicos;
não passam a montar CRUD ou SQL diretamente.

A publicação de invalidações de SUMMARY está na base legada em `backup/`.
Definir seu destino
arquitetural antes da extração, preservando o contrato dos publicadores BKP e
RFM, a atomicidade aplicável e a recuperação de falhas. A base proposta não
deve conhecer tabelas, escopos ou regras do SUMMARY.

Organizar por responsabilidade, sem uma meta artificial de redução de linhas:

- remover duplicações apenas após conferir diferenças semânticas e chamadores;
- eliminar código comprovadamente sem uso, incluindo descoberta dinâmica e scripts;
- tipar entradas, saídas e recursos do driver, evitando `Any` como substituto
  dos contratos conhecidos;
- completar docstrings de classes e métodos, incluindo formatos de dados,
  exceções e responsabilidade de commit/rollback;
- preservar logs estruturados, propagação de falhas e APIs públicas, salvo
  alteração explicitamente registrada;
- não criar um ORM próprio ou uma camada universal de consultas sem necessidade.

Implementar em lotes: base, BKP, RFM e Summary. Cada lote inclui seus testes e
adaptações indispensáveis dos chamadores. Evitar uma reescrita integral sem
validação intermediária.

#### Padrão de docstrings

Funções e métodos terão descrição inicial, `Args:` e `Returns:`, em inglês
simples, conforme as regras de documentação de código. Documentar significado
e tipo de cada entrada, parâmetros opcionais e formatos de dicionários e listas.
Declarar `None` quando não houver retorno útil e distinguir ausência de dados
de falha. Acrescentar `Raises:` para exceções do contrato e explicitar efeitos
de persistência e responsabilidade pelas transações. Classes documentam também
recursos administrados e `Attributes:` quando houver estado.

#### Modularização e funções semelhantes

Inventariar funções semelhantes entre base e handlers, comparando chamadas,
retornos, validações, SQL, locks e fronteiras de commit/rollback. Compartilhar
na base apenas mecanismos genéricos com o mesmo contrato; preservar operações
de domínio nos handlers específicos. Propor extrações por responsabilidade
coesa e registrar o mapa de módulos na arquitetura antes da implementação.
Evitar tanto duplicação quanto uma função genérica com muitas opções para
acomodar comportamentos distintos.

#### Validação de tabelas e colunas

A existência de tabelas e colunas e as chaves primárias pertencem ao DDL
PostgreSQL, sem declarações espelhadas nos handlers. O banco retorna os erros
de nomes inexistentes e de integridade; a base mantém identificadores
delimitados, parâmetros separados e validações estruturais de filtros,
ordenação e escritas. Métodos de domínio preparam filtros que identificam os
registros das operações individuais, conforme o contrato arquitetural.
Não consultar nem armazenar o catálogo para repetir essa validação.

Manter a composição genérica de SQL em métodos internos da própria base;
modularizar apenas responsabilidades que justifiquem um módulo próprio.
Projeções, colunas de escrita e alvos de conflito descrevem cada consulta,
sem criar listas genéricas de permissão por tabela. Uma coluna existente não
é automaticamente gravável por qualquer operação ou usuário: essa autorização
pertence aos métodos públicos do domínio e aos privilégios SQL.

Identificadores usam a API segura do driver; valores são parâmetros separados.
Não aceitar fragmentos SQL provenientes de entrada externa. Usar sequências
explícitas quando a ordem de colunas, parâmetros ou resultados for relevante.
Validar os fluxos e as constraints no PostgreSQL em testes de integração.

### 1.3 Concorrência, filas e publicação do Summary

Preservar a propriedade do ciclo de vida das tasks nos entrypoints. Claim,
DONE, ERROR, FROZEN, cooldown e histórico devem manter seus efeitos observáveis.
Atualizações relacionadas de fila e histórico continuam atômicas.

Definir equivalentes PostgreSQL para locks MariaDB, incluindo conexão
proprietária, duração, perda de conexão e liberação. Não reconectar e continuar
uma seção crítica como se o lock ou a transação anterior ainda existisse.
Retries precisam considerar idempotência e resultado de commit desconhecido.

Validar o Summary contra seu contrato PostgreSQL, sem exigir o schema legado.
Validar a estratégia de publicação e locks da plataforma;
não traduzir mecanicamente `RENAME TABLE` ou `GET_LOCK`. Provar que leitores
não veem resultados vazios ou parciais durante a publicação.

Validar outbox e checkpoint com publicações concorrentes, reinvalidação do mesmo
escopo, falha no refresh, drenagem e reconcile completo. Uma simples troca de
`REPLACE` por UPSERT não garante que uma nova invalidação seja observada pelo
consumidor que avança por ID.

O redesenho estrutural segue o contrato vigente. Integração com consumidores
e medições sob carga continuam pendentes, sem ativação automática em produção.

### 1.4 Conexões e consumidores

Validar o ciclo de vida das conexões durante falhas e encerramento do fluxo
completo com estações e appAnalise. Manter SQL na camada de banco, fornecer
autenticação PostgreSQL pelo ambiente dos processos e usar
`RFFUSION_PG_CONNINFO` quando for necessário substituir o destino do `config.py`.

No WebFusion, adaptar `db/connection.py`, autenticação, handlers SQL de `db/` e
`api/appAnalise_api`. Preservar formatos de linhas, aliases, filtros, paginação,
ordenação e respostas HTTP/MATLAB. Revisar cache, tabelas temporárias e SQL
entre schemas, além da fábrica de conexão.

Os clientes diretos do banco são appCataloga e WebFusion, além do acesso humano
pelo DBeaver na máquina do responsável. O MATLAB não acessa mais o banco
diretamente; seu contrato com a API appAnalise continua coberto pelos testes.
Revisar também credenciais, dependências, deploy e healthchecks; não registrar
segredos no plano ou nos testes.

### 1.5 Testes e critérios de saída

Reescrever testes acoplados ao driver ou dialeto MariaDB, preservando os
cenários de contrato úteis. Acrescentar testes da base e integração real em
PostgreSQL isolado; mocks e SQLite não validam SQL, isolamento ou locks.

Cobertura mínima para concluir a fase:

- CRUD, valores nulos, tipos, IDs retornados, UPSERT e colisões de unicidade;
- commit, rollback, falhas de conexão e transação abortada;
- disputa de claim com duas sessões e atomicidade entre task e histórico;
- locks, outbox, reinvalidação concorrente e recuperação após interrupção;
- persistência analítica, relações geográficas e consultas representativas;
- publicação consistente das projeções e preservação de sinais operacionais;
- WebFusion, autenticação e API appAnalise com seus contratos externos.

Executar cada suíte no [ambiente appCataloga](../../src/appCataloga/.instructions/ENVIRONMENT.md)
ou [WebFusion](../../src/webfusion/.instructions/ENVIRONMENT.md), conforme os
imports. Não iniciar daemons nem escrever em bancos compartilhados nos testes.

Saída: aplicações adaptadas e validadas com dados de teste no PostgreSQL,
contratos e READMEs atualizados, lacunas conhecidas registradas e nenhuma
dependência MariaDB não tratada no caminho previsto para a troca. A fase não
inclui carga de produção nem redesenho da taxonomia.

### 1.6 Validação de acesso sem senha nas aplicações

Ao final da Fase 1, após a compatibilidade funcional e os testes integrados,
realizar uma prova de conceito em desenvolvimento de socket Unix com
autenticação `peer` entre PostgreSQL, appCataloga e WebFusion. O objetivo é
evitar arquivos `.secret` e senhas de banco persistidas nas aplicações.
A escolha definitiva depende dos resultados; não adotar `trust` para dispensar
credenciais.

Validar compartilhamento e permissões do socket, SELinux, namespaces e UIDs
efetivamente reconhecidos entre os containers Podman. Usar identidades distintas
por aplicação, com mapeamento explícito para papéis de banco de privilégio mínimo;
separar execução, migração e administração, sem superusuário nas aplicações.
Testar conexões autorizadas, rejeição de identidades e operações não permitidas,
reinício e recriação dos containers, sem fallback silencioso para senha.

Avaliar separadamente o acesso remoto pelo DBeaver, incluindo túnel SSH e a
autenticação do próprio PostgreSQL. O túnel não habilita `peer` para a conexão
TCP ao banco. A opção final para esse acesso permanece em aberto e não deve
exigir senhas nos serviços locais.

Entrega: resultado documentado da prova de conceito, matriz de permissões e
decisão sobre autenticação para a Fase 2. Se `peer` não for viável, registrar
os motivos e avaliar alternativas antes da troca de ambiente. A validação não
inclui mudanças em produção nem eliminação de credenciais de outras integrações.

## Fase 2 — dados, taxonomia e revisão do SUMMARY

### 2.1 Dump, conversão e carga de dados

Preparar backup consistente e testar sua restauração no MariaDB. O dump SQL
MariaDB não é diretamente restaurável no PostgreSQL. A extração, transformação
e carga usam a [ferramenta de migração](../../src/postgres/migration/README.md),
que lê o dump lógico em fluxo e aplica o DDL PostgreSQL validado. O filesystem
do cluster deve comportar dados, índices e WAL. Em produção,
usar o volume nomeado do Podman sob `/home`; o diretório `.env/postgres-data`
continua disponível como seleção explícita no DEV, conforme o deploy.

Ensaiar a carga em destino isolado, preservando IDs, vínculos e histórico.
Definir ordem de tabelas, transformação de tipos, tratamento de dados inválidos,
ajuste de sequências, validação de constraints e atualização de estatísticas.
No Summary, restaurar conjuntamente apenas `WEBFUSION_USAGE_LOG_CHECKPOINT` e
`WEBFUSION_USAGE_METRIC_MONTHLY`; reconstruir os seis agregados canônicos por
full reconcile. Validar os novos campos de sinais no BPDATA e o cruzamento
automático de hosts/equipamentos no Summary.

Conferir contagens, totais de controle, integridade referencial, amostras de
conteúdo, datas, binários, flags, unicidade e IDs seguintes. Medir duração,
espaço e impacto de CPU/RAM/swap, uma operação pesada por vez.

Para a carga final, definir janela de parada ou mecanismo explícito de captura
e aplicação das alterações posteriores ao snapshot. Delimitar como ficam
workers, escritas web, clientes externos e gravações no repositório de arquivos.
Não presumir que a migração do banco também mova os artefatos do filesystem.

### 2.2 Taxonomia

A referência indicada é
[Taxonomia.md](../../src/appCataloga/.instructions/postgres_migration/Taxonomia.md).
O documento define classes de tabelas (`TB`, `TH`, `TL`, `TT`, `DIM`, `FATO`,
`STG`), prefixos semânticos de colunas e padrões de nomes de constraints,
índices, funções, procedures, triggers e views. Tabelas e colunas têm limite
convencional de 32 caracteres; combinações redundantes de classe e nome são
vedadas.

Criar um mapa origem → destino para schemas, tabelas,
colunas, constraints, índices e valores de domínio afetados. Revisar SQL,
tipos, fixtures, integrações e consumidores. Decidir como preservar APIs
externas e onde são necessárias views ou modelos versionados de compatibilidade.

Classificar cada coluna pelo significado, sem substituição automática de
prefixos: distinguir data (`DT`) de data/hora (`DH`), indicador (`IC`),
quantidade (`QT`), número (`NU`) e valor decimal (`VR`), incluindo unidades e
precisão na documentação.

Os tipos apresentados na taxonomia são de SQL Server. Definir equivalentes
PostgreSQL e registrar os casos sem correspondência direta, incluindo defaults
e objetos espaciais. A exigência de geometria para latitude/longitude e índice
espacial precisa ser conciliada com o `point` nativo do modelo inicial; decidir
se ele atende às operações necessárias ou se será necessária outra solução.
Não introduzir PostGIS implicitamente nem tratar tipos espaciais como simples
renomes. Registrar também a convenção de schemas e o idioma dos nomes, que não
estão definidos neste recorte da taxonomia.

Ensaiar primeiro a equivalência com os nomes atuais e depois as transformações
de taxonomia, mantendo pontos de validação separados. A sequência de ensaio
não exige duas trocas em produção; a ordem final depende do procedimento validado.

### 2.3 Validação do modelo canônico do Summary

Usar as fontes, granularidades e chaves do
[contrato vigente](../architecture/SUMMARY_POSTGRESQL.md). Completar o mapa das
consultas consumidoras e medir frequência, custo de refresh, atraso aceitável
e retenção no ambiente de ensaio.

Medir duração de transações, espera por locks, consultas lentas, índices,
volume de escrita, crescimento e concorrência entre sinais, refresh incremental
e reconcile. Definir metas numéricas de latência, duração de locks e defasagem
com base nessas medições e na capacidade do ambiente.

Validar as consultas sem cópias de cadastro e os totais calculados sobre os
agregados canônicos. Preservar a distinção entre mês de criação de arquivos,
throughput operacional, estado atual e histórico. Verificar a preservação das
duas tabelas de uso e dos eventos publicados durante a reconstrução.

### 2.4 Interfaces, troca de ambiente e retorno

Revisar WebFusion, API appAnalise e integração operacional com Zabbix contra a
taxonomia e as projeções propostas, preservando o contrato HTTP consumido pelo
MATLAB. Validar também o acesso de debug pelo DBeaver. Validar respostas, filtros,
paginação, ordenação, cache e atualidade dos dados. Comparar resultados antigos
e novos em um recorte de referência, documentando diferenças intencionais.

Concluir o procedimento operacional apenas após os ensaios: backup recuperável,
reconciliação aprovada, testes dos consumidores, limites de recursos e metas de
desempenho atendidos. A troca de produção terá autorização própria.

Definir critérios e responsáveis por interromper ou reverter a troca. Antes da
primeira escrita no PostgreSQL, o retorno pode usar a origem preservada; depois
de novas escritas, exige reconciliação ou transferência reversa validada.
Trocar apenas a conexão perderia essas alterações. Não manter dois conjuntos
de workers escrevendo nos mesmos fluxos sem protocolo explícito.

Saída: carga reconciliada, taxonomia aplicada, SUMMARY revisado com medições,
consumidores validados e procedimento de troca/retorno ensaiado. Backups e a
origem só podem ser desativados conforme a política operacional aprovada.

## Ordem de execução proposta

1. Completar o inventário e as decisões dos handlers, conexões e locks.
2. Validar a base em PostgreSQL isolado e prosseguir com BKP → RFM → Summary,
   com testes a cada lote.
3. Adaptar consumidores, concluir os testes integrados e validar o acesso sem
   senha nas aplicações ao final da Fase 1, conforme a seção 1.6.
4. Definir a taxonomia e ensaiar a migração dos dados da Fase 2.
5. Validar o modelo canônico do Summary e seus consumidores sob carga representativa.
6. Ensaiar o conjunto final e preparar a decisão de entrada em produção.

A implementação do modelo canônico não autoriza cargas pesadas nem mudanças
em produção. Essas operações dependem do ensaio e do escopo operacional próprio.
