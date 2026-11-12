# Arquitetura do WebFusion

Este documento rege os contratos específicos do WebFusion. As regras gerais
estão em [INSTRUCTIONS.md](../../../docs/agents/INSTRUCTIONS.md); o ambiente
autorizado está em [ENVIRONMENT.md](ENVIRONMENT.md).

## Identidades e privilégios do WebFusion

O banco `WEBFUSION` mantém os dados pessoais exclusivamente em `USERS`.
`USER_ROLES` relaciona `ID_USER` a `NA_ROLE` (`admin` ou `developer`), com
chave primária composta, chave estrangeira para `USERS` e `IS_ACTIVE`.
Um usuário pode possuir os dois papéis; `admin` mantém precedência no papel
efetivo. Ausência de vínculo ativo mantém o acesso comum, sem privilégios.
As tabelas `ADMINS` e `DEVELOPERS` não fazem parte do modelo ativo. Scripts
de migração que as usam como origem não devem ser repetidos após sua remoção.

O SQL desse domínio pertence a `src/webfusion/db/users.py`.
`auth/service.py` normaliza a identidade encaminhada pelo proxy confiável.
`USERS.NA_URL_PROFILE_IMG` armazena uma URL opcional de foto, sem tokens.
O cabeçalho `X-User-Avatar-Url`, quando presente e válido, atualiza esse campo;
sua ausência não apaga uma foto já cadastrada. O cabeçalho visual usa a URL
persistida quando o proxy não envia foto, com ícone substituto se indisponível.
URLs devem ser HTTPS ou caminhos locais absolutos; credenciais e esquemas
executáveis não são aceitos. Consultar fotos no Microsoft Graph exige integração
autenticada própria; não derivar uma URL pública apenas do e-mail.

O conector `src/webfusion/api/microsoft_api` recebe o e-mail de login do Entra
(UPN), consulta a foto com token do servidor e retorna a URL local versionada
pelo conteúdo. O conector não executa SQL. `auth/service.py` limita a frequência
de consulta e solicita a atualização de `NA_URL_PROFILE_IMG` a `db/users.py`
somente após obter uma foto válida. Ausência de token ou falha do Graph mantém
a foto armazenada. O token nunca aparece no navegador ou na URL persistida.
Quando o e-mail recebido não coincidir com o UPN, a integração deve fornecer
o identificador correto antes de consultar a foto; não assumir equivalência.

O fallback de usuário identificado usa `static/img/profile.svg`, com o texto
substituído pelas iniciais do primeiro e último nome, em branco sobre círculo
colorido e fonte Segoe UI com fallback sans-serif. Nomes únicos usam uma inicial;
quando não houver nome, usa-se a inicial do e-mail. A resposta
SVG personalizada pertence à API de usuários e não pode ser compartilhada
em cache. Anônimos usam `static/img/profile_out.svg`. Fallbacks não são gravados
como fotos reais no banco.

---

## Upload de arquivos no WebFusion

O módulo `src/webfusion/modules/upload/` recebe arquivos de usuários com e-mail
validado pela identidade do proxy existente, sem exigir papel administrativo.
`routes.py` contém a página e a API; `service.py` valida nomes e grava arquivos;
`config.py` define destino e limites. Template, CSS e JavaScript seguem o shell
visual existente. O catálogo de estações reutiliza `modules/host/service.get_all_hosts`,
incluindo estações offline. Não há SQL próprio, criação de tasks ou processamento automático.

A raiz fixa é `/mnt/reposfi/upload`. A classificação obrigatória define subpastas:
`fixas/<ID_HOST>`, `drive-test/smp-romes`, `drive-test/espectro` ou `rni`.
A API valida categorias e o ID da estação no catálogo antes de gravar; não aceita
caminhos enviados pelo cliente. Apenas subpastas são criadas pelo módulo; a raiz
continua sob responsabilidade do deploy. A fila conserva a classificação de cada
arquivo mesmo se os seletores mudarem. Cada requisição envia um arquivo, com
limite de 512 MiB; o navegador sequencia seleções múltiplas. Nomes são reduzidos
a nomes seguros, sem caminhos. Criação exclusiva impede sobrescrita, inclusive
entre requisições concorrentes. Falhas durante a gravação removem o arquivo
incompleto. Uma interrupção abrupta do processo pode exigir limpeza manual.
A API exige cabeçalho próprio, sem habilitar CORS, para impedir envio por
formulários de outras origens. Uploads não são publicados nas rotas de download.
O deploy monta `/mnt/reposfi` com leitura e escrita, incluindo `upload`, conforme
as permissões do compartilhamento no host. O módulo de upload continua restrito
à raiz `/mnt/reposfi/upload`.

Os seletores de classificação usam menus HTML acessíveis, renderizados pelo
componente comum do shell, para não depender do popup nativo do navegador. Os
elementos `select` mantêm os valores e eventos de classificação existentes e
ficam visíveis como fallback até a inicialização dos menus.


## Separação de responsabilidades

### Persistência PostgreSQL

O WebFusion usa exclusivamente PostgreSQL com Psycopg 3.3.8. `db/connection.py` fornece
conexões por requisição com linhas em dicionário, timezone UTC e `search_path`
explícito por schema. `config.py` lê `RFFUSION_PG_CONNINFO` ou `PG_CONNINFO` no
`.secret` local existente; não usa credenciais MariaDB nem fallback de backend.
O destino DEV autorizado é `10.88.0.35:5432`, banco `rffusion_test_migracao`.

SQL pertence ao pacote `db/`, organizado por domínio. `db/summary.py` compõe
projeções SQL compartilhadas que combinam os agregados canônicos do Summary com
BPDATA e RFDATA. Seus construtores são funções tipadas, sem consultas ou montagem
de projeções no import. Não cria tabelas ou views de compatibilidade.
Identificadores são delimitados preservando maiúsculas; valores são parâmetros.
Conexões e transações não são compartilhadas entre requisições.

O pacote não importa handlers em `__init__.py`. Os consumidores importam o módulo
responsável explicitamente, sem abrir conexões ou carregar todos os domínios ao
importar `db`. Não manter handlers ou aliases nos caminhos antigos em `auth/`,
`api/`, `modules/` ou na raiz do WebFusion.

| Domínio | Persistência |
|---|---|
| Conexões e tipos comuns | `db/connection.py` |
| Projeções compartilhadas do Summary | `db/summary.py` |
| Estações e consultas do servidor | `db/host.py` |
| Arquivos, espectros e filtros analíticos da interface | `db/spectrum.py` |
| Filas, histórico e criação de tarefas | `db/tasks.py` |
| Contadores de uso e checkpoints do NGINX | `db/usage_metrics.py` |
| Mapa compartilhado | `db/map.py` |
| Identidades e privilégios | `db/users.py` |
| Contrato MATLAB | `db/appanalise.py` |

`db/connection.py` cuida somente das fábricas de conexão e dos tipos comuns. Handlers de
leitura abrem e fecham suas conexões, inclusive em falhas. Operações de fila e
checkpoint podem receber conexão ou cursor tipado do chamador para manter a
mesma transação. Os limites de commit, rollback, locks e publicação do outbox
são preservados: mudar a localização do SQL não muda sua atomicidade.

Serviços mantêm validação de negócio, caches, sondagens externas e montagem das
respostas; rotas mantêm HTTP. Nenhum handler importa serviços ou rotas. Helpers
de construção de SQL ficam no handler do domínio. Constantes exclusivas de
persistência ficam em `db/config.py`; configurações das funcionalidades continuam
no respectivo `config.py`. Os filtros HTTP do appAnalise permanecem na API; o
handler usa sua anotação somente para tipagem, sem carregar o blueprint.
As consultas da interface e do appAnalise
continuam independentes. A sondagem `SELECT 1` de saúde permanece no componente
de infraestrutura, com seus timeouts próprios.

Totais de servidor são agregados por consulta; cadastros e sinais são lidos
diretamente do BPDATA. Métricas mensais respeitam `DT_REFERENCE_MONTH`. Mapas,
localidades e visitas usam os agregados canônicos e dimensões autoritativas.
Tabelas de uso e checkpoints pertencem ao DDL, sem criação na requisição HTTP;
contadores e offsets continuam atômicos. Tabelas temporárias de seleção de
espectros pertencem à conexão e usam PostgreSQL, sem InnoDB ou hints MariaDB.

A migração preserva integralmente os payloads do Zabbix e appAnalise, incluindo
colunas, tipos, unidades, filtros, ordenação, paginação e linha adicional.

Todos os seletores simples usam o componente do shell em
`static/js/base_page.js` e os estilos de `static/css/webfusion_base.css`.
Os controladores das páginas mantêm os catálogos, filtros e regras de negócio;
o shell mantém a interação e a apresentação dos menus HTML. O `select`
original preserva nomes, valores, validação e eventos dos formulários e fica
visível como fallback sem JavaScript. O componente acompanha inserções de
campos, opções reconstruídas, visibilidade e desabilitação. Listas de seleção
múltipla mantêm a interação própria e usam os estilos comuns de formulário.
Campos de busca do mapa e de Usuários compartilham a aparência dos menus,
preservando seus modelos de pesquisa especializados.

- Rotas tratam HTTP; serviços orquestram regras; handlers de banco concentram SQL.
- Reutilizar as fábricas de conexão de `db/connection.py` e os contratos dos módulos existentes.
  Não aplicar ao WebFusion os nomes de workers ou handlers do appCataloga.
- APIs e serviços chamam métodos públicos dos handlers. Não abrir conexões no import
  nem compartilhá-las entre requisições.
- Preservar o shell visual e consultar o README do módulo alterado.
- Mudanças que afetem o summary seguem o
  [contrato compartilhado](../../../docs/architecture/SUMMARY.md).
- As métricas operacionais e a divisão de responsabilidades com o Zabbix estão em
  [METRICAS_OPERACIONAIS.md](../../../docs/architecture/METRICAS_OPERACIONAIS.md).

## Relação entre host e equipamento

O cruzamento host/equipamento pertence ao contexto de consulta do WebFusion.
Na instalação PostgreSQL, seu modelo de leitura e sua materialização pelo
worker do Summary seguem o [contrato compartilhado](../../../docs/architecture/SUMMARY_POSTGRESQL.md#cruzamento-para-o-webfusion).
Essa relação permite apresentar o acervo analítico e as localidades por estação;
não estabelece dependência para coleta, filas ou ingestão do appCataloga.

## Consultas paginadas do appAnalise

A API mantém integralmente o contrato HTTP do consumidor MATLAB, incluindo
filtros, aliases, ordenação pelo início completo do arquivo e a linha adicional
de paginação. A otimização não depende de tokens, parâmetros ou alterações no
cliente. Não reutilizar diretamente as consultas de `/spectrum`: suas fontes
geográficas, rótulos e regras de consistência têm diferenças intencionais.

O handler de banco materializa os IDs dos espectros compatíveis em tabela
temporária PostgreSQL, exclusiva da conexão. A partir deles, calcula uma única vez
os IDs dos arquivos ordenados pelo conteúdo completo, com os JOINs obrigatórios
existentes. A conexão é fechada também em falhas, removendo a tabela temporária.
Metadados completos são consultados somente para os IDs da página solicitada.
Não há tabelas persistentes, novos índices ou conexões compartilhadas.

`api/appAnalise_api/service.py` coordena um cache de índices de arquivos e de
contagens por filtros, independente da paginação. O estado pertence a uma
instância de cache por processo, em `file_cache.py`; os limites pertencem a
`config.py`. Validade de 300 segundos, até 16 entradas e até 64 MiB de valores
retidos. Entradas vencidas são removidas e o limite usa descarte LRU. Valores
maiores que o limite atendem a requisição sem truncamento, mas não são retidos.
O cache contém IDs imutáveis e inteiros de contagem, sem conexões ou cursores.
O envio de metadados não é armazenado nesse cache.

Requisições concorrentes da mesma chave aguardam uma única construção e recebem
o mesmo resultado ou falha. No máximo uma construção ocorre por processo; o
bloqueio não impede o uso de entradas prontas nem consultas de metadados.
Exceções nunca viram resultados vazios nem entradas válidas. O cache usa relógio
monotônico, não é compartilhado entre processos e não é invalidado por escritas
no catálogo. Índices e contagens podem refletir dados de até cinco minutos;
metadados da página refletem a consulta atual. Após expiração ou descarte, uma
nova seleção pode mudar a composição das páginas: não há snapshot transacional
entre requisições HTTP independentes.

A contagem preserva seu conjunto de JOINs próprio. Não derivá-la do tamanho do
índice da listagem: referências inconsistentes podem produzir resultados
diferentes, conforme o contrato vigente.
