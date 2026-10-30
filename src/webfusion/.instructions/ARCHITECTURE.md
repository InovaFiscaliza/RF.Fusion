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

O SQL desse domínio permanece em `src/webfusion/auth/db_users.py`.
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
de consulta e solicita a atualização de `NA_URL_PROFILE_IMG` a `auth/db_users.py`
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
JavaScript da página, para não depender do popup nativo do navegador. Os
elementos `select` mantêm os valores e eventos de classificação existentes e
ficam visíveis como fallback até a inicialização dos menus.


## Separação de responsabilidades

- Rotas tratam HTTP; serviços orquestram regras; handlers de banco concentram SQL.
- Reutilizar as fábricas de conexão de `db.py` e os contratos dos módulos existentes.
  Não aplicar ao WebFusion os nomes de workers ou handlers do appCataloga.
- APIs e serviços chamam métodos públicos dos handlers. Não abrir conexões no import
  nem compartilhá-las entre requisições.
- Preservar o shell visual e consultar o README do módulo alterado.
- Mudanças que afetem o summary seguem o
  [contrato compartilhado](../../../docs/architecture/SUMMARY.md).
- As métricas operacionais e a divisão de responsabilidades com o Zabbix estão em
  [METRICAS_OPERACIONAIS.md](../../../docs/architecture/METRICAS_OPERACIONAIS.md).
