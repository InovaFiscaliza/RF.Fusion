# Construção de funcionalidades WebFusion

Referência descritiva para páginas, endpoints e persistência do WebFusion.
Consulte ao alterar uma funcionalidade HTTP. A autoridade normativa continua
na [arquitetura local](../../src/webfusion/.instructions/ARCHITECTURE.md);
esta referência não regulariza divergências do código em relação ao contrato.

## Sequência de construção

1. Identifique se a funcionalidade pertence a `modules/<nome>/` ou a uma API
   em `api/<nome>/`. Rotas HTTP recebem parâmetros e devolvem HTML, JSON ou
   redirect; serviços executam a sequência de regras. `tasks` divide as rotas
   entre arquivos e agrega blueprints, em vez de concentrar tudo em `routes.py`.
2. Defina o blueprint e endpoint. Um blueprint novo precisa ser importado e
   registrado em `app.py`; um endpoint em blueprint existente reutiliza o
   registro. O bootstrap também instala `ProxyFix` e os hooks de identidade.
3. Determine a política de acesso efetiva, descrita abaixo, antes de executar
   operações. Não confunda existência de identidade com privilégio.
4. Normalize parâmetros no servidor e use o serviço específico. A validação
   varia por unidade: upload valida multipart e classificação; configuração
   normaliza alvo/ação; mapa normaliza intervalo temporal no serviço.
5. Se página, estenda `templates/base.html`, preencha os blocos de conteúdo,
   breadcrumb e assets pertinentes. Use `url_for` para URLs; o nginx fornece
   o prefixo público `/rffusion`, interpretado por `ProxyFix`. Não acrescente
   esse prefixo fixo às rotas Flask.
6. Preserve o contrato de saída da unidade: HTML, redirect ou JSON e status.
   Falhas operacionais são registradas no logger. Há leituras antigas que
   retornam estruturas vazias após log; isso não autoriza introduzir novos
   fallbacks de sucesso para falhas, contra as regras gerais.
7. Teste o serviço ou a rota no ambiente WebFusion, com rede, banco, identidade
   e filesystem isolados conforme a unidade. Consulte
   [ambientes-e-testes.md](ambientes-e-testes.md).

## Identidade, sessão de requisição e permissão

`load_request_identity` normaliza os cabeçalhos do proxy por `AuthService`,
registra o perfil observado e guarda a identidade em `g.webfusion_user`.
Isso ocorre globalmente; não significa que toda rota exige login.

`require_restricted_access` exige papel ativo para blueprints `tasks`,
`tasks_api`, `users`, `users_admin_api`, `configuration` e
`configuration_api`, além dos endpoints específicos de teste de conectividade
de host. O conjunto está em `auth/service.py` e `auth/config.py`; alterações
de endpoint podem alterar o alcance dessa política.

`USER_ROLES` associa papéis ativos ao usuário. `admin` tem precedência sobre
`developer`; o guard global aceita um papel encontrado, não define sozinho
uma política distinta por ação. `users_api` não é `users_admin_api`.
O context processor fornece `current_user` aos templates e consulta foto e
papel quando necessários; ocultar um botão não substitui proteção da rota.

Upload tem uma regra local: exige e-mail válido da identidade, sem exigir
papel administrativo, e cabeçalho próprio em POST antes de ler multipart.
Mapa e outras leituras não se tornam restritos apenas por terem `/api/` na URL.
Uma nova exigência de acesso depende do contrato da funcionalidade, não de
suposição sobre o framework.

**Alerta — endpoint e privilégio:** antes de renomear/criar endpoint ou copiar
uma API para outro blueprint, confira o conjunto global e os guards locais:
`rg -n 'ACCESS_RESTRICTED|is_restricted_endpoint|before_request' src/webfusion/auth src/webfusion/app.py <rotas-alvo>`.

O proxy confiável é a origem da identidade; o código local não comprova por
si só a configuração de autenticação externa. Não invente uma sessão Flask
de login paralela por ausência de formulário de senha no módulo.

## Persistência: mecanismos diferentes por território

`db.py` lê a configuração local de conexões sem que esta referência precise
abrir os valores. As quatro fábricas `get_connection_*` abrem conexões PyMySQL
com `DictCursor`. Reutilize a fábrica do schema pertinente e feche a conexão
no ciclo da operação; não a compartilhe entre requisições.

Há SQL em serviços de host, spectrum, tasks e mapa, e também em rotas de host
e tasks. Identidade concentra seu SQL em `auth/db_users.py`; a API appAnalise
possui `db_handler.py`. Isso descreve territórios existentes, não uma regra
para copiar SQL em rota nova. O contrato arquitetural exige separação
rota/serviço/handler: uma alteração deve respeitá-lo e explicitar a divergência
se depender de um trecho existente fora dessa separação.

**Alerta — não importar o padrão de banco do worker:** appCataloga usa
mysql-connector e handlers persistentes; WebFusion usa as fábricas PyMySQL por
operação. Antes de escolher um ponto de SQL, localize o mecanismo do módulo:
`rg -n 'get_connection|execute\(|commit\(|rollback\(|close\(' <modulo-alvo>`.
Não crie uma camada nova nem escolha entre convenções conflitantes sem
cobertura no contrato.

Para escrita de identidade, `auth/db_users.py` mostra parâmetros `%s`,
`commit`, `rollback` em erro e fechamento em `finally`. Não suponha que todo
serviço antigo tenha o mesmo lifecycle de erro: mapa fecha a conexão após a
leitura no caminho de sucesso. Aplique o contrato da mudança sem reescrever
áreas não solicitadas.

## Integrações e configuração

- Upload grava no repositório, sem criar tasks nem SQL próprio. Configuração
  altera macros pela API Zabbix e sincroniza somente os campos operacionais
  previstos. Alarmes consulta problemas, sem assumir seu ciclo de resolução.
- Fotos usam o cliente Microsoft Graph do servidor; token não é URL de avatar.
  `AuthService` controla a frequência e preserva a foto persistida nos casos
  de indisponibilidade previstos.
- O runtime web é Python 3.11 com Flask, Waitress e PyMySQL no Containerfile.
  As versões dessas bibliotecas não estão fixadas ali; não inferir a versão
  efetivamente instalada por esse arquivo.
- Consulte [dados-e-integracoes.md](dados-e-integracoes.md) ao alterar summary,
  APIs usadas por clientes externos ou metadados do catálogo.

## Exemplares

| Caminho | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `src/webfusion/app.py` | registro, hooks e contexto de template | arquivo que instancia Flask e registra blueprints |
| `src/webfusion/modules/upload/routes.py` | página/API, guard local, erros HTTP | rotas do módulo que grava upload |
| `src/webfusion/modules/configuration/routes.py` | escrita externa e redirect | rotas do blueprint configuration |
| `src/webfusion/modules/alarms/routes.py` | página de consulta | rotas do blueprint alarms |
| `src/webfusion/modules/host/routes.py` | página leve, detalhes JSON e endpoints restritos nomeados | rotas do blueprint host |
| `src/webfusion/api/map_api/routes.py` | API de leitura usada por clientes | blueprint map_api |

## Evidências

Medições estáticas em 2026-10-27, a partir da raiz; nenhuma importação ou
execução de WebFusion no container do appCataloga.

| Afirmação | Confiança | Onde foi comprovada | Comando | Alcance | Verificado em |
|---|---|---|---|---|---|
| Registro explícito | medido | `src/webfusion/app.py:56`, 14 registros | `rg -n '^app.register_blueprint\(' src/webfusion/app.py \| wc -l` → 14 | bootstrap | 2026-10-27 |
| Identidade e autorização em hooks diferentes | medido | `src/webfusion/app.py:72,92`, dois hooks | `rg -n '^@app.before_request' src/webfusion/app.py \| wc -l` → 2 | bootstrap; corpo dos hooks lido integralmente | 2026-10-27 |
| Persistência não é só handler | medido | SQL ativo em serviços, rotas e handlers | `rg -l -g '*.py' '(cursor\|cur)\.execute\(' src/webfusion/modules src/webfusion/api src/webfusion/auth \| wc -l` → 10 | inventário de arquivos; não mede equivalência de queries | 2026-10-27 |
| Shell de páginas | medido | 11 templates estendem a base | `rg -l -g '*.html' '^\{% extends "base.html" %\}' src/webfusion/templates \| wc -l` → 11 | templates com essa declaração literal | 2026-10-27 |
| Permissão e variantes de erro | inferido | leitura integral de app.py, auth/service.py, auth/config.py e seis rotas exemplares | conferir o corpo dos hooks e o endpoint da tarefa; teste no webserver confirma comportamento | componentes nomeados, não todos os endpoints | 2026-10-27 |
| Divergência de persistência frente ao contrato | medido | `modules/host/routes.py:278,620`, `api/map_api/service.py:74` e arquitetura local | leitura dos pontos SQL e da seção Separação de responsabilidades | trechos nomeados; não autoriza relaxar o contrato | 2026-10-27 |

## Descartado

| Achado | Motivo | Alcance medido | O que mudaria a decisão | Data |
|---|---|---|---|---|
| Todas as APIs exigem papel | contradito pelo guard global e por upload/mapa | hooks e exemplares nomeados | nova política explicitamente definida | 2026-10-27 |
| Impor um handler novo a cada módulo existente | decisão arquitetural, não descoberta | coexistem SQL em rotas, serviços e handlers | decisão do responsável compatível com o contrato | 2026-10-27 |
