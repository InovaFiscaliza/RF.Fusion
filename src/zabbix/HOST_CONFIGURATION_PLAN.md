# Plano: Configuração de Hosts pelo WebFusion

## 1. Objetivo

Criar no WebFusion um módulo de configuração de estações que use o Zabbix como
fonte de verdade para macros e templates. O módulo deve permitir consultar e
alterar a configuração efetiva de cada estação, incluindo sobrescritas por host,
sem transformar `BPDATA.HOST` em uma fonte concorrente de configuração.

O caso principal é uma estação que herda a configuração da sua família, mas
precisa de uma exceção local. Por exemplo, uma RFEye pode usar os valores do
template `RFEye Nodes`, exceto por `{$SSH_PASSWD}`.

## 2. Decisões de Arquitetura

| Componente | Responsabilidade |
| --- | --- |
| Zabbix em `rhzbefipdin01` | Fonte de verdade para templates, macros e sobrescritas de host. |
| WebFusion | Interface operacional e cliente remoto controlado da API do Zabbix. |
| `appCataloga` | Consome a configuração recebida do Zabbix e atualiza a projeção operacional. |
| `BPDATA.HOST` | Projeção operacional usada pelos workers; não é editada diretamente pelo WebFusion para configurar estações. |
| `zbx_export_templates.xml` | Fotografia versionada da configuração; não substitui o Zabbix em produção. |

Fluxo pretendido:

```text
Operador -> WebFusion -> API HTTP do Zabbix em rhzbefipdin01
                              |
                              v
                    macros/templates do Zabbix
                              |
                              v
                   rotina appCataloga/Zabbix
                              |
                              v
                         BPDATA.HOST
```

Em uma primeira fase, a atualização de `BPDATA.HOST` pode ocorrer no próximo
acionamento normal da estação. Uma fase posterior poderá criar uma sincronização
explícita e idempotente, sem gerar backup, descoberta ou outra task.

O diretório `src/zabbix/` contém exportações, templates e scripts de apoio; ele
não representa um Zabbix executando neste container. Portanto, toda leitura e
escrita operacional deverá usar a API remota do servidor `rhzbefipdin01`. O
endereço, porta e protocolo serão configuráveis; a expectativa inicial é HTTP.

## 3. Precedência de Configuração

O WebFusion deve refletir a precedência do Zabbix, sem criar regras próprias:

1. Macro definida diretamente no host.
2. Macro do template de família ligado diretamente ao host.
3. Macro herdada de `appCataloga` ou de outro template ancestral.

Macros globais não são consultadas nem alteradas por este módulo. Elas ficam
fora do escopo para que uma configuração local não pareça resolver um valor
global do Zabbix.

Ao remover uma macro local, o host volta a herdar o valor da camada inferior.
O módulo deve mostrar a origem do valor efetivo: `Sobrescrita do host`,
`Template de família`, `Template appCataloga` ou `Global`.

Templates no mesmo nível não devem definir a mesma macro RF.Fusion. O Zabbix
resolve esse empate por identificador interno do template, o que torna a origem
do valor menos clara para o operador.

## 4. Perfis Identificados no Export Atual

Os valores não devem ser versionados neste documento. Apenas os nomes e tipos
de macros formam o contrato inicial da interface.

| Perfil | Template base | Macros do perfil |
| --- | --- | --- |
| Comum | `appCataloga` | `{$CATALOGA_QUERY}`, `{$CATALOGA_TIMEOUT}`, `{$HOST_ID}`, `{$INFO_QUERY}`, filtros de descoberta e backup. |
| RFEye | `RFEye Nodes` | `{$BACKUP_EXTENSION}`, `{$BACKUP_PATH}`, `{$SNMP_COMMUNITY}`, `{$SSH_PASSWD}`, `{$SSH_PORT}`, `{$SSH_USER}`. |
| CW RMU | `CW RMU` | `{$BACKUP_EXTENSION}`, `{$BACKUP_PATH}`, `{$DIGI_PORT}`, `{$DIGI_TIMEOUT}`, `{$SSH_PASSWD}`, `{$SSH_PORT}`, `{$SSH_USER}`. |
| ERMx | `ERMxAppColeta` | `{$SSH_PASSWD}`, `{$SSH_PORT}`, `{$SSH_USER}`. |
| UMS300 | `UMS300` | `{$BACKUP_EXTENSION}`, `{$BACKUP_PATH}`, `{$SSH_PASSWD}`, `{$SSH_PORT}`, `{$SSH_USER}`. |

Os tipos de macro devem ser preservados conforme definidos no Zabbix. Em
particular, `{$SSH_PASSWD}` é secreto. O tipo de `{$SSH_USER}` varia entre os
templates atuais e não deve ser normalizado sem uma decisão explícita.

## 5. Escopo da Primeira Versão

O objetivo inicial não é reproduzir toda a administração do Zabbix dentro do
WebFusion. A primeira versão deve ser pequena, segura e focada nas macros que
o RF.Fusion utiliza.

### 5.1 Consulta de configuração efetiva

- Listar hosts elegíveis a partir da API do Zabbix.
- Mostrar o perfil de estação identificado pelos templates vinculados.
- Mostrar as macros existentes na cadeia do item, o valor efetivo, a origem e
  o tipo. O módulo não cria nomes de macro novos.
- Mostrar segredos apenas como `Valor protegido`; nunca revelar o valor
  existente.
- Consultar somente o item selecionado. O catálogo de hosts e templates é
  curto e mantido em memória por até 60 segundos para não pressionar a VM nem
  a API do Zabbix.

### 5.2 Sobrescrita por host e template

- Criar uma macro local quando o host ainda apenas herda o valor.
- Alterar uma macro local já existente.
- Remover uma macro local para restaurar a herança.
- Permitir edição no próprio template RF.Fusion ou a criação de uma
  sobrescrita nele para uma macro herdada.
- Não permitir criação arbitrária de nomes, macros globais, alteração do tipo
  ou leitura de valores secretos já armazenados.

Esta fase resolve diretamente casos como a alteração de senha de uma única
RFEye, sem alterar o restante da família.

### 5.3 Fora do escopo inicial

- Criação ou remoção de hosts no Zabbix.
- Vincular ou desvincular templates de hosts.
- Edição genérica de qualquer macro existente no Zabbix.
- Edição de macros globais.
- Exibição, exportação ou recuperação de segredos já gravados.
- Escrita direta de parâmetros de conexão em `BPDATA.HOST`.

## 6. Edição Controlada

Hosts e os cinco templates RF.Fusion identificados neste documento podem ser
abertos na mesma tela. A edição é sempre unitária: atualizar uma macro direta,
criar uma sobrescrita para uma macro herdada ou remover uma sobrescrita para
restaurar a herança. Não há operação que substitua toda a coleção de macros.

Alterações em template exigem revisão operacional, pois podem afetar várias
estações; sobrescritas locais continuam prevalecendo sobre o template.

## 7. Integração Técnica Planejada

### 7.1 Leitura

O WebFusion consultará a API JSON-RPC do endpoint configurado do Zabbix, com
HTTP como expectativa inicial, para obter:

- Templates RF.Fusion e seus vínculos.
- Hosts e seus templates vinculados.
- Macros locais de hosts e de templates.
- Tipos de macros, inclusive segredo.

A configuração efetiva será montada a partir da cadeia de templates e das
sobrescritas locais. O XML exportado não será consultado em tempo de execução.

### 7.2 Escrita

As alterações devem usar a API de macros do Zabbix. O plano de validação deve
confirmar, na instalação em uso, o comportamento de criação, alteração e
remoção tanto para hosts quanto para templates.

Uma escrita deve seguir esta ordem:

1. Validar nome, perfil, tipo e permissão da macro.
2. Gravar a alteração no Zabbix.
3. Redirecionar para uma nova leitura da macro e de sua origem para confirmar
   o valor efetivo.
4. Exibir o resultado sem expor segredo.
5. Marcar a projeção operacional como pendente de sincronização, quando
   aplicável.

Falha na API do Zabbix não deve gerar escrita de compensação no `BPDATA`.

### 7.3 Configuração de ambiente

O container WebFusion pode receber as variáveis abaixo por mecanismo externo
ao repositório (por exemplo, o orquestrador do container):

```text
ZABBIX_API_URL=http://servidor-zabbix/api_jsonrpc.php
ZABBIX_API_TOKEN=<token-da-conta-tecnica>
ZABBIX_API_TIMEOUT_SECONDS=10
```

Quando não houver variáveis de ambiente, o módulo usa como alternativa local o
arquivo ignorado `src/zabbix/.secret/zabbix_api.env`, desde que ele esteja no
volume montado em `/RF.Fusion`. O token não deve constar em arquivo versionado,
log ou saída de erro. A role da conta técnica precisa permitir, no mínimo, `host.get`, `template.get`,
`usermacro.get`, `usermacro.create`, `usermacro.update` e `usermacro.delete`.

### 7.4 Sincronização com BPDATA

O fluxo normal já atualiza os campos operacionais quando o Zabbix aciona o
`appCataloga`. Para ter sincronização imediata, a fase futura deve criar uma
operação específica de sincronização de configuração:

- Recebe a configuração atual já validada pelo Zabbix.
- Atualiza somente a projeção necessária em `BPDATA.HOST`.
- Não cria `HOST_TASK`, `FILE_TASK` ou `FILE_TASK_HISTORY`.
- É idempotente e pode ser repetida com segurança.
- Mantém o `appCataloga` como proprietário da escrita operacional.

## 8. Segurança e Auditoria

- Usar uma conta técnica exclusiva da API do Zabbix.
- Conceder apenas as permissões necessárias às operações previstas.
- Armazenar token e endereço da API fora do repositório, em configuração de
  ambiente protegida.
- Nunca registrar valor de macro secreta em logs, mensagens de erro, cache ou
  resposta HTTP.
- Para segredos, aceitar somente substituição por novo valor; o valor anterior
  não pode ser lido pela interface.
- Usar a auditoria nativa do Zabbix como registro principal da alteração.
- Registrar no WebFusion apenas metadados seguros: operador, host ou template,
  nome da macro, operação, data e resultado.

## 9. Fases de Implantação

| Fase | Entrega | Risco |
| --- | --- | --- |
| 0 | Validar endereço HTTP, porta, autenticação e leitura da API remota em `rhzbefipdin01`; inventariar templates, hosts e macros. | Baixo; somente leitura. |
| 1 | Tela de configuração efetiva e sobrescritas por host/template. | Baixo a médio; a escrita é unitária e reversível pela herança. |
| 2 | Sincronização imediata e idempotente com `BPDATA.HOST`. | Médio; requer evolução isolada do appCataloga. |

Cada fase deve ter validação manual em uma estação conhecida antes de ampliar
o uso. A primeira candidata pode ser uma RFEye com sobrescrita de senha já
conhecida, pois permite confirmar herança, criação, edição e remoção da macro
local sem alterar o template da família.

## 10. Critérios de Aceite

- O WebFusion mostra corretamente o perfil e a origem de cada macro RF.Fusion.
- Uma sobrescrita local prevalece sobre o template da família.
- A remoção da sobrescrita restaura a herança esperada.
- Senhas não são reveladas em nenhuma tela, log ou resposta HTTP.
- Uma falha ao gravar no Zabbix não altera `BPDATA.HOST`.
- A futura sincronização imediata não cria tasks de backup ou descoberta.

## 11. Pontos a Confirmar Antes da Implementação

1. Endereço, versão e método de autenticação da API do Zabbix em produção.
2. Permissões mínimas da conta técnica para leitura e escrita de macros.
3. Permissão da conta técnica para criar, atualizar e remover macros na versão
   instalada, primeiro em uma estação de validação.
4. Mapeamento completo entre macros e os campos operacionais que o
   `appCataloga` recebe.
5. Regra de identificação de perfil quando um host possuir mais de um template
   de estação RF.Fusion.
6. Forma de informar ao WebFusion que a projeção em `BPDATA.HOST` foi
   sincronizada após uma alteração.
