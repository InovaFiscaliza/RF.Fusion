---
name: rffusion-webfusion-fluxo
description: >
  Conduz a implementação ou alteração de páginas, rotas e APIs do WebFusion
  no RF.Fusion. Use ao adicionar uma tela, criar um endpoint ou mudar uma
  operação HTTP, incluindo identidade, permissão, persistência e integração
  associadas. Não usar para trabalho exclusivo de worker appCataloga, deploy,
  manual de usuário, auditoria de segurança ou configuração isolada de testes.
---

# Implementar uma funcionalidade WebFusion

## Entrada e enquadramento

Obtenha comportamento esperado, unidade afetada, público/perfil e saída HTTP.
Reaproveite essas informações do pedido; esclareça uma política de acesso
nova ou ambígua antes de implementá-la.

1. Leia o [AGENTS do WebFusion](../../../src/webfusion/AGENTS.md) e seus
   contratos. Consulte a [construção WebFusion](../../references/webfusion-construcao.md)
   para mecanismos de identidade, persistência e templates. Identifique o
   módulo/API e se o blueprint já existe. Esta etapa termina com o endpoint,
   o contrato aplicável e os arquivos necessários identificados.
2. Rastreie o caminho ativo: registro em `app.py`, guards globais e locais,
   rota, serviço e colaboradores de banco/integração. Diferencie identidade
   em `g.webfusion_user`, papel ativo e permissão da ação. Confira o efeito
   de renomear endpoint ou mudar blueprint no alcance dos guards. Termine
   com a decisão de acesso explícita, inclusive quando não houver restrição.
3. Identifique o mecanismo de dados do território. Há SQL em rotas,
   serviços e handlers existentes; o contrato normativo exige separação.
   Preserve sua autoridade e explicite conflitos antes de escolher uma
   solução não coberta. Reutilize a fábrica PyMySQL do schema apropriado e
   o cliente de integração existente. Se mudar dado compartilhado ou payload
   público, leia [dados e integrações](../../references/dados-e-integracoes.md)
   e liste produtores/consumidores afetados.
4. Implemente a entrada HTTP e a sequência de regras, com validação no
   servidor, saída e tratamento de erro coerentes com a unidade. Upload pode
   não ter SQL; configuração pode redirecionar após escrita externa; consulta
   pode retornar HTML ou JSON. A variação de um módulo não define a política
   de outro. Registre falhas operacionais sem introduzir sucesso vazio.
5. Se página, estenda o shell de templates e associe assets e navegação
   necessários. Gere URLs por `url_for`, respeitando o prefixo interpretado
   pelo proxy. Se blueprint novo, importe e registre; endpoint novo num
   blueprint registrado não exige segundo registro. Termine com todas as
   superfícies da funcionalidade ligadas, não apenas o serviço implementado.
6. Selecione a validação por imports em
   [ambientes e testes](../../references/ambientes-e-testes.md) e
   [test/AGENTS.md](../../../test/AGENTS.md). WebFusion exige webserver;
   esta sessão no appCataloga não autoriza importar, compilar ou testar Flask
   localmente. Use doubles de identidade, rede e banco e temporários quando
   o teste for unitário. Verifique permissão, entrada inválida, saída/efeitos
   e falha pertinentes à mudança; execute somente o grupo necessário.

## Critério de conclusão e saída

Entregue a mudança solicitada com wiring, documentação relacionada e validação.
Informe arquivos alterados, impacto observável, comandos realmente executados
e limitações. Separe verificações concluídas de recomendações não executadas.
Um bloqueio de ambiente não equivale a teste aprovado. Execução operacional,
deploy e correções fora do pedido dependem de autorização própria.

## Conferência do padrão

A recorrência, as variantes e os caminhos completos dos exemplares estão na
[referência de construção](../../references/webfusion-construcao.md#exemplares).
Localize o equivalente atual por blueprint e responsabilidade; use o exemplar
para conferir a forma, não como substituto da sequência acima.

## Casos para avaliação comportamental

Prompts propostos, não executados nem comparados com baseline:

- “Adicione um endpoint de consulta no WebFusion.” Esperado: distinguir
  blueprint novo de endpoint, definir acesso pelo contrato e selecionar
  webserver sem impor banco ou permissão administrativa inexistentes.
- “Mude a operação da tela de configuração.” Esperado: acompanhar
  rota/serviço/cliente externo, retorno e guard efetivos, preservando a
  fronteira Zabbix e isolando os efeitos nos testes.
