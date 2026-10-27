# Mapa CWE para os tópicos do guia de segurança

Ponte do projeto para a skill `owasp-playbook`, que é agnóstica e lê este arquivo por seção: Tradução, Sinais do projeto, Auditores do projeto, Exceções, Saídas opcionais e Correção. O play reporta por CWE; esta tabela diz qual tópico do `guia-seguranca.md` cobre o CWE, com que severidade, e qual seção do ASVS o justifica.

Este arquivo chega da stack com a tradução genérica pronta e as demais seções vazias. É o projeto que preenche as seções vazias com o que tem de próprio: sinais, auditores, exceções, saídas e roteamento da correção. Ao preencher, mantenha os títulos das seções, porque a skill os procura pelo nome.

## Regra de tradução

1. Pegue o CWE do achado do play.
2. Localize a linha correspondente abaixo. O tópico e a severidade são os do guia.
3. Confirme o caminho do dado, da entrada até o ponto de uso, antes de tratar como achado. Sem esse rastro, o item é hipótese.
4. CWE fora da tabela: use a severidade convertida do play e comunique ao desenvolvedor.

## Tradução

| CWE | Tópico do guia | Severidade | ASVS |
|---|---|---|---|
| CWE-862, CWE-306, CWE-284 | S01 Autorização por função | BLOQUEANTE | V8.2, V8.3; CWE-306 em V6.3 |
| CWE-639, CWE-863 | S02 Autorização por objeto (IDOR) | BLOQUEANTE | V8.2 |
| CWE-352 | S01 Autorização por função; o mecanismo anti-CSRF é do framework e fica registrado no `AGENTS.md` do projeto | BLOQUEANTE | V3.5 |
| CWE-287, CWE-384, CWE-916 | S03 Autenticação e sessão | BLOQUEANTE | V6.3, V7.2, V7.5; CWE-916 em V6.2 |
| CWE-20 | S04 Validação e normalização de entrada | ALTA | V2.2 |
| CWE-89 | S05 Injeção em consulta a dados | BLOQUEANTE | V1.2 |
| CWE-78, CWE-77, CWE-94 | S06 Injeção em comando de sistema | BLOQUEANTE | V1.2; CWE-94 em V1.3 |
| CWE-79 | S07 Saída para HTML e JavaScript | ALTA | V1.2 |
| CWE-113, CWE-1236, CWE-117 | S08 Outros destinos de saída | ALTA | V1.2 para CWE-1236; CWE-113 e CWE-117 sem seção clara no índice, use a `security-guidance` |
| CWE-22, CWE-434 | S09 Caminho de arquivo e upload | ALTA | V5.3, V5.2 |
| CWE-502, CWE-611 | S10 Desserialização e parser de documento externo | ALTA | V1.5 |
| CWE-918, CWE-601 | S11 Requisição de saída controlável | ALTA | V1.2 |
| CWE-798, CWE-200, CWE-532 | S12 Segredos e credenciais | BLOQUEANTE | V13.3, V14.2, V16.2 |
| CWE-327, CWE-338 | S13 Criptografia e aleatoriedade | ALTA | V11.2, V11.3, V11.5 |
| CWE-1188 | S14 Configuração segura por padrão | ALTA | V13.4 para debug e mensagem verbosa; demais casos, use a `security-guidance` |
| CWE-1395 | S15 Cadeia de fornecimento | ALTA | V15.1, V15.2 |
| CWE-532, CWE-778 | S16 Log, auditoria e alerta | ALTA | V16.2, V16.3 |
| CWE-209, CWE-755, CWE-390 | S17 Tratamento de erro e falha segura | ALTA | V16.5; CWE-209 também em V13.4 |
| CWE-362 | S18 Integridade da operação | ALTA | V2.3 |
| sem CWE consolidado | S19 Entrada não confiável que alcança modelo de IA | ALTA | sem seção no ASVS; use os plays `llm-risk-assess` e `prompt-injection-testing` |

A coluna ASVS aponta a seção do índice da skill `security-guidance` do upstream. A ficha da seção está em `.agents/skills/owasp-playbook/upstream/plugins/code-security-skills/data/asvs/<seção>.md`. Serve para justificar o achado com ID de requisito; não altera a severidade.

## Sinais do projeto

| Sinal no escopo | Play | Onde está hoje |
|---|---|---|
| Rotas Flask e APIs | `api-security-review` | [app.py](../../src/webfusion/app.py):56 registra blueprints de módulos e APIs; há rotas divididas em [tasks/](../../src/webfusion/modules/tasks/) |
| Manifesto Conda/pip | `sca-audit` | [environment.yml](../../install/appCataloga/linux/environment.yml) declara versões; [Containerfile WebFusion](../../install/webserver/Containerfile) declara instalações pip |
| Containerfile e scripts de implantação | `iac-security-review` | definições em [install/](../../install/), incluindo [imagem WebFusion](../../install/webserver/Containerfile) e [deploy MariaDB](../../install/mariaDB/linux/deploy-debian12-mariadb.sh) |
| Contexto e configuração de agentes | `agent-security-audit` | [AGENTS.md](../../AGENTS.md), [.agents/](../), [.claude/](../../.claude/), [.opencode/](../../.opencode/) e [instruções Copilot](../../.github/copilot-instructions.md) |
| Python próprio, SQL e adaptadores operacionais | `code-review-security` | aplicações em [src/](../../src/), DDL em [scripts MariaDB](../../src/mariadb/scripts/) e adaptadores em [src/zabbix/](../../src/zabbix/) |

O conector [appAnalise_connection.py](../../src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/appAnalise_connection.py):2
é um cliente socket de serviço MATLAB, não evidência de aplicação LLM.
Estes sinais orientam a escolha do play; não representam uma auditoria concluída.

## Auditores do projeto

Nenhum auditor registrado. Para acrescentar, liste aqui os comandos que a skill deve rodar sobre os arquivos do escopo na etapa Framework-Specific Checks do play, um por linha, em bloco `bash`. Sem auditor, a etapa roda só com o conteúdo genérico do play e o relatório diz isso.

## Exceções

As de cada tópico do guia, no bloco "Não é achado". Para acrescentar exceções do projeto, liste aqui, uma por linha, com o motivo.

## Saídas opcionais

Quando o usuário pedir PDF, JSON ou texto de issues, o próprio pedido traz o formato e os caminhos. Modelos prontos em [prompts-exemplo.md, seção Variações de saída da revisão](../../docs/stack-ai/prompts-exemplo.md#variações-de-saída-da-revisão); eles leem o mesmo JSON de findings que esta skill consolida.

Use os dados da revisão solicitada e informe explicitamente o JSON ao gerador que o prompt descreve. As saídas opcionais preservam o resultado da revisão e não autorizam abrir issues em ferramentas externas. Para registrar um gerador próprio do projeto ou outro caminho de gravação, escreva aqui.

## Correção

Correção não é desta skill. Peça a correção citando o número do achado no
relatório e siga o fluxo do projeto:

- HTTP, identidade e interface: [rffusion-webfusion-fluxo](../skills/rffusion-webfusion-fluxo/SKILL.md)
  e [instruções WebFusion](../../src/webfusion/AGENTS.md).
- Processos e handlers: [rffusion-appcataloga-fluxo](../skills/rffusion-appcataloga-fluxo/SKILL.md)
  e [instruções appCataloga](../../src/appCataloga/AGENTS.md).
- DDL: [README MariaDB](../../src/mariadb/scripts/README.md) e
  [dados e integrações](../references/dados-e-integracoes.md), distinguindo
  bootstrap de atualização de banco existente.
- Summary e métricas: contratos [SUMMARY](../../docs/architecture/SUMMARY.md)
  e [METRICAS_OPERACIONAIS](../../docs/architecture/METRICAS_OPERACIONAIS.md).

Esses encaminhamentos não autorizam correção automática, operação de banco ou
deploy; o pedido e os contratos de cada escopo continuam delimitando a mudança.
