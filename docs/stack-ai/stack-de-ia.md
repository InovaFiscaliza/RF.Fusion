# A stack de IA

## Sumário

- [Introdução](#introdução)
- [Filosofia e origem da stack](#filosofia-e-origem-da-stack)
- [O que são modelos de IA](#o-que-são-modelos-de-ia)
- [O que são agentes de IA](#o-que-são-agentes-de-ia)
- [Skills: agentes especializados](#skills-agentes-especializados)
- [O que é SDD](#o-que-é-sdd)
- [Ferramentas de IA suportadas](#ferramentas-de-ia-suportadas)
- [O que foi instalado](#o-que-foi-instalado)
- [Como começar](#como-começar)
- [Referências externas](#referências-externas)

---

## Introdução

Esta é a camada de inteligência artificial do repositório. Mesmo que você nunca tenha trabalhado com IA antes, a leitura a seguir mostra o que existe, como está organizado e como você pode começar a usar.

Exemplos práticos do que você pode pedir ao assistente:

- *"Analise este diretório e me diga quais são os pontos de risco de segurança."*
- *"Crie um plano de implementação para adicionar um novo campo no formulário X."*
- *"Revise o código que acabei de escrever e verifique se segue os padrões do projeto."*

Prompts prontos para adaptar e enviar estão em [`prompts-exemplo.md`](prompts-exemplo.md).

---

## Filosofia e origem da stack

A stack foi construída sobre um padrão aberto adotado por comunidades de desenvolvimento ao redor do mundo: o [AGENTS.md](https://agents.md). Esse padrão, hoje mantido pela Agentic AI Foundation sob a Linux Foundation e presente em mais de 60 mil projetos de código aberto (projetos cujo código-fonte é público e pode ser inspecionado por qualquer pessoa), propõe uma separação clara entre o que é escrito para humanos e o que é escrito para agentes de IA:

- **README.md**: para humanos (visão geral, contexto, como começar)
- **AGENTS.md**: para agentes (regras de codificação, restrições, padrões do projeto)

Ao seguir esse padrão, a stack se torna **agnóstica de ferramenta**: qualquer assistente de IA que respeite o `AGENTS.md` consegue trabalhar no repositório sem configuração adicional. O projeto não fica preso a uma ferramenta específica, e o contexto viaja junto com o código.

Ferramentas como GitHub Copilot, OpenCode e outras da comunidade já seguem esse padrão nativamente. Isso significa que o investimento feito na stack funciona independentemente de qual ferramenta cada pessoa escolher usar.

> **Origem da convenção:** o `AGENTS.md` é estruturado com base na convenção publicada em [agents.md](https://agents.md). O padrão não possui versionamento formal; a referência de atualização é o próprio site.

O arquivo `CLAUDE.md` existe apenas como ponteiro de compatibilidade para o `AGENTS.md`.

---

## O que são modelos de IA

Um **modelo de IA** é o "cérebro" por trás do assistente: o sistema que entende o que você escreve e gera respostas. Modelos conhecidos incluem Claude, GPT e outros modelos compatíveis com ferramentas de desenvolvimento.

Os modelos são acessados por meio de ferramentas já integradas ao fluxo de trabalho. Você não precisa acessar o modelo diretamente; a ferramenta faz isso por você.

| Ferramenta | Modelo / Provedor |
|---|---|
| GitHub Copilot | GPT ou Claude, dependendo do plano da conta |
| OpenCode | Configurável, compatível com múltiplos modelos |
| Claude Code | Modelo Claude (Anthropic) |

Os modelos evoluem com frequência. O que importa para o uso do dia a dia é a **ferramenta**; o modelo é apenas o motor por baixo.

---

## O que são agentes de IA

Um **agente de IA** é um assistente configurado para operar com um conjunto específico de instruções, contexto e regras. Diferente de um modelo genérico que você acessa pelo navegador, um agente:

- Conhece o projeto em que está trabalhando
- Segue **guardrails** (regras que definem o que ele pode e não pode fazer, como quais arquivos pode modificar e quais padrões de código deve seguir)
- Segue um fluxo estruturado em vez de responder de forma livre

Os agentes são configurados na pasta `.agents/` e integrados às ferramentas via `.github/` (Copilot), `.opencode/` (OpenCode) e `.claude/` (Claude Code). Eles não são programas independentes; são instruções que ensinam a ferramenta de IA a agir como especialista no contexto deste projeto.

**Na prática:** quando você abre o repositório no VS Code (editor de código da Microsoft) com o Copilot e pede *"revise este código segundo os padrões do projeto"*, o agente carrega automaticamente as regras do `AGENTS.md`, os guardrails de segurança e os padrões de codificação, e entrega uma revisão contextualizada, não genérica.

---

## Skills: agentes especializados

As **skills** são agentes especializados em tarefas específicas. Cada skill tem um escopo bem definido. Para acionar uma skill, mencione o nome dela na conversa com o assistente (no painel de chat da ferramenta de IA, como o chat do Copilot no VS Code). Por exemplo: *"Use a skill `skill-creator` para criar uma skill de X."* O assistente carrega as instruções da skill e executa o processo correspondente.

A tabela abaixo lista as skills que a stack instala. Parte vem de terceiros e parte é mantida pela Anatel no repositório `ai-skills`, o mesmo da `stack-ai-init`. A origem, a versão e a licença de cada skill de terceiros ficam registradas na skill `stack-ai-init`, no repositório `ai-skills`; a troca de versão é coordenada pela equipe conforme [`manutencao-da-stack.md`](manutencao-da-stack.md).

| Skill | O que faz |
|---|---|
| `speckit-<fase>`, uma por fase | Conduzem as fases do fluxo SDD com SpecKit |
| `skill-creator` | Cria, edita e avalia skills |
| `dicionario-dados-db-scan-codebase-docs` | Cria, atualiza e verifica dicionários de dados e changelogs estruturais de banco de dados a partir da codebase, dos scripts de banco e da documentação |
| `manual-creator-scan-codebase` | Cria, atualiza e verifica manuais de uso em Markdown a partir da codebase, com adaptador, personas e plano de capturas de tela |
| `gauntlet-loop-forge` | Transforma um objetivo, plano, especificação ou prompt existente em um prompt de execução pronto para colar, com critérios de aceite verificáveis, revisão por agente que não construiu o artefato e limite finito de rodadas |
| `caveman` | Comprime a prosa da resposta preservando termo técnico, código e mensagem de erro |
| `grill-me` e `grilling` | Entrevistam o desenvolvedor sobre um plano ou design, em rodadas de perguntas com resposta recomendada, antes de implementar |
| `writing-for-agents` | Orienta a escrita de documento que agente de IA lê: skill, `AGENTS.md`, `CLAUDE.md` e arquivo alcançado por ponteiro de contexto |
| `owasp-playbook` | Revisão de segurança por procedimento OWASP: 17 plays cobrindo código, Top 10, API, segredos, dependências, infraestrutura como código, mobile, agente de IA, servidor MCP e aplicação LLM, mais o índice do ASVS para código novo |
| `stack-ai-build-project-context` | Investiga a codebase e gera ou atualiza a base operacional para agentes: `AGENTS.md`, referências e skills do projeto |
| `configuracao-de-testes-unitarios` | Lê a infraestrutura do projeto e entrega um plano para configurar testes unitários e cobertura, sem meta e sem executar nada |
| `frontend-design` | Orienta o projeto ou a reformulação de interface frontend com direção visual intencional |

As skills `caveman` e `grill-me` são **modos opcionais**: o agente nunca as aciona sozinho, e elas só entram se você invocar. O `caveman` comprime as respostas e o `grill-me` interroga um plano ou um pedido antes de você aprová-lo. O uso delas está em [`prompts-exemplo.md`](prompts-exemplo.md), na seção de modos auxiliares.

A `owasp-playbook` também é **opt-in**: o agente nunca a aciona sozinho. Para pedir, basta uma frase em português, sem conhecer segurança: a skill escolhe os procedimentos pelo que existe no escopo, traduz o resultado pelo guia de segurança e responde com um resumo em linguagem simples antes da tabela técnica. Os prompts estão em [`prompts-exemplo.md`](prompts-exemplo.md#revisão-de-segurança). A pasta `upstream/` dela é cópia literal do projeto de origem e não deve ser editada; o que é do projeto entra pela ponte `.agents/security/mapa-cwe-guia.md`.

O projeto pode ter outras skills além dessas 22, criadas pela própria equipe. Elas ficam no mesmo `.agents/skills/`, são versionadas junto com o repositório e estão descritas no `README.md` da raiz.

---

## O que é SDD

**[SDD](https://martinfowler.com/articles/exploring-gen-ai/sdd-3-tools.html)** (Specification-Driven Development, ou Desenvolvimento Orientado por Especificação) é uma abordagem de trabalho que coloca a especificação antes da implementação.

A ideia central é simples: antes de escrever código (implementar = traduzir uma necessidade em instruções que o computador executa), você produz uma descrição clara e estruturada do que precisa ser feito, e o agente de IA parte sempre dessa descrição. Isso evita o problema mais comum no uso de IA para código: pedir para implementar algo sem ter definido direito o que é esse "algo". Com SDD, o raciocínio vem antes da implementação.

Aqui o SDD é implementado pelo framework **SpecKit**, documentado em [`speckit.md`](speckit.md). O fluxo completo vai de `especificar → clarificar → planejar → decompor → implementar`.

---

## Ferramentas de IA suportadas

O repositório é **agnóstico de ferramenta**: você pode usar qualquer assistente de IA, desde que ele consiga ler o contexto do repositório (especialmente `.agents/skills/` e `AGENTS.md`).

A ferramenta **recomendada** é a extensão do **GitHub Copilot no [VS Code](https://code.visualstudio.com)** (editor de código gratuito da Microsoft), por ser a mais simples de configurar.

| Ferramenta | Como instalar | Arquivos de configuração |
|---|---|---|
| **[GitHub Copilot](https://github.com/features/copilot)** | Instale a extensão "GitHub Copilot" pelo marketplace do VS Code (a loja de extensões do editor, equivalente a uma loja de aplicativos) e faça login com sua conta GitHub | `.github/copilot-instructions.md` e `.vscode/settings.json` |
| **[OpenCode](https://opencode.ai)** | Instale via terminal (a interface de texto do computador onde você digita comandos) com `npm install -g opencode-ai` e configure o modelo desejado | `.opencode/opencode.json` |
| **[Claude Code](https://claude.com/claude-code)** | Instale via terminal com `curl -fsSL https://claude.ai/install.sh \| bash` e faça login com sua conta Claude | `.claude-plugin/marketplace.json` e `.claude/settings.json` |

> **Primeira vez no Claude Code?** As skills aparecem a partir da segunda sessão. A primeira abertura na pasta registra o plugin da stack, e a seguinte já carrega as skills. Se você abriu e não viu nenhuma, feche e abra de novo. Não é preciso rodar nenhum comando.

> **O que é npm?** É o gerenciador de pacotes do Node.js, uma ferramenta de linha de comando usada para instalar softwares de desenvolvimento. Se você nunca usou, peça ajuda a um desenvolvedor da equipe para instalar o OpenCode.

Se você optar por uma ferramenta diferente das listadas acima, confirme antes que ela consegue ler `.agents/skills/` e seguir as instruções do `AGENTS.md`.

---

## O que foi instalado

```text
.agents/
├── references/    # Material de referência consultado pelas skills
├── security/      # Guia de segurança e ponte da skill owasp-playbook
└── skills/        # As skills: agentes especializados por tipo de tarefa

.claude/
└── settings.json  # Registra o marketplace do repositório e liga o plugin stack-ai

.claude-plugin/
└── marketplace.json  # Plugin local stack-ai; é assim que o Claude Code enxerga as skills

.github/
└── copilot-instructions.md  # Instruções curtas carregadas pelo Copilot

.opencode/
└── opencode.json  # Configuração do OpenCode para o repositório

.specify/
├── integrations/  # Manifestos de integração do SpecKit por ferramenta
├── memory/        # constitution.md do SpecKit, mantido intencionalmente vazio
├── scripts/       # Scripts chamados pelas skills em tempo de execução
├── templates/     # Templates de origem do SpecKit
└── workflows/     # Registro de workflows do SpecKit

.vscode/
└── settings.json  # Aponta o Copilot para .agents/skills/

docs/stack-ai/     # Esta documentação
docs/architecture/ # Documentação de arquitetura do projeto, com as decisões registradas em adr/
docs/requisitos-ancorados/padrao/ # Regras e templates dos requisitos do sistema, mantidos em docs/requisitos-ancorados/
AGENTS.md          # Regras do projeto para agentes de IA (leia antes de contribuir)
CLAUDE.md          # Ponteiro de compatibilidade para AGENTS.md
```

As pastas que começam com ponto são pastas de configuração. Elas não contêm código da aplicação, e sim as instruções e ferramentas que orientam o trabalho dos agentes de IA.

A pasta `specs/` não vem na instalação: ela é criada na primeira vez que uma fase do SpecKit rodar, e guarda os documentos gerados para cada funcionalidade.

**Arquivos e pastas no `.gitignore`**

Alguns itens existem apenas localmente em cada máquina e estão listados no `.gitignore` para não serem versionados. Eles devem permanecer assim:

| Item | Por que não é versionado |
|---|---|
| `/specs` | Documentos gerados pelo SpecKit para cada funcionalidade; são locais e descartáveis. A linha só entra quando o repositório ainda não tem a pasta `specs/` |
| `.claude/settings.local.json` | Configuração local do Claude Code, diferente em cada máquina |
| `.specify/feature.json` | Configuração local do SpecKit, por desenvolvedor |
| `.specify/extensions/*/local-config.yml` | Configuração local das extensões do SpecKit, por desenvolvedor |
| `__pycache__/` | Cache de bytecode que o Python gera ao rodar os scripts e os testes das skills; é recriado a cada execução |

Antes de remover qualquer item do `.gitignore`, avalie se a remoção é realmente necessária. Na dúvida, mantenha.

---

## Como começar

**Passo 1: Baixe o repositório.** Faça o clone do repositório (baixe uma cópia local usando Git) e abra a pasta raiz no VS Code.

**Passo 2: Instale uma ferramenta.** A recomendada é a extensão GitHub Copilot no VS Code. Abra o VS Code, clique no ícone de extensões na barra lateral (quatro quadradinhos), pesquise "GitHub Copilot" e clique em instalar. Depois faça login com sua conta GitHub. O Copilot detecta automaticamente os arquivos de configuração do projeto: ao abrir a pasta, ele lê o `AGENTS.md` e as pastas `.github/` e `.agents/`, carregando as regras e os agentes do projeto sem nenhuma ação adicional sua.

**Passo 3: Para uma demanda simples,** abra o chat da ferramenta e descreva o que precisa. Exemplo:

> *"Analise o diretório X sem alterar nada. Quero entender como essa parte funciona."*

**Passo 4: Para uma demanda maior ou ambígua,** use o fluxo SpecKit, começando por `/speckit-specify`. Leia [`speckit.md`](speckit.md) antes.

**Passo 5: Consulte os exemplos** em [`prompts-exemplo.md`](prompts-exemplo.md) para ver como formular bons pedidos para diferentes tipos de demanda.

Você **não** precisa instalar o SpecKit nem qualquer componente adicional da stack. Tudo já está configurado no repositório. O único passo é instalar a ferramenta de IA.

---

## Referências externas

| Ferramenta / Padrão | Link |
|---|---|
| VS Code | [code.visualstudio.com](https://code.visualstudio.com) |
| GitHub Copilot | [github.com/features/copilot](https://github.com/features/copilot) |
| OpenCode | [opencode.ai](https://opencode.ai) |
| Claude Code | [claude.com/claude-code](https://claude.com/claude-code) |
| Git | [git-scm.com](https://git-scm.com) |
| SpecKit | [github.com/github/spec-kit](https://github.com/github/spec-kit) |
| Padrão AGENTS.md | [agents.md](https://agents.md) |
