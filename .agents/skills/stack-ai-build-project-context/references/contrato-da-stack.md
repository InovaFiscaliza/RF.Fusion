# Contrato da stack

Autoridade sobre: o que constitui a stack minima, o que cada destino aceita, o contrato das
secoes do esqueleto, como indices sao atualizados e como a skill se comporta em execucoes
repetidas.

## Pecas da stack

| Peca | Papel | Obrigatoria |
|---|---|---|
| `AGENTS.md` na raiz | Contexto global carregado em toda tarefa | Sim |
| `.agents/references/` | Conhecimento consultivo, carregado sob demanda | Sim |
| `.agents/skills/` | Fluxos de implementacao e de verificacao | Sim |
| `.agents/security/mapa-cwe-guia.md` | Ponte da `owasp-playbook`: traduz o achado do play para o controle local e registra o que o projeto tem de proprio. A skill preenche as quatro secoes de projeto | Nao |
| Ponteiro de compatibilidade (`CLAUDE.md`, `.github/copilot-instructions.md` ou equivalente) | Faz a ferramenta local encontrar o `AGENTS.md` | Nao |
| Indice de skills ou registro de auditoria | Lista as skills instaladas, origem e composicao | Nao |
| `docs/architecture/` | Documentação de arquitetura do destino, com as ADRs em `docs/architecture/adr/` | Não |
| `docs/requisitos-ancorados/` | Regras de negócio, requisitos funcionais e requisitos técnicos vigentes do sistema; a stack traz só as regras do formato e os templates, em `padrao/` | Não |
| Demais pastas da stack (`checklists/`, `memory/` e o restante de `security/`) | Usos especificos do repositorio de destino | Nao |

A skill escreve em `AGENTS.md`, `.agents/references/` e `.agents/skills/`, e preenche as quatro secoes de projeto de `.agents/security/mapa-cwe-guia.md` quando o arquivo existir no destino. Nao escreve nas
demais pastas nem em `.claude/`. Quando um achado pertencer claramente a outra pasta da stack,
ou a uma automacao da ferramenta, registre a recomendacao no relatorio final e deixe a decisao
com o desenvolvedor.

Guardrail nao e uma quarta pasta: e uma forma de conteudo, alocada conforme
`criterios-de-classificacao.md` entre `AGENTS.md`, a skill do fluxo e a reference do modulo.

## Estados possiveis no destino

**Stack minima presente.** As tres pecas obrigatorias existem. Prossiga.

**Stack parcial.** Falta uma das tres. Reporte e peca autorizacao para criar somente a peca
faltante, vazia, antes de prosseguir.

**Stack ausente.** Nenhuma peca existe. Pare e confirme com o desenvolvedor que ele quer
instalar a stack ali. Instalar e decisao do repositorio, nao efeito colateral de uma
investigacao.

**Stack presente e ja povoada.** Situacao normal em reexecucoes. Leia tudo antes de investigar:
o valor da rodada esta no delta.

## Contrato das secoes do esqueleto

O esqueleto distribuido pela instalacao ja traz secoes com nome fixo. Secao existente e um
contrato de conteudo, nao um espaco opcional, e cada uma tem regra propria:

| Secao | Natureza | Forma | Preenchida com |
|---|---|---|---|
| `Contexto do Projeto` | inventario | prosa, duas ou tres linhas | o que o sistema e e faz, qual unidade ele produz repetidamente e onde ela vive, mais como tratar o projeto |
| `Dependencias Tecnicas do Projeto` | inventario | topicos `**<nome>**: <valor>` | runtime com versao, dados, componentes externos, bibliotecas com versao, ferramentas, ausencias |
| `Escopo e Limites de Escrita` | regra | duas listas de caminhos, `Permitido` e `Proibido sem autorizacao` | caminhos com curinga, mais a condicao de excecao quando houver |
| `Hierarquia de Autoridade` | regra | topicos, duas ou tres linhas | qual documento vale e o que fazer no conflito |
| `Disciplina Agentica` | regra fixa da stack | quatro passos numerados em subsecoes `###`, cada um com tese em negrito, topicos e teste de saida; o passo 4 traz o modelo do plano, com o impacto arquitetural e o checklist de critérios verificáveis. Os nomes dos quatro passos abrem o arquivo, como lista numerada, no lugar de um titulo | texto que chega pronto da instalacao e vale igual em todo projeto; a rodada preserva o texto e nao acrescenta principio |
| `Guardrails Universais` | alerta | topicos `**<assunto>**: <regra com o simbolo literal>` | convencoes transversais violaveis por implementacao nova |
| `Qualidade Minima` | regra | topicos `**<assunto>**: <regra>` | o que verificar antes de entregar, com o comando quando existir |
| `Regras de Decisao` | regra | topicos sem prefixo, imperativos | o que fazer diante de ambiguidade, conflito e pedido fora de escopo |
| `Decisões Arquiteturais` | regra fixa da stack | prosa com os valores da linha de impacto, as duas condições numeradas, os fatores de significância e as dispensas | texto que chega pronto da instalação e vale igual em todo projeto; a rodada preserva o texto e não acrescenta critério |
| `Requisitos Ancorados` | regra fixa da stack | parágrafo de abertura com a pasta e o ponteiro para `padrao/README.md`, tópicos `**<momento>**: <regra>` e teste de saída | texto que chega pronto da instalação e vale igual em todo projeto; a rodada preserva o texto e não acrescenta regra |
| `Regras de Escrita` | regra fixa da stack | paragrafos em prosa, imperativos | texto que chega pronto da instalacao e vale igual em todo projeto; a rodada preserva o texto e nao acrescenta regra |

Tres secoes nao vem no esqueleto e aparecem quando a rodada tem material para elas. Nao as crie
vazias:

| Secao | Aparece quando | Forma |
|---|---|---|
| `Padroes de Projeto` | o sistema usa padroes nomeaveis que uma implementacao nova pode substituir por equivalente externo | tabela `Padrao \| Onde \| Regra` |
| `Fontes de Contexto` | existem references, documentos do repositorio ou dicionarios que respondem por assunto | topicos `**<arquivo ou assunto>**: <quando consultar>` |
| `Roteamento` | existem skills instaladas com gate por artefato | passos numerados, mais tabela `Artefato \| Skill de gate` |

Um guardrail que nao cabe em uma linha vira subsecao `###` dentro de `Guardrails Universais`,
com o nome do padrao, tres ou quatro linhas e o ponteiro para a reference que guarda a prova.

As secoes de inventario **nao passam pelos criterios de admissao de regra**. Exigir delas
transversalidade, frequencia ou busca por contraexemplo deixa a secao vazia ou vaga, e vago ali
e falha da rodada: o censo tem os nomes e os numeros.

## Contrato da ponte de seguranca

`.agents/security/mapa-cwe-guia.md` chega da stack com a tabela `Traducao` pronta e as demais secoes com texto padrao. A skill `owasp-playbook` le o arquivo secao por secao e procura cada uma pelo titulo, entao os titulos nao mudam.

| Secao | Quem escreve | Preenchida com |
|---|---|---|
| `Traducao` | a stack | tabela de CWE para topico do guia, severidade e secao ASVS. Nao e alterada |
| `Sinais do projeto` | esta skill | tabela `Sinal no escopo \| Play \| Onde esta hoje`, um sinal por tipo de artefato que a investigacao encontrou, com o play da `owasp-playbook` que ele dispara |
| `Auditores do projeto` | esta skill | bloco `bash` com comandos que existem no repositorio ou derivados dos guardrails do `AGENTS.md`, cada um executado uma vez antes de entrar |
| `Excecoes` | esta skill | lista do que nao e achado neste repositorio, com o motivo e a evidencia |
| `Saidas opcionais` | a stack | gerador de PDF, de issues ou outro formato. Nao e alterada |
| `Correcao` | esta skill | a skill ou o procedimento do projeto que recebe a correcao, por tipo de artefato |

Cada entrada das quatro secoes de projeto se apoia em evidencia do codigo, arquivo e linha ou saida de comando, pela mesma barra dos demais artefatos. Secao sem sinal ou auditor real mantem o texto padrao que veio da stack: a `owasp-playbook` tem comportamento fixo para secao vazia, e texto inventado ali dispara play que nao se aplica. O que preencher em cada secao esta na Fase 8 do `SKILL.md`; a forma esta em `formato-artefatos.md`.

## Custo de contexto por destino

| Destino | Custo permanente | Custo sob demanda | Quem dispara a carga |
|---|---|---|---|
| Regra ou guardrail em `AGENTS.md` | a linha inteira | zero | ninguem, ja esta no contexto |
| Reference com linha de roteamento | a linha, tipicamente 25 a 30 tokens | o corpo | o agente, seguindo a linha |
| Skill model-invocada | a descricao, tipicamente 80 a 250 tokens | o corpo | o runtime, casando o pedido com a descricao |
| Skill user-invocada (`disable-model-invocation: true`) | zero | descricao e corpo | o humano digitando `/nome`, ou o artefato que manda ler o `SKILL.md` |

O eixo de invocacao e o unico controle que reduz custo permanente sem tirar conhecimento da
stack: a skill continua inteira, com a mesma descricao, e deixa de ser paga em toda tarefa.
Criterio de decisao em `criterios-de-classificacao.md`.

Medicao de uma stack madura real, com 42 skills e 7 references: o `AGENTS.md` custa cerca de
2.660 tokens em toda tarefa, as 42 descricoes somam cerca de 3.700, tambem em toda tarefa, e os
corpos das references somam cerca de 12.900 que so entram quando alguem abre.

Dai saem duas regras que puxam para lados diferentes, e as duas valem.

**Quebrar conhecimento em skill nao economiza contexto.** Quem economiza e a reference. A skill
se paga por disparar sozinha, e a parte cara dela, a descricao, e justamente a que nao pode
encolher.

**Economizar contexto nao e o objetivo.** O objetivo e o agente implementar de forma coerente
com o sistema. O orcamento existe contra inflacao de regra global, nunca contra a existencia da
skill de fluxo ou da reference de arquitetura que ensinam a construir.

### Ponto de equilibrio

Uma regra de tamanho `S` usada numa fracao `f` das tarefas custa `S` em `AGENTS.md` e
`30 + S x f` como reference. A reference fica mais barata quando `f < 1 - 30/S`. Para uma regra
de 100 tokens isso da `f` abaixo de 0,7.

`inventario_stack.py stack` reporta esse orcamento em toda rodada, separa descricao
model-invocada de user-invocada e avisa quando a parte permanente passa do `AGENTS.md`. Somar as
duas categorias superestima o piso: e o erro que a propria ferramenta cometia antes de conhecer o
eixo.

## O que cada destino aceita

### `AGENTS.md`

Aceita inventario nas secoes de contexto e dependencias, regra transversal curta, guardrail de
alcance amplo e roteamento. Uma entrada tipica cabe em uma a tres linhas e, quando depende de
detalhe, aponta para o artefato que guarda a prova.

Nao aceita: tutorial, listagem de estrutura, historico, justificativa longa, conhecimento de um
unico modulo, nem regra que o agente descobre abrindo o arquivo que vai editar.

Em `AGENTS.md` ja povoado, se a adicao o empurrar muito alem do tamanho que ele tinha, o
caminho e mover detalhe existente para reference e deixar o ponteiro. Em `AGENTS.md`
praticamente vazio, o crescimento e esperado: o controle e o teste de admissao, item a item.

### `.agents/references/`

Aceita conhecimento consultivo e detalhado: arquitetura, estrutura das unidades, persistencia,
autenticacao e permissao, integracoes, convencoes de modulo, exemplares representativos,
catalogo de guardrails.

Cada arquivo declara, no inicio, sobre o que tem autoridade e quando deve ser consultado. Um
assunto por arquivo.

**Toda reference precisa de alguem que a alcance**: ou uma regra de `AGENTS.md` que dependa do
detalhe aponta para ela, ou `AGENTS.md` carrega uma linha de roteamento, ou uma skill a
consulta no processo. Reference orfa nao e consultada nem mantida, e o verificador avisa.

Quando o catalogo de skills cresce, o roteamento entre elas vira conhecimento por si so: matriz
de demanda, skills complementares, condicao de bloqueio. Isso e reference, e nao `AGENTS.md`.

### `.agents/skills/`

Aceita fluxo de implementacao e fluxo de verificacao, com processo proprio. Criacao e evolucao
passam pela `skill-creator`, sem excecao.

Skill e o destino esperado do conhecimento sobre como se constroi uma unidade do sistema. Uma
rodada que mapeou a construcao de uma unidade e nao produziu skill precisa dizer por que no
relatorio.

## Indices e registros

Se o destino mantiver indice de skills, registro de auditoria, secao de roteamento no
`AGENTS.md` ou catalogo de references, atualize junto com o item novo. Item criado e nao
registrado nasce invisivel. Descubra o formato lendo as entradas existentes e imite o.

## Idempotencia

A skill roda mais de uma vez no mesmo repositorio. Em toda reexecucao:

- Artefato existente e atualizado, nunca duplicado com nome variante.
- Achado ja coberto sai da fila em silencio.
- Achado ja **descartado** em rodada anterior sai da fila pelo mesmo motivo, e nao volta ao
  Auditor. Para isso o descarte precisa estar escrito onde a proxima rodada le: uma secao
  `## Descartado` na reference de evidencias, com o achado, o motivo, a data e o que faria a
  decisao mudar. Descarte que so aparece no relatorio morre com a sessao, e a rodada seguinte
  gasta a mesma janela para chegar na mesma reprovacao.
- Achado que **contradiz** artefato existente e o item mais valioso da rodada.
- Atualizar ou remover artefato existente passa pelo mesmo gate de aprovacao que criar, e
  remover ou reduzir alcance continua exigindo aprovacao mesmo quando a rodada foi
  autorizada a aplicar sem plano previo: autorizar acrescentar nao e autorizar tirar.

Em stack povoada, a rodada comeca por **medicao de conformidade**: reverifique cada regra de
`AGENTS.md`, cada guardrail e cada afirmacao das secoes de evidencias contra o codigo atual,
classificando como conforme, contradicao ou alcance reduzido.

Rode o comando registrado ao lado da afirmacao, **exatamente como esta escrito**. Quando ele
nao reproduzir o proprio numero, a primeira hipotese e defeito do comando, nao mudanca do
sistema: reproduza com a forma canonica, corrija o comando publicado na mesma rodada e diga
isso no relatorio. Tratar erro de comando como contradicao derruba artefato correto e faz a
stack encolher a cada rodada.

## Medicao no corpo do artefato

O corpo de uma reference e lido por quem vai implementar. A contagem exata quase nunca muda o
que essa pessoa escreve, e cria tres passivos: envelhece a cada commit, precisa ser
reverificada em toda medicao de conformidade, e vira "defeito" quando a proxima rodada mede com
outra ferramenta.

**Prefira nomear o territorio a cravar o numero.** "Os arquivos de `X/Avaliacoes` fazem assim"
orienta melhor que "8 de 22 fazem assim", e continua verdadeiro quando alguem acrescenta um
arquivo. Para proporcao, use a linguagem que a evidencia sustenta: dominante, majoritario,
minoritario, excecao, coexistem sem predominancia.

A contagem nao desaparece: ela vive na secao de evidencias, **na forma do comando que a
produz**. Quem quiser o numero de hoje roda o comando e o obtem de hoje, em vez de ler o de
meses atras.

Cuidado para nao esvaziar o corpo junto com o numero. O que sai e a contagem; o que **entra** no
lugar e o comando de trabalho, parametrizado, no passo em que a decisao acontece. Ver os dois
tipos de comando em `formato-artefatos.md`.

Isso nao afrouxa a barra de evidencia. O investigador continua obrigado a medir, contar
contraexemplo e calcular a classificacao: o que muda e o que vai para o texto que o agente le.

O mesmo vale para o **vocabulario da rodada**. "O censo encontrou", "a heuristica apontou", "nao
foi localizado" e "nesta rodada" descrevem a investigacao, nao o sistema, e obrigam o agente a
traduzir antes de agir. No corpo do artefato vale o fato tecnico no presente do indicativo, com o
nome que o sistema usa; o metodo fica na evidencia. Formas comparadas em
`formato-artefatos.md`.

Duas excecoes, onde o numero e a informacao: quando a proporcao e o proprio achado ("incluido
por muitas paginas, chamado por nenhuma") e quando o guardrail depende da ordem de grandeza para
o leitor calibrar o risco.

## Convencoes do destino

Idioma, encoding, terminacao de linha, estilo de titulo e nomenclatura de arquivo sao os do
repositorio de destino, observados na Fase 1. Um repositorio cuja documentacao esta em ingles
nao recebe reference em portugues, e vice versa.

Encoding e terminacao vem do perfil do censo, e o perfil que vale para a stack e o das
extensoes de documentacao do destino: fonte em ISO-8859 com CRLF nao obriga a reference a
nascer assim. Ao editar artefato existente, preserve o que ele ja tem.
