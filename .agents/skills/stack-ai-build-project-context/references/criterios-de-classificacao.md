# Criterios de classificacao

Autoridade sobre: para onde vai cada padrao consolidado.

## Sumario

- [Teste zero: isto muda o que alguem escreve](#teste-zero-isto-muda-o-que-alguem-escreve)
- [Roteamento por natureza](#roteamento-por-natureza)
- [Admissao em AGENTS.md](#admissao-em-agentsmd)
- [Admissao como skill](#admissao-como-skill)
- [Admissao em references](#admissao-em-references)
- [Admissao como guardrail](#admissao-como-guardrail)
- [Recomendacao de automacao](#recomendacao-de-automacao)
- [Custo de contexto](#custo-de-contexto)
- [Exemplos de classificacao](#exemplos-de-classificacao)
- [Antipadroes de classificacao](#antipadroes-de-classificacao)

## Teste zero: isto muda o que alguem escreve

Antes de escolher destino, um filtro so, e ele vale para todos:

**Um agente que precisa implementar ou alterar algo neste sistema decide diferente por saber
disto?**

Se a resposta for nao, o achado nao entra em lugar nenhum, e o motivo vai para o relatorio.
Se for sim, ele **vai** para algum destino: a duvida passa a ser qual, nunca se merece existir.

Verifique tambem se o fato ja esta registrado. Se estiver na stack, descarte em silencio. Se
estiver na documentacao do proprio repositorio, aponte para o documento e registre so a
consequencia operacional que ele nao tira. Se **contradisser** algo ja registrado, nao
classifique: leve ao Auditor e depois ao desenvolvedor, com as duas evidencias lado a lado.

## Roteamento por natureza

O destino sai da natureza do conhecimento, nao de uma cascata de reprovacoes. Comece aqui:

| Natureza do achado | Destino |
|---|---|
| Composicao do sistema: linguagens, runtime, banco, componentes externos, bibliotecas, ausencia de teste ou de CI | secoes de contexto e dependencias do `AGENTS.md` |
| Fluxo de implementacao ou de verificacao que se repete: criar unidade, acessar dados, tornar uma unidade autenticada, verificar permissao, integrar com subsistema, manter modulo | **skill** |
| Convencao que uma implementacao nova pode violar sem perceber | **guardrail**, alocado pelo alcance |
| Detalhe consultivo: arquitetura, estrutura das unidades, persistencia, autenticacao, integracoes, convencoes de modulo, exemplares | **reference** |
| Regra curta, transversal e consequente, necessaria em quase toda tarefa | linha de regra no `AGENTS.md` |
| Regra mecanica que a ferramenta impoe sozinha | recomendacao de automacao, sem escrita |

Um mesmo achado costuma render dois artefatos, e isso e o resultado normal: o fluxo vira
skill, o detalhe que a skill consulta vira reference, e a convencao violavel vira guardrail que
aponta para os dois. O que nao pode e o mesmo conteudo aparecer inteiro em dois lugares.

## Admissao em AGENTS.md

O arquivo tem tres tipos de conteudo, e eles nao passam pelos mesmos criterios.

### Secoes de inventario

`Contexto do Projeto` e `Dependencias Tecnicas do Projeto` sao **inventario em topicos**,
alimentado pelos fatos de censo e pelos sinais de dependencia. Elas nao passam pelos criterios
de regra: nao se exige transversalidade, nem frequencia, nem busca por contraexemplo.

O que entra: linguagens e runtime, onde os dados vivem e como sao alcancados, componentes
externos dos quais o codigo depende, bibliotecas cliente com versao, ferramentas
proprietarias presentes, codificacao padrao dos fontes, ausencia de teste automatizado, de
linter ou de integracao continua.

O que nao entra: arvore de diretorios, historico, inferencia sobre ambiente de execucao.

A forma e lista de topicos curtos, um por dependencia ou fato, sem prosa, cada um nomeando a
tecnologia, o mecanismo ou a unidade. Duas falhas opostas moram aqui. A linha vaga, do tipo "a
base combina varias tecnologias", nao diz nada. A linha que reporta a medicao, do tipo "o censo
encontrou N arquivos em `X/`", conta o trabalho da rodada onde cabia o fato tecnico: o que o
agente precisa e "a unidade e a pagina `.x` em `X/`, escrita em <tecnologia>". Escreva o nome e o
caminho; o numero fica na evidencia.

### Regra

Cinco criterios, todos obrigatorios. Um so que falhe manda o achado para reference ou skill,
conforme a natureza.

| Criterio | Pergunta | Reprovado quando |
|---|---|---|
| Transversal | Vale para varias areas, ou e condicao de uma classe inteira de tarefa? | Alcance de um modulo unico, sem ser condicao de classe de tarefa |
| Estavel | Continua verdadeiro depois da proxima entrega? | Depende de detalhe que muda toda hora |
| Consequente | Ignorar produz entrega errada, quebrada ou incoerente com o sistema? | Ignorar produz apenas codigo menos elegante |
| Curto | Cabe em uma a tres linhas sem perder o que a torna seguivel? | Precisa de exemplo, tabela ou passo a passo |
| Nao local | O agente deixaria de descobrir isso abrindo o arquivo que vai editar? | Esta visivel no proprio arquivo |

Regra que so funciona com detalhe entra como uma linha operacional apontando para a reference
ou para a skill que carrega o detalhe. Nunca vira paragrafo.

### Roteamento

Uma linha sem regra associada, no formato "consulte `<artefato>` quando `<gatilho>`", existe
para que reference e skill sejam alcancadas. Custa uma linha e evita artefato orfao.

## Admissao como skill

Skill e o destino natural de fluxo de implementacao recorrente, e a rodada que descobre como o
sistema constroi suas unidades e **espera** produzir pelo menos uma. Rodada sem skill nenhuma
pede explicacao no relatorio: ou o sistema nao tem fluxo repetido, o que e raro, ou a
investigacao nao reconstruiu nenhum.

Cinco criterios, todos obrigatorios:

| Criterio | Pergunta | Reprovado quando |
|---|---|---|
| Fluxo | Existe uma sequencia de passos, decisoes e verificacoes? | E um conjunto de fatos, mesmo que grande |
| Recorrencia | A tarefa se repete e vai se repetir? | Aconteceu uma vez, ou tende a nao voltar |
| Gatilho | Da para escrever uma descricao que dispara nos pedidos certos e nao nos vizinhos? | O disparo se confunde com o de skill ja existente |
| Ganho | Sem a skill, o agente erra de forma previsivel ou destoa do sistema? | O agente acerta lendo uma reference curta |
| Escopo unico | Uma responsabilidade clara? | Junta duas capacidades que disparam em momentos diferentes |

Evidencia de recorrencia vem do repositorio: quantidade de unidades do mesmo tipo, frequencia
da operacao no historico, existencia de modelo, gerador ou template que alguem escreveu para
automatizar a tarefa na mao. Modelo repetido no repositorio e o sinal mais confiavel de que
existe fluxo esperando por skill.

Dois formatos aparecem quase sempre, e vale reconhece los:

- **Skill de fluxo.** Como criar ou alterar uma unidade: o que fazer, em que ordem, com o que.
- **Skill de verificacao.** Como conferir que uma unidade esta coerente com o sistema: a lista
  de pontos que se checa depois de escrever, cada um com o padrao que o sustenta.

As duas nascem do mesmo material. Quando os dois cabem, a skill de fluxo referencia a de
verificacao no fim do processo.

Toda skill responde tres coisas, e a ausencia de qualquer uma a reprova: **quando usar** o
padrao, **como reconhece lo** no codigo existente, e **como aplica lo** numa implementacao
nova. Skill que so descreve o padrao e uma reference com descricao cara.

Skill aprovada e criada ou evoluida pela `skill-creator`, sem excecao, incluindo a otimizacao
da descricao de disparo. Antes de criar skill nova, verifique se uma existente ja cobre o
gatilho: evoluir costuma ser melhor que somar uma quase igual, porque duas descricoes proximas
dividem o disparo.

### Eixo de invocacao: decidido aqui, escrito no frontmatter

Toda skill nasce em um de dois estados, e nao ha terceiro:

| Estado | Como e alcancada | Custo permanente | Marca |
|---|---|---|---|
| model-invocada | o modelo reconhece a intencao na frase do usuario e dispara sozinho | a `description` inteira, em toda tarefa do repositorio | nenhuma |
| user-invocada | o humano digita `/nome`, ou outro artefato manda **ler o `SKILL.md`** dela | zero | `disable-model-invocation: true` |

O teste que decide, e ele e uma pergunta so:

> O modelo teria motivo para alcancar esta skill sozinho, a partir de uma frase em linguagem
> natural, sem o usuario digitar o comando?

Sim para ponto de entrada de fluxo: o pedido chega em linguagem natural e ninguem sabe que a skill
existe. Nao para skill de etapa, de verificacao ou de detalhe, que so faz sentido depois que outra
coisa ja comecou: essa e alcancada por quem a chama.

**A regra de alcance vem junto, e e onde se erra.** Skill user-invocada nao tem `description` no
contexto do modelo, entao **nao pode ser ativada pelo nome**. Quem a usa aponta o caminho do
arquivo e manda ler: "consulte `.agents/skills/<nome>/SKILL.md`". Skill user-invocada que nenhum
artefato alcanca nasce invisivel, e o verificador avisa.

A conta que sustenta a regra: uma descricao tipica custa de 80 a 250 tokens em **toda** tarefa do
repositorio, inclusive nas que nada tem a ver com o assunto. Marcar as skills de etapa como
user-invocadas nao esconde nada de ninguem, e devolve esse custo. `inventario_stack.py stack`
reporta as duas categorias separadas.

### Skill de conhecimento, a excecao estreita

Conhecimento parado normalmente e reference. A excecao exige as quatro condicoes juntas: nao
ha regra em `AGENTS.md` a que pendurar a reference; o corpo e grande; o uso e raro; e o gatilho
cabe numa descricao discriminante. Faltando uma, e reference. E ela nasce declarando que so o
agente a invoca.

## Admissao em references

Uma reference precisa de:

- **Assunto proprio.** Um tema que se sustenta sozinho. Assunto que so existe como apendice de
  outro pertence ao arquivo do outro.
- **Gatilho de consulta declarado.** Uma frase objetiva de "consulte quando".
- **Densidade.** Conteudo que um agente competente nao produziria sozinho. Reference que repete
  o manual da linguagem so ocupa espaco.
- **Rastreabilidade.** Secao de evidencias com caminhos, comando e alcance.

Reference e o destino de maior capacidade e menor risco: cobra uma linha de roteamento no
contexto global e carrega o corpo so quando alguem abre. Quando a duvida for entre reference e
descarte, e o achado passou no teste zero, reference vence.

Uma reference que descreve a estrutura de uma unidade tem dois deveres a mais: **descrever o
padrao de forma que ele se aplique sem abrir exemplar nenhum**, com os elementos, a ordem e o
criterio de cada decisao, e citar **exemplares representativos** com caminho completo e a regra
que localiza o equivalente atual. O caminho de um exemplar bom poupa paragrafos, porque o agente
abre e ve; a descricao extraida e o que sobrevive quando aquele arquivo e apagado ou renomeado.

## Admissao como guardrail

Guardrail e o alerta que preserva coerencia com o sistema no momento em que alguem esta prestes
a divergir dele. Ele existe porque padrao documentado numa reference que ninguem abriu nao
impede nada.

Quatro condicoes:

| Condicao | Reprovado quando |
|---|---|
| Padrao medido | A convencao nao tem contagem, ou e caso isolado |
| Desvio reconhecivel | Nao da para dizer, olhando o codigo novo, se ele divergiu |
| Consequencia de coerencia | Divergir nao afeta nada alem de gosto |
| Forma de alerta | O texto proibe, em vez de sinalizar e pedir confirmacao |

A quarta condicao e a que se erra mais. Guardrail **alerta sobre divergencia em relacao ao
padrao identificado**; ele nao afirma que o padrao e tecnicamente ideal, e nao proibe a
excecao. Sistema legado tem excecao legitima, e um guardrail que proibe entra em conflito com o
proprio codigo na primeira delas.

Forma, sempre com o padrao e o territorio onde ele vale:

```text
<padrao identificado>, em <territorio comprovado>.
Ao <acao divergente>, confirme <o que confirmar> antes de seguir: <comando de trabalho>.
```

A medicao continua sendo condicao para o guardrail existir; o que nao vai para o texto e a
contagem. Ela vive na evidencia da reference apontada, na forma do comando que a produz, e o
numero so entra no corpo quando a proporcao e o proprio achado.

**Onde o guardrail vive** depende de quando ele precisa ser lido:

| Alcance | Onde | Por que |
|---|---|---|
| Transversal, vale em quase toda mudanca | linha em `AGENTS.md` | precisa estar em contexto antes de qualquer escrita |
| Especifico de um fluxo | dentro da skill daquele fluxo | e lido quando o fluxo dispara, que e o momento do risco |
| Especifico de um modulo | na reference daquele modulo, com linha de roteamento | so interessa a quem esta la |

A evidencia completa fica sempre na reference correspondente, com contagem e comando. O
guardrail carrega o territorio, o comando de trabalho e o ponteiro, nao a prova inteira.

## Recomendacao de automacao

Antes de descartar, pergunte se o achado nao pertence a ferramenta em vez do texto. Regra que a
ferramenta impoe sozinha nao precisa ser lida, e regra imposta erra menos que regra lembrada.

| Forma do achado | Automacao |
|---|---|
| Regra mecanica, verificavel por comando, cuja violacao e sempre erro | hook |
| Verificacao especializada que roda depois da mudanca | subagente, ou skill de verificacao quando o julgamento for necessario |
| Fluxo frequente, com disparo fixo e passos estaveis | comando |

Isto e recomendacao, nao escrita: a skill nomeia o tipo, o gatilho e o achado que motiva, e a
decisao fica com o desenvolvedor. No maximo dois itens por tipo no relatorio. Servidor MCP nao
entra: integracao externa e decisao da stack basica, nao achado de investigacao.

O sinal vem do censo deste repositorio, nunca de uma lista de stacks conhecidas. E automacao
nao substitui o artefato quando o agente precisa saber por que: o hook impede o erro, a
reference evita que alguem lute contra o hook sem entender.

## Custo de contexto

| Destino | Custo permanente | Custo sob demanda |
|---|---|---|
| Regra ou guardrail em `AGENTS.md` | a linha inteira | zero |
| Reference com roteamento | a linha de roteamento, cerca de 30 tokens | o corpo |
| Skill | a descricao, tipicamente 80 a 250 tokens | o corpo |

Duas conclusoes praticas saem dai, e elas puxam para lados diferentes.

**Quebrar conhecimento em skill nao economiza contexto.** Quem economiza e a reference. A skill
se paga por disparar sozinha no momento certo, e a parte cara dela, a descricao, e justamente a
que nao pode encolher.

**Economizar contexto nao e o objetivo da stack.** O objetivo e o agente implementar certo. Uma
stack de 800 tokens permanentes que nao ensina a construir nada custa mais caro, em retrabalho,
que uma de 1.500 que ensina. Aplique o orcamento contra inflacao de regra global, nunca contra
a existencia de skill de fluxo ou de reference de arquitetura.

## Exemplos de classificacao

Exemplos de forma, vindos de ecossistemas variados. Nao sao conteudo esperado em nenhum
repositorio.

**Para as secoes de inventario.** "Runtime X servido por Y, dados em Z alcancados por
componente W, biblioteca de interface na versao V, sem teste automatizado nem integracao
continua." Cada item e um topico, e todos saem do censo.

**Para skill de fluxo.** "Criar uma unidade nova exige oito passos em quatro diretorios, com um
gerador, tres registros e uma verificacao. O repositorio tem dezenove unidades seguindo esse
caminho, e os tres ultimos commits do tipo corrigiram o mesmo passo esquecido."

**Para skill de verificacao.** "Toda unidade do tipo A precisa ter, no topo, a inicializacao I
e a validacao V, e o repositorio mostra 300 casos com as duas e 12 sem. A skill confere os
pontos e diz qual falta."

**Para reference.** "Como o roteamento de eventos funciona entre o produtor e os quatro
consumidores, com a ordem de reprocessamento e os exemplares representativos de cada caso."

**Para guardrail em `AGENTS.md`.** "A conexao vem do componente C em todo o sistema. Ao
criar conexao propria, confirme antes o que o modulo usa: `<comando>`."

**Para guardrail dentro de uma skill.** "Antes de expor uma acao de escrita, confirme a
verificacao ativa no proprio arquivo: incluir o componente de acesso nao a garante."

**Rebaixado de skill para reference.** "Uma skill que explique o modelo de permissoes." Nao ha
fluxo: ha conhecimento. Mas se existir um caminho repetido para **tornar uma unidade
protegida**, esse caminho e skill, e a reference vira o material que ela consulta.

**Descartado.** "Os arquivos de um mesmo modulo ficam no mesmo diretorio." O agente ve sozinho.

## Antipadroes de classificacao

| Antipadrao | Consequencia |
|---|---|
| Aplicar a barra de regra global a todo achado e descartar o que nao passa | Rodada termina sem artefato util, e a stack so acumula proibicoes |
| Rebaixar fluxo de implementacao para reference "por economia de contexto" | O conhecimento que mais orienta implementacao vira arquivo que ninguem abre |
| Promover achado interessante para `AGENTS.md` porque parece importante | Contexto global infla, e o que era importante deixa de se destacar |
| Deixar as secoes de inventario vagas por medo de inflar contexto | Sao inventario, nao regra: o custo e baixo e o valor e alto |
| Escrever guardrail como proibicao | Entra em conflito com a excecao legitima e o agente passa a ignorar o guardrail inteiro |
| Guardrail sem padrao medido, ou sem ponteiro para a evidencia | Vira opiniao, e a primeira excecao encontrada o derruba |
| Escrever no artefato como o achado foi medido, em vez do que o sistema e | O agente recebe o relatorio da rodada onde precisava do fato tecnico, e a frase envelhece a cada commit |
| Ancorar o padrao num arquivo nomeado em vez de extrai lo | No dia em que o arquivo sai, a instrucao vira ponteiro quebrado e ninguem sabe o que era para copiar |
| Criar uma skill por modulo quando o fluxo e o mesmo | Manutencao multiplicada e disparo diluido |
| Registrar a mesma regra em dois destinos | Duas fontes divergem com o tempo |
| Classificar antes de fechar o alcance | Achado de modulo vira regra de repositorio |
| Transformar padroes concorrentes em pergunta e nao registrar nenhum | Os dois existem no codigo; o agente escolhe ao acaso enquanto a pergunta espera |
| Descartar convencao recorrente por parecer antipadrao tecnico | O agente escreve fora do padrao do sistema e o codigo novo destoa |
| Recomendar automacao por lista de stack conhecida | Descreve um sistema imaginario |
