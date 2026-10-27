# Formato dos artefatos

Autoridade sobre: como escrever cada artefato aprovado, como registrar evidencia e como
acionar a `skill-creator`.

## Sumario

- [Regra que precede todas](#regra-que-precede-todas)
- [Disciplina do implementador](#disciplina-do-implementador)
- [O artefato descreve o sistema, nao a rodada](#o-artefato-descreve-o-sistema-nao-a-rodada)
- [O que foi medido e o que foi lido](#o-que-foi-medido-e-o-que-foi-lido)
- [Reclassificacao entre rodadas](#reclassificacao-entre-rodadas)
- [Secoes de inventario do AGENTS.md](#secoes-de-inventario-do-agentsmd)
- [Entrada de regra em AGENTS.md](#entrada-de-regra-em-agentsmd)
- [Guardrail](#guardrail)
- [Reference](#reference)
- [Os dois tipos de comando, e onde cada um vive](#os-dois-tipos-de-comando-e-onde-cada-um-vive)
- [Exemplares](#exemplares)
- [Skill](#skill)
- [Descartado, na reference de evidencias](#descartado-na-reference-de-evidencias)
- [Recomendacao de automacao](#recomendacao-de-automacao)
- [Ponte de seguranca](#ponte-de-seguranca)
- [Verificacao antes de fechar](#verificacao-antes-de-fechar)

## Regra que precede todas

Formato, idioma, encoding, terminacao de linha e nomenclatura sao os do repositorio de
destino, observados na Fase 1. Os modelos abaixo definem **estrutura de informacao**, nao
aparencia.

Encoding e terminacao saem do perfil do censo, e o perfil que vale e o dos arquivos de
documentacao do destino, nao o dos fontes. Um repositorio pode ter fontes em ISO-8859 com CRLF
e `.md` em UTF-8 com LF. Ao editar artefato existente, preserve o que ele ja tem.

## Disciplina do implementador

Quem escreve o artefato e a sessao principal, por decisao registrada em
`protocolo-subagentes.md`: escrever e sequencial e compartilha contexto com a consolidacao, e a
separacao que importa no Gauntlet Loop e entre quem escreve e quem audita. Nao ha subagente aqui, e
por isso a disciplina precisa estar escrita: e a mesma entidade que acabou de consolidar o achado
que agora o redige, e ela ja acredita nele.

`gauntlet-loop.md` chama a ampliacao de alcance na redacao de **origem mais comum de generalizacao
errada**. Ela nao acontece por descuido, acontece porque o texto fica melhor: "os arquivos de
`X/Y`" vira "o sistema", a ressalva encurta, o numero vira "a maioria". Cada uma dessas trocas
melhora a frase e piora o artefato.

Antes de mandar o rascunho ao Auditor, faca uma passagem comparando **o artefato contra o achado
consolidado**, e nada mais. E barata, nao precisa do repositorio e pega o que a auditoria contra o
codigo nao pega, porque o defeito aqui nasce entre os dois textos:

| Confira | O defeito que isso pega |
|---|---|
| O alcance escrito e o alcance classificado, palavra por palavra | O achado valia num modulo e o texto diz "no repositorio" |
| O que veio `inferido:` saiu `inferido:` | A marca some na redacao e a inferencia vira fato |
| Toda contagem do texto veio do achado, sem arredondar | "A maioria" no lugar de "18 de 22" descarta a evidencia e nao pode ser remedido |
| Nenhuma afirmacao no artefato esta ausente do achado | Conector que virou instrucao, exemplo inventado para ilustrar, regra deduzida do padrao vizinho |
| O vocabulario e o do sistema, nao o da arquitetura conhecida | "Camada de servico" e "repositorio" entram como se o codigo os usasse, e o agente procura o que nao existe |
| Ressalva do achado sobreviveu ao corte | O "exceto em `X`" some porque atrapalhava a frase, e o guardrail passa a errar em `X` |

Nesta passagem, trabalhe so com o que a descoberta ja entregou. Achado incompleto volta para a
consolidacao ou entra reduzido ao que a evidencia sustenta, nunca completado com o que parece
razoavel.

## O artefato descreve o sistema, nao a rodada

Quem le e um agente prestes a escrever codigo. Ele precisa do fato tecnico no presente do
indicativo, com o nome que o sistema usa. Como aquele fato foi descoberto nao muda nenhuma linha
do que ele vai escrever.

Fica fora do corpo de qualquer artefato o vocabulario da investigacao: "o censo encontrou", "a
heuristica apontou", "nesta rodada", "nao foi localizado", "a amostra mostrou". Ele tem um lugar
proprio, que e a secao de evidencias.

| Forma que narra a rodada | Forma que declara o sistema |
|---|---|
| "o censo encontrou 1.413 arquivos `.asp` em `src/asp/`" | "o front-end e ASP classico com VBScript; a unidade e a pagina `.asp` em `src/asp/`" |
| "nao foi localizado diretorio de teste" | "sem teste automatizado versionado" |
| "a heuristica achou request, SQL e HTML no mesmo arquivo em 718 de 1.413" | "a pagina concentra entrada, consulta e apresentacao no mesmo arquivo" |

Um teste separa as duas colunas: **se o numero mudar amanha, a frase fica errada?** Se ficar,
ela esta contando a medicao. "A tecnologia e X", "a conexao vem de Y" e "a unidade vive em `Z/`"
nao envelhecem com um commit; a contagem envelhece a cada um.

## O que foi medido e o que foi lido

A barra desta skill e evidencia medida, e o texto do artefato nasce assim por padrao. Mas parte do
que se descobre lendo um arquivo por inteiro nao se mede com busca: a ordem em que as coisas
acontecem, o que um include entrega a quem o carrega, o motivo aparente de uma ramificacao. Isso e
conhecimento util e entra, desde que entre **marcado**.

A marca e da excecao, nao da regra. Afirmacao medida nao recebe nada; afirmacao lida recebe o
prefixo `inferido:` e, junto, **o que a confirmaria**:

```markdown
- A pagina obtem a conexao de `<componente>` antes de qualquer consulta.
- inferido: o include pai entrega a autorizacao quando a propria pagina nao a chama. Confirma-se
  abrindo o include do modulo e procurando a chamada ativa.
```

Duas coisas que a marca **nao** autoriza. Ela nao vale para consequencia de execucao, que continua
fora do artefato enquanto ninguem executar nada. E ela nao e atalho para escrever o que daria
trabalho medir: se existe busca que decide a questao, o lugar dela e a secao de evidencias, nao a
marca.

### A terceira marca: o que foi investigado e nao fechou

Um eixo pode ser investigado e nao fechar. Hoje isso vira uma linha de "nao consolidado" no
relatorio, que morre com a rodada: o artefato fica em silencio, o agente que le assume que nao ha
nada a saber ali, e a rodada seguinte reinvestiga do zero.

Quando a lacuna **muda o que alguem faz**, ela entra no artefato com o prefixo `lacuna:`, e leva
junto o que ja foi buscado:

```markdown
- lacuna: nao se determinou o que valida o formato do documento antes da gravacao. Buscado por
  `<comando literal>` em `<escopo>`, sem ocorrencia ativa. Confirme no modulo antes de assumir
  que nao ha validacao.
```

A marca vale **so em reference**, nunca em `AGENTS.md`. Contexto permanente e caro demais para
carregar o que a rodada nao descobriu; a reference e consultada por quem ja esta no assunto, e ali
a lacuna evita as duas coisas erradas ao mesmo tempo: o agente assumir ausencia e a rodada seguinte
repetir a busca.

Lacuna sem o que foi buscado nao entra. Sem isso ela nao economiza nada da proxima rodada, e vira
so uma confissao ocupando espaco.

### Reclassificacao entre rodadas

As tres marcas nao sao permanentes. A stack e remedida, e a medicao muda o que se sabe:

| De | Para | Quando | Gate |
|---|---|---|---|
| `inferido:` | medido | uma rodada posterior mediu o que faltava | direto: a marca sai e a evidencia entra na tabela |
| `lacuna:` | `inferido:` ou medido | a busca que faltava foi feita e fechou | direto |
| medido | `inferido:` | a medicao de conformidade devolveu `comando-mede-outra-coisa`, ou o alcance encolheu sem que o fato caia | direto: acrescentar ressalva nao e tirar regra |
| medido ou `inferido:` | `lacuna:` | a medicao devolveu contradicao e a afirmacao nao se sustenta mais | **gate do desenvolvedor**: e perda de regra, e cai na regra da Fase 7 |

Promover e barato e deve ser feito assim que a evidencia aparecer: marca `inferido:` que
sobrevive a tres rodadas sem ninguem tentar medi la costuma significar que ninguem leu o artefato,
nao que a questao seja dificil.

## Secoes de inventario do AGENTS.md

`Contexto do Projeto` e `Dependencias Tecnicas do Projeto` sao preenchidas com o censo e os
sinais de dependencia, em topicos curtos, um por fato:

```markdown
## Dependencias Tecnicas do Projeto

- **<runtime ou linguagem>**: <versao ou variante, quando o codigo revelar>
- **Dados**: <onde vivem e por qual mecanismo sao alcancados>
- **<componente externo>**: <o que ele fornece ao codigo local>
- **<biblioteca cliente>**: <versao, quando estiver no nome do arquivo>
- **<ferramenta ou formato proprietario>**: <onde aparece>
- Sem <teste automatizado | linter | integracao continua> versionado
```

Um topico por item, sem prosa e sem justificativa, cada um nomeando a tecnologia, o mecanismo
ou a unidade: "ASP classico com VBScript", "SQL Server alcancado por ADO", "sem teste
automatizado versionado". Quantos arquivos o censo contou nao entra aqui: o numero e o
instrumento de quem investigou, e a tecnologia e o fato de quem implementa. Item que o censo nao
sustenta nao entra; item que ele sustenta e que foi omitido e lacuna da rodada.

`Contexto do Projeto` recebe o que o sistema e, o que ele faz e **qual unidade ele produz
repetidamente**, nomeada pela tecnologia e pelo caminho onde vive, em duas ou tres linhas, mais o
roteamento para as references de arquitetura.

## Entrada de regra em AGENTS.md

Uma linha, uma regra. Regra longa vira uma linha longa, nunca um paragrafo:

```markdown
- **<assunto>**: <regra operacional, com o simbolo literal>. Detalhe em `.agents/references/<arquivo>.md`.
```

Seis exigencias de forma, e a primeira e a que separa regra de conselho:

- **A regra nomeia o simbolo literal do codigo.** O nome da funcao, da constante, do metodo, do
  parametro, do arquivo, como esta escrito no repositorio. "Verificar permissao antes da acao" e
  conselho que serve a qualquer sistema; "`validarLink` mais `validarPermissao` em toda acao" e
  regra deste. Regra sem simbolo nao muda o que ninguem escreve.
- **A proibicao vem colada na alternativa.** "Proibido `<X>`; usar `<Y>`" numa linha so. Proibicao
  sozinha deixa o agente parado, e ele resolve inventando.
- **Nome que varia entra parametrizado.** `<prefixo>_<sigla>_<recurso>` ensina a formar o nome;
  um exemplo concreto so ensina aquele caso.
- **O prefixo em negrito e o termo de busca.** Quem le procura por assunto: `**Transacao**`,
  `**Encoding**`, `**Entrada HTTP**`. Prefixo generico como `**Importante**` nao encontra nada.
- **O ponteiro fecha a propria regra**, e nao vira secao separada. Ele so aparece quando o
  artefato apontado existe.
- **Presente do indicativo e imperativo.** Sem "recomenda-se", sem "idealmente", sem "considere".

**Nao pague duas vezes.** Mover detalhe para uma reference so economiza se a linha que fica
encolher junto. Ao mover, cite no plano quantas linhas saem do `AGENTS.md`, nao apenas quantas
entram na reference.

Fica fora de `AGENTS.md`: justificativa da regra, motivo da alteracao, exemplo de codigo, tabela
de detalhe, historico de decisao, data, contagem, secao de evidencias e descricao de estrutura de
diretorio. Tudo isso tem lugar na reference apontada, onde e lido por quem ja abriu o assunto.

O arquivo tambem nao fala de si mesmo. Nada de explicar o que e `AGENTS.md`, quem o carrega,
quando ele e lido ou como usa lo: quem le ja esta lendo. Cada linha gasta ali e paga em toda
tarefa do repositorio, inclusive nas que nada tem a ver com o assunto.

Tabela quando o conteudo for comparativo e a linha tiver as mesmas colunas em todos os casos,
como padrao de projeto (`Padrao | Onde | Regra`) ou gate por artefato (`Artefato | Skill de
gate`). Topico quando forem regras independentes que so compartilham o assunto da secao.

## Guardrail

Guardrail e alerta de coerencia, nunca proibicao e nunca juizo sobre a qualidade do padrao.
Quando vive em `AGENTS.md`, vale integralmente a forma da secao anterior, e em especial a
exigencia do simbolo literal: guardrail sem o nome que o codigo usa nao e reconhecivel na hora
de aplicar.

Forma minima, com o padrao e o territorio onde ele vale:

```markdown
- **<assunto>**: <padrao identificado>, em <territorio comprovado>. Ao <acao divergente>, confirme <o que confirmar> antes de seguir: `<comando de trabalho>`. Detalhe em `<artefato>`.
```

Quando o sistema tiver dois padroes concorrentes, o guardrail pede confirmacao em vez de
escolher:

```markdown
- **<assunto>**: <A> em <territorio>, <B> em <territorio>. Confirme qual vale no modulo antes de seguir: `<comando de trabalho>`. Detalhe em `<artefato>`.
```

Tres regras fecham o formato:

- **Padrao medido, territorio nomeado.** A medicao e o que separa guardrail de opiniao, mas o
  que o agente precisa ler e onde o padrao vale, nao quantos casos foram contados. A contagem e o
  comando que a produz ficam na evidencia do artefato apontado. O numero so entra no texto quando
  ele e o achado, como em "incluido por quase toda pagina, chamado por poucas".
- **Sem adjetivo de qualidade.** Nem "correto", nem "legado", nem "ideal". O guardrail diz o
  que o sistema faz e pede confirmacao antes do desvio.
- **Um comando de trabalho e um ponteiro.** "Confirme X antes de seguir" sem o comando ao lado e
  torcida; a prova inteira mora na reference.

Quando varios guardrails vierem da mesma rodada, um catalogo em tabela numa reference propria
mantem `AGENTS.md` curto, e o arquivo global fica so com os de alcance transversal:

```markdown
| ID | Padrao identificado | Alerta quando | Evidencia | Onde confirmar |
|---|---|---|---|---|
| G1 | <padrao> | <desvio reconhecivel> | `<comando que mede os dois lados>` | `<artefato>` |
```

## Reference

Modelo em `assets/template-reference.md`. Quatro partes obrigatorias:

1. **Titulo e declaracao de autoridade.** Uma frase dizendo sobre o que este arquivo decide.
2. **Gatilho de consulta.** Uma frase objetiva de "consulte quando".
3. **Corpo.** O conhecimento, do mais consultado para o menos. Tabela quando comparativo, prosa
   quando sequencial. Exemplo de codigo somente quando o padrao nao se descreve em palavras.
4. **Evidencias.** Caminhos, comando, alcance comprovado e data.

Reference que descreve uma unidade do sistema carrega tambem **exemplares representativos**,
com caminho completo e uma linha dizendo o que cada um ilustra.

## Os dois tipos de comando, e onde cada um vive

Um artefato carrega comandos com duas finalidades diferentes, lidos por pessoas diferentes, em
momentos diferentes. Misturar os dois esconde o util e polui o corpo.

| | Comando de evidencia | Comando de trabalho |
|---|---|---|
| Responde | "de onde veio este numero?" | "o que vale no arquivo que estou editando agora?" |
| Quem roda | o Auditor, e a medicao de conformidade da proxima rodada | o agente que esta implementando |
| Quando | so quando a skill roda | em toda tarefa que toca aquele assunto |
| Onde vive | secao **Evidencias**, no rodape | **no corpo**, dentro do passo em que a decisao acontece |
| Forma | fixo, literal, reexecutavel tal como esta | com o alvo parametrizado, para o agente substituir |

O comando de trabalho e o que transforma reference descritiva em ferramenta. Ele aparece sempre
que o artefato disser ao agente para **confirmar alguma coisa antes de escrever**: qual variante
o diretorio usa, se a diretiva esta ativa ou comentada, se o utilitario e chamado ou apenas
incluido, qual definicao de um nome esta em escopo naquele arquivo.

Teste simples para saber onde colocar: se o comando responde uma pergunta sobre **o
repositorio inteiro**, e evidencia. Se responde sobre **o arquivo ou diretorio da tarefa em
curso**, e comando de trabalho e pertence ao corpo.

Uma frase do tipo "confirme que X antes de seguir" sem o comando ao lado nao e instrucao, e
torcida: o agente que nao sabe medir vai supor.

## Exemplares

**O padrao vem escrito; o exemplar so confirma.** Arquivo e apagado, renomeado e movido, e um
artefato que manda "seguir o padrao de `<arquivo>`" morre junto com ele, sem deixar ao agente
como saber o que deveria ter copiado. Escreva os elementos, a ordem, o que e obrigatorio, o que
varia e o criterio de cada decisao, de modo que a implementacao seja possivel sem abrir exemplar
nenhum. O exemplar entra depois, para conferir a forma local do modulo.

Por isso a tabela de exemplares carrega uma coluna a mais: **como localizar o equivalente hoje**,
na forma de regra de nomenclatura, diretorio ou comando. Com ela, um caminho que morreu custa uma
busca; sem ela, custa a instrucao inteira.

```markdown
## Exemplares

| Caminho | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `<caminho completo>` | <o caso tipico, o caso antigo, a excecao nomeada> | `<comando ou regra de nomenclatura>` |
```

O caminho de um exemplar bom poupa paragrafos: o agente abre e ve. Escolha exemplares que
existem hoje e que representam a convencao, nao o caso mais curioso, e confira cada caminho
antes de publicar.

Arquivo acima de trezentas linhas ganha sumario no topo. Arquivo que trata de dois assuntos
vira dois arquivos.

Forma da secao de evidencias:

```markdown
## Evidencias

| Afirmacao | Confianca | Onde foi comprovada | Comando | Alcance | Verificado em |
|---|---|---|---|---|---|
| <resumo> | <medido | inferido | lacuna> | `caminho/arquivo.ext:120` (12 de 340 arquivos) | `<busca literal, com escopo>` | `<alcance>` | <data> |
```

**Todo comando escrito aqui foi executado como esta e devolveu o numero registrado.** Este e o
contrato que permite remedir a conformidade sem refazer a investigacao. Comando que nao reproduz o proprio
numero faz a proxima rodada ler contradicao onde ha erro de comando.

### Historico de conformidade

A coluna `Verificado em` guarda uma data e apaga a anterior. Com ela sozinha nao se distingue regra
que nunca oscilou de regra que quebrou na rodada passada e foi remendada, e as duas merecem
tratamento diferente na proxima medicao.

O historico e um bloco append-only abaixo da tabela, mais recente no topo. Uma linha por afirmacao
remedida, e nunca se reescreve bloco anterior:

```markdown
## Historico de conformidade

### <data>

| Afirmacao | Veredito | Observacao |
|---|---|---|
| <resumo> | <conforme | alcance-reduzido | contradiz | comando-nao-reproduz> | <numero devolvido, ou o que mudou> |
```

Os quatro vereditos sao os que o `agents/medidor-de-conformidade.md` ja devolve, escritos como estao.
Traduzir para vocabulario proprio quebra a correspondencia entre o retorno do auditor e o
historico.

**Afirmacao estavel sai da fila.** Tres vereditos `conforme` consecutivos marcam a afirmacao como
estavel, e a rodada seguinte **nao a reaudita**, com uma condicao: os arquivos do alcance dela nao
podem ter mudado desde a ultima medicao. Isso se confirma com um comando, nao com confianca:

```bash
git log --oneline --since='<data da ultima medicao>' -- <caminhos do alcance> | head
```

Saida vazia confirma o pulo. Saida com commits devolve a afirmacao para a fila, por mais estavel
que ela fosse: o que a tornava estavel era o codigo parado.

E isso que faz a medicao de conformidade ficar mais barata a cada rodada em vez de mais cara. Sem o
historico, a rodada dez paga o mesmo que a rodada um para reconfirmar o que nunca se mexeu.

## Skill

Criacao e evolucao passam pela `skill-creator`, sem excecao. Ela carrega o formato, o teste de
disparo e o processo de refino da descricao.

Acione com a proposta consolidada de `assets/template-proposta-de-skill.md`, para nao reabrir a
investigacao dentro dela. A proposta traz:

- proposito e responsabilidade unica;
- gatilho: frases reais que um desenvolvedor daquele repositorio usaria;
- **quando usar** o padrao, **como reconhece lo** no codigo, **como aplica lo** numa
  implementacao nova;
- o processo observado, passo a passo, com o que e obrigatorio e o que varia;
- exemplares representativos, com caminho completo e a regra que localiza o equivalente atual;
- guardrails daquele fluxo;
- entradas e saidas esperadas;
- evidencia de recorrencia, com numeros;
- o que a skill nao faz, e para onde encaminhar nesses casos;
- eixo de invocacao: model-invocada (ponto de entrada de fluxo) ou user-invocada (skill de
  etapa, alcancada por quem a chama), conforme o teste de `criterios-de-classificacao.md`.

**Gatilho nao cita exemplar, e passo nao delega ao exemplar.** Uma frase de disparo como "crie
um cadastro seguindo o padrao de `<arquivo>`" ensina o agente a depender de um caminho que pode
nao existir mais, e um passo escrito como "faca igual ao exemplar" nao ensina nada no dia em que
o exemplar sai. O gatilho nomeia a operacao no vocabulario do sistema; o processo carrega os
elementos, a ordem e o criterio de decisao; o exemplar aparece como conferencia, com a regra que
localiza o equivalente atual.

O eixo de invocacao e decisao de quem investigou, e ele vira **marca no frontmatter**, nao
observacao no texto:

| Eixo | Frontmatter | Forma da `description` |
|---|---|---|
| model-invocada | sem marca | voltada ao modelo: as frases de pedido que devem dispara la, no vocabulario do repositorio |
| user-invocada | `disable-model-invocation: true` | voltada ao humano: o que a skill faz e quando serve, sem lista de gatilho |

Frase de gatilho em skill user-invocada e ruido nos dois sentidos: o humano le lista de comando, e
o modelo nem chega a ver a descricao. E quem chama uma skill user-invocada **manda ler o
`SKILL.md`** dela pelo caminho; ativar pelo nome nao funciona, porque sem descricao carregada o
modelo nao sabe que ela existe.

Skill com efeito colateral fora do repositorio e user-invocada por padrao, mesmo quando o pedido
chegaria em linguagem natural: o disparo automatico de algo irreversivel e risco, nao conveniencia.

Skill nascida do conhecimento de um repositorio guarda esse conhecimento nas proprias
references, dentro da pasta da skill, quando o assunto so serve a ela. Quando serve a varias
tarefas, vive em `.agents/references/` e a skill aponta para la.

Se o destino mantiver indice ou registro de skills, adicione a entrada nova imitando o formato
das existentes.

## Descartado, na reference de evidencias

O que foi investigado e nao virou artefato tambem e resultado da rodada, e vale escrito: sem isso
a proxima rodada reinvestiga, reprova pelo mesmo motivo e paga de novo. O registro vive na
reference de evidencias da rodada, em secao propria, e e lido na Fase 4 antes de qualquer achado
voltar para a fila.

```markdown
## Descartado

| Achado | Motivo | Alcance medido | O que mudaria a decisao | Data |
|---|---|---|---|---|
| <o que foi considerado> | <nao muda implementacao | caso isolado | ja coberto por <artefato> | evidencia insuficiente> | <N de M, ou "nao medido"> | <o sinal que faria valer a pena reabrir> | <data> |
```

Duas colunas fazem o trabalho. **Motivo** evita a reabertura automatica; **o que mudaria a
decisao** evita o oposto, que e o descarte virar veto permanente sobre um achado que amanha passa
a valer. Achado descartado por evidencia insuficiente e o caso tipico: ele volta no dia em que a
amostra crescer.

## Recomendacao de automacao

Nao e artefato escrito por esta skill: e uma linha no relatorio, com quatro campos.

```text
- <hook | subagente | comando> | <o que faz> | gatilho: <evento ou pedido> | evidencia: <achado desta rodada, com caminho e contagem>
```

A linha do relatorio e a entrega inteira: configuracao, exemplo em JSON e instalacao ficam com
o desenvolvedor. Sintaxe de hook muda com a versao da ferramenta, e o que nao envelhece e o
tipo, o gatilho e o motivo.

## Ponte de seguranca

`.agents/security/mapa-cwe-guia.md` e lido pela `owasp-playbook` secao por secao, pelo titulo. Os titulos ficam como estao, e `Traducao` e `Saidas opcionais` nao sao tocadas. As quatro secoes de projeto recebem a forma abaixo; secao sem material mantem o texto padrao que veio da stack.

**`Sinais do projeto`.** Uma tabela, um sinal por tipo de artefato encontrado, com o play que ele dispara. O sinal e reconhecivel por nome de arquivo, sufixo, diretorio ou simbolo literal; a terceira coluna diz onde o artefato vive hoje e de onde veio a evidencia.

```markdown
| Sinal no escopo | Play | Onde esta hoje |
|---|---|---|
| `<padrao de nome, diretorio ou simbolo literal>` | `<play da owasp-playbook>` | <territorio comprovado>; evidencia: `<caminho:linha ou comando>` |
```

**`Auditores do projeto`.** Um bloco `bash`, um comando por linha, com o alvo parametrizado, seguido de uma linha de texto por comando dizendo o que ele procura. Todo comando foi executado uma vez sobre o repositorio antes de entrar; comando que nao roda fica de fora.

````markdown
```bash
python3 <script de verificacao do repositorio> --input <arquivo ou pasta>
grep -rn --binary-files=text '<simbolo do guardrail>' <escopo>
```

- `<comando>`: procura <o que>, em <territorio>.
````

**`Excecoes`.** Uma lista, um item por linha, com o assunto em negrito, o que nao e achado, o motivo e a evidencia.

```markdown
- **<assunto>**: <o que nao e achado neste repositorio>, porque <area congelada pelo escopo de escrita | biblioteca de terceiros versionada | mecanismo do core ou do framework>. Evidencia: `<caminho:linha ou regra do AGENTS.md>`.
```

**`Correcao`.** Um paragrafo curto que aponta, por tipo de artefato, a skill ou o procedimento do projeto que recebe a correcao, citando o achado pelo numero do relatorio.

```markdown
Correcao nao e desta skill. Peca a correcao citando o numero do achado no relatorio e roteie por tipo de artefato: <tipo> para `<skill ou procedimento>`, <tipo> para `<skill ou procedimento>`.
```

## Verificacao antes de fechar

```bash
python3 scripts/inventario_stack.py verificar --raiz <raiz-do-repo>
```

O verificador aponta problemas mecanicos: skill sem frontmatter valido, campo obrigatorio
ausente, reference orfa que ninguem referencia, ponteiro para arquivo inexistente e reference
sem secao de evidencias. Ele nao avalia conteudo.
