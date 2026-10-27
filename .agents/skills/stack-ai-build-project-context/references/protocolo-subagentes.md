# Protocolo de subagentes

Autoridade sobre: quando dividir a investigacao, o que cada subagente recebe, o que devolve e como divergencias sao resolvidas.

## Quando dividir

Subagente e mecanismo caro. Cada um custa uma janela de contexto inteira, mais o ida e volta da coordenacao, e implementacoes multi-agente costumam gastar de tres a dez vezes mais tokens que a mesma tarefa em um contexto so. O ganho precisa ser maior que isso, e ele vem de tres lugares, nao de mais nenhum:

| Ganho | Quando aparece nesta skill |
|---|---|
| Contexto protegido | A investigacao de uma frente produz muito texto bruto (listagens, trechos, saidas de busca) e quase nada disso interessa a consolidacao |
| Paralelismo | As frentes sao independentes: nenhuma precisa do resultado da outra para comecar |
| Independencia de julgamento | O Auditor precisa avaliar sem ter visto a descoberta e a redacao, o que so um contexto novo garante |

Fora desses tres, o custo de coordenacao supera o beneficio. Nao divida trabalho sequencial que compartilha o mesmo contexto, nem etapas acopladas que exigem sincronizar entendimento a cada passo: transferencia repetida degrada fidelidade a cada salto, e o resultado fica pior do que teria ficado em um contexto so.

Prefira de tres a seis subagentes por rodada. Abaixo disso a divisao raramente compensa; acima disso, o tempo gasto reconciliando dossies passa a superar o que o paralelismo economizou.

Sinal de divisao boa: as frentes podem ser investigadas em paralelo sem que uma precise do resultado da outra. Sinal de divisao ruim: o briefing precisa explicar metade dos achados de outra frente para fazer sentido.

Lance todas as frentes independentes de uma vez. Frente que depende do resultado de outra vai na rodada seguinte.

Quando o censo mostrar um repositorio pequeno e homogeneo, conduza a investigacao direto na sessao principal. Subagente e para dividir investigacao grande, nao para parecer rigoroso.

## Teto da rodada

O limite por item nao limita a rodada. Com muitos itens a soma cresce sem teto, e e assim que uma
rodada consome a sessao inteira antes de escrever qualquer artefato. Declare no registro da
rodada, antes da Fase 3, tres numeros finitos:

| Teto | Valor tipico | O que ele impede |
|---|---|---|
| Subagentes simultaneos | 3 | fan-out que a sessao principal nao consegue reconciliar |
| Subagentes na rodada inteira | 12 | rodada que investiga sem parar e nunca escreve |
| Ciclos de auditoria na rodada inteira | duas por item, com um teto global declarado | auditoria que se repete item a item ate o orcamento acabar |

Subagente nao abre subagente. A profundidade de delegacao e um: a sessao principal lanca os
papeis, e nenhum papel lanca outro. Investigador que precisa de mais uma frente devolve isso como
lacuna, e a sessao principal decide se gasta a proxima janela.

Bater no teto nao e aprovacao. A rodada para, o que foi comprovado e escrito, e o restante vai ao
plano marcado como `teto-atingido`. Subir qualquer teto e decisao do desenvolvedor, nunca do
agente.

## Papeis empacotados

Quatro papeis pagam o proprio custo nesta skill, e sao os unicos com prompt proprio:

| Prompt | Papel | Quantas vezes por rodada | Ganho que justifica |
|---|---|---|---|
| `agents/revisor-do-que-investigar.md` | Tenta derrubar a unidade e as frentes antes do lancamento | uma, no fim da Fase 2 | independencia de julgamento |
| `agents/investigador.md` | Investiga uma fronteira e devolve dossie | uma por frente, tipicamente tres a seis | contexto protegido e paralelismo |
| `agents/auditor-do-rascunho.md` | Tenta reprovar contra o codigo um artefato que ainda vai ser escrito | ao menos uma por rodada do Gauntlet Loop, sempre em contexto novo | independencia de julgamento |
| `agents/medidor-de-conformidade.md` | Mede se uma afirmacao ja escrita na stack continua valendo | uma por regra medida, so em stack povoada | contexto protegido e paralelismo |

O Revisor do que investigar e o mais barato dos quatro e o unico que roda **antes** do gasto. Ele nao investiga:
recebe a proposta pronta, tem orcamento de dez comandos e tres arquivos, e devolve a premissa que
mata o recorte. O ganho e a independencia, e ela some se quem montou a proposta a defender no
mesmo folego. Uma janela ali evita de tres a seis janelas medindo com rigor o territorio errado,
que e o defeito que so aparece na Fase 7.

O que deliberadamente **nao** vira subagente, porque falharia nos tres criterios:

- **Coordenar o loop e consolidar dossies.** Precisa de todo o contexto que a sessao principal ja tem e devolve uma decisao curta. Delegar isso paga uma janela inteira para receber uma frase.
- **Escrever o artefato.** E sequencial e compartilha contexto com a consolidacao. A separacao que importa no Gauntlet Loop e entre quem escreve e quem audita, e ela ja existe com a sessao principal escrevendo e o auditor do rascunho avaliando.
- **Decidir se um candidato merece virar skill.** Depende dos achados consolidados, que so a sessao principal tem inteiros, e e uma decisao unica por rodada. Quando houver duvida de independencia, o proprio auditor do rascunho cobre pelo veredito REBAIXAR.
- **Rodar o censo.** Ja e deterministico em `scripts/inventario_stack.py`.

## Registro no runtime

Distinga prompt empacotado de agente registrado. Os arquivos em `agents/` sao texto de briefing carregado sob demanda: eles nao instalam nem registram subagente nenhum. Para executar um papel em contexto separado, use o mecanismo do runtime alvo, enviando o conteudo do prompt como instrucao do subagente.

Antes de prometer execucao paralela, descubra o que o runtime oferece. Se ele registrar agentes por arquivo, mapear estes quatro papeis para o formato dele e opcional e vale quando o repositorio de destino usa um unico runtime; quando o destino atende varias ferramentas, o prompt empacotado e o que sobrevive a todas.

Quando houver escolha de modelo por agente, ela e uma alavanca real de custo: o investigador e o auditor do rascunho dependem de raciocinio forte e nao devem ser rebaixados, enquanto o medidor de conformidade e majoritariamente contagem e tolera um modelo mais barato.

Sem subagentes disponiveis, nao finja que houve separacao. Investigue uma fronteira por vez em passagens inline separadas, nao reutilize conclusao de uma fronteira como prova de outra, faca a passagem de critica depois de um intervalo e explicitamente contra o codigo, e registre no relatorio que a independencia foi limitada pelo runtime.

## O que o subagente recebe

Contexto minimo suficiente. Enviar a stack inteira, os achados das outras frentes ou a hipotese que se quer confirmar contamina a investigacao e produz confirmacao encomendada.

O briefing tem sete partes, no modelo `agents/investigador.md`:

1. **Escopo de caminhos.** Diretorios e arquivos que ele pode ler, e o aviso de que sair do escopo invalida o achado por sobreposicao com outra frente.
2. **Eixos.** Quais dos eixos obrigatorios de `protocolo-investigacao.md` cabem a esta frente. Sem isso o investigador escolhe sozinho o que olhar, e o que ele escolhe raramente e como se constroi uma unidade nova.
3. **Unidade e exemplares.** O que o sistema produz repetidamente naquele escopo e os caminhos completos dos exemplares recentes, antigos e de modulos diferentes por onde comecar, lidos por inteiro antes de qualquer busca.
4. **Pergunta verificavel.** Uma pergunta cuja resposta e checavel por outra pessoa lendo o mesmo codigo. "Como funciona o modulo X" nao e verificavel. "Qual e a sequencia obrigatoria de operacoes entre receber a requisicao e persistir em X, e o que acontece se ela for invertida" e.
5. **Formato de retorno.** O dossie descrito abaixo, e nada alem dele.
6. **Proibicoes.** Nao escrever nem alterar arquivos do repositorio; nao propor arquitetura nova nem refatoracao; nao classificar destino na stack; nao importar convencao de outro projeto; nao concluir sem ter procurado o que contradiz.
7. **Criterio de qualidade.** A barra de evidencia de `protocolo-investigacao.md`, reproduzida no briefing para nao depender de leitura externa.

O subagente investiga e propoe. Ele nao decide destino, nao escreve artefato e nao negocia com o desenvolvedor. Essa separacao e o que permite ao Auditor avaliar o artefato sem herdar a convicao de quem o descobriu.

## O que o subagente devolve

Um dossie, no formato declarado em `agents/investigador.md`, com:

- lista de achados, cada um com afirmacao, eixo, evidencia, universo, comandos, contraexemplos, alcance e classificacao
- **processos observados**: a unidade, os arquivos que participam, a sequencia, o que e obrigatorio, o que varia e onde se erra
- **candidatos a guardrail**: convencao que uma implementacao nova pode violar sem perceber, com a contagem dos dois lados
- o universo examinado: quantos arquivos comparaveis, quais padroes de busca, o que ficou de fora e por que
- os eixos que a frente cobriu e os que ficaram sem achado, com o que foi buscado
- hipoteses testadas e derrubadas, com o que as derrubou e com o padrao positivo correspondente
- perguntas que o codigo nao responde

Achado sem os campos obrigatorios volta para o subagente com a lacuna apontada. Aceitar achado incompleto na consolidacao e o caminho mais curto para uma regra sem lastro em `AGENTS.md`.

Dossie que volta sem **processos observados** merece uma segunda olhada antes de ser aceito: quando a frente tocou uma unidade do sistema, a secao vazia costuma significar que o investigador mediu padroes isolados e nao reconstruiu nenhuma sequencia. E o dossie sem essa secao que faz a rodada terminar sem skill nenhuma.

Hipotese derrubada e resultado valioso, nao fracasso. Ela impede que a proxima rodada gaste tempo no mesmo caminho e frequentemente aponta a diferenca real entre dois subsistemas.

## Briefing de medicao

Medicao de conformidade e o caso em que a regra de contexto minimo precisa ceder, e ceder de forma controlada. Verificar se uma regra ja escrita continua valendo exige, por definicao, enviar a afirmacao a ser testada, o que em qualquer outra frente seria confirmacao encomendada.

O que impede o vies aqui e a forma da pergunta e a forma do retorno: a afirmacao vai como hipotese a medir, e o retorno exige contagem dos dois lados. Use `agents/medidor-de-conformidade.md`, que ja carrega essa forma.

Um retorno de medicao util e sempre numerico e bilateral. "A regra vale" nao e resposta; "18 conformes, 9 violacoes, todas em `pagamentos`, todas posteriores a mudanca de dependencia daquele servico" e.

## Consolidacao e divergencia

A sessao principal consolida. Ela e a unica que ve todos os dossies e a stack existente ao mesmo tempo.

**Sobreposicao.** Duas frentes descrevem o mesmo fato: mantenha a versao com evidencia mais forte e some as ocorrencias, ampliando o alcance apenas ate onde as duas juntas comprovam.

**Divergencia.** Duas frentes afirmam coisas incompativeis: verifique diretamente no codigo, com busca dirigida ao ponto do conflito. Nunca decida por maioria, por classificacao declarada ou pela ordem de chegada. O resultado tipico e que ambas estavam certas dentro de alcances diferentes, e a divergencia vira o achado: existe uma fronteira ali que ninguem tinha nomeado.

**Contradicao com a stack existente.** Trate como o item mais importante da rodada, conforme `contrato-da-stack.md`.

## Papeis nas fases seguintes

O mesmo mecanismo serve ao Gauntlet Loop, com um cuidado extra: o Auditor precisa de um contexto novo, sem a narrativa do investigador nem a justificativa do implementador. Um Auditor que herda o raciocinio de quem escreveu tende a validar o raciocinio em vez de confrontar o artefato com o codigo. Ver `gauntlet-loop.md`.
