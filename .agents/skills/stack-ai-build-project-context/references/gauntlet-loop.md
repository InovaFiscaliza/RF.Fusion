# Gauntlet Loop

Autoridade sobre: os tres papeis, o que cada um recebe, quanto rigor cada artefato merece, o
checklist do Auditor, os vereditos possiveis e o limite de rodadas.

## Por que separar os papeis

Quem descobre um padrao passa a acreditar nele. Quem escreve o artefato passa a defender o
texto. Nenhum dos dois consegue avaliar o proprio trabalho com a mesma energia com que o
produziu, por atencao e por incentivo.

O Gauntlet Loop separa descoberta, escrita e critica em contextos distintos para que a critica
seja real. A parte que faz o mecanismo funcionar e o que o Auditor **nao** recebe: a narrativa
do investigador e a justificativa do implementador. Ele recebe o artefato e o repositorio, e a
pergunta e sempre contra o codigo, nao contra o raciocinio.

O objetivo do ciclo e **medir**, nao reprovar. Auditor que precisa encontrar defeito para se
justificar produz o mesmo dano que auditor que aprova sem olhar: no primeiro caso a rodada
termina sem artefato, no segundo termina com artefato sem lastro.

## Rigor proporcional

Aplicar o ciclo completo a tudo custa a rodada inteira e foi o que fez rodadas reais
terminarem sem entrega. O rigor acompanha o custo do erro.

| Artefato | Verificacao | Por que |
|---|---|---|
| Regra em `AGENTS.md` | ciclo completo, Auditor em contexto novo | entra em toda tarefa e e a mais cara de reverter |
| Guardrail | ciclo completo | vai alertar contra codigo real; se estiver errado, atrapalha toda implementacao |
| Proposta de skill | Auditor avalia a proposta contra os exemplares | o risco e descrever um fluxo que o codigo nao mostra |
| Reference descritiva | conferencia dirigida: comandos reproduzem, exemplares existem, alcance bate | erro custa uma consulta errada, nao uma tarefa errada |
| Secoes de inventario | conferencia contra a saida do censo | sao deterministicas |
| Ponteiro e linha de roteamento | verificacao mecanica pelo `verificar` | ou o arquivo existe, ou nao |

Conferencia dirigida nao exige contexto novo nem subagente: e uma passagem explicita contra o
codigo, feita depois de escrever, com os comandos do artefato executados como estao.

## Papeis

**Investigador.** Descobre e propoe, com a barra de `protocolo-investigacao.md`. Nao escreve
artefato e nao classifica destino.

**Implementador.** Recebe o achado consolidado e a classificacao, e escreve o artefato conforme
`formato-artefatos.md`. Nao reabre a descoberta e nao amplia o alcance para o texto fluir
melhor. Ampliar alcance na redacao e a origem mais comum de generalizacao errada.

Este papel e da sessao principal, nao de subagente, pelo motivo em `protocolo-subagentes.md`.
Como nao ha contexto novo que o discipline, a passagem que o substitui esta escrita: antes de
mandar o rascunho ao Auditor, compare o artefato contra o achado consolidado pela tabela de
`formato-artefatos.md`, secao Disciplina do implementador. Ela e barata, dispensa o repositorio e
pega o defeito que nasce entre os dois textos, que a auditoria contra o codigo nao alcanca.

**Auditor do rascunho.** Recebe a barra, o artefato pronto e acesso ao repositorio. Nao recebe
dossie, nem justificativa, nem historico. Mede cada afirmacao contra o codigo. Prompt em
`agents/auditor-do-rascunho.md`.

Duas regras estruturais, faceis de perder por conveniencia:

- **Quem escreve nunca julga o proprio trabalho.**
- **Um Auditor que ja viu um rascunho nunca julga a versao corrigida.** Ele passaria a medir a
  melhora, e nao a barra.

## A barra

Escreva a barra **antes** da rodada e envie junto com o artefato: os testes de admissao do
destino, a classificacao de padrao exigida, o alcance declarado e o **protocolo de busca**, com
o comando canonico de cada grupo de codificacao.

O protocolo faz parte da barra porque sem ele o Auditor conta de um jeito e o investigador
contou de outro, e a divergencia nao diz nada sobre o artefato.

Barra vaga produz critica vaga. "Avalie se esta bom" devolve opiniao; "esta afirmacao sustenta
alcance de repositorio, com quantos divergentes em todo o escopo declarado?" devolve uma busca
e um numero.

Em repositorio pequeno, os tres papeis podem ser conduzidos na mesma sessao, desde que a
critica seja uma passagem separada e explicitamente contra o codigo. O que nao funciona e
criticar no mesmo folego em que se escreveu.

## Onde o rascunho vive

O ciclo acontece sobre rascunho em area de trabalho da sessao, fora do repositorio de destino.
Escrever no destino antes da aprovacao contradiz o gate humano e transforma a rejeicao em
desfazimento.

Para skill, o ciclo tem duas etapas: o Auditor avalia a **proposta** (proposito, gatilho,
processo observado, exemplares, evidencia de recorrencia) antes da aprovacao; a skill e gerada
pela `skill-creator` depois dela, e so volta ao Auditor se o processo escrito divergir do
aprovado.

## Artefato que ja esta no repositorio

Quando o artefato nasceu numa rodada anterior, a medicao de conformidade cabe ao
`agents/medidor-de-conformidade.md`. O Auditor do rascunho entra depois, sobre a correcao proposta
a partir dos numeros devolvidos.

Antes de tratar qualquer divergencia como mudanca do sistema, rode o comando registrado no
artefato **exatamente como esta escrito** e confirme que ele reproduz o proprio numero. Comando
que nao reproduz e defeito do artefato: corrija o comando, remeça e so entao compare. Tratar
erro de comando como contradicao derruba artefato correto.

## Checklist do Auditor

Para cada afirmacao do artefato:

| # | Procura | Veredito quando confirmado |
|---|---|---|
| 1 | Afirmacao sem evidencia no repositorio | REPROVAR |
| 2 | Alcance maior do que a evidencia sustenta | REDUZIR ALCANCE |
| 3 | Excecao ou caso isolado apresentado como convencao | REPROVAR |
| 4 | Conteudo ja presente em outro destino da stack | MESCLAR |
| 5 | Skill sem fluxo, recorrencia, gatilho, ganho ou escopo unico | REBAIXAR |
| 6 | Skill que descreve o padrao e nao ensina a aplica lo | CORRIGIR |
| 7 | Guardrail escrito como proibicao, sem contagem, ou com juizo de qualidade | CORRIGIR |
| 8 | Conteudo no destino errado pelos testes de admissao | MOVER |
| 9 | Contexto que nao paga o proprio custo | CORTAR |
| 10 | Afirmacao que o codigo contradiz | REPROVAR |
| 11 | Contagem que nao se reproduz | `rodada-invalida` quando o comando divergiu do canonico; REPROVAR quando o mesmo comando no mesmo escopo devolve outro numero |
| 12 | Comando publicado no artefato que nao devolve o numero registrado | CORRIGIR |
| 13 | Exemplar citado que nao existe no caminho indicado | CORRIGIR |
| 14 | Padrao descrito sem finalidade: diz o que existe e nao para que serve | CORRIGIR |

Itens 1, 3 e 10 pedem busca ativa no codigo. Itens 12 e 13 pedem executar o comando e abrir o
caminho: sao baratos e pegam o defeito que mais envelhece mal.

O item 11 separa contagem errada de contagem medida de outro jeito. Antes de bloquear numero,
repita a busca com o comando canonico. Se nao conseguir reproduzir com ele, devolva
`rodada-invalida` em vez de reprovar.

O item 9 e o mais facil de ignorar: entra ali o que o agente descobriria sozinho, a
justificativa longa em contexto global, o exemplo redundante e a regra escrita em tres frases
quando uma resolve.

O item 14 e o que separa stack que descreve de stack que orienta. "Existe a chamada X em 300
arquivos" nao ensina nada; "paginas autenticadas chamam X antes de processar a requisicao"
ensina.

## Regras de julgamento

- Toda reprovacao cita evidencia propria: caminho, ocorrencia, contagem ou ausencia comprovada
  com o padrao buscado. Reprovacao sem evidencia e opiniao e nao derruba nada.
- Ausencia de divergencia em amostra pequena nao e prova de convencao.
- Zero artefatos analisados nao equivale a aprovado. Lacuna e lacuna, e entra no retorno.
- Se a evidencia disponivel nao permitir julgar o criterio, devolva `rodada-invalida`.
- Fato de censo e sinal de dependencia nao exigem busca por contraexemplo: exigem a saida do
  censo.
- Afirmacao negativa e afirmacao como qualquer outra: "incluir X nao garante Y" se mede
  contando os casos que nao garantem.
- Contrato de consumo de sistema externo se julga com o codigo deste repositorio.
- Convencao que parece antipadrao tecnico **nao e defeito do artefato**. O artefato descreve o
  sistema; adequacao a boa pratica externa nao esta na barra e nao e motivo de reprovacao.
- Nao invente defeito. Afirmacao que se sustenta recebe APROVADO, e uma rodada sem bloqueios e
  resultado legitimo.
- Todo veredito exige busca propria registrada, **inclusive APROVADO**. Retorno com o bloco de
  buscas vazio, ou com menos de uma busca por afirmacao avaliada, e `rodada-invalida`.

## Vereditos

| Veredito | Significa |
|---|---|
| `sem bloqueios` | o artefato pode ser escrito como esta |
| `ajustar` | correcoes pontuais, sem reabrir a descoberta |
| `bloquear` | ao menos um item nao se sustenta contra o codigo |
| `rodada-invalida` | a evidencia nao permitiu julgar, ou a contagem divergiu por comando diferente. Refaz com o comando canonico e nao conta como tentativa |

## Limite de rodadas

Duas rodadas validas, no maximo, contadas **por item**. Rodada devolvida como
`rodada-invalida` nao consome tentativa.

O que continuar em desacordo depois da segunda rodada valida nao e descartado: o artefato e
**reduzido ao que a evidencia sustenta** e escrito assim, e a divergencia vai ao plano como
pergunta, com as duas posicoes e as duas evidencias. Descartar o item inteiro joga fora o que
foi comprovado junto com o que ficou em disputa, e e assim que uma rodada rigorosa termina sem
entrega.

O limite existe porque, passadas duas rodadas, o desacordo raramente e sobre o codigo: e sobre
uma decisao que so o dono do repositorio pode tomar.

Retentativa dentro de um ciclo conta como ciclo. Revisao, auto-correcao, busca nova para
sustentar a mesma afirmacao, segunda opiniao e trabalho de subagente aninhado entram na conta
igual a uma rodada declarada. Nao existe canal de retentativa interna que nao gaste orcamento: e
por ali que uma rodada nominalmente unica vira cinco.

Item que termina pelo limite, e nao pela evidencia, vai ao registro da rodada marcado como
`teto-atingido`. Sem essa marca, o relatorio final confunde parada por evidencia com parada por
orcamento, e as duas pedem decisoes diferentes do desenvolvedor.

## Freio de estagnacao

Quando o mesmo bloqueio dominante volta na segunda rodada sem melhora medivel, repetir a
estrategia esta proibido, mesmo que ainda reste rodada. Trocar de estrategia e uma destas tres:

- dividir o item em partes menores, que se julgam separadamente;
- reforcar a evidencia por outro caminho de busca, com outro universo;
- reabrir a barra, porque o criterio e que estava errado.

Se nenhuma destravar, o item termina como o limite ja manda: reduzido ao que a evidencia
sustenta, com a divergencia no plano. Repetir a mesma tentativa gasta rodada e devolve o mesmo
bloqueio.

## Passagem final sobre o diff

Depois da aprovacao e da escrita, rode uma ultima passagem de Auditor sobre o diff real, com
contexto novo. Ela julga o que foi escrito e nada mais: proposta que nao virou artefato ja saiu
do ciclo.

Ela e barata e pega a classe de defeito que o rascunho nao mostrava: regra que mudou de sentido
ao ser encaixada numa secao existente, ponteiro que nao resolve, duplicacao criada pela redacao
final, alcance que se ampliou na hora de escrever, comando que quebrou ao ser colado numa
tabela.

Se bloquear, corrija e rode de novo somente sobre o ponto bloqueado. Se nao bloquear, a rodada
termina.

## Quando rodar o ciclo completo

Rode ao menos uma rodada quando a decisao for: entrar em `AGENTS.md`, escrever guardrail,
criar skill, contradizer artefato existente, ou declarar alcance de repositorio inteiro.

Um loop bom e pequeno: uma rodada completa para a decisao relevante, e rodada nova apenas
quando a critica muda o artefato ou bloqueia a conclusao. O numero de iteracoes sai dai, nunca
de uma meta fixa.
