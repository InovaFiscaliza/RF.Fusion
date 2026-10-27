# Registro da rodada

Este arquivo nasce na Fase 0, na area de trabalho da sessao e fora do repositorio de destino, e
e preenchido fase a fase. Na Fase 7 ele ja e o plano: nada e redigido do zero.

Preencher fora de ordem nao e permitido. O que uma fase decide e usado pelas seguintes, e
decisao que nao esta escrita aqui e decisao que se perde no meio da rodada.

| Secao | Preenchida na | Usada em |
|---|---|---|
| Enquadramento | Fase 0 | todas |
| Protocolo de busca | Fase 1 | Fases 3 a 8 |
| Unidades e exemplares | Fase 2 | Fases 3, 5 e 8 |
| Frentes e eixos | Fase 2 | Fase 3 |
| Barra da rodada | antes da Fase 6 | Fases 6 e 8 |
| Cobertura por eixo | Fases 3 a 5 | Fase 7 e Fase 9 |
| Itens propostos | Fases 5 e 6 | Fase 7 |
| Automacao, nao consolidado, perguntas | Fases 5 a 7 | Fase 9 |
| Execucao | Fase 8 | Fase 9 |

---

## Estado da rodada

Fechado a cada fronteira, nunca no fim. E o que permite retomar em sessao nova, e o que a proxima
rodada le antes de reinvestigar. Ao retomar, confira contra o repositorio: artefato escrito e
fato, linha marcada aqui e intencao.

| Fase | Estado | Fechada em | O que ficou pendente |
|---|---|---|---|
| 0 Enquadramento | `<pendente | em curso | fechada>` | `<data e hora>` | `<...>` |
| 1 Censo e dependencias | `<...>` | `<...>` | `<...>` |
| 2 Unidades e frentes | `<...>` | `<...>` | `<...>` |
| 3 Investigacao | `<...>` | `<...>` | `<frentes concluidas de N>` |
| 4 Consolidacao | `<...>` | `<...>` | `<...>` |
| 5 Classificacao | `<...>` | `<...>` | `<...>` |
| 6 Verificacao | `<...>` | `<...>` | `<itens auditados de N>` |
| 7 Plano e aprovacao | `<...>` | `<...>` | `<...>` |
| 8 Aplicacao | `<...>` | `<...>` | `<artefatos escritos de N>` |

**Retomar esta rodada:** `<comando ou passo pelo qual a proxima sessao continua>`

## Enquadramento

**Repositorio:** `<raiz>`
**Escopo investigado:** `<sistema, subsistema ou recorte>`
**Nao objetivos declarados:** `<o que ficou de fora por decisao no preflight>`
**Sistemas externos consumidos:** `<quais, e o que fica fora por serem nao versionados>`
**Ferramentas do preflight:** `<disponiveis>` | ausentes: `<o que ficou bloqueado por isso>`
**Tetos da rodada:** `<subagentes simultaneos>` | `<subagentes na rodada>` | `<ciclos de auditoria na rodada>`
**Escopo de escrita autorizado:** `<arquivos e pastas>`
**Premissas assumidas por default:** `<ponto do enquadramento: o que foi assumido, derivado de censo ou do contrato da stack. Escreva "nenhuma" quando o desenvolvedor tiver definido tudo>`

## Protocolo de busca

Fechado na Fase 1 a partir do perfil de codificacao do censo. O mesmo texto vai para todo papel
da rodada. Cada comando foi **executado como esta escrito** e devolveu o numero registrado.

| Grupo de codificacao | Comando canonico | Escopo onde vale | Conferido |
|---|---|---|---|
| `<grupo>` | `<comando>` | `<caminhos>` | `<numero devolvido na conferencia>` |

## Unidades e exemplares

A unidade e o que alguem cria quando implementa uma funcionalidade nova neste sistema.

| Unidade | Como se reconhece | Quantas existem | Comando |
|---|---|---|---|
| `<pagina, endpoint, procedure, relatorio, job>` | `<extensao, formato de nome, diretorio>` | `<N>` | `<busca literal>` |

**Cobertura de territorio da amostra.** Uma familia descrita a partir de exemplares de um
diretorio so nao e uma familia: e aquele diretorio. Preencha antes de escrever qualquer
descricao de unidade.

| Unidade | Diretorios onde existe | Diretorios de onde vieram exemplares lidos por inteiro | Suficiente? |
|---|---|---|---|
| `<unidade>` | `<N, pelo comando de censo>` | `<liste os caminhos>` | `<sim, tres ou mais dos maiores | nao: alcance da afirmacao fica restrito a <caminho>>` |

| Exemplar | Por que foi escolhido |
|---|---|
| `<caminho completo>` | `<recente, pelo commit <ref>>` |
| `<caminho completo>` | `<antigo>` |
| `<caminho completo>` | `<outro modulo: <nome>>` |

## Frentes e eixos

| Frente | Escopo de caminhos | Eixos | Pergunta verificavel |
|---|---|---|---|
| `<nome>` | `<caminhos>` | `<eixos atribuidos>` | `<pergunta>` |

## Barra da rodada

Escrita antes da primeira rodada do Gauntlet Loop e enviada ao Auditor junto com o artefato.

- Destino proposto e seus testes de admissao: `<...>`
- Classificacao de padrao exigida: `<...>`
- Alcance declarado: `<...>`
- Protocolo de busca: ver secao acima

## Cobertura por eixo

Uma linha por eixo obrigatorio. Eixo sem achado precisa de um dos tres motivos, nunca de
silencio.

| Eixo | O que foi encontrado | Destino | Vazio por que |
|---|---|---|---|
| Estrutura e organizacao | `<...>` | `<artefato>` | |
| Ciclo da requisicao | `<...>` | `<artefato>` | |
| Autenticacao e identidade | `<...>` | `<artefato>` | |
| Autorizacao e permissao | `<...>` | `<artefato>` | |
| Construcao da unidade | `<...>` | `<artefato>` | |
| Validacao | | | `<nao existe no sistema | nao objetivo | evidencia insuficiente>` |
| Persistencia e dados | `<...>` | `<artefato>` | |
| Regra de negocio | `<...>` | `<artefato>` | |
| Integracoes externas | `<...>` | `<artefato>` | |
| Erro, log e seguranca | `<...>` | `<artefato>` | |
| Configuracao | | | `<...>` |
| Padroes equivalentes | `<...>` | `<artefato>` | |

## Itens propostos

Acoes possiveis: `criar`, `atualizar`, `reduzir alcance`, `remover`, `criar via skill-creator`,
`evoluir via skill-creator`.

| # | Destino | Acao | O que entra, muda ou sai | Evidencia | Comando | Alcance | Classificacao | Auditor |
|---|---|---|---|---|---|---|---|---|
| 1 | `AGENTS.md`, secoes de inventario | atualizar | `<topicos de censo e dependencia>` | `<saida do censo>` | `<censo>` | repositorio | fato de censo | APROVADO |
| 2 | `.agents/skills/<nome>/` | criar via `skill-creator` | `<fluxo, com quando usar, como reconhecer e como aplicar>` | `<N unidades, M exemplares lidos>` | `<...>` | `<...>` | convencao | APROVADO |
| 3 | `.agents/references/<arquivo>.md` | criar | `<assunto, com exemplares>` | `<...>` | `<...>` | `<...>` | `<...>` | APROVADO |
| 4 | `AGENTS.md`, guardrail | criar | `<padrao identificado e desvio que alerta>` | `<N de M>` | `<...>` | `<...>` | convencao | APROVADO |
| 5 | `AGENTS.md` | reduzir alcance | de `<texto atual>` para `<texto proposto>` | `<conformes vs divergentes, e onde>` | `<...>` | `<alcance comprovado hoje>` | `<...>` | REDUZIR ALCANCE |
| 6 | `AGENTS.md` | remover | `<linha a sair, citada>` | `<o que hoje contradiz>` | `<...>` | `<...>` | `<...>` | REPROVADO na auditoria |

Item de remocao ou de reducao de alcance sempre cita o texto atual por inteiro, porque o
desenvolvedor precisa aprovar a perda, nao apenas o ganho.

## Guardrails propostos

| ID | Padrao identificado | Alerta quando | Evidencia | Onde vai viver |
|---|---|---|---|---|
| G1 | `<X>` | `<desvio reconhecivel>` | `<N de M>` | `<AGENTS.md | skill <nome> | reference <arquivo>>` |

## Nao consolidado

| Achado | Bloqueio | Busca executada | O que resolveria | Dono |
|---|---|---|---|---|
| `<afirmacao>` | `ferramenta` | `<comando e universo>` | `<comando canonico do grupo>` | rodada |
| `<afirmacao>` | `evidencia-insuficiente` | `<comando e universo>` | `<onde olhar, com caminho>` | proxima rodada |
| `<afirmacao>` | `dois-padroes-concorrentes` | `<contagem dos dois lados>` | `<qual padrao passa a valer>` | desenvolvedor |
| `<afirmacao>` | `fora-do-repositorio` | `<o que foi possivel ver deste lado>` | `<acesso, artefato ou pessoa>` | `<quem tem>` |

| Classe | Significa | Acao |
|---|---|---|
| `ferramenta` | a contagem nao se reproduziu por comando ou codificacao | refazer com o comando canonico, na mesma rodada |
| `evidencia-insuficiente` | procurou e achou pouco | registrar onde olhar na proxima rodada |
| `dois-padroes-concorrentes` | o codigo mostra os dois e nenhum vence | registrar os dois com contagem, e perguntar qual preferir daqui para frente |
| `fora-do-repositorio` | a evidencia existe, mas nao aqui | pedir acesso ou aceitar a lacuna |

`dois-padroes-concorrentes` **nao impede a escrita**: os dois padroes existem no codigo e sao
conhecimento. O que fica pendente e a preferencia futura, nao o registro do que ha.

## Nao investigado por decisao

| Assunto | Motivo |
|---|---|
| `<assunto>` | nao objetivo declarado no preflight |

Isto nao e lacuna, e decisao. Fica separado do nao consolidado para que a rodada nao pareca ter
falhado onde ela escolheu nao ir.

## Automacao sugerida

Recomendacao, nao escrita: a skill nao configura `.claude/` nem instala nada. No maximo dois
itens por tipo.

| Tipo | O que faz | Gatilho | Evidencia desta rodada |
|---|---|---|---|
| `<hook / subagente / comando>` | `<uma linha>` | `<evento ou pedido que dispara>` | `<achado, com caminho e contagem>` |

## Perguntas ao desenvolvedor

1. `<pergunta objetiva, com as duas posicoes e as duas evidencias quando for desacordo>`

## Impacto no contexto global

Medido com `inventario_stack.py stack`, antes e depois.

| | Antes | Depois |
|---|---|---|
| `AGENTS.md` | `<X>` linhas, ~`<X>` tokens | `<Y>` linhas, ~`<Y>` tokens |
| Descricoes de skill | ~`<X>` tokens em `<N>` skills | ~`<Y>` tokens em `<N>` skills |
| Piso permanente por tarefa | ~`<X>` tokens | ~`<Y>` tokens |
| References (corpo, sob demanda) | ~`<X>` tokens | ~`<Y>` tokens |

`<Justificativa do crescimento permanente. Skill nova aumenta o piso pela descricao, e isso e
esperado quando ela empacota um fluxo recorrente: o orcamento existe contra inflacao de regra
global, nao contra a skill que ensina a construir.>`

---

Aprove, rejeite ou reduza item a item. Nada e escrito antes disso.

## Execucao

Preenchida na Fase 8, depois da aprovacao.

**O que foi escrito**

- `<caminho>`: `<o que entrou, mudou ou saiu>`

**Diferencas entre o plano e a escrita**

- `<item>`: `<o que mudou na redacao final e por que>`

**Auditoria do rascunho**

| Rodada | Contexto | Veredito | Itens que voltaram |
|---|---|---|---|
| 1 | novo | `<sem bloqueios / ajustar / bloquear / rodada-invalida>` | `<...>` |

Item que parou por ter batido o limite de rodadas, e nao por evidencia, entra aqui como
`teto-atingido`, com o que ficou em disputa e as duas evidencias. `sem bloqueios` significa que a
evidencia sustentou o artefato; `teto-atingido` significa que o orcamento acabou antes disso, e as
duas situacoes pedem decisoes diferentes do desenvolvedor.

**Validacao final**

| Checagem | Resultado |
|---|---|
| `inventario_stack.py validar-achado` por dossie | `<...>` |
| comandos publicados reexecutados como estao | `<N de N reproduzem>` |
| exemplares citados conferidos | `<N de N existem>` |
| `inventario_stack.py verificar --raiz <raiz>` | `<...>` |
| passagem final de Auditor sobre o diff | `<...>` |
