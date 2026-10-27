# Proposta de skill

Preenchida na Fase 5, auditada na Fase 6 e entregue **inteira** a `skill-creator` na Fase 8.
Ela existe para que a `skill-creator` escreva sem reabrir a investigacao: tudo o que ela
precisa saber sobre o repositorio esta aqui.

Campo que voce nao consegue preencher com evidencia e sinal de que a skill ainda nao esta
pronta para nascer. Proposta com o campo de processo vazio nao e skill, e reference.

---

## Identificacao

**Nome proposto:** `<kebab-case, prefixado pelo sistema quando o destino ja usa prefixo>`
**Tipo:** `<fluxo de implementacao | fluxo de verificacao>`
**Responsabilidade unica:** `<uma frase>`
**Eixo de invocacao:** `<model-invocada, ponto de entrada de fluxo | user-invocada, skill de etapa: frontmatter recebe disable-model-invocation: true>`
**Quem a alcanca:** `<a frase em linguagem natural que dispara | o artefato que manda ler o SKILL.md dela, pelo caminho>`

## Gatilho

Frases reais que um desenvolvedor deste repositorio usaria:

- `<pedido tipico>`
- `<pedido tipico>`

**Nao dispara quando:** `<pedidos vizinhos que pertencem a outra skill, com o nome dela>`

Nenhuma frase de gatilho cita nome de arquivo: caminho envelhece, operacao nao. "Crie um cadastro
no modulo X" e gatilho; "crie um cadastro seguindo o padrao de `Arquivo.ext`" e dependencia de um
caminho que pode ter sido apagado.

## Quando usar o padrao

`<em que situacao concreta este padrao se aplica, no vocabulario do repositorio>`

## Como reconhecer o padrao no codigo existente

`<o sinal concreto: nome de arquivo, include obrigatorio, chamada no topo, estrutura de
diretorio, prefixo>`

## Como aplicar numa implementacao nova

Passo a passo observado nos exemplares, na ordem em que aparece. Cada passo precisa ser
executavel sem abrir exemplar: diga o elemento, a posicao e o criterio de decisao, e deixe o
exemplar para conferencia.

1. `<passo>` | obrigatorio em `<N>` de `<M>` exemplares | `<caminho:linha de exemplo>`
2. `<passo>` | `<obrigatorio | varia conforme <o que>>` | `<caminho:linha>`
3. `<passo>` | `<...>` | `<...>`

**O que varia entre exemplares:** `<e conforme o que varia>`
**Onde se erra:** `<passo esquecido com frequencia, divergencia entre exemplares, armadilha>`

## Arquivos que participam

| Caminho ou padrao de caminho | Papel na funcionalidade |
|---|---|
| `<caminho>` | `<o que este arquivo responde>` |

## Exemplares representativos

| Caminho completo | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `<caminho>` | `<caso tipico recente>` | `<comando ou regra de nomenclatura>` |
| `<caminho>` | `<caso antigo, ou excecao nomeada>` | `<comando ou regra de nomenclatura>` |

## Guardrails deste fluxo

- `<padrao identificado>`, em `<territorio>`: ao `<acao divergente>`, confirme
  `<o que confirmar>` antes de seguir: `<comando de trabalho>`. Evidencia: `<N de M>`, comando
  `<busca literal>`.

## Entradas e saidas

**Entrada esperada:** `<o que o desenvolvedor informa ou o que a skill precisa descobrir>`
**Saida esperada:** `<arquivos criados ou alterados, verificacao executada, relatorio>`

## Evidencia de recorrencia

- unidades do mesmo tipo no repositorio: `<N>` | comando: `<busca literal>`
- frequencia da operacao no historico: `<N commits do tipo em <periodo>>` | comando: `<...>`
- modelo, gerador ou template existente: `<caminho, ou "nenhum">`

## Fronteiras

**O que esta skill nao faz:** `<...>`
**Para onde encaminhar nesses casos:** `<skill, reference ou pergunta ao desenvolvedor>`

## Conhecimento de apoio

| Artefato | Onde vive | Por que ali |
|---|---|---|
| `<assunto>` | `<references da propria skill | .agents/references/<arquivo>.md>` | `<so serve a skill | serve a varias tarefas>` |
