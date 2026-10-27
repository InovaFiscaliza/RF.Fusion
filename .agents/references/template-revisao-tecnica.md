# Template do relatório de revisão técnica

Formato de saída de uma revisão técnica de código, para qualquer projeto e qualquer linguagem. Uma skill de revisão técnica do próprio projeto, ou um prompt que peça este formato, manda entregar o relatório assim. Leia este arquivo antes de escrever o relatório e preencha as células do template no fim. Não acrescente seção que o template não tenha e não troque os valores das colunas, que estão escritos nas próprias células.

## Regras de preenchimento

Emita o relatório inteiro dentro de um único bloco de código cercado com a linguagem `markdown`, sem texto antes ou depois do bloco. O terminal desenha tabela Markdown com bordas gráficas, e a cópia dessa tela cola errado no GitLab e no GitHub. Dentro do bloco, a cópia preserva as barras verticais e cola certo.

O template tem cinco seções, nesta ordem: título `Revisão técnica`, `Resultado` logo abaixo do título, cabeçalho com `Escopo` e `Revisão gerada por`, `Verificações acionadas`, `Achados` e `Passo a passo do achado`. `Resultado` é a primeira coisa que o leitor vê.

Uma verificação é qualquer controle que a revisão executa: regra do `AGENTS.md`, tópico do `.agents/security/guia-seguranca.md`, play da `owasp-playbook`, auditor ou script registrado pelo projeto, lint, teste ou checagem manual. Para cada verificação aplicável, registre artefatos inspecionados, controles executados, evidência e estado. Zero artefatos analisados nunca equivale a `PASS`.

### Estado de cada verificação

| Estado | Uso | Como aparece no template |
|---|---|---|
| `PASS` | Ao menos um artefato compatível foi inspecionado, todos os controles aplicáveis foram executados e não houve desvio | linha da verificação com `✅ PASS` |
| `WARN` | Há risco não bloqueante, passivo preexistente sem agravamento ou heurística inconclusiva que não impede o parecer | linha da verificação com `⚠️ WARN` |
| `BLOCK` | Um controle bloqueante foi violado e confirmado na segunda passada | linha da verificação com `❌ BLOCK` |
| `NOT_APPLICABLE` | Não há artefato compatível ou o assunto está fora do escopo pedido | não vira linha; declare a ausência de artefato em uma frase antes da tabela |
| `NOT_EXECUTED` | A verificação é aplicável, mas ferramenta, acesso ou evidência insuficiente impediu a execução completa | linha da verificação com `❌ BLOCK`, porque verificação aplicável que não roda falha fechado |

Um `PASS` cita o conjunto de arquivos e os controles verificados. Para controle de ausência, aceite como evidência uma busca concluída sobre o conjunto nomeado de arquivos. Saída automatizada com zero artefatos, check interrompido ou amostra não declarada resulta em `NOT_EXECUTED` ou `WARN`, nunca em `PASS`.

### Severidade e origem de cada achado

Severidade: `BLOQUEANTE`, `ALTA`, `MEDIA` ou `BAIXA`. Achado com severidade `BLOQUEANTE` produz `BLOCK` quando permanece confirmado após a segunda passada.

Origem temporal, quando a revisão compara uma mudança com uma base:

| Origem | Critério |
|---|---|
| `introduzido` | O desvio nasce na mudança ou em arquivo novo |
| `ampliado` | O desvio já existia, mas a mudança aumenta alcance, exposição, impacto ou dependência |
| `preexistente` | O desvio está comprovadamente na base e a mudança não o agrava |
| `incerto` | A base, o histórico ou a relação causal não permitem classificação segura |

Havendo base de comparação, passivo `preexistente` sem agravamento não produz `BLOCK`: registre como `WARN` e candidato a tarefa. Achado `ampliado` pertence à mudança atual e pode produzir `BLOCK`. Achado `incerto` não vira bloqueio sem evidência. Sem base de comparação, como em revisão de pasta inteira, o estado do achado reflete o controle violado e a coluna `Origem` registra `preexistente`.

Todo achado exige o caminho do dado demonstrado, da entrada até o ponto de uso, com `arquivo:linha` em cada salto. Sem esse rastro, o item é hipótese e entra como `WARN`, nunca como `BLOCK`. Antes de fechar o relatório, tente derrubar cada achado com evidência contrária; falso positivo derrubado fica registrado na coluna `Evidência` da verificação.

### Resultado

| Condição | Linha em `Resultado` |
|---|---|
| Existe `BLOCK` após a segunda passada, ou existe `NOT_EXECUTED` aplicável, ou uma incerteza relevante impede a conclusão técnica | `❌ BLOCKED` |
| Não há `BLOCK`, com ou sem `WARN`, e cada `PASS` tem cobertura positiva | `✅ PASS` |

### Limite de cada célula

Célula longa deixa a linha da tabela ilegível no bloco de código e na ferramenta de revisão. Os limites abaixo mantêm cada linha curta.

| Célula | Conteúdo aceito | Limite |
|---|---|---|
| `Escopo` | caminho do escopo e contagem por tipo de arquivo, como `src/pedidos/ (8 PHP, 3 JS)`; a base de comparação entra só quando há mudança comparada | uma linha |
| `Revisão gerada por` | nome da skill ou do procedimento, ou `leitura manual` | uma linha |
| frase de verificação sem artefato | as verificações `NOT_APPLICABLE` e o motivo da ausência de artefato | uma frase, antes da tabela de verificações |
| coluna `Verificação` | nome curto do controle, como `escopo de escrita` ou `guia S07 injeção` | até 6 palavras |
| coluna `Artefatos` | grupo nomeado, como `8 arquivos PHP` ou `5 páginas *_lista.php`; lista de nomes só com até 3 arquivos | até 6 palavras |
| coluna `Estado` da verificação | só `✅ PASS`, `⚠️ WARN` ou `❌ BLOCK` | um valor |
| coluna `Evidência` | controles, contagem e veredito, em fragmentos separados por ponto e vírgula, sem frase narrativa | até 30 palavras |
| coluna `Estado` do achado | só `✅ PASS`, `⚠️ WARN` ou `❌ BLOCK` | um valor |
| coluna `Severidade` | só `BLOQUEANTE`, `ALTA`, `MEDIA` ou `BAIXA` | um valor |
| coluna `Origem` | só `introduzido`, `ampliado`, `preexistente` ou `incerto` | um valor |
| coluna `Local` | `arquivo:linha`, aceitando até um segmento de diretório, como `rn/PedidoRN.php:16` | um par |
| coluna `Achado` | o desvio afirmado, mais a regra, o tópico do guia ou o CWE aplicável | até 2 frases e 30 palavras |
| coluna `Menor ajuste` | a ação de correção, no imperativo | uma frase, até 15 palavras |
| seção `Passo a passo do achado` | saltos do dado, aceitando o atalho `:linha` depois de o arquivo já ter sido nomeado na mesma linha | uma linha de setas por achado |
| seção `Resultado` | só `✅ PASS` ou `❌ BLOCKED` | uma linha |

Quando o mesmo controle for violado em vários arquivos, abra uma linha por arquivo, com `Local` no primeiro ponto daquele arquivo e a contagem das demais ocorrências na coluna `Achado`.

### Lugar único de cada fato

| Fato | Célula única |
|---|---|
| comando executado, saída e controles verificados | coluna `Evidência` da verificação |
| resultado da segunda passada e falso positivo derrubado | coluna `Evidência` da verificação, ou coluna `Achado` da linha reclassificada |
| rastro do dado, da entrada até o ponto de uso | seção `Passo a passo do achado` |
| base de comparação e refs comparadas | campo `Escopo` |
| ressalva sobre alcance do parecer | coluna `Achado` da linha que a sustenta |

Não repita evidência nem rastro dentro da coluna `Achado`. Não escreva ressalva, nota ou parágrafo fora das células acima. Mascare segredo e dado pessoal em qualquer célula; nunca reproduza um segredo válido em texto claro.

O relatório padrão não é persistido: não grave arquivo, não abra tarefa, issue ou PR e não altere código. PDF, JSON de achados e texto de issues são saídas a pedido do desenvolvedor; elas derivam do relatório padrão já pronto e nunca o substituem.

## Template

````markdown
```markdown
## Revisão técnica

### Resultado

<✅ PASS | ❌ BLOCKED>

**Escopo**: <caminho do escopo> (<contagem por tipo de arquivo>)
**Revisão gerada por**: <skill ou procedimento | leitura manual>

### Verificações acionadas

| Verificação | Artefatos | Estado | Evidência |
|---|---|---|---|
| <nome curto do controle> | <8 arquivos PHP> | <✅ PASS / ⚠️ WARN / ❌ BLOCK> | <fragmentos com ponto e vírgula, até 30 palavras> |
| <nome curto do controle> | <5 páginas `*_lista.php`> | | |

### Achados

| # | Estado | Severidade | Origem | Local | Achado | Menor ajuste |
|---|---|---|---|---|---|---|
| 1 | <✅ PASS / ⚠️ WARN / ❌ BLOCK> | <BLOQUEANTE / ALTA / MEDIA / BAIXA> | <introduzido / ampliado / preexistente / incerto> | `arquivo:linha` | <o desvio, mais a regra, o tópico do guia ou o CWE; até 2 frases e 30 palavras> | <ação no imperativo; até 15 palavras> |
| 2 | | | | `arquivo:linha` | | |

### Passo a passo do achado

**Achado 1**: `arquivo:linha` começo -> `arquivo:linha` passo -> `arquivo:linha` fim
```
````
