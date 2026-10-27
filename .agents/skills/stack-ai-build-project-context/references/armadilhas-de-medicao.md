# Armadilhas de medicao

Autoridade sobre: os testes que separam um numero que reproduz de um numero que mede a coisa
certa, e a amostra minima antes de descrever um padrao.

**Quando usar:** Fase 3, antes de registrar qualquer contagem no dossie; Fase 6, pelo Auditor;
Fase 8, antes de publicar qualquer comando.

## Por que este arquivo existe

A barra de evidencia exige que o comando publicado **reproduza** o numero afirmado. Isso e
necessario e nao e suficiente. Um comando pode reproduzir o proprio numero para sempre e estar
medindo outra coisa desde o primeiro dia.

Todos os casos abaixo vieram de rodadas reais, em artefatos que passavam na barra atual. Metade
das afirmacoes de uma rodada caiu quando estes testes foram aplicados. **Nao sao hipoteses.**

O erro que os une tem um nome: **descrever o territorio mais organizado e chamar de sistema.**
Um sistema legado e mais irregular que qualquer um dos seus melhores exemplos.

## As nove perguntas

Cada uma tem um teste mecanico. Nao julgue: rode e compare dois numeros.

### 1. O comando mede o que a afirmacao diz, ou so reproduz um numero?

```bash
python3 <caminho-da-skill>/scripts/inventario_stack.py auditar-comando --raiz <raiz> --cmd '<comando>'
```

Roda o comando e as variantes de controle: com ancora, com `-i`, sem restricao de extensao, com
o binario real da ferramenta, e contando as linhas comentadas. **Divergencia nao e erro
automatico: e pergunta.** Decida qual dos dois numeros responde a afirmacao e publique esse.

Tres formas do mesmo defeito, todas vistas em rodada real:

- **sufixo**: um termo curto sem ancora casa dentro de palavras maiores do dominio. `id` casa
  `uuid`, `valid` e `hidden`; `user` casa `superuser`; um termo de quatro letras chegou a inflar
  uma contagem em quase mil linhas.
- **definicao contada como uso**: o padrao `NOME(` casa tambem a linha que **declara** a funcao,
  entao uma funcao morta aparece com uso maior que zero.
- **comparacao contada como atribuicao**: o padrao `X =` casa `if X == ""` e `if X = ""`, entao
  quem le acha que encontrou onde o valor e escrito.

### 2. Quantos exemplares voce leu por inteiro, e de quantos territorios diferentes?

```bash
# liste os territorios onde a unidade existe, e conte quantos voce abriu
find <escopo> -name '<padrao da unidade>' -printf '%h\n' | sed 's#/[^/]*$##' | sort | uniq -c | sort -rn
```

**Um exemplar nunca descreve uma familia.** Se a unidade aparece em N diretorios, leia por
inteiro pelo menos um de cada um dos tres maiores, e nao dois vizinhos do mesmo.

Caso real: uma familia de 22 arquivos foi descrita a partir de 1, e o que se descreveu era a
linhagem de copy-paste de 7 deles. Os outros 15 tinham tres formas diferentes de fazer a mesma
coisa, e uma delas nao usava o mecanismo que o artefato dava como obrigatorio.

### 3. Isto e frequencia ou consequencia? Voce executou alguma coisa?

Frequencia (`X aparece em N arquivos`) se mede com busca. **Consequencia** (`sem X o valor chega
zerado`, `a consulta falha silenciosamente`, `isso quebra em producao`) descreve execucao, e
voce nao executou nada.

Regra: se a frase afirma o que **acontece em tempo de execucao**, ou voce mostra o codigo que
trata aquele caso, ou reescreve para dizer so o que o codigo mostra.

Caso real: um artefato afirmava que um valor mal formatado "quebra a consulta silenciosamente".
O codigo tratava o caso: a maioria dos pontos testava o resultado vazio e desviava para uma tela
de erro com mensagem propria. Nao era silencioso, e era pior: a mensagem apontava outra causa, e
o defeito seria investigado na direcao errada. A versao corrigida ficou mais util que a original,
porque diz onde o sintoma aparece.

### 4. A afirmacao e negativa? Quanto voce buscou para poder dizer "nao ha"?

"Nao existe", "nunca", "nenhum", "e o unico" exigem varredura ampla, nao ausencia numa busca.
Busque de tres formas diferentes antes de afirmar ausencia, e declare o que buscou.

Casos reais: "nao ha string de conexao no codigo" era falso, havia nove arquivos com provedor,
servidor e usuario embutidos; "e o unico uso de estado de aplicacao" era falso, havia tres
chaves, e uma delas era lida sem nunca ser atribuida no repositorio.

Afirmacao negativa errada e a mais perigosa da stack: alguem confia nela para **nao procurar**.

### 5. Incluir e usar? Definir e chamar? Estar ativo e estar comentado?

Tres perguntas com o mesmo formato, e as tres precisam de duas contagens, nunca uma.

Adapte a forma de importar a da linguagem do destino: `#include`, `import`, `require`, `use`,
`from ... import`, `using`, ou a diretiva que o censo mostrou.

```bash
# carregado contra chamado
grep -ril "<forma de importar>.*NOME" <escopo> | wc -l
grep -rl  "NOME[[:space:]]*(" <escopo> | grep -v '<arquivo onde NOME e definido>' | wc -l
# ativo contra comentado, comparando LINHA com LINHA, no marcador da linguagem
grep -rn "^[[:space:]]*NOME" <escopo> | wc -l
grep -rn "^[[:space:]]*(#|//|--|')[[:space:]]*NOME" <escopo> | wc -l
```

Casos reais: um utilitario carregado por mais de cem arquivos e chamado por nenhum; quatro
copias de um arquivo de constantes que nenhum outro arquivo importa; metade das chamadas de
verificacao de acesso comentada, convivendo no mesmo diretorio com as ativas.

Em codebase legada, **codigo morto convive com codigo vivo sem nenhuma marca**. A unica forma de
distinguir e contar os dois lados.

### 6. Seu escopo de busca alcanca todos os lugares onde isso pode viver?

```bash
# onde mais esse termo aparece, fora da extensao que voce restringiu
grep -rl '<termo>' <escopo> | sed -E 's/.*\.//' | sort | uniq -c | sort -rn
```

Restringir por extensao e necessario e cega. Antes de concluir alcance, rode uma vez sem
restricao.

Caso real: todos os comandos de uma reference restringiam a busca a uma unica extensao, e um
mecanismo inteiro do sistema vivia em arquivos de outra extensao, incluidos por aqueles. A
afirmacao "este e praticamente o unico uso de X" nasceu de um instrumento que nao conseguia ver
a outra metade. O mesmo acontece com codigo gerado, migracoes, templates e arquivos de
configuracao: eles participam do padrao e raramente estao na extensao principal.

### 6b. O que voce contou e codigo do sistema, ou biblioteca de terceiros?

Repositorio legado costuma versionar dependencia junto com o codigo proprio, sem separacao. A
busca nao distingue, e o padrao "do sistema" passa a incluir o que veio de fora.

```bash
# onde vive codigo de terceiros neste repositorio
find <escopo> -type d \( -name 'node_modules' -o -name 'vendor' -o -name 'third_party' \
  -o -name 'lib*' -o -name 'dist' -o -name '*.min.*' -o -name 'packages' \) 2>/dev/null | head
# quem sao os donos das ocorrencias que voce contou
grep -rhoiE '[A-Za-z_]*<termo>' <escopo> | sort | uniq -c | sort -rn | head
```

A segunda busca e a que decide: se os prefixos mais frequentes forem nomes de biblioteca, a
contagem esta medindo a dependencia, nao a convencao.

Caso real: uma reference afirmava que um mecanismo de carregamento assincrono aparecia em N
arquivos do modulo. Cerca de um quarto das ocorrencias vinha de um editor de texto rico
vendorizado e de bibliotecas de XML, que chamam um metodo de mesmo nome com outra finalidade.

Nem sempre a resposta e excluir. Dependencia versionada **e** parte do sistema, e as vezes o
achado e justamente que o modulo carrega aquilo. O que nao pode e contar as duas coisas juntas
e chamar o resultado de convencao do time.

### 7. O universo comparavel esta certo?

Contagem vira proporcao contra o universo **das coisas que poderiam ter o padrao**, nao contra
o total de arquivos.

Caso real: "619 de 1567 arquivos normalizam o identificador" classificava como padrao fraco. O
universo certo eram os 726 arquivos que **obtem** aquele identificador: 619 de 726 e outra faixa
e outra instrucao.
`validar-achado` avisa quando a convencao cobre menos da metade do universo declarado.

### 8. Nomes iguais tem comportamentos iguais?

```bash
# todas as definicoes do mesmo nome, e o corpo de cada uma
grep -rn "\(Function\|Sub\|def\|function\)[[:space:]]\+NOME" <escopo>
```

Quando o mesmo nome e definido em mais de um lugar, **compare os corpos** antes de descrever o
comportamento.

Casos reais: um utilitario de escape com tres comportamentos distintos sobre o mesmo caractere,
todos sob o mesmo nome, em copias por modulo; um arquivo cujo nome sugeria uma funcao e que
declarava outra, colidindo com a do vizinho; a mesma constante de configuracao declarada com
dois valores diferentes em arquivos diferentes, e qual vale depende de qual e carregado.

### 9. Este simbolo e definido aqui, ou chega de fora?

```bash
grep -rn '<simbolo>' <escopo> | wc -l                    # onde e lido
grep -rn '^[[:space:]]*<simbolo>[[:space:]]*=' <escopo>  # onde e atribuido
```

Simbolo lido e nunca atribuido vem de fora do repositorio, e isso e contrato de fronteira, nao
detalhe.

Caso real: uma chave de estado global lida por varios arquivos e nunca atribuida em lugar
nenhum do repositorio, e usada para montar uma consulta. Ela vinha de um componente externo, e
codigo novo que dependesse dela em outro contexto montaria a consulta com o valor vazio.

## Duas verificacoes sobre o artefato inteiro

### O artefato se contradiz?

Leia as regras que voce escreveu, duas a duas, e pergunte se existe um diretorio do repositorio
onde elas apontam para lados opostos.

Caso real: uma reference mandava criar propriedade numa forma canonica e, tres linhas abaixo,
mandava seguir o vizinho. Num modulo inteiro do sistema as duas se contradiziam, e o agente que
seguisse a primeira produziria arquivo destoante de todos os vizinhos.

Quando houver conflito, **declare a hierarquia** em vez de deixar as duas: o codigo vizinho vence
a forma canonica descrita em qualquer artefato.

### Voce descreveu o sistema ou o seu melhor exemplo?

Pergunta final, antes de publicar. Para cada padrao afirmado, nomeie o territorio onde ele vale.
Se voce nao conseguir nomear o territorio, voce nao mediu: generalizou.

Preferir **nomear o territorio** a cravar a contagem. "Os arquivos de `<modulo>/<subpasta>`
fazem assim" e mais acionavel que "8 de 22 fazem assim", e nao envelhece quando alguem
acrescenta um arquivo.

## A ferramenta de busca faz parte da medicao

Os exemplos acima usam `grep` por ser o denominador comum. Adapte a do destino: `rg`, `ag`, a
busca do proprio ecossistema, ou o que o censo indicou. Duas cautelas valem em qualquer uma:

- **O binario pode nao ser o que voce pensa.** Em ambiente agentico, `grep` pode ser funcao,
  alias ou wrapper com opcoes forcadas. Quando dois numeros divergirem, teste com o caminho
  absoluto do binario antes de concluir qualquer coisa sobre o codigo.
- **Ferramentas discordam sobre as mesmas opcoes.** Exclusao de diretorio, tratamento de arquivo
  binario e globs de inclusao tem semantica diferente entre elas. O comando publicado precisa
  dizer com qual ferramenta o numero foi obtido.

`auditar-comando` entende comandos no estilo `grep` terminados em `wc -l`. Com outra ferramenta,
as perguntas continuam valendo: o que muda e que voce roda as variantes a mao.

**Ele alerta demais, de proposito.** Duas divergencias comuns nao sao defeito, e cabe a voce
descartar: um padrao que **ja** comeca com `^` esta ancorado na linha, e a variante gerada
relaxa isso em vez de apertar; e uma contagem de linhas comentadas pequena diante do total
costuma ser ruido normal, nao o achado. Alerta e pergunta: a ferramenta nao sabe qual dos dois
numeros responde a sua afirmacao, e voce sabe.

## Ordem de aplicacao

| Fase | O que rodar |
|---|---|
| 3, ao registrar cada achado | perguntas 1 a 9 sobre a afirmacao que esta sendo escrita |
| 4, na consolidacao | `validar-achado`, que cobre a 7 |
| 6, pelo Auditor | todas, com amostra propria e sem a narrativa do investigador |
| 8, antes de publicar | `auditar-comando` em cada comando que vai para a secao de evidencias |
