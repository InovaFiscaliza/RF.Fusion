# Protocolo de investigacao

Autoridade sobre: o que cada frente precisa cobrir, como escolher exemplares, o que qualifica
um padrao, como padroes concorrentes sao tratados e como alcance e classificacao sao
atribuidos.

## Sumario

- [Principio](#principio)
- [Unidades do sistema](#unidades-do-sistema)
- [Exemplares representativos](#exemplares-representativos)
- [Eixos obrigatorios](#eixos-obrigatorios)
- [Padrao orientado por intencao](#padrao-orientado-por-intencao)
- [Processo observado](#processo-observado)
- [Historia do repositorio](#historia-do-repositorio)
- [Fronteira de sistema externo](#fronteira-de-sistema-externo)
- [Barra de evidencia](#barra-de-evidencia)
- [Classificacao do padrao](#classificacao-do-padrao)
- [Padroes concorrentes](#padroes-concorrentes)
- [Antipadrao recorrente e convencao](#antipadrao-recorrente-e-convencao)
- [Codificacao e protocolo de busca](#codificacao-e-protocolo-de-busca)
- [Fato de censo e sinais de dependencia](#fato-de-censo-e-sinais-de-dependencia)
- [Alcance](#alcance)
- [Busca por contraexemplo](#busca-por-contraexemplo)
- [Hipotese derrubada vira afirmacao negativa](#hipotese-derrubada-vira-afirmacao-negativa)
- [Nao objetivos](#nao-objetivos)
- [Sinais de que o achado nao vale a pena](#sinais-de-que-o-achado-nao-vale-a-pena)

## Principio

A codebase e a autoridade. Documentacao interna, nome de pasta, framework declarado no
manifesto e experiencia previa com tecnologias parecidas sao hipoteses a confirmar, nunca
conclusoes.

A pergunta que orienta tudo nao e "isto esta certo?", e sim **"como este sistema faz isto, e o
que alguem precisa saber para fazer igual?"**. Adequacao a boa pratica externa nao e criterio
de nada aqui. Coerencia com o que ja existe e.

Quando o codigo contradiz a documentacao do proprio repositorio, o codigo vence, e a
contradicao vira achado: alguem precisa saber que aquele documento envelheceu.

## Unidades do sistema

Antes de investigar comportamento, identifique **o que este sistema produz repetidamente**: a
coisa que alguem cria quando implementa uma funcionalidade nova. Pagina, tela, endpoint,
formulario, relatorio, procedure, job, componente, modulo, migracao.

A identificacao segue a tabela abaixo, na ordem em que ela aparece. **Pare no primeiro sinal
claramente dominante** e declare qual foi; se nenhum dominar, use o ultimo e diga que foi
fallback. Duas rodadas que aplicam a mesma tabela ao mesmo repositorio chegam ao mesmo recorte, e
e isso que a tabela existe para garantir.

| Sinal observado | Onde olhar | Unidade provavel |
|---|---|---|
| Massa de arquivos com a mesma extensao e o mesmo formato de nome | contagem por extensao e por sufixo de nome no censo | o arquivo desse tipo: pagina, tela, script, procedure |
| Diretorios que se repetem com a mesma estrutura interna em modulos diferentes | primeiro e segundo nivel do escopo | o modulo, com a unidade dentro dele |
| Roteamento centralizado que enumera destinos | tabela de rotas, controlador, mapa de URL, despachante | o endpoint ou a acao roteada |
| Familia de arquivos de comportamento nomeada por caso | testes de aceitacao, cenarios, fixtures de fluxo | o caso de uso |
| Modelo, gerador ou template que alguem escreveu para repetir a tarefa na mao | diretorio de template, script de scaffold, arquivo com "modelo" ou "exemplo" no nome | o que aquele modelo produz |
| Nenhum sinal domina | fallback declarado | a unidade mais frequente no recorte, com a duvida registrada |

Tres campos acompanham a escolha, e nenhum e opcional:

- **unidade**: o nome que o repositorio usa para a coisa, nao o nome teorico dela;
- **sinal**: qual linha da tabela decidiu, e por que ela domina;
- **evidencia**: os caminhos que comprovam o sinal, mais o comando que os produziu.

Escolha sem esses tres campos e escolha de gosto, e a rodada seguinte vai recortar diferente sem
saber que discordou desta.

Uma unidade identificada gera tres perguntas, e as tres precisam de resposta escrita:

1. Do que ela e feita? Que arquivos participam de uma so funcionalidade, e o que cada um faz.
2. Como ela nasce? A sequencia observada, do inicio do arquivo ate o fim do fluxo.
3. O que e obrigatorio nela? O que aparece em todos os exemplares, e o que varia.

Sem esta secao a rodada nao produz skill nenhuma, porque skill descreve fluxo, e fluxo so
existe se alguem reconstruiu a sequencia.

## Exemplares representativos

Nao e necessario analisar todos os arquivos do repositorio. E necessario escolher bem.

Para cada unidade, leia **integralmente** um conjunto pequeno e deliberado:

- dois ou tres exemplares recentes, pelos commits mais novos que os tocaram: mostram a
  convencao viva;
- um ou dois exemplares antigos: mostram o que mudou e o que sobreviveu;
- um exemplar por modulo ou subsistema relevante: revela se existe um padrao ou varios;
- quando houver, o exemplar que o proprio repositorio trata como modelo, gerador ou template.

Leitura integral de dez arquivos ensina mais sobre como o sistema e construido que mil buscas
por padrao isolado. A busca entra depois, para medir a extensao do que a leitura revelou.

Encontrado um padrao, meça antes de chama lo de convencao. A contagem precisa separar quatro
coisas, e a diferenca entre elas decide o destino:

| Situacao | Como se reconhece |
|---|---|
| Convencao recorrente | domina o universo comparavel, com exemplares em varios diretorios |
| Variacao legitima | dois ou mais jeitos, cada um com massa e territorio proprios |
| Excecao | poucos casos contra um padrao claro, muitas vezes explicaveis pelo contexto |
| Caso isolado ou historico | uma ou duas ocorrencias, frequentemente antigas |

Caso isolado nunca vira regra, guardrail ou skill. Pode virar nota numa reference quando
alguem for tropecar nele.

O exemplar e instrumento da investigacao, nao o conteudo do artefato. Feche cada padrao na forma
que se aplica sem abrir arquivo nenhum: os elementos, a ordem, o que e obrigatorio, o que varia e
o criterio de cada decisao. Um padrao que so existe como "faca igual a `<arquivo>`" nao foi
extraido, e desaparece junto com o arquivo.

## Eixos obrigatorios

Toda rodada declara cobertura sobre os eixos abaixo. Nem todo eixo existe em todo sistema:
eixo ausente e declarado como ausente, com o que foi buscado. O que nao vale e silencio.

| # | Eixo | O que procurar |
|---|---|---|
| 1 | Estrutura e organizacao | divisao de diretorios, o que cada tipo de arquivo responde, o que e reaproveitado, convencoes de nomenclatura, estrutura tipica de um arquivo do tipo |
| 2 | Ciclo da requisicao | ponto de entrada, inicializacao e bootstrap, dependencias obrigatorias no topo, o que roda antes do conteudo, navegacao, redirecionamento e encerramento |
| 3 | Autenticacao e identidade | qual componente autentica, o que ele entrega ao codigo local, como a identidade do usuario e obtida e onde a sessao vive |
| 4 | Autorizacao e permissao | como o acesso a unidade e validado, como a permissao por acao e verificada, o que guarda a escrita e o que guarda apenas a interface |
| 5 | Construcao da unidade | como uma pagina, tela ou endpoint e montada, componentes e includes obrigatorios, formulario, submissao, JavaScript associado, chamadas AJAX ou API |
| 6 | Validacao | validacao de entrada, no cliente e no servidor, bibliotecas e funcoes reutilizadas, o que e validado sempre e o que nao e |
| 7 | Persistencia e dados | mecanismo de acesso, procedure, query direta, ORM ou abstracao propria, de onde vem a conexao, controle de transacao, tratamento de erro de banco |
| 8 | Regra de negocio | onde ela vive, como e acionada, se esta separada da renderizacao ou junto dela |
| 9 | Integracoes externas | onde a integracao e usada, para que, qual o padrao local de uso, quais arquivos participam, que pre condicoes existem |
| 10 | Erro, log e seguranca | tratamento de erro, o que e registrado e onde, mecanismos de seguranca presentes no codigo |
| 11 | Configuracao | o que e configuravel, onde vive, o que quebra quando falta |
| 12 | Padroes equivalentes | duas funcionalidades do mesmo tipo implementadas em lugares diferentes: o que elas tem em comum e o que muda |

O eixo 12 e o que mais revela convencao real, e o mais esquecido. Comparar duas
implementacoes equivalentes separa o que e padrao do que foi escolha de quem escreveu.

Nada aqui pressupoe separacao em camadas. Se persistencia, validacao, regra e renderizacao
estao no mesmo arquivo, esse e o achado do eixo 1 e do eixo 8, e ele se escreve como e.

## Padrao orientado por intencao

Nao documente apenas o que existe: documente **para que serve** e **em que momento aparece**.

| Forma pobre | Forma que orienta implementacao |
|---|---|
| "existe uma chamada `X` em 300 arquivos" | "paginas autenticadas inicializam `X` e executam a validacao `Y` antes de processar a requisicao" |
| "o objeto `C` aparece no topo dos arquivos" | "a conexao de banco vem de `C`; nenhuma pagina cria conexao propria" |
| "ha funcoes de validacao em `lib/`" | "validacao de campo usa as funcoes de `lib/`, incluidas por caminho relativo antes do formulario" |

A diferenca entre as duas colunas e a diferenca entre uma stack que descreve e uma stack que
orienta. Um agente lendo a coluna da direita sabe o que fazer; lendo a da esquerda, sabe
apenas que algo existe.

Nao e preciso explicar o funcionamento interno de um mecanismo externo para descrever a
intencao. Basta o que o codigo local mostra: quem chama, quando, com o que, e o que acontece
depois.

## Processo observado

Padrao responde "o que este sistema faz". Processo responde "o que eu faco para criar uma
unidade nova". Todo investigador devolve os dois quando a frente tocar uma unidade.

Um processo observado tem:

- **unidade**: o que esta sendo criado;
- **arquivos que participam**, com o papel de cada um;
- **sequencia**, na ordem em que aparece nos exemplares;
- **obrigatorio contra variavel**: o que esta em todos os exemplares e o que muda entre eles;
- **quantos exemplares sustentam**, e quais foram lidos por inteiro;
- **como localizar um exemplar equivalente hoje**: a regra de nomenclatura, o diretorio ou o
  comando que devolve o equivalente atual, porque o caminho de hoje pode nao existir na proxima
  rodada;
- **onde se erra**: passo esquecido com frequencia, segundo o historico de correcoes, ou
  divergencia entre exemplares que indica armadilha.

Processo com um exemplar nao e processo, e um arquivo. Tres exemplares que concordam ja
descrevem uma sequencia; quando discordam, a divergencia e parte do achado.

## Historia do repositorio

O codigo diz o que o sistema faz. O historico diz **onde ele quebra e o que ja foi tentado**, e e
a unica fonte de dois campos que o dossie pede e que costumam voltar vazios: "onde se erra" e a
recorrencia que sustenta uma skill.

Tres consultas resolvem a maior parte, e as tres sao baratas:

```bash
# arquivos que mais recebem correcao no escopo: onde o sistema erra de verdade
git log --format= --name-only --grep='fix\|corrig\|ajust\|hotfix' -i -- <escopo> \
  | sort | uniq -c | sort -rn | head -20

# com que frequencia a operacao acontece: recorrencia que sustenta skill
git log --oneline --since='<periodo>' -- <caminho da unidade> | wc -l

# o que foi desfeito, e o que a mensagem diz do motivo
git log --oneline --grep='revert' -i -- <escopo> | head -20
```

Como ler o resultado, sem inventar:

- **Arquivo que concentra correcao** e candidato a "onde se erra" do processo observado, e o passo
  esquecido aparece lendo dois ou tres desses commits, nao a lista.
- **Frequencia da operacao** e evidencia de recorrencia para admissao de skill, e vale mais que a
  contagem de arquivos existentes: vinte unidades criadas ha cinco anos nao provam que alguem crie
  a vigesima primeira.
- **Revert** e decisao revertida, e a mensagem costuma nomear a razao. E pista de convencao, nao
  prova: confirme no codigo atual antes de escrever.
- **Comentario, TODO e FIXME** revelam intencao nao implementada. Entram como contexto do achado,
  nunca como padrao do sistema.

Repositorio sem historico util, por importacao em commit unico ou por migracao de versionamento,
e um fato a declarar no registro, e nao um campo a preencher no chute.

## Fronteira de sistema externo

Todo repositorio consome coisa que nao versiona: framework corporativo, servico de
autenticacao, API de terceiro, biblioteca binaria, banco de outro time. O interior disso nao e
investigavel, e insistir produz linha de "nao confirmavel".

Investigue apenas o necessario para entender **como a integracao participa da arquitetura
local**:

- onde ela e usada, e em quantos pontos;
- para qual finalidade;
- qual o padrao local de uso, na ordem em que aparece;
- quais arquivos e componentes participam;
- que pre condicoes existem antes de usar;
- o que ela entrega ao codigo local, e o que o codigo local deixa de definir por causa disso.

O ultimo item costuma ser o mais importante e o mais esquecido. Quando um objeto externo e a
origem da conexao de banco, da identidade do usuario ou da verificacao de permissao, isso e
arquitetura local: o codigo daqui depende dele em todo lugar, e nenhuma implementacao nova
funciona sem repetir aquele passo. Registrar que "o sistema consome X e nao define X
localmente" descreve a fronteira; registrar "toda unidade obtem a conexao a partir de X, desta
forma, neste ponto" ensina a implementar.

## Barra de evidencia

Nenhuma afirmacao entra num artefato sem estes campos:

| Campo | Conteudo | Falha comum |
|---|---|---|
| Afirmacao | Uma frase operacional que um agente consegue seguir, com a finalidade explicita | Descricao vaga que nao muda decisao nenhuma |
| Evidencia | Caminhos completos a partir da raiz e linhas, com contagem de ocorrencias e de arquivos distintos | Um exemplo unico apresentado como padrao, ou caminho abreviado que existe duas vezes no repositorio |
| Universo | Quantos arquivos comparaveis existem no escopo, para a contagem virar proporcao | "300 arquivos" sem dizer de quantos |
| Comando | A busca literal de confirmacao, com o escopo, executada como esta escrita | Numero sem comando, ou comando que nao reproduz o numero |
| Comando de contraexemplo | A busca literal que tentaria derrubar a afirmacao | Repetir a busca de confirmacao e chamar de contraexemplo |
| Contraexemplos | Quantos casos divergem, comecando pela quantidade, e quais | Campo em branco, porque ninguem contou |
| Fonte declarativa | Arquivo do repositorio que declara o mesmo, ou "nenhuma" | Tratar experiencia previa como fonte declarativa |
| Alcance | Repositorio, subsistema nomeado ou modulo nomeado | Alcance de repositorio para achado de uma pasta so |
| Classificacao | Calculada a partir dos numeros, pela regra abaixo | Convencao declarada por conviccao |

O dossie e verificado por `inventario_stack.py validar-achado` antes da consolidacao.

**Comando executado como esta escrito.** O comando registrado no dossie e o mesmo que vai para
a secao de evidencias da reference, e a proxima rodada vai rodar exatamente aquele texto. Rode
o antes de registrar e confirme que o numero devolvido e o numero afirmado. Comando publicado
que devolve outro numero faz a proxima medicao ler contradicao onde ha erro de digitacao, e
derruba artefato correto.

**Reproduzir nao e medir certo, e este e o ponto cego da barra acima.** Um comando errado
reproduz o proprio numero com fidelidade perfeita, rodada apos rodada: e exatamente o que ele
faz de melhor. Antes de registrar qualquer contagem, rode

```bash
python3 <caminho-desta-skill>/scripts/inventario_stack.py auditar-comando --raiz <raiz> --cmd '<comando>'
```

e resolva o que ele acusar. Os testes completos, com os casos reais que os originaram, estao em
`armadilhas-de-medicao.md`. Em rodada real, metade das afirmacoes de uma stack caiu quando esses
testes foram aplicados a artefatos que ja passavam na barra desta pagina.

## Classificacao do padrao

Calculada a partir de quatro numeros: ocorrencias, arquivos distintos, divergentes e universo
comparavel. `inventario_stack.py validar-achado` aplica exatamente esta regra e devolve a faixa;
declarar acima do que os numeros sustentam e erro, declarar abaixo vira aviso.

| Faixa | Regra aplicada |
|---|---|
| `convencao` | divergentes ate 5% dos casos, com cinco ou mais ocorrencias em tres ou mais arquivos. Fonte declarativa do repositorio com zero divergentes e duas ocorrencias tambem entra aqui |
| `dominante-com-excecoes` | divergentes abaixo de 30% dos casos |
| `concorrentes` | divergentes em 30% ou mais dos casos |
| `isolado` | menos de tres ocorrencias |

Universo comparavel nao muda a faixa, mas entra na evidencia e vira aviso quando a convencao
cobre menos da metade dele: contagem alta sobre universo maior costuma significar que o universo
declarado esta errado, nao que a convencao e fraca.

O que cada faixa autoriza:

| Faixa | Destino possivel |
|---|---|
| `convencao` | qualquer destino, inclusive regra de `AGENTS.md` e guardrail |
| `dominante-com-excecoes` | qualquer destino, com as excecoes nomeadas no texto e o alcance reduzido ao territorio onde domina. O guardrail sai com a ressalva, nunca como regra fechada |
| `concorrentes` | reference e skill, registrando os dois padroes com contagem. Ver a secao propria |
| `isolado` | nota numa reference, quando alguem for tropecar nele. Nunca regra, guardrail ou skill |

Os numeros sao piso, nao teto. Em repositorio pequeno, cinco ocorrencias podem ser o universo
inteiro; em repositorio grande, cinco em dez mil nao sustentam alcance de repositorio. Declare
sempre o universo examinado.

## Padroes concorrentes

Dois padroes concorrentes sao um achado, nao um impedimento. O erro caro aqui e transformar a
divergencia em pergunta e nao escrever nada: os dois padroes existem no codigo, e um agente
que nao souber disso vai escolher ao acaso.

O tratamento e sempre o mesmo:

1. Registre os dois, com contagem e territorio de cada um.
2. Diga o que separa um do outro quando houver correlacao visivel: modulo, epoca, tipo de
   operacao, autor do subsistema.
3. Escreva o guardrail na forma de confirmacao, nao de escolha: "este sistema usa `A` em `<N>`
   casos e `B` em `<M>`; confirme qual vale no modulo antes de seguir um dos dois".
4. Leve a pergunta ao desenvolvedor **em paralelo**, sobre qual deve ser preferido daqui para
   frente. A resposta muda o artefato numa proxima rodada; a ausencia dela nao apaga o que foi
   medido.

## Antipadrao recorrente e convencao

Padrao que parece tecnicamente inadequado e que o sistema usa de forma recorrente **e uma
convencao do sistema** e se registra como tal. O agente precisa saber que ele existe, porque
um codigo novo que ignore a convencao destoa de tudo em volta, e porque o proximo que abrir o
arquivo vai encontrar aquilo.

Registre pelo que se observa, sem juizo de qualidade e sem recomendacao de correcao. "O
sistema concentra acesso a dados em procedures, e as unidades chamam procedure como mecanismo
padrao de persistencia" e a forma correta, valha o que valer a decisao original.

Duas coisas continuam fora:

- **Recomendar refatoracao.** Nao e o objetivo da rodada e nao e o escopo de escrita da skill.
- **Transformar em regra o que o codigo nao sustenta.** Convencao se mede como qualquer outro
  padrao.

Quando a pratica recorrente tiver consequencia de seguranca ou de corretude que o desenvolvedor
possa desconhecer, registre o fato observado com a contagem e leve uma linha ao relatorio, sem
transformar o artefato em parecer tecnico.

## Codificacao e protocolo de busca

O censo devolve, por extensao, a codificacao padrao, a cobertura entre os arquivos decisivos,
as excecoes nomeadas com caminho de exemplo, a terminacao de linha dominante e o comando de
busca de cada grupo. Sufixo nao decide codificacao: quem decide sao os bytes.

Arquivo ASCII e arquivo vazio nao sao excecao. Eles cabem em qualquer codificacao e sao
contados a parte: a cobertura se mede sobre os arquivos que revelam codificacao.

Antes de qualquer contagem, fixe o protocolo de busca e envie o mesmo texto a todos os papeis.
Um comando canonico por grupo, com o escopo onde vale.

**Cuidado com a ordem das opcoes de `grep`.** Vale a ultima opcao passada, e `-I` anula `-a`:
`grep -aRIl` ignora arquivo tratado como binario e devolve contagem muito menor que
`grep -RIal`, sem avisar. Em codificacao mista, prefira a forma explicita
`grep -rl --binary-files=text`, que nao depende de ordem. O censo ja imprime a forma segura de
cada familia presente.

Contagem sem comando registrado nao e reproduzivel, e contagem nao reproduzivel nao sustenta
artefato. Quando dois papeis chegam a numeros diferentes, a primeira pergunta e se usaram o
mesmo comando no mesmo escopo. Se nao usaram, a divergencia e da ferramenta e a rodada e
invalida. Se usaram, a divergencia e o achado.

## Fato de censo e sinais de dependencia

Sao deterministicos, saem do script e **nao passam por busca por contraexemplo**: linguagens
presentes, volume por diretorio, codificacao padrao por extensao, ausencia de integracao
continua, ausencia de diretorio de teste, componentes instanciados, includes e imports mais
frequentes, driver e string de conexao, bibliotecas cliente com versao no nome, extensoes
proprietarias.

Eles alimentam as secoes de contexto e de dependencias tecnicas do `AGENTS.md`, que sao
inventario em topicos. Reprova los por falta de contraexemplo e erro de aplicacao da barra, e
foi o que deixou uma secao inteira de dependencias vazia em uma rodada real.

Manifesto nao e a unica forma de declarar dependencia, e em sistema legado quase nunca e a
usada. Componente instanciado por nome, include repetido em centenas de arquivos e biblioteca
com versao no nome do arquivo sao declaracoes de dependencia tao boas quanto uma linha de
manifesto, e sao as unicas que existem em muita codebase.

Fora dos dois esta a inferencia sobre execucao: runtime, agendador, ambiente e infraestrutura
nao se provam por censo nem por busca. "O repositorio contem um projeto que declara um
executavel" e fato de censo; "esse executavel roda como tarefa agendada" e inferencia, e nao
entra.

## Alcance

O alcance e a parte do repositorio onde a afirmacao foi comprovada, nunca a parte onde ela
parece razoavel.

Diferenca entre subsistemas e frequentemente legitima: sistemas nascem em epocas distintas e
absorvem decisoes distintas. Uniformizar por estetica destroi informacao. Registre dois
achados com alcances separados em vez de um achado errado com alcance amplo.

Achado com alcance de modulo unico raramente pertence a `AGENTS.md`. Ele pertence a uma
reference, muitas vezes a uma reference sobre aquele modulo, ou a uma skill daquele fluxo.

## Busca por contraexemplo

Obrigatoria para toda afirmacao que vira regra ou guardrail, e o unico mecanismo que impede
promover excecao a convencao. Depois de formular a afirmacao, tente derruba la:

- procure a operacao inversa, a ausencia do elemento afirmado, a variacao de nome;
- olhe o codigo mais antigo e o mais novo dentro do alcance, nao apenas o que a busca trouxe
  primeiro;
- olhe o caminho de erro, nao so o caminho feliz;
- verifique se a busca cobriu todo o alcance declarado ou apenas o diretorio onde a hipotese
  nasceu.

Registre o que foi buscado, mesmo quando nao encontrou nada. "Procurei X em todo o alcance e
encontrei zero divergencias" e evidencia forte. "Nao procurei" e achado incompleto.

Ferramenta de busca falha em silencio com mais frequencia do que parece: arquivo tratado como
binario por encoding, padrao que nao casa por variacao de espacamento, diretorio ignorado por
configuracao, opcao de linha de comando anulada por outra. Conferir o total de arquivos
examinados custa pouco e evita concluir ausencia por engano.

## Hipotese derrubada vira afirmacao negativa

Hipotese derrubada nao termina no relatorio. Ela e reformulada na forma negativa ou condicional
e reentra na barra como afirmacao propria:

| Hipotese derrubada | Afirmacao que reentra |
|---|---|
| "toda pagina que inclui o arquivo de acesso esta protegida" | "incluir o arquivo de acesso nao protege a pagina; confirme a chamada ativa no proprio arquivo" |
| "os dois caminhos do mesmo objeto sao mantidos em sincronia" | "os dois caminhos divergem; compare os dois antes de alterar qualquer um" |

A classificacao e recalculada sobre a afirmacao nova, e costuma subir: os casos que derrubavam
a hipotese sao as ocorrencias que sustentam a negacao.

Conhecimento negativo tem alto retorno, e tem um limite: ele diz o que nao assumir e nao ensina
a fazer. Uma rodada cujos unicos artefatos sao negacoes cobriu hipoteses e nao cobriu o
sistema. Para cada negacao consolidada, verifique se o padrao positivo correspondente foi
procurado: se incluir o arquivo nao protege a pagina, **o que protege**, e em quantos casos?

## Nao objetivos

Declarados no preflight e excluidos da derivacao de frentes, por decisao e nao por falta de
evidencia:

- fluxo de branches e promocao entre ambientes;
- procedimento de deploy e publicacao;
- agendamento de tarefa, servidor, runtime e ambiente de execucao;
- processo de time que o codigo nao registra.

Nada disso esta no codigo, muda sem aviso e nao muda o que o agente escreve. Continua dentro o
que esta versionado: script de migracao, arquivo de integracao continua, gancho de
versionamento, script de release, modelo de mudanca.

## Sinais de que o achado nao vale a pena

- o agente descobriria abrindo o arquivo que vai editar, sem contexto adicional;
- e verdade sobre a linguagem ou o framework, nao sobre este sistema;
- descreve estrutura de diretorio sem consequencia pratica;
- e preferencia estetica sem efeito sobre coerencia, corretude, seguranca ou entrega;
- ja esta na stack, em qualquer destino;
- so seria util para uma tarefa que ninguem repete.

O teste final e sempre o mesmo: **isto muda o que alguem escreve na proxima implementacao?**
Se nao muda, nao entra, em nenhum destino.
