# Prompts de exemplo

## Sumário

- [Introdução](#introdução)
- [Como escrever um bom prompt](#como-escrever-um-bom-prompt)
- [Pré-requisitos de Automação de Navegador (Playwright e Selenium)](#pré-requisitos-de-automação-de-navegador-playwright-e-selenium)
- [Correção de bugs](#correção-de-bugs)
	- [Investigar e corrigir bug](#investigar-e-corrigir-bug)
	- [Investigar e corrigir bug com automação de navegador](#investigar-e-corrigir-bug-com-automação-de-navegador)
- [Fluxo SpecKit para funcionalidade nova](#fluxo-speckit-para-funcionalidade-nova)
	- [Fluxo padrão](#fluxo-padrão)
	- [Fluxo alternativo, com passos opcionais](#fluxo-alternativo-com-passos-opcionais)
- [Revisão de segurança](#revisão-de-segurança)
	- [Revisar alterações](#revisar-alterações)
	- [Revisar uma pasta ou um arquivo](#revisar-uma-pasta-ou-um-arquivo)
	- [Saber o que precisa de cuidado antes de implementar](#saber-o-que-precisa-de-cuidado-antes-de-implementar)
	- [Descobrir o que dá para verificar](#descobrir-o-que-dá-para-verificar)
	- [Variações de saída da revisão](#variações-de-saída-da-revisão)
		- [Gerar revisão no formato PDF](#gerar-revisão-no-formato-pdf)
		- [Gerar as issues ou tarefas técnicas](#gerar-as-issues-ou-tarefas-técnicas)
		- [Para uma revisão já entregue](#para-uma-revisão-já-entregue)
	- [Depois da revisão: marcar ou planejar a correção](#depois-da-revisão-marcar-ou-planejar-a-correção)
- [stack-ai-build-project-context](#stack-ai-build-project-context)
	- [Enriquecer uma stack recém instalada](#enriquecer-uma-stack-recém-instalada)
	- [Atualizar a stack depois de uma mudança grande](#atualizar-a-stack-depois-de-uma-mudança-grande)
- [configuracao-de-testes-unitarios](#configuracao-de-testes-unitarios)
	- [Projeto sem testes](#projeto-sem-testes)
	- [Projeto que já tem testes](#projeto-que-já-tem-testes)
	- [Projeto que roda em Docker](#projeto-que-roda-em-docker)
	- [Projeto sem Docker](#projeto-sem-docker)
	- [Analisar o repositório e planejar os testes unitários](#analisar-o-repositório-e-planejar-os-testes-unitários)
- [manual-creator-scan-codebase](#manual-creator-scan-codebase)
	- [Preparar o adaptador do manual](#preparar-o-adaptador-do-manual)
	- [Criar um manual de uso](#criar-um-manual-de-uso)
	- [Atualizar um manual de uso](#atualizar-um-manual-de-uso)
	- [Verificar um manual de uso sem escrita](#verificar-um-manual-de-uso-sem-escrita)
- [dicionario-dados-db-scan-codebase-docs](#dicionario-dados-db-scan-codebase-docs)
	- [Criar o adaptador](#criar-o-adaptador)
	- [Criar o dicionário de dados](#criar-o-dicionário-de-dados)
	- [Atualizar o dicionário](#atualizar-o-dicionário)
	- [Revisar a qualidade do dicionário](#revisar-a-qualidade-do-dicionário)
	- [Gerar prints de tela como insumo complementar](#gerar-prints-de-tela-como-insumo-complementar)
- [gauntlet-loop-forge](#gauntlet-loop-forge)
	- [Criar o prompt a partir de uma ideia](#criar-o-prompt-a-partir-de-uma-ideia)
	- [Criar o prompt a partir de uma especificação](#criar-o-prompt-a-partir-de-uma-especificação)
	- [Otimizar um prompt que já existe](#otimizar-um-prompt-que-já-existe)
	- [Executar o prompt criado](#executar-o-prompt-criado)
- [Documentos para agentes de IA](#documentos-para-agentes-de-ia)
	- [Revisar o AGENTS.md do projeto](#revisar-o-agentsmd-do-projeto)
	- [Revisar a escrita de uma skill](#revisar-a-escrita-de-uma-skill)
	- [Revisar um documento que o AGENTS.md aponta](#revisar-um-documento-que-o-agentsmd-aponta)
- [Modos auxiliares](#modos-auxiliares)
	- [Interrogar um plano ou um pedido utilizando o grill-me](#interrogar-um-plano-ou-um-pedido-utilizando-o-grill-me)
	- [Comprimir a resposta utilizando o caveman](#comprimir-a-resposta-utilizando-o-caveman)

---

## Introdução

Este arquivo reúne prompts prontos para as skills que a stack instala. Escolha o que corresponde à sua necessidade, preencha os campos indicados e copie o bloco inteiro para a conversa com o agente.

O projeto pode ter um segundo arquivo de prompts, com templates específicos do sistema dele. Os dois se complementam: aqui estão os prompts que valem em qualquer projeto.

A seção "Modos auxiliares", no fim do arquivo, não tem prompt de demanda. Ela reúne dois modos opcionais que se combinam com qualquer prompt anterior, para comprimir a resposta ou para interrogar um plano ou um pedido antes de implementar.

---

## Como escrever um bom prompt

O `AGENTS.md` do projeto é carregado em toda sessão e concentra as regras dele, então os prompts abaixo não repetem o que já está lá. Repita uma orientação do `AGENTS.md` dentro de um prompt só quando quiser reforçar um ponto muito crítico, para reduzir o risco de o agente falhar justamente nesse ponto.

O `AGENTS.md` e as skills definem as regras de execução. Os prompts informam o objetivo, o alvo e as escolhas específicas de cada pedido.

**Parâmetros.** Nos blocos para copiar, os campos entre `<` e `>` são informações que você deve preencher. Se um campo trouxer uma condição, como "se souber", "se houver", "se existir", "se não for" ou "opcional", e ela não valer para o seu caso, apague a linha inteira. Substitua os demais campos antes de enviar e nunca envie um prompt com texto entre `<` e `>` ainda dentro dele. Quando faltar uma informação necessária, descreva o que você sabe; o agente pode investigar ou pedir o dado que falta. Datas, identificadores e nomes de arquivos de saída são gerados pelo agente.

Estes são os campos que aparecem em mais de um prompt:

| Campo | O que preencher | Exemplo |
|---|---|---|
| `<sistema ou módulo>` | O nome do sistema inteiro, de um módulo ou de uma base de dados | `pedidos` |
| `<caminho da pasta ou do arquivo>` | Qualquer pasta ou arquivo do repositório | `src/pedidos/` |
| `<caminho do arquivo>` | Um arquivo só | `src/pedidos/listagem.py` |

Os demais campos aparecem em um prompt só e o próprio texto entre `<` e `>` diz o que preencher.

**Adapte à vontade.** Nenhum prompt precisa ser enviado igual ao template. Apague a linha que não se aplica ao seu caso e mande só o que você tem de fato: quanto mais informação e validação você der, mais assertivo o agente tende a ser, mas um prompt enxuto também funciona.

**Trate o que você informa como pista.** O que você escreve no prompt orienta a investigação, não substitui a evidência. Apresente suspeitas como hipóteses e peça ao agente para confirmar cada item contra o código ou a fonte estrutural e avisar quando algo divergir.

**Nunca cole segredo.** Antes de enviar mensagem de erro, print de tela ou qualquer evidência bruta, oculte estes valores reais, independentemente do ambiente:

- senha ou qualquer outra credencial
- dado pessoal (CPF, nome completo, e-mail, endereço)
- segredo, que são as senhas usadas pelos sistemas para conversarem entre si (token, chave de API, credencial na configuração de conexão)

Troque cada valor por um texto genérico, mantendo claro o que está errado. Ambientes de teste e desenvolvimento também podem conter credenciais válidas ou dados reais.

**Regra de negócio não documentada.** Se a regra não estiver escrita em lugar nenhum do repositório, escreva a regra no próprio prompt.

**Vocabulário.** Onde os prompts pedem "a raiz do repositório", leia a pasta principal do projeto, aquela que contém todas as outras.

**A sessão continua.** Todo prompt daqui é só o ponto de partida da sessão, não uma interação única. Depois do resultado, continue na mesma janela para ajustar, aprofundar ou corrigir o que vier. Se quiser guardar o conteúdo da sessão (achados, relatório, decisão), peça ao agente para salvar em um arquivo Markdown (`.md`) na pasta `specs/`. Ela fica fora do versionamento quando o `.gitignore` do projeto a ignora, como faz a `stack-ai-init` quando o projeto ainda não tem essa pasta.

---

## Pré-requisitos de Automação de Navegador (Playwright e Selenium)

Consulte esta seção antes de usar qualquer prompt deste arquivo que peça Playwright ou Selenium.

Qualquer prompt com automação de navegador exige um ambiente de teste do sistema no ar, local ou remoto. Se o alvo for um módulo específico, confirme que ele está ativo nesse ambiente antes de usar o prompt: veja o arquivo de configuração em que o sistema lista os módulos ativos. Se o módulo não estiver ativo, peça ao agente que o ative (edição local, sem comitar) antes de continuar; não deixe o agente editar esse arquivo sem a sua confirmação.

**Playwright:** não vem configurado na stack. Você precisa trazer a sua própria automação de navegador, por exemplo a imagem Docker `mcr.microsoft.com/playwright` na tag que você for usar. Confirme que a ferramenta de IA da sua sessão consegue executá-la. Qualquer ajuste de configuração feito só para habilitá-la é local e não deve ser comitado.

---

## Correção de bugs

### Investigar e corrigir bug

Use este prompt como ponto de partida para qualquer bug. Quanto mais insumos você informar (print, mensagem de erro, chamado, suspeita de causa), menor a investigação exploratória e mais assertiva a correção.

**Prompt:**

```text
Objetivo: investigar o defeito abaixo e apresentar o plano de correção. Depois do meu OK, implementar a correção.

Defeito: <descreva o defeito em uma frase>.
Local: <caminho da pasta ou do arquivo>.

Fora de escopo: refatorar código adjacente, renomear método ou arquivo, alterar comportamento de outras funcionalidades da mesma tela.

Fatos verificados:
- comportamento atual: <descreva o erro com os valores necessários para reproduzi-lo, ocultando segredos e dados pessoais>
- comportamento esperado: <descreva o correto>
- frequência: <sempre reproduz, intermitente, só em certas condições>
- ambiente onde ocorre: <desenvolvimento, homologação, produção>
- perfil ou usuário afetado: <perfil, permissão ou tipo de usuário>
- mensagem de erro: <cole aqui, se existir, com segredo e dado pessoal mascarados>
- print de tela: <anexe o arquivo de imagem, se existir>

Como reproduzir:
1. <passo 1>
2. <passo 2>
3. <passo 3>

Hipóteses: investigue estas pistas no código e informe quais foram confirmadas, refutadas ou permaneceram sem confirmação.
- desde quando ocorre: <sempre ocorreu, ou começou após uma mudança específica, ex.: "não ocorria na versão 2.1, passou a ocorrer na 2.2">
- arquivos ou classes suspeitas: <se já tiver uma suspeita, ex.: "deve estar no repositório, no método que monta o filtro">
- mudança recente relacionada: <commit, PR ou release que pode ter causado, se souber, ex.: "a mesma atualização que trocou o componente de data">
- relato do usuário: <cole aqui o texto que ele reportou, ex.: "a listagem está trazendo pedidos de fevereiro mesmo eu filtrando só janeiro">

Critério de aceite:
1. Sucesso: <entrada concreta e resultado esperado após a correção>
2. Validação: <condição limite e comportamento esperado>
3. Regressão: <o que já funciona nessa tela ou fluxo e precisa continuar funcionando>

Impacto: <negócio, segurança, usabilidade, regressão>

Protocolo de execução:
1. Investigue o defeito. Se confirmar a causa, apresente as evidências (arquivo e linha) e o plano de correção; caso contrário, informe o que falta verificar. Aguarde meu OK antes de editar qualquer coisa.
2. Só então implemente e valide.
```

**Exemplo preenchido, filtro de período que ignora o intervalo:** use este padrão de preenchimento para qualquer outro prompt deste arquivo. O exemplo descreve um defeito hipotético em uma listagem de pedidos.

```text
Objetivo: investigar o defeito abaixo e apresentar o plano de correção. Depois do meu OK, implementar a correção.

Defeito: o filtro de período da listagem de pedidos ignora o intervalo e retorna somente os registros com data exatamente igual à informada.
Local: src/pedidos/listagem/.

Fora de escopo: refatorar código adjacente, renomear método ou arquivo, alterar comportamento dos demais filtros da tela.

Fatos verificados:
- comportamento atual: com Início 01/01/2026 00:00 e Fim 31/01/2026 23:59, a listagem volta vazia, mesmo havendo pedidos cadastrados dentro desse intervalo
- comportamento esperado: a listagem deve mostrar todos os pedidos do intervalo informado, respeitando início e fim
- frequência: sempre reproduz quando os dois campos de data são preenchidos
- ambiente onde ocorre: homologação
- perfil ou usuário afetado: qualquer perfil com acesso à listagem de pedidos
- mensagem de erro: nenhuma, o filtro só não funciona
- print de tela: enviado junto com esta mensagem

Como reproduzir:
1. Abrir a listagem de pedidos
2. Preencher Início 01/01/2026 00:00 e Fim 31/01/2026 23:59
3. Clicar em pesquisar

Hipóteses: investigue estas pistas no código e informe quais foram confirmadas, refutadas ou permaneceram sem confirmação.
- desde quando ocorre: sempre ocorreu, ninguém do time lembra do filtro funcionando por intervalo
- arquivos ou classes suspeitas: os campos de data inicial e final parecem ser aplicados como igualdade na consulta, e não como faixa de datas
- relato do usuário: "filtrei o mês de janeiro e a tela não trouxe nenhum pedido, mas eles aparecem na lista sem filtro"

Critério de aceite:
1. Sucesso: com Início 01/01/2026 00:00 e Fim 31/01/2026 23:59, a listagem mostra os pedidos do intervalo, incluindo o que começa em 01/01/2026 e o que termina em 31/01/2026
2. Validação: com apenas um dos dois campos preenchido, o filtro aplica só o limite informado e não lança erro
3. Regressão: os demais filtros e a ordenação da listagem continuam funcionando isolados e combinados com o filtro de período

Impacto: usabilidade; o relatório mensal é montado a partir dessa listagem.

Protocolo de execução:
1. Investigue o defeito. Se confirmar a causa, apresente as evidências (arquivo e linha) e o plano de correção; caso contrário, informe o que falta verificar. Aguarde meu OK antes de editar qualquer coisa.
2. Só então implemente e valide.
```

### Investigar e corrigir bug com automação de navegador

Use esta variação quando quiser reproduzir o bug na tela e validar a correção visualmente, com os prints de antes e de depois como evidência de cada critério de aceite.

Pré-requisito: veja "Pré-requisitos de Automação de Navegador (Playwright e Selenium)", no início deste arquivo.

**Prompt:**

```text
Objetivo: investigar o defeito abaixo e apresentar o plano de correção. Depois do meu OK, implementar a correção. Use <ferramenta de automação de navegador disponível, ex.: Playwright ou Selenium> para reproduzir o bug e validar a correção visualmente.

Defeito: <descreva o defeito em uma frase>.
Local: <caminho da pasta ou do arquivo>.

Fora de escopo: refatorar código adjacente, comitar qualquer ajuste de configuração feito só para habilitar a automação de navegador.

Fatos verificados:
- comportamento atual: <descreva o erro com os valores necessários para reproduzi-lo, ocultando segredos e dados pessoais>
- comportamento esperado: <descreva o correto>
- frequência: <sempre reproduz, intermitente, só em certas condições>
- mensagem de erro: <cole aqui, se existir, com segredo e dado pessoal mascarados>
- print de tela anterior: <anexe o arquivo de imagem, se existir>

Hipóteses: investigue estas pistas no código e informe quais foram confirmadas, refutadas ou permaneceram sem confirmação.
- desde quando ocorre: <sempre ocorreu, ou começou após uma mudança específica>
- arquivos ou classes suspeitas: <se já tiver uma suspeita>
- relato do usuário: <cole aqui o texto que ele reportou>

Critério de aceite:
1. Sucesso: <entrada concreta e resultado esperado, verificável na tela>
2. Validação: <condição limite e comportamento esperado>
3. Regressão: <o que já funciona nesse fluxo e precisa continuar funcionando>

Acesso ao ambiente de teste:
- endereço: <URL do ambiente de teste>
- autenticação: leia a credencial de teste em <variável de ambiente ou arquivo local não versionado>; se precisar de outro perfil, pare e me peça para configurar a credencial dele nesse local, sem recebê-la pela conversa

Passos:
1. Abra a seguinte tela ou fluxo: <tela ou fluxo onde o bug ocorre>. Se a reprodução exigir um estado específico na base (registro, status, permissão), prepare esse estado você mesmo antes de tentar reproduzir o problema.
2. Capture um print do estado com o bug, antes da correção, e salve em <pasta de destino>.
3. Apresente a causa, se confirmada, com evidências (arquivo e linha) e o plano de correção. Se a investigação for inconclusiva, informe o que falta verificar. Aguarde meu OK antes de corrigir.
4. Corrija a causa raiz no local informado acima.
5. Repita a navegação, capture um novo print de cada critério de aceite confirmado e salve os arquivos na mesma pasta.

Referencie os prints de antes e depois como evidência de cada critério de aceite verificado na tela.
```

---

## Fluxo SpecKit para funcionalidade nova

Use o fluxo SpecKit sempre que a demanda criar algo que ainda não existe: nova funcionalidade, nova entidade completa, nova tela ou nova operação de API. Para uma mudança pontual em algo que já existe, converse direto com o agente.

Se você tem a tarefa na cabeça e não sabe como escrever esse pedido, interrogue a ideia com o [grill-me](#interrogar-um-plano-ou-um-pedido-utilizando-o-grill-me) antes de a fase rodar, e peça no fim da entrevista o texto do pedido.

Quanto mais detalhado o pedido em `/speckit-specify`, melhor a especificação gerada. Descreva contexto, comportamento esperado e critério de aceite em linguagem de negócio; deixe stack técnica, classe e padrão de implementação para o `/speckit-plan`, que é a fase certa para essa decisão.

Os exemplos abaixo usam como demanda "um relatório de pedidos agrupados por categoria, com filtro de período", no módulo `pedidos`. Troque pelo objetivo real antes de enviar.

### Fluxo padrão

Especificar, planejar, decompor em tarefas e implementar, nessa ordem.

**1. Especificar:**

```text
/speckit-specify

Quero <descreva a funcionalidade nova em uma frase>.

Sistema ou módulo: <sistema ou módulo>.

Contexto:
- quem usa: <perfil, papel ou permissão>
- comportamento esperado: <resultado esperado>
- fora de escopo: <o que essa funcionalidade não faz>

Critério de aceite inicial:
1. Sucesso: <entrada concreta e resultado esperado>
2. Validação: <condição limite e comportamento esperado>

Fatos verificados:
- regra de negócio confirmada: <se houver, ex.: "só usuários com perfil Administrador veem pedidos cancelados">
- tela ou fluxo semelhante que serve de referência: <se houver, ex.: "a listagem de categorias já resolve a leitura e a exibição">

Hipóteses: investigue estas pistas no código e informe quais foram confirmadas, refutadas ou permaneceram sem confirmação.
- impacto suposto em permissão, menu ou estrutura de banco: <se souber, ex.: "deve exigir permissão e item de menu novos, sem mudança de estrutura no banco">
```

**Exemplo preenchido, relatório de pedidos por categoria:**

```text
/speckit-specify

Quero um relatório de pedidos agrupados por categoria, com filtro de período.

Sistema ou módulo: pedidos.

Contexto:
- quem usa: usuário com o perfil Administrador, que já acessa os cadastros do módulo
- comportamento esperado: a tela mostra a quantidade de pedidos registrados por categoria dentro do período informado, com link para abrir cada pedido
- fora de escopo: alterar o cadastro de pedidos, criar categoria nova e exportar o resultado para outro formato

Critério de aceite inicial:
1. Sucesso: com o período de 01/01/2026 a 31/01/2026, a tela lista cada categoria com a contagem de pedidos registrados no intervalo
2. Validação: com um período sem pedido registrado, a tela informa que não há resultado e não apresenta erro

Fatos verificados:
- regra de negócio confirmada: um pedido pode ter mais de uma categoria associada e deve ser contado uma vez em cada categoria
- tela ou fluxo semelhante que serve de referência: a listagem de categorias já resolve a leitura e a exibição das categorias

Hipóteses: investigue estas pistas no código e informe quais foram confirmadas, refutadas ou permaneceram sem confirmação.
- impacto suposto em permissão, menu ou estrutura de banco: tela nova deve exigir permissão e item de menu novos, sem mudança de estrutura no banco
```

**2. Planejar:**

```text
/speckit-plan

Reaproveite tela, componente ou padrão semelhante já existente antes de propor algo novo.

Sistema ou módulo: <sistema ou módulo>.
Parâmetros novos: <parâmetros da funcionalidade>.

No plano, declare explicitamente os impactos em <as dimensões sensíveis do seu projeto, ex.: "estrutura de banco, permissões e scripts de instalação">. Se algum desses impactos existir, marque-o como ponto de parada para minha decisão antes da implementação.
```

**Exemplo preenchido, plano do relatório de pedidos por categoria:**

```text
/speckit-plan

Reaproveite tela, componente ou padrão semelhante já existente antes de propor algo novo.

Sistema ou módulo: pedidos.
Parâmetros novos: data inicial, data final e identificador de categoria do relatório.

No plano, declare explicitamente os impactos em estrutura de banco, permissões e scripts de instalação. Se algum desses impactos existir, marque-o como ponto de parada para minha decisão antes da implementação.
```

**3. Decompor em tarefas:** a decomposição usa a spec e o plano já aprovados nas fases anteriores. Não precisa de argumento adicional, então o bloco abaixo já é o texto final para copiar.

```text
/speckit-tasks
```

**4. Implementar:** executa as tarefas geradas. Não precisa de argumento adicional, então o bloco abaixo já é o texto final para copiar.

```text
/speckit-implement
```

### Fluxo alternativo, com passos opcionais

Mesmo fluxo, com três passos opcionais inseridos: clarificar dúvidas antes de planejar, gerar um checklist de requisitos sensíveis antes de decompor, e analisar consistência antes de implementar.

**1. Especificar:** igual ao fluxo padrão.

**2. Clarificar:** use quando a spec tiver ambiguidade. A skill faz até 5 perguntas objetivas e grava as respostas na própria spec. Não precisa de argumento adicional, então o bloco abaixo já é o texto final para copiar.

```text
/speckit-clarify
```

**3. Planejar:** igual ao fluxo padrão.

**4. Gerar checklist:** use quando o domínio for sensível, como permissão, auditoria ou modelagem de dados, e você quiser validar a qualidade dos requisitos antes de decompor em tarefas.

```text
/speckit-checklist

Gere um checklist para validar os requisitos de <dimensão sensível, ex.: permissão e auditoria> antes de implementar.

Funcionalidade: <nome da funcionalidade>.
```

**Exemplo preenchido, checklist de permissão e auditoria:**

```text
/speckit-checklist

Gere um checklist para validar os requisitos de permissão e auditoria antes de implementar.

Funcionalidade: relatório de pedidos por categoria.
```

**5. Decompor em tarefas:** igual ao fluxo padrão.

**6. Analisar:** checa a consistência entre spec, plano e tarefas antes de implementar. Não precisa de argumento adicional, então o bloco abaixo já é o texto final para copiar.

```text
/speckit-analyze
```

**7. Implementar:** igual ao fluxo padrão.

---

## Revisão de segurança

Use estes prompts quando quiser uma revisão de segurança sem conhecer o assunto. Você diz o que quer olhar, em uma frase, e a skill `owasp-playbook` escolhe os procedimentos OWASP pelo que existe no escopo, roda os auditores que o projeto registrou na ponte, traduz o resultado pelo guia de segurança e devolve um resumo em linguagem simples antes da tabela técnica. Não é preciso citar procedimento, arquivo ou regra.

A skill só lê. Ela nunca altera código nem abre tarefa ou issue, e o relatório termina dizendo como pedir a correção, que é outra tarefa. Todo achado vem marcado como "confirmado" ou "precisa de conferência": o segundo é uma pista que o agente não conseguiu comprovar de ponta a ponta, não uma vulnerabilidade.

Sem escopo na frase, a skill olha as mudanças não commitadas; se não houver, compara a branch atual com a principal. O relatório abre dizendo o que foi olhado.

### Revisar alterações

Sempre que fizer alterações antes de realizar um merge para branches de ambiente utilize estes exemplos abaixo para garantir a qualidade da entrega.

| O que você quer revisar | O que escrever no lugar de `<o que revisar>` |
|---|---|
| Uma branch antes do merge | `da branch <nome da branch>, comparada com <branch ou commit de referência>` |
| Um commit | `do commit <identificador do commit>` |
| Mais de um commit | `dos commits <identificador 1> e <identificador 2>`, etc... |
| As mudanças ainda sem commit | `das alterações sem commit` |

**Prompt:**

```text
/owasp-playbook revise a segurança <o que revisar>
```

Se quiser dar contexto ao agente, acrescente as duas linhas abaixo ao final do prompt. Elas são opcionais: apague a que você não tiver como preencher.

```text
Escopo da mudança: <resumo do que foi alterado>.
Pontos de atenção conhecidos: <o que você já sabe que merece olhar>.
```

**Exemplo preenchido, avaliar o diff da branch antes do merge:**

```text
/owasp-playbook revise a segurança da branch origin/filtro-periodo-pedidos, comparada com origin/main
```

**Exemplo preenchido, um commit com o contexto opcional no final:**

```text
/owasp-playbook revise a segurança do commit 8bf4ab729d

Escopo da mudança: o cadastro de pedidos passou a exigir categoria ou fornecedor vinculado.
Pontos de atenção conhecidos: a validação entrou na regra de negócio e na tela de cadastro, e quero saber se ela continua valendo em chamada direta pela URL.
```

### Revisar uma pasta ou um arquivo

Use para levantar o estado atual do código que já está no repositório, incluindo o passivo antigo. Pasta grande demora mais, e a skill avisa antes de seguir.

**Prompt para uma pasta:**

```text
/owasp-playbook revise a segurança da pasta <caminho da pasta ou do arquivo>
```

**Prompt para um arquivo:**

```text
/owasp-playbook revise a segurança do arquivo <caminho do arquivo>
```

### Saber o que precisa de cuidado antes de implementar

Use antes de escrever algo sensível: entrada de usuário, upload, permissão, integração externa, log. A resposta vem em linguagem simples, com o requisito ASVS entre parênteses.

**Prompt:**

```text
/owasp-playbook vou implementar <a funcionalidade>; o que preciso cuidar em segurança?
```

### Descobrir o que dá para verificar

Use uma vez, para conhecer as verificações disponíveis e quais têm alvo neste repositório. Não executa nenhuma.

**Prompt:**

```text
/owasp-playbook o que dá para verificar de segurança neste repositório?
```

### Variações de saída da revisão

Peça uma variação quando precisar de um arquivo para compartilhar o resultado ou para alimentar o acompanhamento do time. A `owasp-playbook` consolida os achados no JSON que o gerador lê. São duas saídas:

- **PDF**: documento com capa, resumo por severidade, gráficos, tabela de achados e recomendações priorizadas.
- **Texto de issues ou tarefas técnicas**: arquivo Markdown com um texto pronto por achado, para você colar na ferramenta que o time usa. Gerar esse texto não abre issue nem tarefa em lugar nenhum.

Cada trecho abaixo é autossuficiente: cole no final de qualquer prompt de revisão das subseções anteriores, separado por uma linha em branco. Você preenche só o alvo da revisão; o agente cuida de identificadores, datas e nomes de arquivo.

#### Gerar revisão no formato PDF

Adicione este trecho ao final do prompt de revisão.

```text
Gere relatório em PDF desta revisão técnica com todos os achados.

Consolide antes os achados em `specs/relatorios/achados-<slug>-<AAAA-MM-DD-HHhMM>.json`, com mesmo carimbo gravado no campo `gerado_em`. Os números do relatório sairão desse arquivo preliminar: gráficos e tabela leem dele e nada é recalculado durante a geração da formatação final do relatório. Cada achado leva `id`, `dimensão`, `categoria`, `subtipo`, `severidade` (`critica`, `alta`, `media` ou `baixa`), `estado` (`CONFIRMED` ou `HYPOTHESIS`), `arquivo`, `linha_inicio`, `linha_fim`, `descricao`, `evidencia`, `impacto`, `sugestao_correcao`, `criterios_aceite`, `acionavel` e `grupo_issue`.
	- `categoria` é o nome da vulnerabilidade na nomenclatura internacional, em inglês, escolhido desta lista fechada: SQL Injection; Cross-Site Scripting (XSS); Broken Access Control; Broken Authentication; Sensitive Data Exposure; Security Misconfiguration; Vulnerable and Outdated Components; Information Disclosure; Unrestricted File Upload; Path Traversal; XML External Entity (XXE); Server-Side Request Forgery (SSRF); Cross-Site Request Forgery (CSRF); Command Injection; HTML Injection (Email); Insecure Deserialization; Open Redirect; Weak Cryptography; Insufficient Logging and Monitoring; Business Logic Flaw. Não invente categoria fora da lista; se nenhuma couber, use a mais próxima e explique no `subtipo`.
	- `subtipo` é a forma específica encontrada no código, em português (por exemplo: "comando de escrita sem parâmetro vinculado, exploração por consulta empilhada ou cega"; "consulta de leitura com resultado na tela"; "refletido em atributo HTML"; "armazenado"; "IDOR: identificador do registro vindo do cliente sem conferir o dono"; "credencial na URL").
	- O JSON traz também a lista `categorias`, com quantidade por categoria e por subtipo e os ids de cada um, que alimenta a tabela e o gráfico por categoria.
	- Mascare segredo real na evidência; nunca reproduza um segredo válido em texto claro. Mascare também login de pessoa física, CPF, e-mail pessoal e nome de servidor de banco. Achado que é hipótese continua marcado como hipótese.

Salve o PDF em `specs/relatorios/relatorio-<slug>-<AAAA-MM-DD-HHhMM>.pdf`, reaproveitando o carimbo do JSON. Conteúdo:
	- **Capa** com:
		- Título: **Revisão Técnica <"Completa"/escopo/"por Dimensões Específicas"> - <alvo da revisão técnica>**
		- Subtítulo em itálico: "Relatório de Revisão Estática e de Conformidade Técnica"
		- Tabela com cabeçalho na primeira coluna, com as seguintes linhas:
			- "Data da Execução": a data e hora da execução da revisão técnica em BRT;
			- "Alvo da Revisão Técnica": <alvo da revisão técnica> igual ao do título - **<se aplicável, Versão do alvo no formato vN.N.N>** (onde o projeto registra a versão do alvo, quando houver);
			- "Commit de Referência": identificação do commit de referência conforme o alvo;
			- "Resultado Global": da revisão técnica executada como um todo.
		- "### Dimensões da Revisão Técnica": tabela com as colunas "Dimensão" transcrevendo o nome de cada dimensão abrangida pela revisão técnica executada; "Estado"; e "Cobertura" da dimensão.
		- "### Escopo da Revisão Técnica": informando a lista do que está "Incluído":`#059669` e a lista do que está "Excluído":`#B91C1C` do escopo da revisão técnica, em termos de pastas, arquivos, camadas, situações do ambiente computacional e outras circunstâncias do código, da execução ou do ambiente que contextualiza sobre o que e como foi executada a revisão técnica.
		- "### Nota Metodológica".
		- "### Como ler este relatório": tabela com as colunas "Termo" e "Significado", explicando em linguagem simples cada código usado no relatório: o identificador do achado; categoria e subtipo; os códigos de dimensão do guia de segurança (S01 a S19); CWE; OWASP Top 10; os níveis de severidade; os estados do achado (CONFIRMED e HYPOTHESIS); as prioridades (P1, P2, P3, P4...); os estados de verificação (PASS, WARN, BLOCK, NOT_EXECUTED, NOT_APPLICABLE); e as técnicas de exploração citadas (por exemplo, consulta empilhada e injeção cega).
	- "## Resumo Executivo", com um texto de introdução no estilo de resumo geral da execução/achados; tabela centralizada com as colunas "Severidade" (com nome sobre chip colorido) e "Quantidade" com total de achados por Severidade, e última linha de total geral na mesma tabela com "Total" na coluna "Severidade" em negrito e alinhado à direita na célula e o número do total geral na célula da coluna "Quantidade".
		- "### Achados por Categoria de Vulnerabilidade": tabela com as colunas "Categoria" (nome em inglês, em negrito, uma vez por categoria); "Subtipo"; "Qtde."; e "Achados" (ids), com linha de total por categoria quando houver mais de um subtipo e linha de total geral ao fim, ordenada da categoria com mais achados para a com menos.
		- "### Distribuição Visual", com gráfico de rosca por Severidade, gráfico de barras por categoria de vulnerabilidade e gráfico de barras por dimensão, os dois últimos empilhados por Severidade com a paleta abaixo e com o total ao fim de cada barra.
		- "### Status de Verificação": tabela com as colunas "Verificação"; "Estado"; e "Evidência".
	- "## Pontos Fortes": o que está protegido, com evidência - título curto de cada ponto forte na cor `#059669`.
	- "## Pontos Fracos": riscos centrais priorizados, com evidência - título curto de cada ponto fraco na cor `#B91C1C`.
	- "## Tabela de Achados", tabela ordenada por Severidade **dentro de cada dimensão**. Colunas: "ID", "Severidade" (com nome sobre chip colorido), "Categoria" (nome em inglês em negrito e, abaixo, o subtipo em português), "Arquivo:Linha-Linha" e "Descrição" (descrição curta; achado em hipótese recebe o prefixo `[HYPOTHESIS]`).
	- "## Recomendação de Ordem de Priorização": justificar as prioridades P1, em seguida apresentar tabela com as colunas: "Prioridade" (P1, P2, P3, P4...); "ID do Achado"; "Severidade" (com nome sobre chip colorido); "Categoria" (nome em inglês e subtipo); e "Ação Necessária";
	- "## Issues dos Achados": seguindo a ordem constante em "Recomendação de Ordem de Priorização", criar **para cada achado acionável** uma subseção com número da Issue e Título, seguida da linha "Achados cobertos" com o id e a categoria de cada achado.
		- Incluir o texto COMPLETO da Issue em Markdown dentro de um bloco delimitado pronto para copiar e colar (ex: entre --- ISSUE n --- e --- FIM ISSUE n ---). Cada Issue deve conter:
			- Título no formato "# [<categoria da vulnerabilidade em inglês>] <descrição curta do problema>";
			- "**Labels sugeridas:**": com lista de tags para indexar a issue;
			- "Problema": com descrição de cada achado coberto, precedida do id, do arquivo:linha-linha, da severidade, do estado e da categoria com subtipo;
			- "## Por que é explorável" se o achado envolver segurança/vulnerabilidade ou "## Por que é um problema" nos demais casos;
			- "## Evidência": informando **cada** arquivo:linha-linha e dentro de snippet o trecho de código correspondente ao problema, seguido do caminho do dado (da entrada até o ponto de uso, com arquivo:linha em cada salto) e das ocorrências adicionais;
			- "## Impacto";
			- "## Sugestão de Correção";
			- "## Critérios de Aceite" com checklist verificável.
		- Quando fizer sentido, agrupe achados **triviais** ou relacionados com a mesma causa em uma Issue única.
	- **Atenção**:
		- Em todos os pontos do relatório QUANDO tratar de "Estado", ENTÃO escrever o texto no estilo de variável em inglês, em MAIÚSCULO e nunca use chip colorido nisso.
		- Em todos os pontos do relatório QUANDO tratar de "Severidade", ENTÃO utilizar apenas esses níveis e paleta de cor do chip colorido correspondente: "CRÍTICA":`#B91C1C`; "ALTA":`#EA580C`; "MÉDIA":`#D97706`; e "BAIXA":`#2563EB`.
		- Em todos os pontos do relatório QUANDO tratar de "Categoria", ENTÃO usar o nome em inglês da lista fechada acima, sempre acompanhado do subtipo em português onde houver espaço.
		- Em todo texto corrido (descrição, explorabilidade, impacto, sugestão, critérios, notas), na primeira menção de cada código de dimensão (S01 a S19) e de cada CWE, escreva o nome por extenso ao lado, entre parênteses. Um leitor sem familiaridade com os códigos precisa entender o achado sem consultar outra fonte.
		- Não inclua achados sobre a stack de agentes de IA (arquivos de configuração e skills dos agentes): ela não roda na aplicação. Registre a inspeção no "Status de Verificação" e no escopo excluído.
		- Não cite identificadores intermediários de trabalho (numeração provisória de agentes ou de rodadas) nem notas de bastidor entre agentes; o relatório usa somente os ids finais.

Use ambiente isolado, sem instalar nada globalmente: venv Python com reportlab e matplotlib. Deixe o script gerador em `specs/relatorios/gerar_relatorio-<slug>-<AAAA-MM-DD-HHhMM>.py`, recebendo o JSON como entrada, para permitir regenerar apenas o PDF sem repetir a revisão. Se o script já existir, reutilize em vez de reescrever. Página A4, margens de 2 cm, cabeçalho e rodapé com nome do relatório à esquerda e número da página à direita.

Antes de entregar, confirme que a contagem do JSON bate com os gráficos e com as tabelas (por severidade, por categoria, por dimensão, linhas da Tabela de Achados, linhas da priorização e blocos de issue). Antes de entregar, renderize temporariamente cada página do PDF como imagem para inspeção visual, sem rasterizar o PDF final. Confira se há texto cortado, tabelas que ultrapassam a largura da página, títulos separados de seus gráficos ou legendas ilegíveis, cabeçalho de grupo órfão no fim da página e página quase vazia sem motivo.
	- Garanta que nenhum segredo aparece em texto claro em nenhuma página. Corrija qualquer defeito antes de entregar.
	- NÃO cite o arquivo JSON em nenhuma parte do relatório.

Quando o conteúdo estiver preparado, antes de finalizar, execute uma revisão e pelo menos uma verificação independente de **correção gramatical e ortográfica em Português brasileiro** seguindo as 'Regras de Escrita' do AGENTS.md. Os nomes de categoria em inglês, os identificadores de código e os trechos de programa ficam fora da revisão gramatical.

É muito relevante para mim o batimento dos números computados dos achados com o constante no PDF; a estética do PDF; correção gramatical e ortográfica; e que na dimensão de Segurança tenha maior verificação que o problema é de fato explorável.
- Assim, dispare times de agentes de execução e de verificação correspondente sobre cada ponto relevante para mim, com um agente orquestrador e um agente final consolidador e verificador do todo antes da entrega final ao orquestrador.
	- Utilize no máximo três turnos de execução-verificação do que é relevante em qualquer parte da execução desta demanda.

Se o projeto tiver dicionário de dados do alvo, estude-o bem antes da revisão para compreender o negócio.

FAÇA APENAS o solicitado; nada mais.

Entregue o relatório em PDF, a lista de achados no chat (uma tabela com id, severidade, estado, prioridade, categoria em inglês, **cada** arquivo:linha-linha e o número da issue que o cobre) e o caminho de todos os arquivos gerados.
```

**Exemplo preenchido, revisão de uma pasta com o trecho do PDF já colado:**

```text
/owasp-playbook revise a segurança da pasta src/pedidos/

Gere relatório em PDF desta revisão técnica com todos os achados.

Consolide antes os achados em `specs/relatorios/achados-<slug>-<AAAA-MM-DD-HHhMM>.json`, com mesmo carimbo gravado no campo `gerado_em`. Os números do relatório sairão desse arquivo preliminar: gráficos e tabela leem dele e nada é recalculado durante a geração da formatação final do relatório. Cada achado leva `id`, `dimensão`, `categoria`, `subtipo`, `severidade` (`critica`, `alta`, `media` ou `baixa`), `estado` (`CONFIRMED` ou `HYPOTHESIS`), `arquivo`, `linha_inicio`, `linha_fim`, `descricao`, `evidencia`, `impacto`, `sugestao_correcao`, `criterios_aceite`, `acionavel` e `grupo_issue`.
	- `categoria` é o nome da vulnerabilidade na nomenclatura internacional, em inglês, escolhido desta lista fechada: SQL Injection; Cross-Site Scripting (XSS); Broken Access Control; Broken Authentication; Sensitive Data Exposure; Security Misconfiguration; Vulnerable and Outdated Components; Information Disclosure; Unrestricted File Upload; Path Traversal; XML External Entity (XXE); Server-Side Request Forgery (SSRF); Cross-Site Request Forgery (CSRF); Command Injection; HTML Injection (Email); Insecure Deserialization; Open Redirect; Weak Cryptography; Insufficient Logging and Monitoring; Business Logic Flaw. Não invente categoria fora da lista; se nenhuma couber, use a mais próxima e explique no `subtipo`.
	- `subtipo` é a forma específica encontrada no código, em português (por exemplo: "comando de escrita sem parâmetro vinculado, exploração por consulta empilhada ou cega"; "consulta de leitura com resultado na tela"; "refletido em atributo HTML"; "armazenado"; "IDOR: identificador do registro vindo do cliente sem conferir o dono"; "credencial na URL").
	- O JSON traz também a lista `categorias`, com quantidade por categoria e por subtipo e os ids de cada um, que alimenta a tabela e o gráfico por categoria.
	- Mascare segredo real na evidência; nunca reproduza um segredo válido em texto claro. Mascare também login de pessoa física, CPF, e-mail pessoal e nome de servidor de banco. Achado que é hipótese continua marcado como hipótese.

Salve o PDF em `specs/relatorios/relatorio-<slug>-<AAAA-MM-DD-HHhMM>.pdf`, reaproveitando o carimbo do JSON. Conteúdo:
	- **Capa** com:
		- Título: **Revisão Técnica <"Completa"/escopo/"por Dimensões Específicas"> - <alvo da revisão técnica>**
		- Subtítulo em itálico: "Relatório de Revisão Estática e de Conformidade Técnica"
		- Tabela com cabeçalho na primeira coluna, com as seguintes linhas:
			- "Data da Execução": a data e hora da execução da revisão técnica em BRT;
			- "Alvo da Revisão Técnica": <alvo da revisão técnica> igual ao do título - **<se aplicável, Versão do alvo no formato vN.N.N>** (onde o projeto registra a versão do alvo, quando houver);
			- "Commit de Referência": identificação do commit de referência conforme o alvo;
			- "Resultado Global": da revisão técnica executada como um todo.
		- "### Dimensões da Revisão Técnica": tabela com as colunas "Dimensão" transcrevendo o nome de cada dimensão abrangida pela revisão técnica executada; "Estado"; e "Cobertura" da dimensão.
		- "### Escopo da Revisão Técnica": informando a lista do que está "Incluído":`#059669` e a lista do que está "Excluído":`#B91C1C` do escopo da revisão técnica, em termos de pastas, arquivos, camadas, situações do ambiente computacional e outras circunstâncias do código, da execução ou do ambiente que contextualiza sobre o que e como foi executada a revisão técnica.
		- "### Nota Metodológica".
		- "### Como ler este relatório": tabela com as colunas "Termo" e "Significado", explicando em linguagem simples cada código usado no relatório: o identificador do achado; categoria e subtipo; os códigos de dimensão do guia de segurança (S01 a S19); CWE; OWASP Top 10; os níveis de severidade; os estados do achado (CONFIRMED e HYPOTHESIS); as prioridades (P1, P2, P3, P4...); os estados de verificação (PASS, WARN, BLOCK, NOT_EXECUTED, NOT_APPLICABLE); e as técnicas de exploração citadas (por exemplo, consulta empilhada e injeção cega).
	- "## Resumo Executivo", com um texto de introdução no estilo de resumo geral da execução/achados; tabela centralizada com as colunas "Severidade" (com nome sobre chip colorido) e "Quantidade" com total de achados por Severidade, e última linha de total geral na mesma tabela com "Total" na coluna "Severidade" em negrito e alinhado à direita na célula e o número do total geral na célula da coluna "Quantidade".
		- "### Achados por Categoria de Vulnerabilidade": tabela com as colunas "Categoria" (nome em inglês, em negrito, uma vez por categoria); "Subtipo"; "Qtde."; e "Achados" (ids), com linha de total por categoria quando houver mais de um subtipo e linha de total geral ao fim, ordenada da categoria com mais achados para a com menos.
		- "### Distribuição Visual", com gráfico de rosca por Severidade, gráfico de barras por categoria de vulnerabilidade e gráfico de barras por dimensão, os dois últimos empilhados por Severidade com a paleta abaixo e com o total ao fim de cada barra.
		- "### Status de Verificação": tabela com as colunas "Verificação"; "Estado"; e "Evidência".
	- "## Pontos Fortes": o que está protegido, com evidência - título curto de cada ponto forte na cor `#059669`.
	- "## Pontos Fracos": riscos centrais priorizados, com evidência - título curto de cada ponto fraco na cor `#B91C1C`.
	- "## Tabela de Achados", tabela ordenada por Severidade **dentro de cada dimensão**. Colunas: "ID", "Severidade" (com nome sobre chip colorido), "Categoria" (nome em inglês em negrito e, abaixo, o subtipo em português), "Arquivo:Linha-Linha" e "Descrição" (descrição curta; achado em hipótese recebe o prefixo `[HYPOTHESIS]`).
	- "## Recomendação de Ordem de Priorização": justificar as prioridades P1, em seguida apresentar tabela com as colunas: "Prioridade" (P1, P2, P3, P4...); "ID do Achado"; "Severidade" (com nome sobre chip colorido); "Categoria" (nome em inglês e subtipo); e "Ação Necessária";
	- "## Issues dos Achados": seguindo a ordem constante em "Recomendação de Ordem de Priorização", criar **para cada achado acionável** uma subseção com número da Issue e Título, seguida da linha "Achados cobertos" com o id e a categoria de cada achado.
		- Incluir o texto COMPLETO da Issue em Markdown dentro de um bloco delimitado pronto para copiar e colar (ex: entre --- ISSUE n --- e --- FIM ISSUE n ---). Cada Issue deve conter:
			- Título no formato "# [<categoria da vulnerabilidade em inglês>] <descrição curta do problema>";
			- "**Labels sugeridas:**": com lista de tags para indexar a issue;
			- "Problema": com descrição de cada achado coberto, precedida do id, do arquivo:linha-linha, da severidade, do estado e da categoria com subtipo;
			- "## Por que é explorável" se o achado envolver segurança/vulnerabilidade ou "## Por que é um problema" nos demais casos;
			- "## Evidência": informando **cada** arquivo:linha-linha e dentro de snippet o trecho de código correspondente ao problema, seguido do caminho do dado (da entrada até o ponto de uso, com arquivo:linha em cada salto) e das ocorrências adicionais;
			- "## Impacto";
			- "## Sugestão de Correção";
			- "## Critérios de Aceite" com checklist verificável.
		- Quando fizer sentido, agrupe achados **triviais** ou relacionados com a mesma causa em uma Issue única.
	- **Atenção**:
		- Em todos os pontos do relatório QUANDO tratar de "Estado", ENTÃO escrever o texto no estilo de variável em inglês, em MAIÚSCULO e nunca use chip colorido nisso.
		- Em todos os pontos do relatório QUANDO tratar de "Severidade", ENTÃO utilizar apenas esses níveis e paleta de cor do chip colorido correspondente: "CRÍTICA":`#B91C1C`; "ALTA":`#EA580C`; "MÉDIA":`#D97706`; e "BAIXA":`#2563EB`.
		- Em todos os pontos do relatório QUANDO tratar de "Categoria", ENTÃO usar o nome em inglês da lista fechada acima, sempre acompanhado do subtipo em português onde houver espaço.
		- Em todo texto corrido (descrição, explorabilidade, impacto, sugestão, critérios, notas), na primeira menção de cada código de dimensão (S01 a S19) e de cada CWE, escreva o nome por extenso ao lado, entre parênteses. Um leitor sem familiaridade com os códigos precisa entender o achado sem consultar outra fonte.
		- Não inclua achados sobre a stack de agentes de IA (arquivos de configuração e skills dos agentes): ela não roda na aplicação. Registre a inspeção no "Status de Verificação" e no escopo excluído.
		- Não cite identificadores intermediários de trabalho (numeração provisória de agentes ou de rodadas) nem notas de bastidor entre agentes; o relatório usa somente os ids finais.

Use ambiente isolado, sem instalar nada globalmente: venv Python com reportlab e matplotlib. Deixe o script gerador em `specs/relatorios/gerar_relatorio-<slug>-<AAAA-MM-DD-HHhMM>.py`, recebendo o JSON como entrada, para permitir regenerar apenas o PDF sem repetir a revisão. Se o script já existir, reutilize em vez de reescrever. Página A4, margens de 2 cm, cabeçalho e rodapé com nome do relatório à esquerda e número da página à direita.

Antes de entregar, confirme que a contagem do JSON bate com os gráficos e com as tabelas (por severidade, por categoria, por dimensão, linhas da Tabela de Achados, linhas da priorização e blocos de issue). Antes de entregar, renderize temporariamente cada página do PDF como imagem para inspeção visual, sem rasterizar o PDF final. Confira se há texto cortado, tabelas que ultrapassam a largura da página, títulos separados de seus gráficos ou legendas ilegíveis, cabeçalho de grupo órfão no fim da página e página quase vazia sem motivo.
	- Garanta que nenhum segredo aparece em texto claro em nenhuma página. Corrija qualquer defeito antes de entregar.
	- NÃO cite o arquivo JSON em nenhuma parte do relatório.

Quando o conteúdo estiver preparado, antes de finalizar, execute uma revisão e pelo menos uma verificação independente de **correção gramatical e ortográfica em Português brasileiro** seguindo as 'Regras de Escrita' do AGENTS.md. Os nomes de categoria em inglês, os identificadores de código e os trechos de programa ficam fora da revisão gramatical.

É muito relevante para mim o batimento dos números computados dos achados com o constante no PDF; a estética do PDF; correção gramatical e ortográfica; e que na dimensão de Segurança tenha maior verificação que o problema é de fato explorável.
- Assim, dispare times de agentes de execução e de verificação correspondente sobre cada ponto relevante para mim, com um agente orquestrador e um agente final consolidador e verificador do todo antes da entrega final ao orquestrador.
	- Utilize no máximo três turnos de execução-verificação do que é relevante em qualquer parte da execução desta demanda.

Se o projeto tiver dicionário de dados do alvo, estude-o bem antes da revisão para compreender o negócio.

FAÇA APENAS o solicitado; nada mais.

Entregue o relatório em PDF, a lista de achados no chat (uma tabela com id, severidade, estado, prioridade, categoria em inglês, **cada** arquivo:linha-linha e o número da issue que o cobre) e o caminho de todos os arquivos gerados.
```

#### Gerar as issues ou tarefas técnicas

Adicione este trecho ao final do prompt de revisão.

```text
Gere também o texto completo das issues desta revisão, prontas para copiar e colar. Não crie issue, tarefa ou PR em ferramenta nenhuma: apenas gere o conteúdo em Markdown.

Consolide antes os achados em `specs/relatorios/achados-<slug>-<AAAA-MM-DD-HHhMM>.json`, se ainda não existir, com `id`, `dimensão`, `categoria`, `subtipo`, `severidade` (`critica`, `alta`, `media` ou `baixa`), `estado` (`CONFIRMED` ou `HYPOTHESIS`), `arquivo`, `linha_inicio`, `linha_fim`, `descricao`, `evidencia`, `impacto`, `sugestao_correcao`, `criterios_aceite`, `acionavel` e `grupo_issue`. `categoria` é o nome da vulnerabilidade em inglês, escolhido desta lista fechada: SQL Injection; Cross-Site Scripting (XSS); Broken Access Control; Broken Authentication; Sensitive Data Exposure; Security Misconfiguration; Vulnerable and Outdated Components; Information Disclosure; Unrestricted File Upload; Path Traversal; XML External Entity (XXE); Server-Side Request Forgery (SSRF); Cross-Site Request Forgery (CSRF); Command Injection; HTML Injection (Email); Insecure Deserialization; Open Redirect; Weak Cryptography; Insufficient Logging and Monitoring; Business Logic Flaw; se nenhuma couber, use a mais próxima e explique no `subtipo`. `subtipo` é a forma específica encontrada no código, em português. Mascare segredo real na evidência.

Salve o resultado em `specs/relatorios/issues-<slug>-<AAAA-MM-DD-HHhMM>.md`, reaproveitando o carimbo do JSON. Para cada achado com `acionavel` verdadeiro, gere o texto de uma issue delimitado por `--- ISSUE <id> ---` e `--- FIM ISSUE <id> ---`, contendo:

- Título no formato `[<categoria da vulnerabilidade em inglês>] descrição curta do problema`.
- Labels sugeridas: a categoria e a severidade.
- O ID do achado, para rastreabilidade.
- Descrição do problema e por que ele importa.
- Evidência: arquivo:linha com o trecho de código, com segredo mascarado.
- Impacto.
- Sugestão de correção.
- Critérios de aceite, como checklist verificável.

Agrupe em uma única issue os achados correlatos que compartilhem o mesmo `grupo_issue`, para não gerar spam de issues, preservando todos os IDs e a severidade de cada um. Achado sem grupo vira issue própria.

Antes de entregar, confirme que toda issue referencia um ID válido, que nenhum segredo aparece em texto claro e que os achados do mesmo grupo viraram uma issue só, e não uma por achado.

Entregue o arquivo, a lista de issues no chat com quantas são e quais achados cada uma cobre, e o caminho do arquivo gerado.
```

Nos dois exemplos abaixo o trecho aparece abreviado, para não repetir o texto inteiro. Na hora de enviar, cole o trecho completo no lugar da linha entre `<` e `>`.

**Exemplo preenchido, revisão de commit com o bloco das tarefas técnicas:**

```text
/owasp-playbook revise a segurança do commit 8bf4ab729d

<cole aqui o trecho das issues, sem alterar nada>
```

**Exemplo preenchido, revisão de uma pasta com o bloco das tarefas técnicas:**

```text
/owasp-playbook revise a segurança da pasta src/pedidos/

<cole aqui o trecho das issues, sem alterar nada>
```

#### Para uma revisão já entregue

Use na mesma conversa em que recebeu a revisão, quando decidir pelo arquivo só depois de ler o relatório. Troque `o PDF` por `o texto das issues` quando quiser a outra saída. O bloco abaixo não tem campo para preencher.

```text
Gere o PDF do relatório de revisão que você acabou de entregar, nos caminhos e no formato do trecho do PDF acima. Os números vêm do relatório que você entregou: não refaça a análise nem recalcule contagem, severidade ou resultado.
```

### Depois da revisão: marcar ou planejar a correção

Os dois prompts abaixo são continuação da mesma conversa, logo depois que o agente entregou o relatório de uma das revisões desta seção. Não cole nem repita os achados: o agente já tem o relatório na conversa. Escolha um dos dois caminhos; nenhum dos dois corrige nada sozinho.

**Marcar TODO: a partir de um relatório de revisão**

Use quando quiser só registrar a dívida no código, sem planejar a correção agora. O bloco abaixo não tem campo para preencher, então já é o texto final para copiar.

```text
No relatório que você acabou de me entregar, pegue os achados confirmados de severidade MEDIA e BAIXA e registre cada um em um comentário no código, junto ao local do achado, neste formato:

// TODO: <descrição objetiva em uma linha>

Aplique achado por achado, com minha confirmação antes de cada um. Nunca marque achados BLOQUEANTE ou ALTA; eles exigem correção, não marcação. Achado que "precisa de conferência" não é marcado: ele ainda não foi comprovado.

Não corrija a causa de nenhum achado nesta etapa, só marque.
```

**Transformar achados em especificação e tarefas**

Use quando tiver vários achados para coordenar e quiser uma lista de tarefas rastreável, para corrigir depois (em outra sessão, ou por outra pessoa) ou agora mesmo.

```text
Transforme em uma especificação de correção e uma lista de tarefas os achados do relatório que você acabou de me entregar.

Alvo do relatório: <caminho da pasta ou do arquivo>.

Para cada achado, gere uma tarefa com: título objetivo, arquivo e linha, ação mínima, critério de aceite (o controle volta a passar) e dependência entre tarefas, se houver. Ordene as tarefas da maior para a menor severidade (BLOQUEANTE, ALTA, MEDIA e BAIXA).

Salve a especificação e a lista de tarefas em um arquivo Markdown (`.md`) na pasta `specs/`.

Pare aqui. Se eu pedir para implementar agora, use o prompt "Investigar e corrigir bug" deste arquivo para cada tarefa, achado por achado, com minha confirmação antes de cada um.
```

**Exemplo preenchido, achados de uma pasta:**

```text
Transforme em uma especificação de correção e uma lista de tarefas os achados do relatório que você acabou de me entregar.

Alvo do relatório: src/pedidos/.

Para cada achado, gere uma tarefa com: título objetivo, arquivo e linha, ação mínima, critério de aceite (o controle volta a passar) e dependência entre tarefas, se houver. Ordene as tarefas da maior para a menor severidade (BLOQUEANTE, ALTA, MEDIA e BAIXA).

Salve a especificação e a lista de tarefas em um arquivo Markdown (`.md`) na pasta `specs/`.

Pare aqui. Se eu pedir para implementar agora, use o prompt "Investigar e corrigir bug" deste arquivo para cada tarefa, achado por achado, com minha confirmação antes de cada um.
```

---

## stack-ai-build-project-context

Investiga o código de um repositório que já tem a estrutura instalada e transforma o que descobre em base de trabalho para os agentes: inventário e regras no `AGENTS.md`, skills de fluxo, references de detalhe e guardrails, todos derivados de evidência do próprio código. É a segunda metade da dupla: a `stack-ai-init` põe a estrutura de pé, esta aqui a preenche.

**Pré-requisito da seção:** a estrutura mínima já instalada pela `stack-ai-init`. Para a parte que empacota skills novas, a skill `skill-creator` precisa estar instalada junto, e a própria skill avisa antes de começar se ela faltar. Com agentes auxiliares a investigação corre em paralelo e a auditoria fica independente. Sem eles a rodada continua com independência menor, e a skill declara isso no relatório.

### Enriquecer uma stack recém instalada

**Pré-requisito:** stack instalada.

**Prompt:**

```text
Use a skill `stack-ai-build-project-context` neste repositório. A stack mínima já está instalada.

Descubra como este sistema é construído e como se desenvolve nele.

O que eu já sei sobre ele:
- o que o sistema faz: <se souber>
- partes que costumam dar problema para quem chega: <se souber>
- pontos que eu considero fora do escopo desta rodada: <se houver>
```

### Atualizar a stack depois de uma mudança grande

Use quando o sistema mudou e o conhecimento que a skill escreveu precisa acompanhar. Para atualizar os arquivos que a `stack-ai-init` distribui, use [Como atualizar os demais arquivos da stack](manutencao-da-stack.md#como-atualizar-os-demais-arquivos-da-stack).

**Pré-requisito:** stack já com conteúdo escrito em rodadas anteriores.

**Prompt:**

```text
Use a skill `stack-ai-build-project-context` neste repositório. A stack já tem conteúdo.

Contexto da mudança: <o que mudou no sistema, se souber>
```

---

## configuracao-de-testes-unitarios

Lê o projeto e devolve um plano para configurar os testes unitários e a cobertura, sem meta de cobertura. A skill só lê: quem executa o plano é você, com o agente, depois de aprovar.

Os prompts de situação, com ou sem testes, e os de ambiente, com ou sem Docker, se combinam: junte em um só pedido as linhas que descrevem o seu projeto.

### Projeto sem testes

Use quando o projeto nunca teve teste automatizado.

**Prompt:**

```text
Use a skill `configuracao-de-testes-unitarios` para planejar a configuração de testes unitários neste repositório.
- Situação do projeto: ainda não tem nenhum teste
```

### Projeto que já tem testes

Use quando o projeto já tem testes e você quer saber como rodá-los e como medir quanto do código eles exercitam.

**Prompt:**

```text
Use a skill `configuracao-de-testes-unitarios` neste repositório.
- Situação do projeto: já tem testes
```

### Projeto que roda em Docker

Use quando a aplicação sobe em contêiner, pelo Docker.

**Prompt:**

```text
Use a skill `configuracao-de-testes-unitarios` para planejar a configuração de testes unitários neste repositório.
- Ambiente: a aplicação roda em Docker
- Serviço da aplicação: <nome do serviço, se souber>
```

### Projeto sem Docker

Use quando a aplicação roda direto na máquina, sem contêiner.

**Prompt:**

```text
Use a skill `configuracao-de-testes-unitarios` para planejar a configuração de testes unitários neste repositório.
- Ambiente: o projeto não usa Docker e roda direto na máquina
```

### Analisar o repositório e planejar os testes unitários

Use quando, além do plano de configuração, você quer saber por onde começar a escrever os testes. Essa análise fica fora do que a skill entrega e vem depois do plano.

**Prompt:**

```text
Use a skill `configuracao-de-testes-unitarios` para analisar este repositório e montar o plano de configuração dos testes unitários.

Depois de entregar o plano, traga uma análise com:
- Testável sem refatoração: as unidades que já podem ser testadas isoladamente, sem mexer no código, com o caminho de cada uma
- Partes críticas: as que merecem prioridade nos primeiros testes, com o caminho e o motivo de cada uma; considere crítica a unidade com regra de negócio, cálculo ou validação, priorizando as que mais mudaram no histórico do versionamento
```

---

## manual-creator-scan-codebase

Prepara adaptadores de sistemas ou módulos e cria, atualiza ou verifica manuais de uso em Markdown a partir da codebase. O adaptador registra convenções, contexto, idiomas e recursos; a skill investiga antes de perguntar pelas lacunas. Informe o caso de uso, o alvo e o que já souber nos campos abaixo. Informações previamente fornecidas são aproveitadas na entrevista.

Na criação e atualização, a skill prepara ou revalida o adaptador, investiga as tarefas do público escolhido, aplica as Regras de Escrita e articula o texto com capturas disponíveis ou avisos para coleta manual dos prints. O destino padrão é `docs/manuais/<slug_do_manual>.md`, com imagens em `docs/manuais/imagens-<slug_do_manual>/`, salvo destino diferente registrado no adaptador.

A criação, atualização e verificação exigem adaptador existente e atualizado frente à codebase do alvo; a skill avalia e executa sua preparação prévia quando necessário. A atualização preserva conhecimento prévio e inclui somente melhorias editoriais necessárias, com conferências próprias e revisão adversarial de necessidade e equivalência. Mudança de mérito exige decisão específica. Depois da preparação do adaptador, a verificação entrega somente o relatório, sem alterar manual e imagens. Código disponível não comprova versão implantada ou conteúdo dinâmico; lacunas e prints pendentes são reportados no chat.

### Preparar o adaptador do sistema/módulo

Use para criar ou atualizar somente o adaptador, sem gerar manuais. A escolha dos destinatários e do alcance de cada futuro manual fica para sua criação.

```text
Use a skill `manual-creator-scan-codebase` para **criar ou atualizar somente o adaptador** do <sistema ou módulo> neste repositório.

- Versão: <se souber>.
- Logo: <arquivo, anexo ou local onde encontrá-la, se houver>.
- Idioma da interface: <idioma da interface do sistema/módulo>.
- Idioma para o Manual: <idioma de redação dos futuros manuais>.
- Contexto do sistema/módulo: <o que faz e quem o usa, se souber>.
- Materiais complementares para o adaptador: <arquivos, anexos ou links de referência, se houver>.
```

### Criar um manual de uso

```text
Use a skill `manual-creator-scan-codebase` para **criar o manual** de uso do <sistema ou módulo> neste repositório.

- Público-alvo: <persona ou grupo de personas e seus objetivos - p. ex. Administração, Usuário Interno e Usuário Externo>.
- Materiais complementares: <arquivos, anexos ou links de referência, se houver>.
```

### Atualizar um manual de uso

```text
Use a skill `manual-creator-scan-codebase` para **atualizar o manual** em <docs/manuais/nome-do-manual.md> referente ao <sistema ou módulo> neste repositório.

- Público-alvo: <persona ou grupo de personas e seus objetivos - p. ex. Administração, Usuário Interno e Usuário Externo>.
- Materiais complementares: <arquivos, anexos ou links de referência, se houver>.
```

### Verificar um manual de uso sem escrita

```text
Use a skill `manual-creator-scan-codebase` para **verificar o manual** em <docs/manuais/nome-do-manual.md> referente ao <sistema ou módulo> neste repositório, sem alterar o manual nem suas imagens.

- Materiais complementares: <arquivos, anexos ou links de referência, se houver>.
```

---

## dicionario-dados-db-scan-codebase-docs

Investiga o código do sistema, os arquivos que criam e alteram o banco e a documentação disponível para gerar e manter dicionários de dados. Além do dicionário, ela mantém o changelog estrutural, que é o histórico das mudanças na estrutura do banco. Ela nasceu de um documento conceitual apoiado na família de normas ISO/IEC de qualidade de dados e metadados (ISO 8000-1, ISO/IEC 25012/25024 e ISO/IEC 11179), guardado em `references/` dentro da própria skill. O alvo pode ser o sistema, um módulo, uma base corporativa ou um data warehouse.

Todo alvo reconhecido precisa de um adaptador registrado. O adaptador é o arquivo que ensina a skill a localizar, investigar e versionar esse alvo: onde fica a fonte estrutural, qual fonte tem precedência quando houver mais de uma, como a versão é identificada e onde buscar no código. Ele fica em `adapters/<família>/<caminho-do-adaptador>.md`, listado em `registro-adaptadores.md`, ambos na raiz da skill. Sem ele registrado, a skill se recusa a gerar o dicionário e aponta a lacuna.

A skill gera três artefatos de saída: `dicionario_tabelas.md` e `dicionario_colunas.md`, com as descrições semânticas, e `CHANGELOG.md`, com o changelog estrutural.

A ordem das seções abaixo é a ordem recomendada de uso. Crie o adaptador do alvo primeiro, em um pedido só dele. Depois, ao criar ou atualizar o dicionário, anexe os materiais complementares que você tiver (manuais, prints de tela, dicionário anterior, planilhas). Eles ajudam a confirmar a semântica de tabelas e colunas. Se ainda não tiver prints mas houver um ambiente de teste disponível, gere-os primeiro com "Gerar prints de tela como insumo complementar".

Não é preciso citar o adaptador do alvo: uma vez criado e registrado, a skill já sabe localizá-lo sozinha. Também não é preciso citar critérios de qualidade ou comandos de verificação internos à skill.

**Pré-requisito da seção:** acesso às fontes que definem a estrutura do banco, que podem ser o código do sistema, os arquivos de instalação e de migração ou o esquema do banco.

### Criar o adaptador

Use este prompt no primeiro contato com um alvo que a skill ainda não reconhece, seja o sistema, um módulo ou uma base de dados corporativa. Ele cria só o adaptador. Gere o dicionário depois, em um pedido separado, com o prompt "Criar o dicionário de dados" a seguir. Informe as convenções que você já souber, para reduzir as perguntas que a skill vai precisar fazer. O que você não souber, ela tenta inferir da codebase antes de perguntar.

A skill pode concluir que um adaptador já registrado cobre o alvo e apenas registrar o alvo nele, em vez de criar um arquivo novo. Isso não é falha do pedido.

**Prompt:**

```text
Use a skill `dicionario-dados-db-scan-codebase-docs` para criar o adaptador do alvo abaixo. Investigue a codebase, crie o adaptador completo e registre-o. Não gere o dicionário de dados nem o changelog nesta etapa.

Alvo: <sistema ou módulo>.

O que eu já sei sobre ele:
- pistas que identificam este sistema ou módulo (nome, caminhos, prefixo de tabela, pasta de documentação, ou a própria codebase, se for o sistema inteiro): <se souber, ex.: "prefixo pedidos_, models em app/Models/Pedido.php, migrations em database/migrations/">
- fonte estrutural e convenção de versionamento: <scripts de instalação, migrations, schema ou DDL, se souber, ex.: "migrations em migrations/Version20240115120000.php">
- precedência entre fontes, se houver mais de uma fonte estrutural para a mesma informação: <qual prevalece, se souber, ex.: "migration mais recente prevalece sobre o schema.sql legado">
- codificação, camadas e onde buscar no código: <se souber, ex.: "camadas Entity/Repository/Controller">
- títulos e pasta de destino dos dicionários: <se souber, ex.: "docs/dicionario_dados/pedidos/">
- particularidades ou exceções conhecidas: <se houver, ex.: "módulo tem tabela compartilhada com outro módulo, não duplicar no dicionário">

Confira no código cada item que informei acima e me avise se encontrar divergência.

Pergunte objetivamente só sobre o que não puder ser inferido nem tiver sido informado acima.
```

### Criar o dicionário de dados

**Pré-requisito:** o adaptador do alvo criado e registrado, com o prompt anterior.

Use este prompt na primeira vez que for gerar o dicionário de um sistema ou módulo. Anexar material complementar aumenta o contexto negocial disponível para a skill e tende a melhorar a qualidade do resultado. Pode ser qualquer formato: esquema de banco em outra ferramenta, dicionário anterior, prints de tela, manuais, vídeos ou transcrições. Cite os materiais que tiver e, se houver mais de um, indique qual é o material prioritário. A skill sempre confere as informações com base nas fontes estruturais disponíveis (codebase, esquema de banco ou DDL) antes de publicar qualquer descrição, com ou sem material complementar.

**Prompt:**

```text
Use a skill `dicionario-dados-db-scan-codebase-docs` para criar o dicionário de dados e o changelog estrutural do alvo abaixo.

Alvo: <sistema ou módulo>.

Investigue as fontes estruturais disponíveis (codebase, scripts de instalação, migrations, schemas ou DDLs) para identificar a convenção de versionamento e, quando houver codebase, onde ficam as regras de negócio. Aproveite também qualquer documentação já existente. Só publique uma descrição quando as evidências encontradas sustentarem a semântica dela; sem essa sustentação, marque a descrição como não confirmada.

Materiais complementares: <arquivos anexados, esquema em outro formato, dicionário anterior, capturas de tela, manuais, vídeos ou transcrições, se houver>.
Material prioritário: <arquivo ou material ao qual dar mais atenção, opcional>.

Confira as informações dos materiais com base na fonte estrutural disponível (código, esquema de banco ou DDL) antes de publicar qualquer descrição.

Ao final, informe explicitamente: 1) as tabelas ou colunas cuja semântica você não conseguiu confirmar nas fontes disponíveis; 2) o que faltou para confirmá-las; 3) os pontos em que algum material complementar divergiu da fonte estrutural, com a fonte que você adotou.
```

### Atualizar o dicionário

**Pré-requisito:** dicionário e changelog já publicados para esse alvo.

Use quando quiser sincronizá-los com as mudanças estruturais do sistema ou do módulo (novas tabelas, colunas ou regras de negócio), inclusive depois de uma nova versão. Também serve para corrigir uma descrição anterior, mas só quando houver divergência estrutural ou domínio incompleto já confirmado, nunca por ajuste de estilo.

**Prompt:**

```text
Use a skill `dicionario-dados-db-scan-codebase-docs` para atualizar o dicionário de dados conforme o estado mais recente das fontes disponíveis.

Alvo: <sistema ou módulo>.

Compare o dicionário de dados e o changelog já existentes com a estrutura e as regras de negócio atuais e altere somente o que realmente mudou desde a última atualização, preservando os demais conteúdos.

Referência da atualização: <versão, commits ou scripts de mudança, se souber>. Se eu não indicar a referência, identifique o escopo da diferença pelas próprias fontes estruturais.

Materiais complementares: <arquivos anexados, esquema em outro formato, dicionário anterior, capturas de tela, manuais, vídeos ou transcrições, se houver>.
Material prioritário: <arquivo ou material ao qual dar mais atenção, opcional>.

Confira as informações dos materiais com base na fonte estrutural disponível (código, esquema de banco ou DDL) antes de publicar qualquer descrição.

Ao final, liste o que foi alterado e o que foi preservado intencionalmente.
```

### Revisar a qualidade do dicionário

**Pré-requisito:** dicionário já publicado para esse alvo.

Use quando quiser avaliar se ele segue os critérios de qualidade e as convenções da própria skill (completude, consistência, terminologia, distinções documentadas), sem alterar arquivos. A revisão também compara a estrutura documentada com a fonte estrutural e aponta as divergências sem corrigi-las; para incorporar mudanças do código, use "Atualizar o dicionário".

**Prompt:**

```text
Use a skill `dicionario-dados-db-scan-codebase-docs` para revisar a qualidade e a consistência do dicionário de dados do alvo abaixo.

Alvo: <sistema ou módulo>.

Classifique cada achado como problema de qualidade documental (descrição incompleta, inconsistente, genérica ou fora das convenções da skill), como lacuna (elemento sem descrição) ou como divergência estrutural (tabela ou coluna documentada de forma diferente da fonte estrutural).
```

### Gerar prints de tela como insumo complementar

**Pré-requisito:** um ambiente de teste do sistema no ar, local ou remoto, uma ferramenta que controle o navegador, como Playwright ou Selenium, que a ferramenta de IA da sua sessão consiga executar, e uma credencial de teste que não seja a de produção. Qualquer ajuste de configuração feito só para habilitar a automação é local e não deve ser comitado.

Os prints gerados aqui podem ser apontados como material complementar em "Criar o dicionário de dados".

**Prompt:**

```text
Gere os prints de tela do alvo abaixo para servirem de insumo complementar ao dicionário de dados (skill `dicionario-dados-db-scan-codebase-docs`).

Alvo: <sistema ou módulo>.

O ambiente de teste já está disponível em <endereço ou instrução para subir o ambiente>. Para autenticar, leia a credencial na <variável de ambiente ou arquivo local não versionado>; se não houver, pare e me peça para configurá-la nesse local, sem recebê-la pela conversa. Nunca use credencial de produção.

Use <ferramenta de automação de navegador disponível, ex.: Playwright ou Selenium> para rodar um script de automação que:

1. Faça login no ambiente de teste.
2. Navegue pelas telas que leem ou gravam as tabelas do alvo, criando os dados de teste (seeds) necessários para preencher cada tela. Se o ambiente for remoto ou compartilhado, confirme comigo antes de criar dados.
3. Capture um print de cada uma dessas telas (listagem, formulário vazio, formulário preenchido, modal, estado intermediário).
4. Salve os PNGs em <pasta de destino>/screenshots/, organizados por subpasta.
5. Exporte os seeds e dados de teste usados no passo 2 (SQL de insert ou passo a passo reprodutível) em <pasta de destino>/seeds/, para reaproveitar em futuras implementações sem recriar a massa de teste do zero.

Ao final, liste as telas que não conseguiu capturar e o motivo de cada uma.
```

---

## gauntlet-loop-forge

A skill `gauntlet-loop-forge` monta um prompt com a técnica de loop de verificação a partir do que você tiver: uma ideia, uma especificação ou um prompt que já existe. Ela só escreve o prompt e para: executar é um pedido separado, em uma janela nova.

Para agregar a técnica, acrescente esta linha ao início ou ao fim de qualquer prompt deste arquivo, antes de enviar:

```text
Use a skill `gauntlet-loop-forge` para acrescentar a técnica de loop ao prompt <acima/abaixo>.
```

Antes de acionar o loop, confirme que a tarefa é complexa ou que um erro nela é difícil de reverter. Em tarefa pequena e reversível, o loop custa mais do que resolve.

Onde o loop compensa:

| Demanda | Quando o loop compensa |
|---|---|
| Correção de bug | A correção muda comportamento em fluxo crítico, mexe em transação ou migra dado, e precisa preservar o que já funciona. |
| Pedido inicial e entregáveis do Speckit | O documento de uma fase é insumo de todas as fases seguintes, e o `/speckit-analyze` só roda depois do `/speckit-tasks`. |
| Revisão técnica e de segurança | O código vai para produção, trata dado pessoal ou já causou incidente. |

**Modelo para executar o prompt com loop:** prefira modelos intermediários e mais baratos. Exemplos em 2026: Luna e Terra (GPT), Haiku e Sonnet (Claude). Eles tendem a gastar menos tokens e, com o loop e os critérios bem definidos no prompt, entregam bom resultado. Modelos de topo, como Opus e Fable (Claude) ou Sol e Astra (GPT), tendem a alongar o loop, porque o mecanismo de execução do próprio modelo já repete e reverifica passos por conta própria, e o gasto de tokens cresce muito.

### Criar o prompt a partir de uma ideia

Use quando o ponto de partida é um objetivo na sua cabeça, sem prompt e sem documento nenhum.

**Prompt:**

```text
Use a skill `gauntlet-loop-forge` para transformar o objetivo abaixo em um prompt com loop de verificação, pronto para colar.

Objetivo: <o que precisa existir no final, em uma ou duas frases>
Onde o prompt vai rodar: <Claude Code, Codex, outro agente, ou chat comum>
Padrão de qualidade que eu já tenho: <se souber, ex.: "a suíte de testes atual precisa continuar passando", "quero no nível do relatório de pedidos que já existe">
Material que serve de fonte da verdade: <se houver: arquivos, issue, especificação, repositório>
O que não pode mudar: <se houver: comportamento, formato, compatibilidade>
O que os agentes podem fazer no ambiente: <ler, rodar teste, editar arquivo, acessar rede, publicar, gastar>
Ações que exigem minha aprovação: <se houver>
```

### Criar o prompt a partir de uma especificação

Use quando já existe um documento, uma issue ou um repositório que manda no resultado.

**Pré-requisito:** o arquivo, a issue ou o repositório acessível ao agente dentro da sessão.

**Prompt:**

```text
Use a skill `gauntlet-loop-forge` para transformar <arquivo, issue ou especificação> em um prompt com loop de verificação, pronto para colar.

Trate <arquivo, issue ou especificação> como fonte da verdade: extraia dali os critérios de aceite, as restrições e o que está fora de escopo antes de me perguntar qualquer coisa. Me avise se encontrar contradição em vez de escolher um lado sozinho.

Me pergunte só o que não estiver no material. Não prescreva arquitetura, divisão de tarefas nem ordem de implementação: isso é decisão do agente líder na hora de executar.
```

### Otimizar um prompt que já existe

Use quando o prompt já tem bloco de loop e você quer saber onde ele falha. A skill troca só o bloco de loop e preserva o seu texto e as restrições que você já escreveu.

**Prompt:**

```text
Use a skill `gauntlet-loop-forge` para diagnosticar e reescrever o bloco de loop do prompt abaixo.

<cole aqui o prompt atual, com o bloco de loop>

Aponte onde ele deixa quem constrói aprovar o próprio trabalho, onde o padrão de qualidade não é conferível, onde a repetição não tem limite finito e onde ele manda usar comando ou flag que o meu ambiente não tem. Me diga o que mudou e por quê.
```

### Executar o prompt criado

Use em uma janela nova, colando o prompt que a skill devolveu. As linhas depois do prompt servem para você conferir logo no começo que os limites e o registro de evidências ficaram claros.

**Pré-requisito:** uma ferramenta de IA que consiga abrir agentes auxiliares em conversa separada. Quem constrói e quem confere são agentes diferentes, e quem confere trabalha em conversa nova, sem ter acompanhado a construção. Sem essa segunda conversa, o mesmo agente faz os dois papéis e acaba aprovando o próprio trabalho. O prompt ainda roda assim, com resultado mais fraco, e o bloco de loop manda o agente avisar você de que a separação não foi real.

**Prompt:**

```text
<cole aqui o prompt criado>

Antes de começar, confirme em uma linha: quantas rodadas de melhoria e conferência você tem por frente de trabalho, quantos agentes vai abrir ao mesmo tempo e onde vai manter o registro de evidências.

Se algum limite for atingido antes da aprovação, pare e me devolva o estado CAPPED com os achados em aberto e a próxima ação recomendada. Atingir o limite nunca é aprovação, e nenhum limite é ampliado sem eu autorizar.
```

---

## Documentos para agentes de IA

A skill `writing-for-agents` revisa a escrita de um documento que um agente de IA lê: skill, `AGENTS.md`, `CLAUDE.md` e qualquer arquivo que um deles aponte. Ela não cria skill nem documento novo; criar skill é trabalho da `skill-creator`. Use-a em um `.md` que já existe, para o texto ficar mais claro e mais previsível para o agente. O texto da skill está em inglês; os prompts abaixo podem ir em português.

O que ela avalia em um documento:

- **Ponteiro**: a frase que cita outro documento e diz quando lê-lo. A descrição de uma skill e a linha do `AGENTS.md` que aponta um arquivo são ponteiros. A redação do ponteiro decide se o agente chega ao material na hora certa.
- **Carga**: o que fica carregado em toda sessão, como o `AGENTS.md` e a descrição de cada skill, custa atenção do agente em toda resposta. Cada linha ali precisa mudar o comportamento do agente; linha que ele já obedece por padrão sai.
- **Hierarquia**: passo, na ordem em que o agente executa, fica no arquivo principal. Referência que só alguns casos usam vai para um arquivo à parte, atrás de um ponteiro. Definição, regra e ressalva de um mesmo assunto ficam juntas, sob um mesmo título.
- **Critério de pronto**: cada passo termina com uma condição que o agente consegue conferir e que exige o trabalho completo.
- **Redação**: orientação escrita pelo que fazer, em vez de pelo que evitar, e uma palavra curta que o agente já conhece no lugar de uma frase que explica a mesma ideia.

### Revisar o AGENTS.md do projeto

Use quando o `AGENTS.md` cresceu, quando o agente ignora uma orientação que está lá ou quando ele lê um documento na hora errada. A revisão é de escrita: o sentido de cada orientação se preserva.

**Prompt:**

```text
Use a skill `writing-for-agents` para revisar o `AGENTS.md` em <caminho da raiz>. Preserve o sentido de cada orientação: a revisão é de escrita, e a regra em si não muda.

Para cada linha, me diga se ela muda o comportamento do agente em relação ao que ele já faria sem ela, se ela repete algo que o próprio repositório já responde (script, configuração, estrutura de pastas) e se ela repete algo que outro documento já diz. Para cada documento que o `AGENTS.md` aponta, avalie se a frase que aponta deixa claro o que é o documento e em que situação o agente deve lê-lo, e me diga se o conteúdo merece ficar no `AGENTS.md` ou atrás do ponteiro.

Orientação que precisa ficar como está: <se houver, ex.: "as regras de permissão e de transação">

Me traga a proposta antes de escrever: o que sai, o que muda de lugar e o que fica.
```

### Revisar a escrita de uma skill

Use quando uma skill existente dispara na hora errada, ficou longa demais ou o agente pula um passo dela. A skill decide entre dois modos de acionamento: a skill que o agente aciona sozinho tem descrição carregada em toda sessão; a skill acionada só pelo nome, quando você a chama, não custa carga, mas depende de você lembrar que ela existe.

**Prompt:**

```text
Use a skill `writing-for-agents` para revisar a skill em <caminho da pasta da skill>. Preserve o que a skill faz: a revisão é de escrita.

Avalie a descrição do cabeçalho como o gatilho da skill: se ela diz o que a skill é e em que casos o agente deve acioná-la, com uma palavra-chave por caso e sem sinônimos do mesmo caso. Me diga se a skill deve ser acionada pelo agente sozinho ou só quando eu chamar pelo nome, e o que muda no cabeçalho em cada opção. No corpo, separe o que é passo, na ordem de execução, do que é referência consultada sob demanda, e aponte o que deveria sair do arquivo principal para um arquivo à parte. Confira se cada passo termina com uma condição clara de pronto.

Problema que eu vejo hoje: <se houver, ex.: "a skill dispara em pedido que não tem a ver com ela", "o agente pula o passo de conferência">

Me traga a proposta antes de escrever.
```

### Revisar um documento que o AGENTS.md aponta

Use para um arquivo de referência que o `AGENTS.md` ou uma skill aponta: guia de padrões, regras de um tipo de artefato, procedimento de release. Esse documento entra no contexto só quando o ponteiro dispara, então a revisão olha as duas pontas: o ponteiro, que decide quando o agente lê, e o documento, que precisa ler como instrução de trabalho.

**Prompt:**

```text
Use a skill `writing-for-agents` para revisar <caminho do documento>, que o `AGENTS.md` aponta em <a linha ou a seção que aponta>. Preserve o sentido do conteúdo: a revisão é de escrita.

Avalie primeiro o ponteiro: se a frase diz o que o documento é e em que situação o agente deve lê-lo, e se o agente chega ao documento nos casos em que ele é obrigatório. Depois o documento: se ele repete algo que o `AGENTS.md` ou outro documento já diz, se cita algo que o repositório responde sozinho (script, configuração, estrutura de pastas), se tem trecho que já não vale, e se definição, regra e ressalva de um mesmo assunto estão juntas ou espalhadas.

Me traga a proposta antes de escrever: o que sai, o que muda de lugar, o que fica e a nova redação do ponteiro.
```

---

## Modos auxiliares

Esta seção é diferente das anteriores. As skills aqui não resolvem uma demanda: elas mudam o modo como o agente trabalha em qualquer prompt deste arquivo. O `caveman` muda como o agente escreve, e o `grill-me` muda como o agente entrevista você.

As duas são opcionais e nenhuma liga sozinha. O agente nunca aciona esses modos por conta própria: eles só entram se você os invocar.

Nenhum desses modos afrouxa o `AGENTS.md`. Se a instrução do modo colidir com uma regra do `AGENTS.md`, vale o `AGENTS.md`.

Envie o comando sozinho, em uma mensagem separada, antes do prompt real. O `caveman` continua ativo nas respostas seguintes até você desligá-lo ou até a sessão acabar. O `grill-me` vale só para a resposta que ele gera.

### Interrogar um plano ou um pedido utilizando o grill-me

Use antes de aprovar um plano ou antes de escrever um pedido. São três momentos típicos: antes do `/speckit-specify`, quando a tarefa ainda está só na sua cabeça e você não sabe como escrever o pedido; entre o `/speckit-specify` e o `/speckit-plan`, para interrogar a especificação antes de aprová-la; e antes de dar o OK para o agente implementar o que um prompt deste arquivo pediu. Quando a entrevista vier antes do `/speckit-specify`, peça no fim o texto do pedido da fase, pronto para colar. Comece em uma janela nova.

A entrevista vem em rodadas. Cada rodada traz as perguntas que já dá para responder agora, numeradas e com a resposta que o agente recomenda, e o agente espera as suas respostas antes de abrir a rodada seguinte. Quando a pergunta puder ser respondida lendo o código, o agente busca o dado em vez de perguntar. A entrevista acaba quando não sobra pergunta em aberto. Nada é gravado em arquivo: se quiser guardar o que ficou decidido, peça no fim da entrevista.

Não peça a entrevista nem descreva o formato das perguntas: a skill já cuida das duas coisas. Diga só três coisas: o que vai ser interrogado, a decisão que ainda está em aberto e o que mais preocupa você. Responder "tanto faz" ou concordar com tudo esvazia a entrevista: são as suas objeções que fazem o agente mudar de direção.

**Prompt:**

```text
/grill-me

<o plano, a spec ou a decisão a ser interrogada>

Ainda em aberto: <a decisão que você não tomou>

Me preocupa: <o que mais preocupa você>
```

**Exemplo preenchido, interrogar um plano do SpecKit:**

```text
/grill-me

O plano gerado pelo /speckit-plan para o relatório de pedidos por categoria do módulo pedidos.

Ainda em aberto: se o relatório entra como opção nova no menu do módulo ou como aba dentro da listagem de categorias que já existe.

Me preocupa: permissão nova, desempenho da consulta quando uma categoria tem muitos pedidos, e o que a tela mostra para pedidos sem categoria associada.
```

### Comprimir a resposta utilizando o caveman

Use quando a sessão for gerar texto longo, como a revisão de segurança de um diretório inteiro, e você quiser o conteúdo sem a prosa em volta. O `caveman` corta artigo, ressalva e frase de cortesia. Ele não corta conteúdo técnico: termo técnico, trecho de código, nome de classe ou de API e mensagem de erro saem literais, iguais ao modo normal. Código, mensagem de commit e descrição de PR também continuam em texto normal.

A skill tem seis níveis, e três servem para o trabalho do dia a dia. O `lite` mantém frase completa e tira só o excesso. O `full` é o padrão e usa frase fragmentada. O `ultra` abrevia palavra de prosa (DB, config, req) e nunca abrevia nome de função, símbolo de código ou mensagem de erro. Os outros três (`wenyan-lite`, `wenyan-full` e `wenyan-ultra`) escrevem em chinês clássico e não têm uso aqui.

Na sequência, na mesma janela, envie o prompt real. Exemplo: envie `/caveman full` e depois o prompt de "Revisar uma pasta ou um arquivo" deste arquivo, para receber o mesmo relatório sem o texto de apoio. Para voltar ao normal, envie `stop caveman`.

O bloco abaixo não tem campo para preencher, então já é o texto final para copiar.

```text
/caveman full
```
