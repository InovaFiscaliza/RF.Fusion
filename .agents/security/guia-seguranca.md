# Guia de segurança da stack

Guia agnóstico de linguagem, plataforma e framework. Ele cobre os riscos que existem em qualquer sistema que receba entrada, guarde dado e execute operação em nome de alguém.

Nenhum item cita função, biblioteca ou framework: cada tópico descreve a regra invariante, como o risco costuma aparecer, como confirmar se ele existe e o que não deve ser reportado. Cabe ao projeto traduzir cada regra para a sua stack, e registrar essa tradução nos próprios arquivos.

## Como usar

**Ao escrever ou alterar código.** Percorra os tópicos que o artefato tocar. Código que recebe entrada externa passa por S04, S05, S06, S07 e S08. Código que lê ou grava registro de alguém passa por S01 e S02.

**Ao revisar.** Cada achado cita o identificador do tópico, o arquivo, a linha e o caminho do dado, da entrada até o ponto de uso. Achado sem caminho de dado demonstrado é hipótese, e deve ser reportado como tal.

**Antes de reportar.** Todo tópico tem a seção "Não é achado". Leia antes de abrir o item. Falso positivo custa confiança, e revisão com ruído deixa de ser lida.

## Severidade

| Nível | Definição | Ação requerida |
|---|---|---|
| **BLOQUEANTE** | Quebra autenticação, autorização ou integridade, ou expõe dado sensível. | Corrigir antes do merge. |
| **ALTA** | Risco real de exploração, sem quebra direta de controle de acesso. | Corrigir antes da entrega ou registrar com prazo. |
| **MÉDIA** | Viola boa prática consolidada, sem caminho de exploração no contexto atual. | Registrar e corrigir na sequência. |
| **BAIXA** | Melhoria de robustez, sem risco imediato. | Registrar como dívida técnica. |

Severidade qualifica o impacto. Ela não diz se o achado está confirmado: um item só é reportado como confirmado quando houver caminho de dado demonstrado.

## Índice

| # | Tópico | Severidade |
|---|---|---|
| S01 | [Autorização por função](#s01-autorização-por-função) | BLOQUEANTE |
| S02 | [Autorização por objeto (IDOR)](#s02-autorização-por-objeto-idor) | BLOQUEANTE |
| S03 | [Autenticação e sessão](#s03-autenticação-e-sessão) | BLOQUEANTE |
| S04 | [Validação e normalização de entrada](#s04-validação-e-normalização-de-entrada) | ALTA |
| S05 | [Injeção em consulta a dados](#s05-injeção-em-consulta-a-dados) | BLOQUEANTE |
| S06 | [Injeção em comando de sistema](#s06-injeção-em-comando-de-sistema) | BLOQUEANTE |
| S07 | [Saída para HTML e JavaScript](#s07-saída-para-html-e-javascript) | ALTA |
| S08 | [Outros destinos de saída](#s08-outros-destinos-de-saída) | ALTA |
| S09 | [Caminho de arquivo e upload](#s09-caminho-de-arquivo-e-upload) | ALTA |
| S10 | [Desserialização e parser de documento externo](#s10-desserialização-e-parser-de-documento-externo) | ALTA |
| S11 | [Requisição de saída controlável](#s11-requisição-de-saída-controlável) | ALTA |
| S12 | [Segredos e credenciais](#s12-segredos-e-credenciais) | BLOQUEANTE |
| S13 | [Criptografia e aleatoriedade](#s13-criptografia-e-aleatoriedade) | ALTA |
| S14 | [Configuração segura por padrão](#s14-configuração-segura-por-padrão) | ALTA |
| S15 | [Cadeia de fornecimento](#s15-cadeia-de-fornecimento) | ALTA |
| S16 | [Log, auditoria e alerta](#s16-log-auditoria-e-alerta) | ALTA |
| S17 | [Tratamento de erro e falha segura](#s17-tratamento-de-erro-e-falha-segura) | ALTA |
| S18 | [Integridade da operação](#s18-integridade-da-operação) | ALTA |
| S19 | [Entrada não confiável que alcança modelo de IA](#s19-entrada-não-confiável-que-alcança-modelo-de-ia) | ALTA |

---

## S01: Autorização por função

**Severidade**: BLOQUEANTE
**Referência**: OWASP A01:2025 | CWE-862, CWE-306, CWE-284

**Regra.** Toda operação decide a autorização no servidor, antes de executar, a partir da identidade da sessão. Esconder o botão na tela não é autorizar. Confiar em um campo enviado pelo cliente para dizer o papel do usuário também não é autorizar.

**Como aparece.** Em todo ponto de entrada: rota HTTP, endpoint de API, ação assíncrona, tarefa agendada, comando de linha, consumidor de fila, hook de evento. Ponto de entrada novo costuma nascer sem a verificação, porque ela não é necessária para o código funcionar.

**Como verificar.** Liste os pontos de entrada do artefato alterado e confirme, um a um, que existe verificação de autorização antes de qualquer efeito. Verifique também a ordem: validação que roda depois da escrita não protege nada.

**Não é achado.** Ponto de entrada público por decisão registrada, como página aberta, verificação de saúde ou o próprio login. Verificação feita em camada anterior comprovadamente obrigatória, como um filtro que cobre toda a rota.

---

## S02: Autorização por objeto (IDOR)

**Severidade**: BLOQUEANTE
**Referência**: OWASP A01:2025 | CWE-639, CWE-863

**Regra.** Identificador vindo do cliente nunca é suficiente para decidir acesso. O servidor resolve o dono ou o escopo pela sessão e confirma que o objeto pedido pertence a esse escopo. Autorizar a função sem autorizar o objeto deixa o usuário legítimo ler e alterar o registro de outro.

**Como aparece.** Em qualquer parâmetro que nomeie um registro: id numérico, uuid, slug, chave composta, número de protocolo, nome de arquivo. Vale igualmente para identificador que chega pelo corpo da requisição, por cabeçalho, por cookie ou dentro de um token. Aparece também em listagem, exportação e relatório, quando o filtro por dono depende de um parâmetro em vez da sessão.

**Como verificar.** Repita a mesma requisição trocando o identificador pelo de outro dono. Resposta de sucesso com dado alheio confirma o achado. Verifique a leitura e também a escrita, porque é comum a leitura filtrar por dono e a atualização não. Prefira responder "não encontrado" a "proibido": a segunda resposta confirma que o registro existe.

**Não é achado.** Recurso deliberadamente público. Identificador difícil de adivinhar não é controle: um uuid reduz a descoberta por tentativa, mas não substitui a verificação de dono.

---

## S03: Autenticação e sessão

**Severidade**: BLOQUEANTE
**Referência**: OWASP A07:2025 | CWE-287, CWE-384, CWE-916

**Regra.** A credencial é verificada no servidor. A senha é guardada apenas como hash de função lenta e com sal, nunca cifrada de forma reversível e nunca em hash rápido. O identificador de sessão é trocado no login, expira por inatividade e é invalidado no servidor no logout.

**Como aparece.** Login, troca e recuperação de senha, permanência entre sessões, token de API, credencial de serviço, integração entre sistemas.

**Como verificar.** Confirme a função de hash e o custo dela. Confirme que o identificador de sessão muda no login, o que fecha fixação de sessão. Confirme que o logout invalida no servidor, e não apenas apaga o cookie no cliente. Confirme que o fluxo de recuperação não revela se o usuário existe e que o token de recuperação expira e é de uso único.

**Não é achado.** Ausência de segundo fator, quando o projeto registrou a decisão. Política de senha diferente da sua preferência pessoal, desde que compatível com a orientação vigente do projeto.

---

## S04: Validação e normalização de entrada

**Severidade**: ALTA
**Referência**: OWASP A05:2025 | CWE-20

**Regra.** Toda entrada é validada no servidor por lista de valores permitidos, tipo, faixa e tamanho, antes de ser usada. Validação no cliente é conforto de uso, não controle. Validar não substitui neutralizar na saída: os dois são necessários e resolvem problemas diferentes.

**Como aparece.** Parâmetro de rota, query, corpo, cabeçalho, cookie, arquivo enviado, variável de ambiente e resposta de sistema externo. Resposta de terceiro é entrada não confiável, mesmo quando o terceiro é interno da organização.

**Como verificar.** Para cada campo novo, localize onde o tipo e o domínio são fixados. Campo que atravessa a aplicação inteira como texto livre, sem limite de tamanho, é achado. Confirme que a validação é por lista de permitidos, e não por lista de proibidos: negar o que se conhece hoje deixa passar o de amanhã.

**Não é achado.** Campo genuinamente livre, como observação ou descrição, desde que tratado no destino conforme S07 e S08, e com limite de tamanho.

---

## S05: Injeção em consulta a dados

**Severidade**: BLOQUEANTE
**Referência**: OWASP A05:2025 | CWE-89

**Regra.** A estrutura da consulta é sempre estática e definida no código. Dado do usuário entra somente como parâmetro vinculado, nunca como texto concatenado. A regra vale para SQL, para bancos de documento, para diretórios, para consultas em XML e para APIs de busca.

**Como aparece.** Concatenação de texto, interpolação de variável na string, template de consulta, e o método "cru" que a maioria das bibliotecas de acesso a dados oferece. Aparece também dentro de mapeadores objeto-relacional, exatamente por esse método cru, e em procedimentos armazenados que montam a consulta internamente.

**Como verificar.** Procure toda montagem dinâmica de consulta e confirme parâmetro vinculado. Atenção aos pontos onde o vínculo não funciona: nome de tabela, nome de coluna, direção de ordenação e cláusula de limite não aceitam parâmetro, e nesses casos a única defesa é uma lista fechada de valores permitidos, comparada por igualdade.

**Não é achado.** Consulta montada apenas com literais do próprio código, sem qualquer parte controlável.

---

## S06: Injeção em comando de sistema

**Severidade**: BLOQUEANTE
**Referência**: OWASP A05:2025 | CWE-78, CWE-77, CWE-94

**Regra.** Prefira a biblioteca da própria linguagem à chamada de processo externo. Quando o processo for necessário, passe o executável e os argumentos como lista, sem intermediar um shell, com o executável vindo de lista fechada.

**Como aparece.** Geração de relatório, conversão de documento, manipulação de imagem, compactação, ferramenta de rede, chamada a utilitário do sistema, e qualquer construção que interprete texto como código na própria linguagem.

**Como verificar.** Se a string do comando é montada com parte controlável e passa por um shell, é achado. Escapar aspas não resolve, porque a lista de metacaracteres depende do shell e do sistema. Verifique também o argumento que parece inofensivo: um nome de arquivo que começa com hífen vira opção do programa.

**Não é achado.** Comando totalmente estático. Argumento que vem de lista fechada de valores, comparada por igualdade antes do uso.

---

## S07: Saída para HTML e JavaScript

**Severidade**: ALTA
**Referência**: OWASP A05:2025 | CWE-79

**Regra.** Escape na saída, de acordo com o contexto de destino. Corpo do HTML, valor de atributo, URL dentro de atributo, bloco de JavaScript e bloco de CSS têm regras de escape diferentes, e usar a errada não protege. Use o escape automático do mecanismo de template e nunca o desligue para um valor que tenha origem externa.

**Como aparece.** Template do servidor, concatenação de HTML no código, e no navegador qualquer atribuição de HTML bruto ao documento. Vale também para o valor que chega por API e só vira HTML depois, na tela: o dado que entrou sem escape ontem é o que executa hoje.

**Como verificar.** Procure os pontos que desligam o escape do template e as atribuições de HTML bruto no cliente. Cada um exige sanitização por lista de permitidos, com biblioteca dedicada. Verifique também os valores que viram atributo de evento, destino de link e conteúdo de bloco de script, que são os contextos onde o escape comum de HTML não basta.

**Não é achado.** Conteúdo estático escrito no próprio código. HTML que passou por sanitizador de lista de permitidos antes de chegar ao documento.

---

## S08: Outros destinos de saída

**Severidade**: ALTA
**Referência**: OWASP A05:2025 | CWE-113, CWE-1236, CWE-117

**Regra.** Cada destino de saída tem a sua própria regra de neutralização, e nenhuma serve para o outro. Um valor pode ser inofensivo em HTML e perigoso em uma planilha.

**Como aparece.** Cabeçalho HTTP e destino de redirecionamento, onde a quebra de linha divide a resposta. Planilha exportada, onde um valor iniciado por sinal de igual, mais, menos ou arroba vira fórmula executada pelo leitor. Arquivo de log, onde a quebra de linha forja uma entrada inteira. Nome de arquivo em download, corpo de e-mail e template renderizado no servidor.

**Como verificar.** Percorra o caminho do campo de entrada até cada destino que ele alcança, e confirme a neutralização específica daquele destino. Um único campo costuma alcançar vários destinos, e é comum estar tratado só no mais visível.

**Não é achado.** Destino que só recebe valor de domínio fechado, definido pelo próprio código.

---

## S09: Caminho de arquivo e upload

**Severidade**: ALTA
**Referência**: OWASP A01:2025, A05:2025 | CWE-22, CWE-434

**Regra.** Nunca monte um caminho de arquivo com dado vindo do cliente. Gere um nome próprio, resolva o caminho absoluto e confirme que o resultado está dentro do diretório permitido antes de abrir. No recebimento de arquivo, valide o tipo pelo conteúdo, limite o tamanho, guarde fora da área servida pelo servidor web e garanta que o arquivo não pode ser executado.

**Como aparece.** Download por nome, anexo, importação, exportação, escolha de template por parâmetro, carregamento dinâmico de arquivo, extração de pacote compactado.

**Como verificar.** Teste com sequência de subida de diretório, com caminho absoluto e com codificação alternativa dos mesmos caracteres. Confirme que a decisão sobre o tipo do arquivo não vem da extensão nem do tipo declarado pelo cliente, porque os dois são escolhidos por quem envia. Na extração de pacote, confirme que nenhuma entrada escapa do diretório de destino.

**Não é achado.** Caminho montado apenas com identificador interno já validado contra lista fechada, sem qualquer parte livre.

---

## S10: Desserialização e parser de documento externo

**Severidade**: ALTA
**Referência**: OWASP A08:2025 | CWE-502, CWE-611

**Regra.** Não reconstrua objetos a partir de dado externo. Troque por um formato de dados puro, com esquema validado. Em leitores de documento, desligue entidades externas e definições de tipo antes de processar qualquer documento que não seja seu.

**Como aparece.** Cache, cookie, mensagem de fila, campo de banco que guarda objeto serializado, importação, integração entre sistemas, upload de documento estruturado, arquivo de configuração enviado pelo usuário.

**Como verificar.** Localize o ponto de desempacotamento e recue até a origem do dado. Origem externa, ou origem interna persistida sem verificação de integridade, é achado. Em leitores de documento, confirme explicitamente que entidade externa está desligada, porque em várias plataformas o padrão histórico é vir ligada.

**Não é achado.** Serialização interna que nunca cruza a fronteira do processo e cuja integridade é garantida por assinatura ou por armazenamento controlado.

---

## S11: Requisição de saída controlável

**Severidade**: ALTA
**Referência**: OWASP A01:2025 | CWE-918, CWE-601

**Regra.** O destino de uma requisição que o servidor faz nunca vem cru do cliente. Use lista fechada de esquema, host e porta, verificada depois da resolução de nome, e não siga redirecionamentos automaticamente. A mesma regra vale para o destino de um redirecionamento devolvido ao navegador.

**Como aparece.** Webhook, importação por URL, geração de documento a partir de uma página, prévia de link, proxy, verificação de imagem remota, quadro embutido e o retorno para a página anterior depois do login.

**Como verificar.** Teste com endereço de rede interna, com o próprio servidor, com o endereço de metadados do provedor de nuvem e com um domínio público que resolve para um endereço interno. Validar o texto da URL não basta, porque a resolução acontece depois: valide o endereço resolvido, imediatamente antes de conectar. No redirecionamento, confirme que o destino é relativo ou pertence a lista fechada.

**Não é achado.** Destino fixo definido em configuração do projeto, sem participação do cliente.

---

## S12: Segredos e credenciais

**Severidade**: BLOQUEANTE
**Referência**: OWASP A02:2025, A04:2025 | CWE-798, CWE-200, CWE-532

**Regra.** Segredo não fica no código, no repositório, na imagem de container, no log, na URL nem na resposta ao cliente. Ele vem de variável de ambiente ou de um cofre, e precisa ser rotacionável sem alterar código.

**Como aparece.** Chave de API, senha, token, string de conexão com credencial embutida, chave privada, certificado, e a credencial de teste que também funciona em outro ambiente. Aparece também em arquivo de exemplo preenchido com valor real e em parâmetro de URL, que fica no histórico do navegador e no log do servidor.

**Como verificar.** Procure valores de alta entropia e nomes típicos no código, na configuração, nos artefatos de build e no histórico de commits. Segredo que já foi enviado ao repositório é considerado vazado a partir daquele momento: remover o arquivo não desfaz nada, porque o histórico permanece. A ação correta é rotacionar a credencial e só então limpar.

**Não é achado.** Marcador de posição, exemplo obviamente inválido e credencial de teste local sem valor fora do ambiente de teste.

---

## S13: Criptografia e aleatoriedade

**Severidade**: ALTA
**Referência**: OWASP A04:2025 | CWE-327, CWE-338

**Regra.** Use a biblioteca criptográfica da plataforma, com algoritmo e tamanho de chave vigentes. Nunca implemente primitiva própria e nunca invente um esquema de cifra. Todo valor que precise ser imprevisível vem do gerador criptográfico, não do gerador comum.

**Como aparece.** Senha, token de sessão, token de recuperação, código de convite, valor de uso único, assinatura, cifra de campo no banco e transporte de dado entre sistemas.

**Como verificar.** Procure algoritmo obsoleto, modo de operação sem verificação de integridade, valor inicial fixo, chave embutida no código e uso do gerador comum onde o valor precisa ser imprevisível. Confirme que dado sensível em trânsito usa canal cifrado, inclusive entre serviços internos.

**Não é achado.** Hash não criptográfico usado para particionar, indexar ou detectar mudança, sem qualquer função de segurança.

---

## S14: Configuração segura por padrão

**Severidade**: ALTA
**Referência**: OWASP A02:2025 | CWE-1188

**Regra.** O estado padrão é o mais fechado, e afrouxar exige decisão explícita. Depuração desligada, listagem de diretório desligada, conta e credencial padrão removidas, serviço exposto apenas onde precisa ser alcançado, permissão de arquivo mínima.

**Como aparece.** Configuração de produção, imagem de container, servidor de aplicação, banco de dados, armazenamento de objetos, política de origem cruzada e cabeçalhos de resposta.

**Como verificar.** Compare a configuração de produção com a de desenvolvimento e liste toda diferença que afrouxa um controle. Confirme que a aplicação recusa subir com credencial padrão. Confirme que a política de origem cruzada não aceita qualquer origem junto com envio de credencial.

**Não é achado.** Afrouxamento em ambiente local que não é distribuído nem publicado, e que está claramente separado da configuração de produção.

---

## S15: Cadeia de fornecimento

**Severidade**: ALTA
**Referência**: OWASP A03:2025 | CWE-1395

**Regra.** Dependência com versão fixada, origem conhecida e atualização acompanhada. A regra vale igualmente para imagem base, passo de automação de build e script baixado durante a construção.

**Como aparece.** Manifesto de dependências, arquivo de trava de versões, imagem de container, extensão do sistema de build, e a linha que baixa um script da internet e executa direto no pipeline.

**Como verificar.** Confirme que o arquivo de trava está versionado, que dependência sensível não usa faixa aberta de versão e que nada é baixado e executado sem verificação de integridade. Verifique se a dependência nova é mantida, se tem histórico e se o nome não é parecido demais com o de um pacote popular.

**Não é achado.** Dependência desatualizada sem vulnerabilidade conhecida relevante ao uso, quando não há relatório disponível ao revisor. A ausência de ferramenta de varredura não é, por si só, um achado.

---

## S16: Log, auditoria e alerta

**Severidade**: ALTA
**Referência**: OWASP A09:2025 | CWE-532, CWE-778

**Regra.** Registre quem fez, o que fez, quando e sobre qual objeto, nas operações sensíveis. Nunca registre segredo, dado pessoal além do necessário ou o corpo completo da requisição. Registro que ninguém lê não detecta nada: operação crítica precisa gerar alerta.

**Como aparece.** Autenticação e falha de autenticação, mudança de permissão, exclusão, exportação em massa, operação financeira e acesso a dado sensível.

**Como verificar.** Verifique se a operação sensível deixa rastro suficiente para reconstruir o que aconteceu. Verifique o conteúdo do registro, procurando segredo, documento de identificação, endereço e outros dados pessoais que não precisavam estar ali. Confirme que o registro não pode ser forjado por quebra de linha vinda da entrada, conforme S08.

**Não é achado.** Ausência de registro em leitura corriqueira que não envolve dado sensível.

---

## S17: Tratamento de erro e falha segura

**Severidade**: ALTA
**Referência**: OWASP A10:2025 | CWE-209, CWE-755, CWE-390

**Regra.** Diante de erro, negue. A mensagem devolvida ao usuário é genérica, e o detalhe fica no registro do servidor. Nenhum caminho de exceção pode deixar o sistema em estado mais permissivo do que o caminho normal.

**Como aparece.** Bloco que captura a exceção e segue adiante, verificação de permissão dentro de um bloco cujo tratamento continua o fluxo, valor de retorno padrão que significa "permitido", tempo esgotado tratado como sucesso, e rastro de pilha devolvido na resposta.

**Como verificar.** Para cada captura de exceção, pergunte o que acontece se aquela operação falhar. Se a resposta for "o fluxo continua", é achado. Procure também a diferença de mensagem e de tempo de resposta entre casos válidos e inválidos, que revela informação sem precisar de erro nenhum.

**Não é achado.** Captura que registra e relança, ou que converte a falha em erro de negócio explícito e interrompe o fluxo.

---

## S18: Integridade da operação

**Severidade**: ALTA
**Referência**: OWASP A08:2025 | CWE-362

**Regra.** Operação que altera vários registros é atômica: ou tudo é confirmado, ou nada. Efeito externo irreversível acontece depois da confirmação, nunca dentro dela. Operação sensível a concorrência precisa de trava ou de verificação de versão.

**Como aparece.** Cadastro que grava em várias tabelas, importação em lote, fluxo que grava e em seguida notifica, e o par de leitura e escrita que decide com base em um valor que pode ter mudado no intervalo.

**Como verificar.** Procure envio de mensagem, cobrança, chamada a sistema externo e indexação dentro do bloco transacional: se a transação for desfeita, o efeito externo já aconteceu e não volta. Procure escrita múltipla sem transação. Em operação concorrente, confirme trava ou verificação de versão.

**Não é achado.** Efeito externo idempotente e reprocessável, com o controle de reprocessamento registrado.

---

## S19: Entrada não confiável que alcança modelo de IA

**Severidade**: ALTA
**Referência**: OWASP Top 10 para aplicações com LLM, itens de injeção de prompt, agência excessiva e divulgação de informação sensível

Este tópico se aplica somente quando o sistema integra um modelo de linguagem, um agente ou uma automação que decide ação. Não se aplica ao uso de assistentes de IA no desenvolvimento.

**Regra.** Texto vindo de terceiro é dado, nunca instrução. O componente de IA opera com a menor permissão possível, e toda ação com efeito passa por autorização própria, com confirmação quando for irreversível. A saída do modelo é entrada não confiável.

**Como aparece.** Busca sobre documento enviado por usuário, resumo de mensagem recebida, leitura de página externa, ferramenta que executa comando ou consulta, e componente que carrega uma credencial ampla porque isso simplificou a integração.

**Como verificar.** Confirme que instrução do sistema e conteúdo do usuário estão separados, e que o conteúdo recuperado de fontes externas não é tratado como instrução. Confirme o escopo da credencial que o componente carrega. Confirme que a saída do modelo passa pelos mesmos tratamentos das demais entradas antes de virar consulta, comando ou HTML: os tópicos S05, S06 e S07 continuam valendo.

**Não é achado.** Uso do modelo sem ferramentas e sem efeito colateral, com a saída apenas exibida e devidamente escapada.

---

## Rastreio para os referenciais externos

| Tópico | OWASP Top 10:2025 | CWE |
|---|---|---|
| S01 Autorização por função | A01 Broken Access Control | CWE-862, CWE-306, CWE-284 |
| S02 Autorização por objeto | A01 Broken Access Control | CWE-639, CWE-863 |
| S03 Autenticação e sessão | A07 Authentication Failures | CWE-287, CWE-384, CWE-916 |
| S04 Validação de entrada | A05 Injection | CWE-20 |
| S05 Injeção em consulta a dados | A05 Injection | CWE-89 |
| S06 Injeção em comando | A05 Injection | CWE-78, CWE-77, CWE-94 |
| S07 Saída para HTML e JavaScript | A05 Injection | CWE-79 |
| S08 Outros destinos de saída | A05 Injection | CWE-113, CWE-1236, CWE-117 |
| S09 Caminho de arquivo e upload | A01 Broken Access Control, A05 Injection | CWE-22, CWE-434 |
| S10 Desserialização e parser | A08 Software or Data Integrity Failures | CWE-502, CWE-611 |
| S11 Requisição de saída controlável | A01 Broken Access Control | CWE-918, CWE-601 |
| S12 Segredos e credenciais | A02 Security Misconfiguration, A04 Cryptographic Failures | CWE-798, CWE-200, CWE-532 |
| S13 Criptografia e aleatoriedade | A04 Cryptographic Failures | CWE-327, CWE-338 |
| S14 Configuração segura por padrão | A02 Security Misconfiguration | CWE-1188 |
| S15 Cadeia de fornecimento | A03 Software Supply Chain Failures | CWE-1395 |
| S16 Log, auditoria e alerta | A09 Security Logging and Alerting Failures | CWE-532, CWE-778 |
| S17 Tratamento de erro | A10 Mishandling of Exceptional Conditions | CWE-209, CWE-755, CWE-390 |
| S18 Integridade da operação | A08 Software or Data Integrity Failures | CWE-362 |
| S19 Entrada que alcança modelo de IA | Sem correspondência direta no Top 10 de aplicações | Sem CWE consolidado |

Duas categorias do Top 10:2025 não ganham tópico próprio aqui, e isso é intencional. **A06 Insecure Design** é resolvida antes do código, na fase de especificação, e a stack trata disso no fluxo do SpecKit. Falsificação de requisição entre sites, que o CWE registra como CWE-352, é tratada dentro de A01 e depende inteiramente do mecanismo do framework do projeto: registre a regra do seu framework no `AGENTS.md`.

## Referenciais consultados

| Referencial | Edição | Fonte |
|---|---|---|
| OWASP Top 10 | 2025 | <https://owasp.org/Top10/2025/> |
| OWASP Top 10 Proactive Controls | v4, 2024 | <https://top10proactive.owasp.org/> |
| OWASP Application Security Verification Standard | 5.0.0, maio de 2025 | <https://owasp.org/www-project-application-security-verification-standard/> |
| CWE Top 25 Most Dangerous Software Weaknesses | 2025 | <https://cwe.mitre.org/top25/archive/2025/2025_cwe_top25.html> |
| OWASP Cheat Sheet Series | contínua | <https://cheatsheetseries.owasp.org/> |
| OWASP Top 10 for LLM Applications | 2025 e posteriores | <https://owasp.org/www-project-top-10-for-large-language-model-applications/> |

Este guia é a camada de tópicos. Para o detalhe de verificação de cada requisito, o ASVS é o documento mais profundo, organizado em 17 capítulos, entre eles codificação e sanitização, validação, autenticação, sessão, autorização, criptografia, configuração e registro de eventos. Para a receita de implementação em uma tecnologia específica, use a Cheat Sheet Series.
