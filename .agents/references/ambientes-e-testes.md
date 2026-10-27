# Ambientes e testes do RF.Fusion

Este arquivo orienta a seleção de ambiente e teste durante desenvolvimento.
Os caminhos e acessos autorizados são definidos nos documentos
[do appCataloga](../../src/appCataloga/.instructions/ENVIRONMENT.md),
[do WebFusion](../../src/webfusion/.instructions/ENVIRONMENT.md) e
[da suíte](../../test/AGENTS.md); consulte-os antes de executar comandos.

Consulte ao preparar validação, investigar dependência ausente ou selecionar
uma suíte que cobre mais de uma aplicação.

## Identidade do ambiente e configuração local

O [contrato de ambientes](../../docs/architecture/AMBIENTES.md) define a
identificação dos containers. A configuração local fica em `.env/runtime.env`,
fora do Git; o [README da configuração](../../.env/README.md) descreve seu
preenchimento. O loader compartilhado lê `RFFUSION_ENV` sem executar o arquivo
como shell. Os deploys validam a configuração antes de comandos Podman, e os
entrypoints instalam a identidade visual pelo helper comum.

Essa identidade não seleciona conexões nem substitui credenciais ou as
restrições de execução dos `ENVIRONMENT.md`. Não deduza produção/desenvolvimento
pelo IP, branch ou montagem. Alterar o arquivo local não atualiza a identidade
de containers existentes; ativação é uma operação de deploy separada, com
preservação de dados especialmente relevante para o MariaDB.

## Escolher pelo import, não pela pasta

| Trabalho | Ambiente | Atenção |
|---|---|---|
| appCataloga exclusivo | container `debian12-python`; Python do ambiente `appdata` | o Python de sistema que roda ferramentas da stack não é o runtime do aplicativo |
| WebFusion, importação, compilação ou testes mistos | webserver por SSH | runtime `/usr/local/bin/python`, código montado em `/RF.Fusion` |
| Configuração de identidade e scripts de instalação | Python `appdata`, para `test/tests/install` | os testes usam Bash, árvores temporárias e Podman simulado, sem deploy real |
| SQL/locks MariaDB | banco isolado explicitamente selecionado | doubles não comprovam a semântica real de SQL e locks |
| Scripts Zabbix | ambiente conforme o componente importado | `test/tests/zabbix/test_zabbix_api.py` importa o cliente do WebFusion |

O workspace em `/RFFusion` é do appCataloga. Não conclua que esse caminho
existe no webserver; seu ambiente usa outra montagem. O acesso a outros
containers é por SSH, com os dados de acesso mantidos nas fontes operacionais.
Não ler credenciais para uma investigação estática.

**Alerta — suíte mista:** `test/test_all.sh` usa o Python do appCataloga por
padrão, mas sem argumentos seleciona toda a suíte. Antes de usá-lo, confira os
imports dos testes escolhidos:
`rg -n 'webfusion|from flask|import flask|from api|from modules' <testes-alvo>`.
A busca orienta a leitura dos imports, não classifica sozinha todas as
dependências transitivas. Siga a seleção de ambiente de `test/AGENTS.md`.

lacuna: a execução da suíte completa no webserver depende de compatibilidade
dos paths e das dependências de ambos os runtimes. `test/tests/_support.py`
define raízes absolutas em `/RFFusion`; o contrato WebFusion define
`/RF.Fusion`. A busca
`rg -n '^APP_ROOT|^CONFIG_ROOT|/RF.Fusion' test/tests/_support.py src/webfusion/.instructions/ENVIRONMENT.md`
mostra as duas raízes do helper e a montagem diferente no contrato. Confirme
os helpers e caminhos do conjunto selecionado antes de
prometer uma execução mista. A leitura de `_support.py` comprova o literal,
não uma adequação versionada da suíte. Ajustar `sys.path` num ensaio focado
não corrige os caminhos de todos os testes.

## Construir e selecionar testes

1. Escolha `test/tests/<território>/test_*.py` pela unidade coberta. A suíte usa
   `unittest` com descoberta e execução por `pytest`; preserve esse estilo.
2. Para carregar appCataloga, reutilize `_support.py` quando apropriado.
   `bind_real_package` evita que pacotes da suíte, como `shared` e `db`,
   ocultem os pacotes reais durante a importação.
3. Para rotas, use o cliente de teste do Flask, substitua efeitos externos da
   identidade e faça as requisições com os cabeçalhos do contrato testado.
   Para serviços, teste a regra diretamente quando HTTP não for o objeto.
4. Modele banco e rede com doubles compatíveis com as interfaces utilizadas.
   Use diretórios temporários para efeitos de filesystem. Não inicie workers,
   não escreva em banco compartilhado nem em pastas de dados de produção.
5. Confira resultados observáveis: status, payload, efeitos persistidos,
   idempotência e cenário de falha/concorrência aplicável.
6. Selecione o menor grupo que cobre a mudança e execute no ambiente autorizado.
   Os comandos existentes ficam no [README da suíte](../../test/README.md).
   Registre o comando real, quantidade de testes, resultado e limitações.

Para identidade dos containers, selecione `test/tests/install/test_environment.py`.
Esse grupo cobre o loader, a recusa de configuração inválida antes do Podman e
os arquivos/prompt gerados numa raiz temporária. O comando de descoberta está
no README da suíte; não execute deploys ou entrypoints reais para validar
documentação.

**Alerta — estilo de teste:** os arquivos `test_*.py` importam `unittest`.
Ao propor fixtures ou outro padrão para testes novos, compare a unidade
equivalente antes de introduzir uma segunda convenção:
`rg -n 'unittest|setUp|patch|TemporaryDirectory' <teste-equivalente>`.

## Dependências de validação

O runtime do appCataloga é definido por `environment.yml` e o WebFusion por
seu Containerfile, não pelo interpretador que estiver no PATH da sessão.
Verifique a disponibilidade de `pytest` e de cobertura no interpretador
selecionado antes de executar a suíte. Ausência de Flask no appCataloga não
autoriza instalar WebFusion nele.

Configurar cobertura, dependências de desenvolvimento ou um serviço de teste
é decisão separada, orientada pela skill `configuracao-de-testes-unitarios`.
Pacotes instalados apenas num container em execução desaparecem ao recriá-lo;
arquivos gerados como root podem ficar com esse dono no host. O plano deve
considerar persistência e o usuário do serviço, sem colocar ferramentas de
teste na imagem de produção por conveniência.

## Exemplares

| Caminho | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `test/tests/shared/test_filter.py` | carregamento explícito de pacotes appCataloga e contrato de filtro | `test_*.py` do helper em `tests/shared/` |
| `test/tests/workers/test_appcataloga_runtime_health.py` | subprocesso isolado com executável simulado e temporário | teste do script de health em `tests/workers/` |
| `test/tests/webfusion/test_upload.py` | cliente Flask, identidade substituída e armazenamento temporário | teste de rota do módulo em `tests/webfusion/` |
| `test/tests/zabbix/test_zabbix_api.py` | cliente externo com chamadas simuladas, apesar da localização da pasta | teste que importa `api.zabbix_api.client` |
| `test/tests/install/test_environment.py` | configuração dos componentes, Bash e identidade com filesystem temporário | teste do loader e do helper em `tests/install/` |

## Evidências

Comandos de busca executados em 2026-10-27, a partir da raiz. Nenhuma suíte de
produto foi executada como parte desta investigação.

| Afirmação | Confiança | Onde foi comprovada | Comando | Alcance | Verificado em |
|---|---|---|---|---|---|
| unittest nos testes automatizados | medido | 80 linhas de import em 48 de 48 arquivos; `pytest.ini` configura a descoberta | `rg -n -g 'test_*.py' '^import unittest\|^from unittest' test/tests \| wc -l` → 80; `rg -l -g 'test_*.py' '^import unittest\|^from unittest' test/tests \| wc -l` → 48 | `test/tests` | 2026-10-27 |
| Nenhum arquivo comparável sem o import | medido | zero contraexemplos | `rg --files-without-match -g 'test_*.py' '^import unittest\|^from unittest' test/tests` → vazio | `test/tests` | 2026-10-27 |
| Helper não escolhe container | medido | `test/test_all.sh:5,10,13` | leitura integral do helper, `test/AGENTS.md` e dos dois ENVIRONMENTs | helper da suíte | 2026-10-27 |
| Colisão de nomes de pacote | medido | `test/tests/_support.py:74` documenta e implementa binding temporário | leitura integral de `_support.py` | testes que usam o helper | 2026-10-27 |
| Paths da suíte mista | lacuna | `_support.py:19,20` e ENVIRONMENT WebFusion; caminhos versionados distintos | `rg -n '^APP_ROOT\|^CONFIG_ROOT\|/RF.Fusion' test/tests/_support.py src/webfusion/.instructions/ENVIRONMENT.md` mostra as duas raízes do helper e a montagem diferente | conjunto misto precisa de verificação própria; ensaio com path ajustado não adequa toda a suíte | 2026-10-27 |
| Loader comum nos deploys | medido | três deploys chamam a validação antes de comandos Podman; `install/common/environment.sh` valida o arquivo sem executá-lo | `rg -l -g 'deploy*.sh' '^rffusion_load_environment ' install \| wc -l` → 3; leitura do loader e dos três pontos de chamada | appCataloga, WebFusion e MariaDB | 2026-10-27 |
| Identidade nos entrypoints | medido | três entrypoints chamam o helper antes dos serviços | `rg -l -g '*entrypoint.sh' '^bash /RF[.]?Fusion/install/common/container-identity[.]sh$' install \| wc -l` → 3; leitura de `install/common/container-identity.sh` | entrypoints dos três containers; não comprova ativação em containers existentes | 2026-10-27 |
