# WebFusion

O WebFusion é a interface operacional do RF.Fusion. Ele concentra a consulta
dos dados processados, o acompanhamento da infraestrutura, a operação
controlada de filas e a configuração de estações integradas ao Zabbix.

Este documento descreve apenas este módulo. Para a visão geral da plataforma,
consulte [README.md](../../README.md).

## Papel no Sistema

O WebFusion não executa backup, descoberta, processamento de espectros ou
testes ICMP/SSH das estações no processo HTTP. Ele:

1. consulta dados operacionais e analíticos para apresentar ao operador;
2. cria ou ajusta registros persistidos que o appCataloga consumirá;
3. consulta e atualiza macros do Zabbix nos fluxos autorizados;
4. publica métricas consolidadas para navegação e monitoramento.

Essa separação evita que uma requisição do navegador bloqueie os workers de
coleta ou processamento. O teste de conectividade, por exemplo, cria uma
HOST_TASK prioritária e acompanha seu estado por polling. ICMP e SSH das estações são
executados pelo appCataloga. A verificação da infraestrutura dos containers é
um fluxo separado, descrito em Panorama do Servidor.

## Acesso e Runtime

O processo Python usa Flask, Jinja e Waitress. O Nginx do container WebFusion
fica à frente da aplicação, encaminha as requisições HTTP e pode servir arquivos
do repositório sem transferi-los pelo processo Python.

    navegador/cliente MATLAB -> host :9082 -> NGINX do container :80 -> Waitress :8000 -> Flask

O NGINX remove `/rffusion` antes de encaminhar ao Flask. O `ProxyFix` em
[app.py](app.py) utiliza `X-Forwarded-Prefix` para gerar os caminhos públicos.
As URLs de acesso são:

| Acesso | URL-base |
|---|---|
| Público, via F5 | `https://fiscalizacao.anatel.gov.br/rffusion` |
| Rede interna/VPN, IP | `http://172.16.18.11:9082/rffusion` |
| Rede interna/VPN, hostname | `http://rhfisnspdex02.anatel.gov.br:9082/rffusion` |

Salvo indicação contrária, os caminhos `/api/...`, `/spectrum` e `/tasks/...`
abaixo são relativos a essa base. O prefixo é acrescentado uma única vez.
O acesso público continua sujeito às políticas do F5; conhecer a URL não
substitui os requisitos de acesso do ambiente.

O runtime autorizado é o container `rffusion-web`, com Python 3.11 e o
interpretador `/usr/local/bin/python`. O projeto fica montado em `/RF.Fusion`.
O container escuta HTTP na porta 80; a porta 9082 pertence ao host.

Endpoints de verificação:

- `GET /api/users`: identidade encaminhada, resposta 204;
- `GET /api/server/runtime-health`: estado dos serviços, resposta JSON;
- `GET /api/map/stations`: pontos do mapa, resposta JSON.

O endpoint `GET /api/users/login` responde com status HTTP 204 e corpo vazio.
O cabeçalho `X-User-Profile` contém o perfil persistido como um objeto JSON no
mesmo formato retornado no body de `GET /api/users/me`, incluindo `NA_ROLE`,
`IS_ADMIN` e `IS_DEVELOPER`. Sem identidade ou perfil persistido, o valor do
cabeçalho é `{}`.

### Controle de acesso

O controle de acesso usa os cabeçalhos de identidade encaminhados pelo F5:
`X-User-Name`, `X-User-Email`, `X-User-Job-Title`, `X-User-Department` e
`X-User-Location`. O endpoint `GET /api/users` devolve esses valores em
cabeçalhos de resposta, junto de `X-User-Roles` e `X-User-Avatar-Url`, com
status HTTP 204. A identidade é exibida na barra superior. Quando o e-mail
é observado, o WebFusion o registra em `WEBFUSION.USERS` ou atualiza os
atributos já cadastrados para o mesmo e-mail. O e-mail em minúsculas é a chave
inequívoca da identidade.

As páginas de criação e manutenção de tarefas, administração de usuários e
configuração de estações exigem que o e-mail encaminhado esteja ativo em
`WEBFUSION.ADMINS` ou `WEBFUSION.DEVELOPERS`. A tabela `ADMINS` tem precedência
quando um e-mail estiver nas duas listas. Os demais módulos permanecem
disponíveis em modo de consulta, inclusive quando o acesso ocorre sem F5 e,
portanto, sem cabeçalhos: nesse caso a interface identifica o visitante como
`Anônimo`.

Antes de publicar o controle, aplique o schema
`src/mariadb/scripts/createWebFusionDB.sql` no MariaDB e cadastre os usuários
autorizados. O e-mail deve ser normalizado em minúsculas, pois é a chave usada
na autorização. Exemplo de cadastro:

```sql
USE WEBFUSION;

INSERT INTO DEVELOPERS (
    NA_USER_NAME,
    NA_USER_EMAIL,
    NA_JOB_TITLE,
    NA_DEPARTMENT,
    NA_LOCATION
) VALUES (
    'Nome do usuário',
    'nome.usuario@anatel.gov.br',
    'Cargo',
    'Unidade',
    'Brasília'
);
```

O proxy público deve remover quaisquer cabeçalhos `X-User-*` recebidos do
cliente e encaminhar somente os valores produzidos pelo F5. Sem essa regra, um
cliente que alcance diretamente o upstream poderia forjar uma identidade.

## Dados e Integrações

| Origem | Uso no WebFusion |
|---|---|
| BPDATA | hosts, HOST_TASK, FILE_TASK e FILE_TASK_HISTORY |
| RFDATA | espectros, equipamentos, localidades e metadados analíticos |
| RFFUSION_SUMMARY | mapa, snapshots, erros, volumes, métricas de uso e checkpoints |
| WEBFUSION | usuários observados e perfis de acesso |
| Zabbix | macros efetivas por template e estação |
| /mnt/reposfi | arquivos disponíveis para download pelo Nginx |

Os módulos que integram o Zabbix usam o pacote
[zabbix_api](api/zabbix_api/README.md), que centraliza a conexão e
as chamadas compartilhadas da API.
As variáveis ZABBIX_API_URL, ZABBIX_API_TOKEN e
ZABBIX_API_TIMEOUT_SECONDS podem vir do ambiente ou de
src/zabbix/.secret/zabbix_api.env. Esse arquivo contém segredo operacional e
não deve ser versionado nem exposto na interface.

Ao atualizar macros SSH suportadas em uma estação, o fluxo tenta sincronizar o
valor operacional correspondente em BPDATA.HOST depois de a alteração no
Zabbix ser confirmada. A interface informa quando a alteração no Zabbix foi
aceita, mas a sincronização operacional não foi concluída.

## APIs compartilhadas e appAnalise

O diretório [api/](api/README.md) concentra os contratos compartilhados:

| Pacote | Finalidade | Documentação |
|---|---|---|
| `map_api` | Pontos e detalhes do mesmo mapa usado pela interface | [README](api/map_api/README.md) |
| `appAnalise_api` | Catálogos, arquivos, contagem, espectros e estatísticas | [README](api/appAnalise_api/README.md) |
| `zabbix_api` | Cliente Python da API externa JSON-RPC do Zabbix | [README](api/zabbix_api/README.md) |

O appAnalise consulta operações nomeadas; o SQL permanece no WebFusion.
A base é `/api/appanalise`, sem `/v1`. Filtros podem ser enviados por GET
ou POST JSON, conforme a operação. As respostas tabulares mantêm `columns`
e `rows`, inclusive quando o resultado é vazio.

O mapa é compartilhado em `/api/map/stations?include_details=true`.
Não existe `/api/appanalise/map`, nem endpoints paralelos para suas linhas
brutas. As consultas de interface em `modules/spectrum` e as do appAnalise
mantêm contratos próprios; não são URLs intercambiáveis.

O cliente MATLAB pode escolher a base pública ou interna. A integração não
introduz autenticação própria no cliente nem detecção automática de VPN.
Os READMEs dos pacotes documentam o contrato HTTP; arquivos MATLAB mantidos
externamente devem ser conferidos contra esse contrato.

## Módulos

| Módulo | Rota pública relativa | Finalidade |
|---|---|---|
| Mapa | / | distribuição geográfica de estações e localidades atuais ou históricas |
| Consulta aos Arquivos | /spectrum | consulta de arquivos e espectros, com download controlado |
| Panorama do Servidor | /server | indicadores de host, filas, acervo, erros, uso e repositório |
| Consulta às Estações | /host | visão operacional, geográfica e histórica de uma estação |
| Tarefas | /tasks/ | criação de solicitações, consulta e intervenção controlada nas filas e no histórico |
| Usuários | /users/ | consulta, cadastro manual e concessão ou revogação de privilégios |
| Configurações | /configuration/ | macros Zabbix por template ou estação |
| Alarmes | /alarms/ | problemas operacionais consultados no Zabbix |

As rotas da tabela são relativas ao prefixo público. Por exemplo, o panorama
do servidor é acessado em http://<servidor>:9082/rffusion/server.

### Mapa

O mapa usa tabelas de resumo para evitar joins analíticos pesados. Pontos e
detalhes são carregados sob demanda por /api/map/stations e
/api/map/stations/<site_id>.

### Consulta aos Arquivos

/spectrum oferece filtros geográficos, de período, frequência e descrição. Os
detalhes de espectro e localidades são carregados depois da página inicial.

Endpoints relevantes:

- /api/spectrum/filters
- /api/spectrum/localities
- /api/spectrum/file/<file_id>/spectra
- /spectrum/download/<spectrum_id>
- /spectrum/download-file/<file_id>

### Downloads

Os links diretos usam `/downloads/...`, na raiz do servidor, **fora de
`/rffusion`**. O NGINX entrega os arquivos de `/mnt/reposfi` por `alias`.
As rotas Flask de download também podem delegar a transferência ao NGINX por
`X-Accel-Redirect`, com a localização interna `/_repo_download/`.

A configuração do container usa:

- `WEBFUSION_PUBLIC_DOWNLOAD_PREFIX=/downloads`;
- `WEBFUSION_ACCEL_REDIRECT_PREFIX=/_repo_download`;
- `WEBFUSION_ACCEL_REDIRECT_ROOT=/mnt/reposfi`.

Sem a delegação configurada, as rotas Flask conservam o fallback de envio do
arquivo. Na implantação normal, mantenha o NGINX responsável pela transferência.
A API appAnalise retorna metadados, inclusive `NA_PATH`; consultar `/files`
não equivale a baixar os arquivos binários.

A tentativa de selecionar uma rota interna por JavaScript foi removida.
O endereço usado para abrir a página determina a origem dos links relativos:
HTTPS pelo acesso público e HTTP pelo acesso interno. Não existe endpoint
`/api/download-route`, cookie de seleção ou sondagem de VPN no fluxo atual.

### Panorama do Servidor

O dashboard /server separa o shell inicial dos painéis de consulta tardia. Ele
também fornece uma leitura plana para o template Zabbix RFFusion_server:

    /api/server/zabbix_metrics

O payload é JSON simples. Valores de memória, repositório e volume são expostos
em bytes nas chaves correspondentes, para consumo numérico direto pelo Zabbix.

O card "Saúde dos Containers" é consultado manualmente e não depende de
BPDATA, RFDATA ou RFFUSION_SUMMARY. A cada clique, o WebFusion verifica o seu
Nginx, Waitress e endpoint interno; em paralelo, chama por SSH os scripts
somente-leitura do appCataloga e do MariaDB. O resultado usa um cache curto no processo, descrito abaixo; ele não é
um histórico persistido de disponibilidade.

Os scripts executados diretamente pelos volumes montados são:

- src/webfusion/scripts/webfusion_runtime_health.sh
- src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/appCataloga_runtime_health.sh
- src/mariadb/scripts/mariadb_runtime_health.sh

O acesso remoto usa uma chave exclusiva com comando forçado. Essa chave não
recebe terminal, encaminhamento de portas, agente SSH nem permissão para
executar outros comandos. O WebFusion não recebe acesso ao socket ou à CLI do
Podman.

Para habilitar a verificação remota na VM já em operação, gere a chave fora do
repositório:

    install -d -m 700 /RFFusion-dev/.secrets
    ssh-keygen -t ed25519 -f /RFFusion-dev/.secrets/rffusion_runtime_health_ed25519 -N '' -C rffusion-runtime-health
    chmod 600 /RFFusion-dev/.secrets/rffusion_runtime_health_ed25519
    export RUNTIME_HEALTH_SSH_PUBLIC_KEY="$(cat /RFFusion-dev/.secrets/rffusion_runtime_health_ed25519.pub)"

Não use os scripts de deploy apenas para habilitar esta leitura: eles podem
recriar containers e não são necessários para a configuração inicial. Com os
containers em execução, instale a chave pública restrita por `podman exec` e
recarregue somente o SSHD. Depois, atualize o arquivo de chaves dos hosts
internos:

    podman exec -e "RUNTIME_HEALTH_SSH_PUBLIC_KEY=${RUNTIME_HEALTH_SSH_PUBLIC_KEY}" debian12-python bash -lc '
      install -d -m 700 /root/.ssh
      health_key="restrict,command=\"/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/appCataloga_runtime_health.sh\" ${RUNTIME_HEALTH_SSH_PUBLIC_KEY}"
      touch /root/.ssh/authorized_keys
      grep -Fqx "${health_key}" /root/.ssh/authorized_keys || printf "%s\n" "${health_key}" >> /root/.ssh/authorized_keys
      chmod 600 /root/.ssh/authorized_keys
      sshd -t && pkill -HUP sshd
    '
    podman exec -e "RUNTIME_HEALTH_SSH_PUBLIC_KEY=${RUNTIME_HEALTH_SSH_PUBLIC_KEY}" debian12-mariadb bash -lc '
      install -d -m 700 /root/.ssh
      health_key="restrict,command=\"/RFFusion/src/mariadb/scripts/mariadb_runtime_health.sh\" ${RUNTIME_HEALTH_SSH_PUBLIC_KEY}"
      touch /root/.ssh/authorized_keys
      grep -Fqx "${health_key}" /root/.ssh/authorized_keys || printf "%s\n" "${health_key}" >> /root/.ssh/authorized_keys
      chmod 600 /root/.ssh/authorized_keys
      sshd -t && pkill -HUP sshd
    '
    ssh-keyscan -H 10.88.0.2 > /RFFusion-dev/.secrets/rffusion_runtime_health_known_hosts
    ssh-keyscan -H -p 2828 10.88.0.33 >> /RFFusion-dev/.secrets/rffusion_runtime_health_known_hosts
    chmod 600 /RFFusion-dev/.secrets/rffusion_runtime_health_known_hosts

Copie a chave privada e o `known_hosts` para `/run/secrets` do container
WebFusion em execução, com permissão `0600`. Em um deploy futuro, os scripts
de deploy aceitam RUNTIME_HEALTH_SSH_PUBLIC_KEY,
RUNTIME_HEALTH_SSH_PRIVATE_KEY_PATH e RUNTIME_HEALTH_SSH_KNOWN_HOSTS_PATH para
reaplicar a mesma configuração sem incluir segredos na imagem.

    podman exec rffusion-web mkdir -p /run/secrets
    podman cp /RFFusion-dev/.secrets/rffusion_runtime_health_ed25519 rffusion-web:/run/secrets/rffusion_runtime_health_ed25519
    podman cp /RFFusion-dev/.secrets/rffusion_runtime_health_known_hosts rffusion-web:/run/secrets/rffusion_runtime_health_known_hosts
    podman exec rffusion-web sh -c 'chmod 600 /run/secrets/rffusion_runtime_health_ed25519 /run/secrets/rffusion_runtime_health_known_hosts'

Os endereços e portas acima correspondem à rede Podman atual. Caso sejam
alterados, o WebFusion aceita as variáveis
WEBFUSION_RUNTIME_HEALTH_APPCATALOGA_HOST,
WEBFUSION_RUNTIME_HEALTH_APPCATALOGA_PORT,
WEBFUSION_RUNTIME_HEALTH_MARIADB_HOST e
WEBFUSION_RUNTIME_HEALTH_MARIADB_PORT. O timeout pode ser limitado entre um e
cinco segundos por WEBFUSION_RUNTIME_HEALTH_TIMEOUT_SECONDS.

Para evitar rodadas repetidas de SSH, o último snapshot é reutilizado por 15
segundos. Ajuste esse intervalo entre cinco e 60 segundos com
WEBFUSION_RUNTIME_HEALTH_CACHE_SECONDS. Durante uma atualização, novas
consultas recebem o último snapshot disponível sem abrir novos checks.

Os deploys dos containers recriam as chaves de host SSH. Por isso, depois de
reimplantar appCataloga ou MariaDB, gere novamente o arquivo known_hosts e
copie-o novamente para o WebFusion antes de voltar a consultar o card.

### Métricas de uso

`GET /api/server/usage-metrics` agrega os contadores por mês, ano e total:
visualizações, consultas de espectro, cliques de download e requisições de
arquivos observadas no NGINX. Cliques (`download_action_count`) e entregas
registradas (`nginx_download_count`) são métricas distintas.

A ingestão considera status 200/206 e mantém os contadores e o ponto de leitura
em `WEBFUSION_USAGE_METRIC_MONTHLY` e `WEBFUSION_USAGE_LOG_CHECKPOINT`, no summary.
A implementação atual usa uma assinatura SHA-256 do conteúdo já lido para
preservar o ponto de leitura quando inode/dispositivo mudam após reinícios.
Linhas incompletas ficam para a próxima passagem.

Consulte [contagem, reconciliação e migração do checkpoint](../../install/webserver/DOWNLOAD_METRICS.md).
A existência da correção no código não confirma sua ativação em um processo
antigo: mudanças Python exigem recarregar o WebFusion pelo procedimento de deploy.

### Consulta às Estações

/host apresenta a situação de uma estação, erros agregados, localização e
histórico. O teste de conectividade cria uma tarefa prioritária e a janela
acompanha seu progresso sem executar ICMP ou SSH no WebFusion.

- POST /api/host/<host_id>/connectivity-test
- GET /api/host/<host_id>/connectivity-test/<task_id>

### Tarefas

/tasks/create transforma a intenção do operador em uma solicitação persistida.
As ações incluem envio ou retirada da fila de backup, descoberta incremental,
descoberta completa e teste de conectividade.

Para backup, caminho e extensão são consultados das macros efetivas do Zabbix,
respeitando sobrescritas da estação. A página não se conecta diretamente à
estação remota.

/tasks/stations, /tasks/files e /tasks/history separam as tarefas de estação, a
fila de arquivos e a recuperação pelo histórico. As consultas são limitadas.
Uma consulta ao histórico sem filtros é ordenada pelas entradas processadas
mais recentes, evitando carregamentos integrais da tabela de histórico.

As ações disponíveis são validadas no servidor conforme o estágio e a situação
selecionados. O módulo não substitui a máquina de estados do appCataloga.

Os endpoints JSON do módulo seguem a raiz unificada `/api/tasks`:

- GET /api/tasks/file-task-hosts
- GET /api/tasks/host/<host_id>/backup-defaults
- GET /api/tasks/hosts/backup-defaults

### Configurações

/configuration/ apresenta macros por template ou por estação. Para uma
estação, separa valores próprios e herdados, indicando sobrescritas.

As alterações de macros são enviadas por POST para
/api/configuration/macro.

Campos protegidos e valores secretos não são revelados. As alterações exigem
confirmação na interface e são aplicadas ao Zabbix antes de qualquer tentativa
de sincronização com o banco operacional.

## Identidade e permissões

A autorização das páginas protegidas usa a identidade encaminhada e os perfis
persistidos descritos em **Controle de acesso**. Não há autenticação HTTP Basic
nos módulos de tarefas e configuração. As operações de conectividade em `/api/host/.../connectivity-test` ainda
verificam HTTP Basic no handler de host; são uma exceção ao controle por perfil
dos blueprints protegidos. Não registre as credenciais operacionais na documentação.

As APIs compartilhadas de mapa e appAnalise não têm um token próprio nem um
fluxo de login implementado nesses pacotes. Seu acesso segue o perímetro do
ambiente e a configuração do proxy. A autenticação técnica do cliente Zabbix
é uma integração separada, descrita no README desse pacote.

Valores de macros secretas permanecem ocultos. Para alterá-los, o operador
informa um novo valor, sem recuperar o segredo armazenado no Zabbix.

## Organização do Código

```text
src/webfusion/
├── app.py                 montagem Flask e registro dos blueprints
├── db.py                  fábricas de conexão MariaDB
├── auth/                  identidade, perfis e persistência de usuários
├── api/
│   ├── appAnalise_api/     contrato de consulta para o cliente MATLAB
│   ├── map_api/            mapa compartilhado
│   └── zabbix_api/         cliente JSON-RPC externo
├── modules/
│   ├── spectrum/          arquivos, filtros e downloads
│   ├── server/            panorama, métricas de uso e saúde
│   ├── host/              consulta e teste de estações
│   ├── tasks/             criação, filas e histórico
│   ├── users/             administração de usuários e privilégios
│   ├── configuration/     macros e sincronização operacional
│   └── alarms/            problemas operacionais
├── scripts/               verificação do runtime
├── templates/             páginas Jinja
└── static/                JavaScript, CSS, fontes e imagens
```

As APIs específicas da interface permanecem nos módulos correspondentes.
O diretório `api/` não pretende conter toda rota cujo caminho comece com `/api`.
O mapa inicial recebe o dataset diretamente do serviço compartilhado, sem
uma requisição HTTP do Flask para ele próprio.

O padrão de cada feature é:

1. routes.py recebe a requisição, valida a entrada e monta a resposta HTTP;
2. service.py contém consultas, normalização e regras da feature;
3. templates renderizam o shell inicial;
4. JavaScript carrega detalhes, filtros e tabelas sob demanda.

## Configuração e execução no container

### Configuração local do banco

Antes de iniciar o WebFusion, crie `src/webfusion/.secret`. O arquivo é
ignorado pelo Git e deve conter os quatro dicionários `DB_CFG_RFDATA`,
`DB_CFG_BPDATA`, `DB_CFG_RFFUSION_SUMMARY` e `DB_CFG_WEBFUSION`. Cada
dicionário requer as chaves `host`, `port`, `user`, `password` e `database`.
Não versione, publique ou registre os valores desse arquivo.

O entrypoint é [app.py](app.py). As variáveis aceitas
em execução direta são:

- WEBFUSION_HOST, padrão 127.0.0.1
- WEBFUSION_PORT, padrão 8000
- WEBFUSION_THREADS, padrão 8
- WEBFUSION_CHANNEL_TIMEOUT, padrão 300

Para execução manual de desenvolvimento, somente no webserver e sem outro
processo ocupando a porta escolhida:

```bash
cd /RF.Fusion/src/webfusion
/usr/local/bin/python app.py
```

O acesso operacional ao container é por `ssh root@10.88.0.34`; use a credencial
local conforme as instruções de ambiente, sem copiá-la para comandos ou Git.
Não instale dependências WebFusion no container appCataloga para executar testes.

Na implantação normal, use o container descrito em
[install/webserver/README.MD](../../install/webserver/README.MD). Não
substitua o Nginx pelo servidor de desenvolvimento Flask em ambiente
operacional.

## Validação

Os testes do WebFusion ficam em [test/tests/webfusion](../../test/tests/webfusion).
Execute os testes no webserver com o interpretador autorizado. Exemplo de
verificação das APIs compartilhadas e da montagem da aplicação:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest \
  test.tests.webfusion.test_appanalise_api \
  test.tests.webfusion.test_map_api \
  test.tests.webfusion.test_map_service \
  test.tests.webfusion.test_app_headers -q
```

O [índice de APIs](api/README.md) lista as verificações por pacote. Para mudanças
em contadores, execute `test.tests.webfusion.test_server_routes` separadamente.
As suítes usam stubs em alguns módulos; os testes unitários não substituem a
verificação HTTP pelo NGINX nem a integração com o cliente MATLAB.

Antes de publicar alterações de rota, valide pelo menos:

1. GET /rffusion/api/server/runtime-health pelo Nginx;
2. carregamento das páginas afetadas sob o prefixo /rffusion/;
3. uma operação de escrita em ambiente controlado, quando houver;
4. ausência de segredos no HTML, JSON, logs e mensagens de erro.

## Referências

- [README.md](../../README.md)
- [install/webserver/README.MD](../../install/webserver/README.MD)
- [src/webfusion/api/zabbix_api](api/zabbix_api/README.md)
- [src/zabbix](../zabbix)
- [src/mariadb/scripts/README.md](../mariadb/scripts/README.md)
