# WebFusion

O WebFusion e a interface operacional do RF.Fusion. Ele concentra a consulta
dos dados processados, o acompanhamento da infraestrutura, a operação
controlada de filas e a configuração de estações integradas ao Zabbix.

Este documento descreve apenas este módulo. Para a visão geral da plataforma,
consulte [README.md](/RFFusion/README.md).

## Papel no Sistema

O WebFusion não executa backup, descoberta, processamento ou teste de rede no
processo HTTP. Ele:

1. consulta dados operacionais e analíticos para apresentar ao operador;
2. cria ou ajusta registros persistidos que o appCataloga consumirá;
3. consulta e atualiza macros do Zabbix nos fluxos autorizados;
4. publica métricas consolidadas para navegação e monitoramento.

Essa separação evita que uma requisição do navegador bloqueie os workers de
coleta ou processamento. O teste de conectividade, por exemplo, cria uma
HOST_TASK prioritária e acompanha seu estado por polling. ICMP e SSH são
executados pelo appCataloga.

## Acesso e Runtime

O processo Python usa Flask, Jinja e Waitress. O Nginx do container WebFusion
fica à frente da aplicação, encaminha as requisições HTTP e pode servir arquivos
do repositório sem transferi-los pelo processo Python.

    navegador -> Nginx :9082 -> Waitress 127.0.0.1:8000 -> Flask

O prefixo público é configurado pelo Nginx:

    http://<servidor>:9082/rffusion/

Internamente, a aplicação continua servindo suas rotas a partir de /. O
ProxyFix em [app.py](/RFFusion/src/webfusion/app.py) usa o cabeçalho
X-Forwarded-Prefix enviado pelo Nginx para gerar URLs públicas corretas sob
/rffusion/.

Endpoints de verificação:

    interno: http://127.0.0.1:8000/api/users
    público: http://<servidor>:9082/rffusion/api/server/runtime-health

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
| RFFUSION_SUMMARY | mapa, snapshots, erros, volumes e agregações rápidas |
| Zabbix | macros efetivas por template e estação |
| /mnt/reposfi | arquivos disponíveis para download pelo Nginx |

Os módulos que integram o Zabbix usam o pacote
[zabbix_api](/RFFusion/src/webfusion/api/zabbix_api), que centraliza a conexão e
as chamadas compartilhadas da API.
As variáveis ZABBIX_API_URL, ZABBIX_API_TOKEN e
ZABBIX_API_TIMEOUT_SECONDS podem vir do ambiente ou de
src/zabbix/.secret/zabbix_api.env. Esse arquivo contém segredo operacional e
não deve ser versionado nem exposto na interface.

Ao atualizar macros SSH suportadas em uma estação, o fluxo tenta sincronizar o
valor operacional correspondente em BPDATA.HOST depois de a alteração no
Zabbix ser confirmada. A interface informa quando a alteração no Zabbix foi
aceita, mas a sincronização operacional não foi concluída.

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

### Panorama do Servidor

O dashboard /server separa o shell inicial dos painéis de consulta tardia. Ele
também fornece uma leitura plana para o template Zabbix RFFusion_server:

    /api/server/zabbix_metrics

O payload é JSON simples. Valores de memória, repositório e volume são expostos
em bytes nas chaves correspondentes, para consumo numérico direto pelo Zabbix.

O card "Saúde dos Containers" é consultado manualmente e não depende de
BPDATA, RFDATA ou RFFUSION_SUMMARY. A cada clique, o WebFusion verifica o seu
Nginx, Waitress e endpoint interno; em paralelo, chama por SSH os scripts
somente-leitura do appCataloga e do MariaDB. Nenhum valor anterior é armazenado
ou reutilizado: se um container não responder, a interface mostra a falha atual.

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

## Autenticação e Segurança

Os módulos de escrita e configuração usam autenticação HTTP Basic leve:

- /tasks/
- /configuration/
- operações de teste em /host

Essa proteção é temporária e voltada ao ambiente operacional atual. As
credenciais não devem ser copiadas para documentação, JavaScript ou URLs. Uma
evolução futura deve migrar esses módulos para uma autenticação centralizada.

Valores de macros secretas permanecem ocultos. Para alterá-los, o operador
informa um novo valor, sem recuperar o segredo armazenado no Zabbix.

## Organização do Código

    src/webfusion/
    |-- app.py                         aplicação Flask e rotas de infraestrutura
    |-- db.py                          conexões MariaDB
    |-- .secret                        configuração local de conexões MariaDB (não versionada)
    |-- modules/
    |   |-- map/                       dados do mapa inicial
    |   |-- spectrum/                  consulta e download de espectros
    |   |-- server/                    panorama e métricas Zabbix
    |   |-- host/                      consulta e teste de estações
    |   |-- task/                      criação e acompanhamento de tarefas
    |   |-- maintenance/               manutenção de filas e histórico
    |   +-- configuration/             macros Zabbix e sincronização operacional
    |-- templates/                     páginas Jinja
    +-- static/                        CSS, JavaScript, fontes e imagens

O padrão de cada feature é:

1. routes.py recebe a requisição, valida a entrada e monta a resposta HTTP;
2. service.py contém consultas, normalização e regras da feature;
3. templates renderizam o shell inicial;
4. JavaScript carrega detalhes, filtros e tabelas sob demanda.

## Execução Local

### Configuração local do banco

Antes de iniciar o WebFusion, crie `src/webfusion/.secret`. O arquivo é
ignorado pelo Git e deve conter os quatro dicionários `DB_CFG_RFDATA`,
`DB_CFG_BPDATA`, `DB_CFG_RFFUSION_SUMMARY` e `DB_CFG_WEBFUSION`. Cada
dicionário requer as chaves `host`, `port`, `user`, `password` e `database`.
Não versione, publique ou registre os valores desse arquivo.

O entrypoint é [app.py](/RFFusion/src/webfusion/app.py). As variáveis aceitas
em execução direta são:

- WEBFUSION_HOST, padrão 127.0.0.1
- WEBFUSION_PORT, padrão 8000
- WEBFUSION_THREADS, padrão 8
- WEBFUSION_CHANNEL_TIMEOUT, padrão 300

Exemplo para desenvolvimento, dentro de um ambiente Python com as dependências
do projeto instaladas:

    cd /RFFusion/src/webfusion
    python app.py

Na implantação normal, use o container descrito em
[install/webserver/README.MD](/RFFusion/install/webserver/README.MD). Não
substitua o Nginx pelo servidor de desenvolvimento Flask em ambiente
operacional.

## Validação

Os testes do WebFusion ficam em [test/tests/webfusion](/RFFusion/test/tests/webfusion).
Para executar somente essa suíte:

    /opt/conda/envs/appdata/bin/pytest -q /RFFusion/test/tests/webfusion

Antes de publicar alterações de rota, valide pelo menos:

1. GET /rffusion/api/server/runtime-health pelo Nginx;
2. carregamento das páginas afetadas sob o prefixo /rffusion/;
3. uma operação de escrita em ambiente controlado, quando houver;
4. ausência de segredos no HTML, JSON, logs e mensagens de erro.

## Referências

- [README.md](/RFFusion/README.md)
- [install/webserver/README.MD](/RFFusion/install/webserver/README.MD)
- [src/webfusion/api/zabbix_api](/RFFusion/src/webfusion/api/zabbix_api)
- [src/zabbix](/RFFusion/src/zabbix)
- [src/mariadb/scripts/README.md](/RFFusion/src/mariadb/scripts/README.md)
