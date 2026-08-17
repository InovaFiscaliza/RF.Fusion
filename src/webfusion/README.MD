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

    interno: http://127.0.0.1:8000/health
    público: http://<servidor>:9082/rffusion/health

## Dados e Integrações

| Origem | Uso no WebFusion |
|---|---|
| BPDATA | hosts, HOST_TASK, FILE_TASK e FILE_TASK_HISTORY |
| RFDATA | espectros, equipamentos, localidades e metadados analíticos |
| RFFUSION_SUMMARY | mapa, snapshots, erros, volumes e agregações rápidas |
| Zabbix | macros efetivas por template e estação |
| /mnt/reposfi | arquivos disponíveis para download pelo Nginx |

O módulo de configuração usa [zabbix_api.py](/RFFusion/src/zabbix/zabbix_api.py).
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
| Criação de Tarefas | /task/ | criação individual ou coletiva de operações de backup e descoberta |
| Manutenção de Tarefas | /maintenance/ | consulta e intervenção controlada nas filas e no histórico |
| Configuração de Estações | /host-configuration/ | macros Zabbix por template ou estação |

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

    /server/zabbix_metrics

O payload é JSON simples. Valores de memória, repositório e volume são expostos
em bytes nas chaves correspondentes, para consumo numérico direto pelo Zabbix.

### Consulta às Estações

/host apresenta a situação de uma estação, erros agregados, localização e
histórico. O teste de conectividade cria uma tarefa prioritária e a janela
acompanha seu progresso sem executar ICMP ou SSH no WebFusion.

- POST /api/host/<host_id>/connectivity-test
- GET /api/host/<host_id>/connectivity-test/<task_id>

### Criação de Tarefas

/task/ transforma a intenção do operador em HOST_TASK persistida. As ações
incluem envio ou retirada da fila de backup, descoberta incremental, descoberta
completa e teste de conectividade.

Para backup, caminho e extensão são consultados das macros efetivas do Zabbix,
respeitando sobrescritas da estação. A página não se conecta diretamente à
estação remota.

### Manutenção de Tarefas

/maintenance/ oferece painéis para tarefas de host, fila de arquivos e
histórico de arquivos. As consultas são limitadas. Uma consulta ao histórico
sem filtros é ordenada pelas entradas processadas mais recentes, evitando
carregamentos integrais de FILE_TASK_HISTORY.

As ações disponíveis são validadas no servidor conforme o estágio e a situação
selecionados. O módulo não substitui a máquina de estados do appCataloga.

### Configuração de Estações

/host-configuration/ apresenta macros por template ou por estação. Para uma
estação, separa valores próprios e herdados, indicando sobrescritas.

Campos protegidos e valores secretos não são revelados. As alterações exigem
confirmação na interface e são aplicadas ao Zabbix antes de qualquer tentativa
de sincronização com o banco operacional.

## Autenticação e Segurança

Os módulos de escrita e configuração usam autenticação HTTP Basic leve:

- /task/
- /maintenance/
- /host-configuration/
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
    |-- modules/
    |   |-- map/                       dados do mapa inicial
    |   |-- spectrum/                  consulta e download de espectros
    |   |-- server/                    panorama e métricas Zabbix
    |   |-- host/                      consulta e teste de estações
    |   |-- task/                      criação e acompanhamento de tarefas
    |   |-- maintenance/               manutenção de filas e histórico
    |   +-- zabbix_configuration/      macros Zabbix e sincronização operacional
    |-- templates/                     páginas Jinja
    +-- static/                        CSS, JavaScript, fontes e imagens

O padrão de cada feature é:

1. routes.py recebe a requisição, valida a entrada e monta a resposta HTTP;
2. service.py contém consultas, normalização e regras da feature;
3. templates renderizam o shell inicial;
4. JavaScript carrega detalhes, filtros e tabelas sob demanda.

## Execução Local

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

1. GET /rffusion/health pelo Nginx;
2. carregamento das páginas afetadas sob o prefixo /rffusion/;
3. uma operação de escrita em ambiente controlado, quando houver;
4. ausência de segredos no HTML, JSON, logs e mensagens de erro.

## Referências

- [README.md](/RFFusion/README.md)
- [install/webserver/README.MD](/RFFusion/install/webserver/README.MD)
- [src/zabbix](/RFFusion/src/zabbix)
- [src/mariadb/scripts/README.md](/RFFusion/src/mariadb/scripts/README.md)
