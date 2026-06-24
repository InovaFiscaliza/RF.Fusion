# webfusion

O `webfusion` é a interface web operacional do RF.Fusion. Ele expõe, no
navegador, o estado das estações, o catálogo de espectros, os dashboards
operacionais e um conjunto pequeno de ações que geram tarefas no backend.

Este README descreve apenas o módulo desta pasta. Para a visão geral da
plataforma, consulte [README.md](/RFFusion/README.md).

## Papel no Sistema

O `webfusion` tem quatro responsabilidades principais:

1. apresentar a página inicial e o mapa de estações
2. permitir navegação de espectros e arquivos do repositório
3. exibir painéis operacionais globais e por host
4. converter ações do operador em registros persistidos de `HOST_TASK`

O desenho é predominantemente de leitura. A maior parte do código monta HTML
ou JSON a partir dos bancos para consumo incremental pelo navegador.

## Arquitetura de Runtime

O runtime é propositalmente simples:

- `Flask` cuida das rotas, renderização inicial e endpoints JSON
- `Jinja` renderiza as páginas HTML
- `waitress` publica a aplicação WSGI
- `nginx` fica à frente da aplicação e, quando possível, entrega downloads
  diretamente do repositório

Esse split é importante porque o volume montado em `/mnt/reposfi` costuma ser
servido com mais eficiência pelo `nginx` do que pelo processo Python.

## Fontes de Dados

O `webfusion` lê três schemas MariaDB:

### `BPDATA`

Banco operacional usado para:

- inventário e estado dos hosts
- filas `HOST_TASK` e `FILE_TASK`
- histórico `FILE_TASK_HISTORY`

### `RFDATA`

Banco analítico usado para:

- espectros
- localidades
- equipamentos
- metadados de arquivos do repositório

### `RFFUSION_SUMMARY`

Banco de leitura materializado usado para:

- mapa da página inicial
- snapshots globais e por host
- agregações rápidas de erro e volume

## Modelo de Aplicação

O fluxo principal segue este padrão:

1. uma rota Flask recebe parâmetros da URL
2. a rota delega para um `service.py`
3. o service consulta e transforma os dados
4. a resposta volta como HTML renderizado ou JSON

As páginas combinam um shell HTML inicial com endpoints menores carregados via
JavaScript. Isso reduz o peso do primeiro render e facilita atualizar apenas os
trechos que mudam.

## Organização do Código

### [app.py](/RFFusion/src/webfusion/app.py)

Entrypoint da aplicação.

Responsável por:

- criar a aplicação Flask
- registrar blueprints
- expor `/health`
- manter a rota inicial `/`
- expor as APIs do mapa
- manter o helper legado `/popup`

### [db.py](/RFFusion/src/webfusion/db.py)

Centraliza as conexões para `BPDATA`, `RFDATA` e `RFFUSION_SUMMARY`.

### `modules/*`

Organização por feature.

- `routes.py`: recebe requests HTTP e retorna HTML ou JSON
- `service.py`: executa consultas e monta payloads para a interface
- `builder.py` e `worker.py`: suporte aos fluxos de criação e acompanhamento
  de tarefas, quando a feature precisa

### `templates/`

Templates Jinja usados no render inicial das páginas.

### `static/js/`

Comportamento no navegador para filtros, tabelas, mapa e carregamentos
incrementais.

### `static/css/`

Folhas de estilo comuns e por página.

## Módulos Ativos

### Mapa e Home

- shell: `/`
- APIs: `/api/map/stations`, `/api/map/stations/<site_id>`
- service: [modules/map/service.py](/RFFusion/src/webfusion/modules/map/service.py)
- template: [templates/index.html](/RFFusion/src/webfusion/templates/index.html)

O mapa usa dados de `RFFUSION_SUMMARY` para evitar joins pesados em tempo de
requisição.

### Spectrum

- rotas: [modules/spectrum/routes.py](/RFFusion/src/webfusion/modules/spectrum/routes.py)
- service: [modules/spectrum/service.py](/RFFusion/src/webfusion/modules/spectrum/service.py)
- template: [templates/spectrum/spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)

Esse módulo suporta navegação por espectro e por arquivo de repositório.

### Server

- rota: `/server`
- rotas: [modules/server/routes.py](/RFFusion/src/webfusion/modules/server/routes.py)
- apoio de dados: [modules/host/service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [templates/server/server.html](/RFFusion/src/webfusion/templates/server/server.html)

É o dashboard operacional global do sistema.

### Host

- rota: `/host`
- rotas: [modules/host/routes.py](/RFFusion/src/webfusion/modules/host/routes.py)
- service: [modules/host/service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [templates/host/host.html](/RFFusion/src/webfusion/templates/host/host.html)

É o drill-down por estação, combinando estado operacional, filas, histórico e
localidade reconciliada.

### Task

- prefixo: `/task`
- rotas: [modules/task/routes.py](/RFFusion/src/webfusion/modules/task/routes.py)
- services: [modules/task/service.py](/RFFusion/src/webfusion/modules/task/service.py)
- suporte: [modules/task/builder.py](/RFFusion/src/webfusion/modules/task/builder.py),
  [modules/task/worker.py](/RFFusion/src/webfusion/modules/task/worker.py)
- templates: [task_builder.html](/RFFusion/src/webfusion/templates/task/task_builder.html),
  [task_list.html](/RFFusion/src/webfusion/templates/task/task_list.html)

É o módulo mais orientado a escrita, usado para acionar tarefas no backend.

## Estrutura do Diretório

```text
src/webfusion/
|-- app.py
|-- db.py
|-- modules/
|   |-- host/
|   |-- map/
|   |-- server/
|   |-- spectrum/
|   `-- task/
|-- static/
|   |-- css/
|   |-- fonts/
|   `-- js/
`-- templates/
    |-- host/
    |-- popup/
    |-- server/
    |-- spectrum/
    `-- task/
```

Observação: há diretórios auxiliares ou legados em `modules/`, mas os fluxos
ativos hoje se concentram nos módulos listados acima.

## Endpoints Assíncronos Mais Relevantes

- `/api/map/stations`
- `/api/map/stations/<site_id>`
- `/api/spectrum/localities`
- `/api/spectrum/file/<file_id>/spectra`
- `/api/server/summary-metrics`
- `/api/server/processing-errors`
- `/api/server/backup-errors`
- `/api/server/hosts`
- `/api/host/<host_id>/locations`

## Execução Local

O entrypoint é [app.py](/RFFusion/src/webfusion/app.py). Em execução direta, a
aplicação usa as variáveis:

- `WEBFUSION_HOST`
- `WEBFUSION_PORT`
- `WEBFUSION_THREADS`
- `WEBFUSION_CHANNEL_TIMEOUT`

Na implantação normal do projeto, o módulo é publicado pelo container do
webserver descrito em [install/webserver/README.MD](/RFFusion/install/webserver/README.MD).

## Referências Relacionadas

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/install/webserver/README.MD](/RFFusion/install/webserver/README.MD)
- [/RFFusion/src/mariadb/scripts/README.md](/RFFusion/src/mariadb/scripts/README.md)
