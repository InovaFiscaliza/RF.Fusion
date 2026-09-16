# MariaDB Scripts

Este README concentra a visão arquitetural e operacional dos bancos do
RF.Fusion neste diretório.

O diretório [src/mariadb/scripts](/RFFusion/src/mariadb/scripts) contém os
artefatos de bootstrap dos três bancos usados pelo RF.Fusion:

- `BPDATA`: estado operacional e filas
- `RFDATA`: catálogo analítico de arquivos e espectros
- `RFFUSION_SUMMARY`: read models materializados para consultas e mapas

## Visão Geral

No desenho atual, os bancos são complementares. Não existe ponte de chave
estrangeira entre eles. A integração é feita pela aplicação.

![Arquitetura dos bancos do RF.Fusion](/RFFusion/docs/images/mariadb-databases-overview.svg)

## Arquitetura dos Bancos

### BPDATA

Banco operacional do pipeline. Ele registra hosts, filas e histórico de execução.

Tabelas centrais:

- `HOST`
- `HOST_TASK`
- `FILE_TASK`
- `FILE_TASK_HISTORY`
- `SUMMARY_OUTBOX`

Responde perguntas como:

- quais hosts estão online, offline ou ocupados
- quais tarefas estão pendentes, em execução ou com erro
- qual foi o ciclo de descoberta, backup e processamento de um arquivo

Observação: `FILE_TASK` e `FILE_TASK_HISTORY` não possuem FK entre si no schema.

### RFDATA

Banco analítico e de catálogo. Ele organiza os arquivos recuperados, suas
localidades, equipamentos e espectros gerados.

Tabelas centrais:

- `DIM_SPECTRUM_SITE`
- `DIM_SPECTRUM_EQUIPMENT`
- `DIM_SPECTRUM_FILE`
- `FACT_SPECTRUM`
- `BRIDGE_SPECTRUM_FILE`

Responde perguntas como:

- onde uma medição foi realizada
- qual equipamento gerou um espectro
- quais espectros vieram de um arquivo do repositório

### RFFUSION_SUMMARY

Banco de leitura derivado de `BPDATA` e `RFDATA`. Ele materializa relações e
agregações pesadas para o `webfusion` e outros consumidores.

Tabelas centrais:

- `HOST_EQUIPMENT_LINK`
- `SITE_EQUIPMENT_OBS_SUMMARY`
- `HOST_LOCATION_SUMMARY`
- `HOST_LOCATION_TIMELINE_SUMMARY`: passagens consecutivas, incluindo retornos
- `MAP_SITE_STATION_SUMMARY`
- `MAP_SITE_SUMMARY`
- `HOST_MONTHLY_METRIC`
- `HOST_ERROR_SUMMARY`
- `SERVER_ERROR_SUMMARY`
- `HOST_CURRENT_SNAPSHOT`
- `SERVER_CURRENT_SUMMARY`

Semânticas importantes:

- `HOST_MONTHLY_METRIC` é mensal por `DT_FILE_CREATED`
- métricas mensais de backup em `HOST_CURRENT_SNAPSHOT` usam `DT_BACKUP`
- `SERVER_CURRENT_SUMMARY` agrega os snapshots correntes dos hosts

Observação: `RFFUSION_SUMMARY` não modela essas relações com FKs entre bancos.
O diagrama acima representa dependência de refresh e derivação lógica.

## Fluxo Entre os Bancos

O fluxo lógico do dado é este:

1. `appCataloga` descobre arquivos e atualiza filas em `BPDATA`.
2. O backup e o processamento geram catálogo e espectros em `RFDATA`.
3. O summary consolida sinais operacionais e analíticos em `RFFUSION_SUMMARY`.
4. O `webfusion` consulta `BPDATA` e `RFDATA` quando precisa do detalhe, e
   usa `RFFUSION_SUMMARY` para mapas, snapshots e métricas agregadas.

## Scripts de Bootstrap

### Migração da linha do tempo de localidades

Aplicar [migrateHostLocationTimeline.sql](migrateHostLocationTimeline.sql) em
instâncias existentes, usando o cliente MariaDB com as credenciais operacionais.
Não executar o bootstrap completo para essa atualização.

```bash
mariadb RFFUSION_SUMMARY < src/mariadb/scripts/migrateHostLocationTimeline.sql
```

A tabela antiga `HOST_LOCATION_SUMMARY` mantém uma linha por host/site e continua
atendendo seus consumidores. A nova tabela mantém uma linha por passagem, com
início/fim das medições, quantidade de espectros e sinalização de sobreposição
entre sites. Não representa permanência comprovada nos intervalos sem medições.

Depois da migração, preencher o novo modelo pelo método público
`SummaryRefreshEngine.refresh_location_timeline()` no container appCataloga.
Para atualização dirigida, passar `host_ids={ID_HOST}`. Coordenar o preenchimento
manual com o worker para evitar publicações simultâneas da mesma estação.
O método percorre uma estação por vez, limita cada consulta a
`SUMMARY_TIMELINE_QUERY_TIMEOUT_SEC` e publica cada histórico em uma transação.
Monitorar CPU, memória e swap durante o preenchimento.

Recarregar o worker do SUMMARY e reiniciar `rffusion-web` no host Podman após
preencher a tabela. Eventos de site/equipamento recompõem as estações afetadas;
a reconciliação diária recompõe todas. Eventos exclusivamente operacionais de
host não provocam uma nova leitura dos espectros.

Validação focada:

```bash
# Container appCataloga
/opt/conda/envs/appdata/bin/python -m pytest test/tests/workers/test_summary_refresh_engine.py test/tests/db/test_dbhandler_summary.py -q
# Container webserver, com pytest disponível no ambiente de validação
/usr/local/bin/python -m pytest /RF.Fusion/test/tests/webfusion/test_host_service.py -k location_history -q
```

Na revisão visual de `/host`, conferir retornos ao mesmo site como entradas
separadas, datas em português e avisos apenas quando os registros se sobrepõem.

### Schemas

- [createProcessingDB.sql](/RFFusion/src/mariadb/scripts/createProcessingDB.sql):
  cria o `BPDATA`
- [createMeasureDB.sql](/RFFusion/src/mariadb/scripts/createMeasureDB.sql):
  cria o `RFDATA`
- [createFusionSummaryDB.sql](/RFFusion/src/mariadb/scripts/createFusionSummaryDB.sql):
  cria o `RFFUSION_SUMMARY`

### Seeds

- [equipmentType.csv](/RFFusion/src/mariadb/scripts/equipmentType.csv):
  tipos de equipamento
- [fileType.csv](/RFFusion/src/mariadb/scripts/fileType.csv):
  tipos de arquivo
- [measurementUnit.csv](/RFFusion/src/mariadb/scripts/measurementUnit.csv):
  unidades de medição
- [IBGE-BR_UF_2020_BULKLOAD.csv](/RFFusion/src/mariadb/scripts/IBGE-BR_UF_2020_BULKLOAD.csv):
  estados
- [IBGE-BR_Municipios_2020_BULKLOAD.csv](/RFFusion/src/mariadb/scripts/IBGE-BR_Municipios_2020_BULKLOAD.csv):
  municípios

## Ordem de Criação

A ordem esperada de bootstrap é:

1. `BPDATA`
2. `RFDATA`
3. `RFFUSION_SUMMARY`

Exemplo manual:

```bash
mysql -u root -p < /RFFusion/src/mariadb/scripts/createProcessingDB.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createMeasureDB.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createFusionSummaryDB.sql
```

Na operação normal, o caminho suportado é o deploy do container MariaDB:
[install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md).

## Notas Operacionais

### `createMeasureDB.sql` depende dos CSVs montados no repositório

O script usa `LOAD DATA INFILE` com caminhos absolutos em `/RFFusion`, então o
repositório precisa estar montado nesse caminho durante a carga.

### A carga geográfica é parte do funcionamento

Os CSVs do IBGE não são apenas apoio documental. Eles sustentam a resolução de
UF, município e localidade no `RFDATA`.

### O summary hoje é mantido pelo worker Python

O modelo atual não depende do event scheduler do MariaDB como caminho canônico
de refresh. Hoje o fluxo é:

1. a aplicação publica escopos sujos em `SUMMARY_OUTBOX`
2. `appCataloga_summary_database.py` consome o outbox
3. o worker atualiza as tabelas públicas de `RFFUSION_SUMMARY`

## Estrutura do Diretório

- [README.md](/RFFusion/src/mariadb/scripts/README.md): visão consolidada dos bancos
- [environment.yml](/RFFusion/src/mariadb/scripts/environment.yml):
  referência de ambiente legada

## Referências Relacionadas

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md)
- [/RFFusion/src/appCataloga/README.md](/RFFusion/src/appCataloga/README.md)
