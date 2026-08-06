# ISSUES_MANAGEMENT

## Objetivo

Registrar o planejamento da integração operacional entre RF.Fusion/appCataloga,
WebFusion e Zabbix.

O foco imediato é organizar métricas operacionais confiáveis em
`RFFUSION_SUMMARY.HOST_CURRENT_SNAPSHOT` e entregá-las ao Zabbix. O schema
aditivo e o primeiro lote de métricas deriváveis já foram materializados; este
documento também registra o planejamento dos próximos lotes, templates e
triggers.

---

## Decisão Arquitetural Atual

A responsabilidade fica dividida de forma simples:

- **RF.Fusion** é a fonte dos fatos operacionais e dos sinais booleanos que
  dependem de uma ocorrência concreta do catálogo.
- **Zabbix** é o dono das políticas operacionais, thresholds, macros de
  template, overrides por host, severidade, triggers e ciclo de vida de
  `Problems`.
- **WebFusion** exibe o mesmo contexto operacional e oferece drill-down; não
  gerencia issues, acknowledgements ou comentários de resolução.

Portanto, o RF.Fusion não deve ler, persistir ou calcular thresholds
operacionais nesta fase. Um `ISSUESDB`, `ISSUE_CFG` e um worker local de
cálculo de thresholds deixam de fazer parte do escopo atual.

Essa escolha aproveita o que já pertence naturalmente ao Zabbix:

- templates por família de estação;
- macros e valores-padrão por template;
- overrides específicos por host;
- severidade, histórico e tratamento dos `Problems`.

---

## Fluxo Operacional

```text
BPDATA + RFDATA
        |
        v
RFFUSION_SUMMARY.HOST_CURRENT_SNAPSHOT
        |  métricas e flags atuais
        +---------------------------> WebFusion
        |
        v
trapper / items do Zabbix
        |
        v
templates + macros + triggers
        |
        v
Problems, acknowledgements e tratamento no Zabbix
```

O trapper recebe valores estruturados. Ele não recebe um alarme já resolvido
por comparação de threshold, nem deve precisar conhecer a política da estação.

Exemplo: `VL_BACKUP_DONE_GB_THIS_MONTH` é um fato medido pelo RF.Fusion. Os
triggers `MONTHLY_BACKUP_QUOTA_WARNING` e
`MONTHLY_BACKUP_QUOTA_CRITICAL` são calculados pelo Zabbix, a partir das macros
aplicadas ao template ou ao host.

---

## Convenção de Nomenclatura do Banco

As colunas do `HOST_CURRENT_SNAPSHOT` seguem a convenção já usada nos bancos
do RF.Fusion:

| Prefixo | Uso |
|---|---|
| `DT_` | Instante ou data de referência. |
| `IS_` | Estado booleano. |
| `NU_` | Contagem. |
| `VL_` | Medida numérica, inclusive volume. |
| `NA_` | Texto descritivo. |

Regras complementares:

- instantes operacionais devem ser materializados em UTC;
- medidas de volume no summary devem ser expressas em GB;
- todo volume operacional usa `VL_FILE_SIZE_KB_HOST`, pois representa o
  artefato efetivamente movimentado do host para o servidor;
- colunas novas devem expressar o escopo no próprio nome, como
  `*_THIS_MONTH`, `*_TOTAL`, `*_QUEUE_*` e `*_OPEN_COUNT`;
- nomes em `snake_case` podem ser usados como chaves de payload, item keys ou
  nomes conceituais, mas não substituem o padrão físico das colunas.

---

## Semântica dos Dados

### Acervo por data de geração

`HOST_MONTHLY_METRIC` continua representando o acervo por mês de criação do
arquivo no host:

- data de referência: `DT_FILE_CREATED_HOST`;
- finalidade: entender o acervo histórico, inclusive quanto ainda falta
  transferir de determinado período;
- não representa throughput operacional do mês em que o trabalho ocorreu.

### Operação do mês corrente

O snapshot operacional mede o trabalho executado no período:

- backup concluído no mês: `DT_BACKUP`;
- processamento concluído no mês: `DT_PROCESSED`;
- volume: `VL_FILE_SIZE_KB_HOST` em ambos os casos.

Um `.zip` que gera um `.mat` menor no servidor continua contribuindo com o
tamanho do arquivo de origem no host. O `.mat` não altera a contabilidade de
transferência.

### Fila de execução e estado atual do acervo

Existem duas visões complementares, que não devem ser confundidas:

- fila de execução: estado presente em `FILE_TASK`, usado para saber o que o
  runtime efetivamente pode executar agora;
- estado atual do acervo: fase corrente de cada artefato em
  `FILE_TASK_HISTORY`, usado para entender a distribuição de todo o acervo da
  estação.

A fila de execução é definida em `FILE_TASK`:

- fila de backup: `NU_TYPE = 1` e `NU_STATUS = 1`;
- fila de processamento: `NU_TYPE = 2` e `NU_STATUS = 1`.

Já o estado atual do acervo é lido das colunas de fase em
`FILE_TASK_HISTORY`. Uma linha de history mantém o estado mais recente de
discovery, backup e processamento, inclusive quando uma fase é reativada de
`ERROR` ou `SUSPENDED` para `PENDING`.

Assim, um contador `*_FILES_CURRENT` não é um total histórico de falhas: ele
mede somente os arquivos que permanecem naquele estado no momento do refresh.

---

## Contrato Canônico de Métricas

`RFFUSION_SUMMARY.HOST_CURRENT_SNAPSHOT` será o contrato canônico por estação
para dashboard, WebFusion e publicação ao Zabbix. `SERVER_CURRENT_SUMMARY`
continua sendo apenas uma agregação derivada desse snapshot.

`BPDATA.HOST` permanece a tabela de controle operacional: identidade, acesso,
marca d'água técnica `DT_LAST_DISCOVERY`, últimos marcos de backup e
processamento, conectividade e lock de execução. Ela não armazena contadores,
volumes nem resultado agregado das fases do acervo.

Detalhes internos do lock também permanecem exclusivamente em `BPDATA.HOST`:
`NU_PID`, `DT_BUSY`, `DT_LAST_FAIL` e `NU_HOST_CHECK_ERROR`. Eles sustentam a
exclusão mútua e a confirmação de falhas de conectividade, mas não fazem parte
do contrato de métricas nem devem ser replicados no snapshot.

Também não pertencem ao `HOST_CURRENT_SNAPSHOT` os campos genéricos de “último
erro” (`NA_LAST_ERROR_CODE`, `NA_LAST_ERROR_SUMMARY` e `DT_LAST_ERROR_AT`) nem
`NU_MATCHED_EQUIPMENT_TOTAL`. O primeiro grupo não identifica uma fase
operacional de forma confiável; o segundo é um diagnóstico temporário do
vínculo host-equipamento, não uma métrica da estação.

### Estado operacional

| Coluna | Situação | Descrição |
|---|---|---|
| `DT_LAST_CHECK` | existente | Última checagem operacional concluída. |
| `IS_OFFLINE` | existente | Estado operacional atual de indisponibilidade. |
| `IS_BUSY` | existente | Espelho do lock operacional atual do host, útil como contexto de runtime. |
| `DT_LAST_OFFLINE_AT` | nova | Última ocorrência de transição ou confirmação de indisponibilidade. |
| `NA_LAST_OFFLINE_DESCRIPTION` | nova | Contexto operacional da última indisponibilidade. |

`IS_BUSY` continua sendo um mecanismo interno de exclusão e prevenção de race
conditions em `BPDATA.HOST`. Ele permanece espelhado no snapshot como contexto
operacional, mas não é condição de alarme nem deve ser usado isoladamente para
inferir atividade útil da estação.

### Fluxo do pipeline

| Coluna | Situação | Descrição |
|---|---|---|
| `DT_LAST_DISCOVERY` | existente | Marca d'água técnica do filtro incremental da discovery. Não representa uma varredura vazia concluída. |
| `DT_LAST_DISCOVERY_COMPLETED_AT` | nova | Última discovery concluída, inclusive quando a deduplicação não persistiu arquivos. |
| `NU_LAST_DISCOVERY_FILE_COUNT` | nova | Quantidade de registros efetivamente persistidos na última discovery, já após deduplicação. |
| `VL_LAST_DISCOVERY_KB` | nova | Volume, em KB, dos registros efetivamente persistidos na última discovery, já após deduplicação. |
| `DT_LAST_DISCOVERY_WITH_FILES` | nova | Última discovery cuja quantidade deduplicada foi maior que zero. |
| `DT_LAST_BACKUP` | existente | Último backup concluído com sucesso. |
| `DT_LAST_PROCESSING` | existente | Último processamento concluído com sucesso. |
| `NU_BACKUP_QUEUE_FILES_TOTAL` | existente | Arquivos atualmente enfileirados para backup. |
| `VL_BACKUP_QUEUE_GB_TOTAL` | existente | Volume atual da fila de backup. |
| `NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL` | nova | Tarefas de backup em transferência no `FILE_TASK`. |
| `VL_BACKUP_QUEUE_RUNNING_GB_TOTAL` | nova | Volume das transferências de backup em curso. |
| `NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL` | nova | Tarefas de backup suspensas por indisponibilidade do host. |
| `VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL` | nova | Volume das tarefas de backup suspensas. |
| `NU_PROCESSING_QUEUE_FILES_TOTAL` | existente | Arquivos atualmente enfileirados para processamento. |
| `VL_PROCESSING_QUEUE_GB_TOTAL` | existente | Volume atual da fila de processamento. |
| `NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL` | nova | Tarefas em processamento no `FILE_TASK`. |
| `VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL` | nova | Volume das tarefas em processamento. |
| `NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL` | nova | Tarefas de processamento retidas para decisão manual. |
| `VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL` | nova | Volume das tarefas de processamento frozen. |
| `NU_BACKUP_ERROR_FILES_CURRENT` | nova | Arquivos cujo estado atual de backup é `ERROR`. |
| `NU_PROCESSING_ERROR_FILES_CURRENT` | nova | Arquivos cujo estado atual de processamento é `ERROR`. |

Não serão criadas simultaneamente `NU_LAST_DISCOVERY_FILE_COUNT` e uma
variante `*_NEW_FILE_COUNT`. A discovery elimina o que já havia sido
descoberto antes de persistir a nova entrada; portanto, as duas medidas teriam
a mesma semântica e criariam ruído.

### Consumo operacional do mês corrente

| Coluna | Situação | Descrição |
|---|---|---|
| `NU_BACKUP_DONE_THIS_MONTH` | existente | Arquivos com backup concluído no mês, por `DT_BACKUP`. |
| `VL_BACKUP_DONE_GB_THIS_MONTH` | existente | Volume de backup concluído no mês, por `DT_BACKUP`. |
| `NU_PROCESSING_DONE_THIS_MONTH` | nova | Arquivos processados com sucesso no mês, por `DT_PROCESSED`. |
| `VL_PROCESSING_DONE_GB_THIS_MONTH` | nova | Volume processado com sucesso no mês, por `DT_PROCESSED`. |

### Acervo histórico acumulado

| Coluna | Situação | Descrição |
|---|---|---|
| `NU_DISCOVERED_FILES_TOTAL` | nova | Total acumulado de arquivos descobertos. |
| `VL_DISCOVERED_GB_TOTAL` | nova | Volume acumulado de arquivos descobertos. |
| `NU_BACKUP_DONE_FILES_TOTAL` | nova | Total acumulado de arquivos com backup concluído. |
| `VL_BACKUP_DONE_GB_TOTAL` | nova | Volume acumulado de backup concluído. |
| `NU_PROCESSING_DONE_FILES_TOTAL` | nova | Total acumulado de arquivos processados com sucesso. |
| `VL_PROCESSING_DONE_GB_TOTAL` | nova | Volume acumulado de processamento concluído. |
| `NU_FACT_SPECTRUM_TOTAL` | existente | Total acumulado de espectros gerados. |
| `NU_PAYLOAD_DELETED_FILES_TOTAL` | nova | Artefatos do repositório removidos pelo garbage collector. |
| `VL_PAYLOAD_DELETED_GB_TOTAL` | nova | Espaço liberado pelos artefatos removidos, pelo tamanho no servidor. |

Os aliases legados de contagem e volume não fazem parte do contrato. O
WebFusion deve consumir os nomes canônicos; `NU_HOST_FILES`,
`VL_PENDING_BACKUP_GB`, `VL_DONE_BACKUP_GB` e os antigos contadores de fila
serão removidos do snapshot e de `BPDATA.HOST` na migração controlada.

### Estado atual do acervo por fase

As métricas abaixo não são acumulados históricos. Elas representam a quantidade
e o volume dos arquivos que se encontram em cada estado no instante do refresh.

| Fase | Estado | Contagem | Volume | Fonte |
|---|---|---|---|---|
| Backup | `PENDING` | `NU_BACKUP_PENDING_FILES_CURRENT` | `VL_BACKUP_PENDING_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_BACKUP` |
| Backup | `ERROR` | `NU_BACKUP_ERROR_FILES_CURRENT` | `VL_BACKUP_ERROR_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_BACKUP` |
| Backup | `SUSPENDED` | `NU_BACKUP_SUSPENDED_FILES_CURRENT` | `VL_BACKUP_SUSPENDED_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_BACKUP` |
| Processamento | `PENDING` | `NU_PROCESSING_PENDING_FILES_CURRENT` | `VL_PROCESSING_PENDING_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_PROCESSING` |
| Processamento | `ERROR` | `NU_PROCESSING_ERROR_FILES_CURRENT` | `VL_PROCESSING_ERROR_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_PROCESSING` |
| Processamento | `FROZEN` | `NU_PROCESSING_FROZEN_FILES_CURRENT` | `VL_PROCESSING_FROZEN_GB_CURRENT` | `FILE_TASK_HISTORY.NU_STATUS_PROCESSING` |

### Diagnóstico de estados retidos

`HOST_ERROR_SUMMARY` e `SERVER_ERROR_SUMMARY` são diagnósticos de trabalho que
requer atenção, não somente um histórico de exceções. Cada agrupamento possui
`NA_TASK_STATE` com `ERROR`, `SUSPENDED` ou `FROZEN`.

- `FILE_TASK_HISTORY` contribui estados de backup e processamento;
- `FILE_TASK` contribui estados retidos ainda presentes na fila;
- `HOST_TASK` contribui estados retidos de discovery e outras tarefas operacionais do host;
- o estado integra a chave de agrupamento para que uma suspensão não seja
  somada indevidamente a um erro com a mesma mensagem.

Não serão criadas combinações artificiais de fase e estado:

- `FROZEN` é, hoje, uma resolução específica do processamento que exige
  revisão manual;
- `SUSPENDED` representa trabalho dependente do host e é aplicado às fases de
  discovery e backup;
- processamento não deve receber uma métrica `SUSPENDED` enquanto o runtime
  não produzir esse estado de forma canônica.

As métricas de fila, como `NU_BACKUP_QUEUE_FILES_TOTAL`, continuam necessárias.
Elas mostram o subconjunto do acervo pendente que está materializado na fila de
execução; não substituem `NU_BACKUP_PENDING_FILES_CURRENT` nem o seu volume.

### Contexto operacional

| Coluna | Situação | Descrição |
|---|---|---|
| `NA_CURRENT_LOCALITY_LABEL` | nova | Localidade atual da estação. |
| `NA_CURRENT_SITE_LABEL` | existente | Site atual associado à estação. |
| `NA_CURRENT_STATE_CODE` | existente | UF atual associada ao site/localidade. |
| `VL_CURRENT_LATITUDE` | nova | Latitude operacional atual. |
| `VL_CURRENT_LONGITUDE` | nova | Longitude operacional atual. |

---

## Sinais Booleanos para o Zabbix

Há quatro ocorrências que o RF.Fusion deve publicar diretamente como booleanos,
porque decorrem de um fato operacional concreto, não de uma comparação de
threshold:

| Código de alarme no Zabbix | Estado atual | Última avaliação válida | Última falha | Contexto da última falha |
|---|---|---|---|---|
| `HOST_OFFLINE` | `IS_OFFLINE` | `DT_LAST_CHECK` | `DT_LAST_OFFLINE_AT` | `NA_LAST_OFFLINE_DESCRIPTION` |
| `SSH_FAILURE` | `IS_SSH_FAILURE` | `DT_LAST_SSH_EVALUATED_AT` | `DT_LAST_SSH_FAILURE_AT` | `NA_LAST_SSH_FAILURE_CODE`, `NA_LAST_SSH_FAILURE_DESCRIPTION` |
| `GPS_GNSS_UNAVAILABLE` | `IS_GPS_GNSS_UNAVAILABLE` | `DT_LAST_GPS_GNSS_EVALUATED_AT` | `DT_LAST_GPS_GNSS_UNAVAILABLE_AT` | `NA_LAST_GPS_GNSS_UNAVAILABLE_DESCRIPTION`, `NA_LAST_GPS_GNSS_UNAVAILABLE_HOST_FILE_NAME` |

Para cada sinal, as colunas têm papéis distintos:

- `IS_*` representa o estado atual e só deve voltar a `0` diante de uma ação
  válida de recuperação no mesmo domínio;
- `DT_LAST_*_AT` de avaliação é atualizado tanto em sucesso quanto em falha;
- `DT_LAST_*_FAILURE_AT` só é atualizado quando a avaliação falha e preserva a
  evidência depois da recuperação;
- `NA_LAST_*` descreve a última falha conhecida.

`NA_LAST_SSH_FAILURE_CODE` é o classificador estável do domínio SSH. O
primeiro contrato usa `AUTHENTICATION` e `CONNECTIVITY`; novos modos podem ser
introduzidos como códigos sem expandir o schema. As colunas específicas antigas
de autenticação e conectividade permanecem somente durante a transição de
workers e não fazem parte do contrato canônico.

Para GPS/GNSS, o nome do arquivo no host identifica o artefato de origem e
evita confundir o arquivo de estação com o `.mat` eventualmente gerado no
servidor.

---

## Alarmes Gerados pelo Zabbix

O Zabbix calcula os alarmes a partir de métricas e flags recebidas. Os
thresholds ficam em macros de template, com override por host quando necessário.

| Código de alarme | Fatos recebidos do RF.Fusion | Responsabilidade do Zabbix |
|---|---|---|
| `HOST_OFFLINE` | `IS_OFFLINE` e sua evidência associada | Abrir ou fechar o Problem a partir da flag. |
| `SSH_FAILURE` | `IS_SSH_FAILURE` e sua evidência associada | Abrir ou fechar o Problem a partir da flag; expor o código e a descrição como contexto. |
| `GPS_GNSS_UNAVAILABLE` | `IS_GPS_GNSS_UNAVAILABLE` e sua evidência associada | Abrir ou fechar o Problem a partir da flag. |
| `HOST_CHECK_STALE` | `DT_LAST_CHECK` | Comparar a idade da checagem com a macro aplicável. |
| `NO_NEW_DISCOVERY` | `DT_LAST_DISCOVERY_WITH_FILES` | Comparar a idade da última discovery útil com a macro aplicável. |
| `BACKUP_STALE` | `DT_LAST_BACKUP` | Comparar a idade do último backup com a macro aplicável. |
| `PROCESSING_STALE` | `DT_LAST_PROCESSING` | Comparar a idade do último processamento com a macro aplicável. |
| `BACKUP_ERRORS_OPEN` | `NU_BACKUP_ERROR_FILES_CURRENT` | Avaliar a quantidade atual de arquivos em erro contra o limite definido. |
| `PROCESSING_ERRORS_OPEN` | `NU_PROCESSING_ERROR_FILES_CURRENT` | Avaliar a quantidade atual de arquivos em erro contra o limite definido. |
| `MONTHLY_BACKUP_QUOTA_WARNING` | `VL_BACKUP_DONE_GB_THIS_MONTH` | Comparar com o limite de atenção do template/host. |
| `MONTHLY_BACKUP_QUOTA_CRITICAL` | `VL_BACKUP_DONE_GB_THIS_MONTH` | Comparar com o limite crítico do template/host. |

Os alarmes `EMPTY_GPS_DATA_RATE`, `PROCESSING_ERROR_RATE` e quaisquer outras
taxas ficam fora do primeiro ciclo. Eles exigem denominador, janela temporal e
semântica de amostragem definidos antes de serem publicados ou triggerados.

`NO_NEW_DISCOVERY` também deixa de ser uma flag calculada pelo RF.Fusion. O
RF.Fusion publica a última discovery útil; o Zabbix aplica a janela adequada a
RFeye, Celplan, ERMx, UMS ou a um host específico.

---

## Integração por Trapper

O atual
[`queryFileMetadata_trapper.py`](/RFFusion/src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py)
deve evoluir de adaptador de metadata para adaptador do contrato operacional.

Sua responsabilidade futura é transportar, por host:

- métricas do `HOST_CURRENT_SNAPSHOT`;
- flags `IS_*` do estado atual;
- contexto operacional útil para dashboards ou diagnóstico.

Ele não deve:

- interpretar macros de template;
- aplicar threshold;
- decidir severidade;
- criar ou encerrar `Problems` diretamente.

O formato final pode ser payload JSON com items dependentes ou envio de items
individuais. Essa escolha é de implementação; em ambos os casos, a semântica
das métricas deve permanecer a mesma.

---

## WebFusion e Zabbix

Os dois produtos espelham a mesma realidade operacional, mas têm papéis
diferentes:

- WebFusion exibe métricas, evolução operacional e contexto da estação;
- Zabbix exibe métricas e é o lugar oficial para Problems, severidade,
  acknowledgement, comentários e histórico de tratamento.

O WebFusion não deve manter uma cópia independente do ciclo de vida de alarmes.
Um totalizador de problemas ativos só deve ser exibido no WebFusion quando houver
uma integração explícita e confiável com o Zabbix; até lá, o painel deve focar
nos fatos operacionais que explicam o estado da estação.

---

## Planejamento da Migração de Métricas

### Premissas

- não remover colunas ou alterar semânticas existentes na primeira entrega;
- cada coluna nova deve ter fonte SQL, semântica, unidade e consulta de
  validação registradas antes de ser entregue;
- o `DBHandler.m` e as tabelas de compatibilidade externa continuam intocados
  nesta fase:
  - `HOST_LOCATION_SUMMARY`;
  - `MAP_SITE_SUMMARY`;
  - `MAP_SITE_STATION_SUMMARY`;
  - `SITE_EQUIPMENT_OBS_SUMMARY`.

### Fase 1. Fechar o contrato e validar as fontes

1. Confirmar a definição de cada métrica canônica.
2. Registrar a origem em `BPDATA`, `RFDATA` ou summary derivado.
3. Preparar uma query SQL de validação por métrica.
4. Confirmar, por status e fase, a query que materializa a distribuição atual
   do acervo em `FILE_TASK_HISTORY`.

### Fase 2. Expandir o summary de forma aditiva

Estado: concluída para o contrato atual de métricas e sinais operacionais.

1. Foram adicionadas as novas colunas em `HOST_CURRENT_SNAPSHOT` e
   `HOST_CURRENT_SNAPSHOT_shadow`.
2. Foram atualizados `createFusionSummaryDB.sql` e a migração correspondente.
3. Já são materializadas no
   `summary_handler/refresh_engine.py`.
4. O lote entregue cobre fila de execução, throughput mensal de backup e
   processamento, acervo acumulado, distribuição atual de estados por fase e
   localidade/coordenadas.
5. Os campos legados permanecem apenas até a migração dos consumidores.
6. As métricas foram conferidas contra as agregações de `FILE_TASK` e
   `FILE_TASK_HISTORY`.
7. O resultado da última discovery é persistido diretamente no snapshot, sem
   alterar a marca d'água técnica `BPDATA.HOST.DT_LAST_DISCOVERY`.
8. O runtime persiste os sinais SSH e GPS/GNSS diretamente em
   `HOST_CURRENT_SNAPSHOT`, que é o contrato canônico para dashboard e Zabbix.
9. A escrita de sinal e o refresh completo do snapshot usam o mesmo lock curto
   do MariaDB. Assim, uma troca atômica não perde uma falha ou recuperação
   ocorrida concorrentemente.
10. `BPDATA.HOST` não armazena sinais SSH ou GPS/GNSS. Ele preserva somente
    dados cadastrais e controles necessários ao runtime da estação.
11. O snapshot não inventa evidência para ocorrências anteriores à implantação.

Os novos sinais terão valores de estado padrão até a primeira avaliação válida
do respectivo domínio. Isso é esperado: a migração não deve inferir uma falha
ou uma recuperação a partir do histórico antigo.

### Fase 3. Completar os agregados derivados

1. Atualizar `SERVER_CURRENT_SUMMARY` apenas com métricas que façam sentido
   como total de servidor.
2. Não usar esse agregado como fonte semântica para uma estação.
3. Não alterar o papel de `HOST_MONTHLY_METRIC` como acervo por
   `DT_FILE_CREATED_HOST`.

### Fase 4. Publicar no Zabbix

1. Revisar o contrato do trapper.
2. Criar items para métricas, timestamps e flags.
3. Configurar macros e triggers nos templates de RFeye, Celplan, ERMx e UMS.
4. Aplicar overrides somente onde uma estação fugir da política da família.
5. Validar abertura e recuperação de Problems em uma estação controlada.

### Fase 5. Migrar consumidores e reduzir legado

1. Atualizar o dashboard `/host` e demais consumidores para os nomes
   canônicos.
2. Revisar `BPDATA.HOST`, `host_update_statistics` e seus consumidores.
3. Só então decidir quais campos históricos de `HOST` podem ser aposentados.

#### Limpeza iniciada: sinais de SSH e GPS/GNSS

Os campos de sinal não pertencem a `BPDATA.HOST`. A primeira limpeza remove do
runtime e do schema de `HOST` os estados e evidências de SSH e GPS/GNSS, sem
criar nova tabela. O próprio `HOST_CURRENT_SNAPSHOT` retém esse estado.

Ordem obrigatória de implantação:

1. Aplicar a migração aditiva do Summary que contém os campos canônicos.
2. Publicar e reiniciar os workers com a escrita direta no snapshot.
3. Aplicar `alterProcessingDB-v4-remove-host-signal-metrics.sql`.
4. Aplicar `alterFusionSummaryDB-v8-clean-host-signal-columns.sql` para
   retirar somente as antigas colunas SSH divididas por tipo do snapshot e de
   sua shadow.

#### Migração dos agregados legados

A implementação migra o WebFusion para `HOST_CURRENT_SNAPSHOT` e faz
`SERVER_CURRENT_SUMMARY` agregar somente métricas canônicas. O
`HOST_MONTHLY_METRIC` permanece exclusivamente para as tabelas anuais por mês
de criação do arquivo, não para totalizadores operacionais.

`host_update_statistics` foi substituído por uma invalidação de escopo no
`SUMMARY_OUTBOX`. Os entrypoints de discovery, backup, processamento e backlog
publicam somente o host afetado; a projeção é recalculada pelo worker de
summary. Tarefas legadas de atualização de estatísticas ainda são concluídas,
mas apenas solicitam esse refresh para esvaziar a fila com segurança.

Ordem obrigatória de implantação:

1. Parar o worker de summary antes de alterar tabelas com shadow.
2. Publicar o código compatível no WebFusion e nos workers appCataloga.
3. Aplicar `alterFusionSummaryDB-v8-clean-host-signal-columns.sql`,
   `alterFusionSummaryDB-v9-canonical-host-metrics.sql`,
   `alterFusionSummaryDB-v10-error-summary-task-state.sql` e
   `alterFusionSummaryDB-v11-remove-host-snapshot-runtime-legacy.sql`.
4. Aplicar `alterFusionSummaryDB-v12-pipeline-queue-and-gc-metrics.sql`
   antes de publicar consumidores das métricas de fila e garbage collection.
5. Aplicar `alterProcessingDB-v4-remove-host-signal-metrics.sql` e
   `alterProcessingDB-v5-remove-host-statistics-metrics.sql`.
6. Reiniciar WebFusion e os workers appCataloga.
7. Executar uma reconciliação controlada do summary e validar uma estação por
   vez antes de remover a compatibilidade da antiga `HOST_TASK` de estatística.

---

## Mapa de Impacto

Os principais pontos que precisam ser revisados antes de cada entrega são:

| Área | Papel na migração |
|---|---|
| `src/mariadb/scripts/createFusionSummaryDB.sql` | Schema canônico do snapshot e agregados. |
| `summary_handler/refresh_engine.py` | Materializa métricas no snapshot e no agregado de servidor. |
| `appCataloga_summary_database.py` | Orquestra o refresh incremental e completo. |
| `db/dbHandlerSummary.py` | Suporta a persistência e os contratos do worker de summary. |
| `db/dbHandlerBKP.py` e `host_handler/host_runtime.py` | Publicam invalidações de escopo e evidências diretas do snapshot. |
| `src/webfusion/modules/host` | Consome snapshot e agregado de servidor canônicos; usa mensal somente nas tabelas anuais de coorte. |
| `src/webfusion/modules/task` e `src/webfusion/modules/maintenance` | Ainda usam contexto operacional de `BPDATA.HOST`. |
| `src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py` | Publica o contrato operacional ao Zabbix. |
| `src/webfusion/DBHandler.m` | Compatibilidade dormente que precisa ser revisada antes de remover colunas legadas. |

---

## Direção Arquitetural de Bridge

`RFFUSION_SUMMARY` deve continuar sendo um read model. Vínculos oficiais entre
domínios não devem ter o summary como destino definitivo.

O principal candidato identificado é o vínculo entre host operacional e
equipamento analítico:

- `BPDATA.HOST.NA_HOST_NAME`;
- `RFDATA.DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT`;
- atual `HOST_EQUIPMENT_LINK`.

Permanece no backlog avaliar um banco `RFFUSION_BRIDGE` e migrar esse vínculo
para lá. Essa mudança não faz parte da criação das métricas e não deve bloquear
a evolução do `HOST_CURRENT_SNAPSHOT`.

A linhagem `arquivo -> espectro` permanece, por enquanto, como parte nativa do
`RFDATA`, e projeções como localidade atual, site atual e totais de espectros
continuam no summary.

---

## Escopo Futuro

Issues analíticas, como `EMISSION_DETECTED` e `EMISSION_RECURRENT`, ficam fora
do ciclo operacional inicial. Quando houver o componente analítico que detecta
esses eventos, será necessário decidir se o Zabbix ainda é suficiente como
motor de política ou se um domínio local adicional se justifica.

Nenhum `ISSUESDB`, `ISSUE_CFG` ou worker de cálculo de threshold deve ser
criado antes dessa necessidade concreta.

---

## Próximo Passo

Executar a implantação em janela controlada, sem rodar reconciliação global em
uma VM pressionada por memória. A validação deve comparar, para uma estação de
referência, os totais canônicos do snapshot com `FILE_TASK_HISTORY`, o estado
da fila com `FILE_TASK` e os grupos de diagnóstico com os três estados
retidos.
