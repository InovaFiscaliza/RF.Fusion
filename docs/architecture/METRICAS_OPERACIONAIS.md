# Métricas operacionais e integração com Zabbix

## Escopo

Definir a semântica das métricas operacionais compartilhadas por appCataloga,
WebFusion e Zabbix. A compatibilidade externa segue [SUMMARY.md](SUMMARY.md).
As pendências de integração estão em [Planejamento](../planejamento/README.md).

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
operacionais. `ISSUESDB`, `ISSUE_CFG` e worker local de cálculo de thresholds
não fazem parte do contrato operacional.

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

`RFFUSION_SUMMARY.HOST_CURRENT_SNAPSHOT` é o contrato canônico por estação
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

| Coluna | Descrição |
|---|---|
| `DT_LAST_CHECK` | Última checagem operacional concluída. |
| `IS_OFFLINE` | Estado operacional atual de indisponibilidade. |
| `IS_BUSY` | Espelho do lock operacional atual do host, útil como contexto de runtime. |
| `DT_LAST_OFFLINE_AT` | Última ocorrência de transição ou confirmação de indisponibilidade. |
| `NA_LAST_OFFLINE_DESCRIPTION` | Contexto operacional da última indisponibilidade. |

`IS_BUSY` continua sendo um mecanismo interno de exclusão e prevenção de race
conditions em `BPDATA.HOST`. Ele permanece espelhado no snapshot como contexto
operacional, mas não é condição de alarme nem deve ser usado isoladamente para
inferir atividade útil da estação.

### Fluxo do pipeline

| Coluna | Descrição |
|---|---|
| `DT_LAST_DISCOVERY` | Marca d'água técnica do filtro incremental da discovery. Não representa uma varredura vazia concluída. |
| `DT_LAST_DISCOVERY_COMPLETED_AT` | Última discovery concluída, inclusive quando a deduplicação não persistiu arquivos. |
| `NU_LAST_DISCOVERY_FILE_COUNT` | Quantidade de registros efetivamente persistidos na última discovery, já após deduplicação. |
| `VL_LAST_DISCOVERY_KB` | Volume, em KB, dos registros efetivamente persistidos na última discovery, já após deduplicação. |
| `DT_LAST_DISCOVERY_WITH_FILES` | Última discovery cuja quantidade deduplicada foi maior que zero. |
| `DT_LAST_BACKUP` | Último backup concluído com sucesso. |
| `DT_LAST_PROCESSING` | Último processamento concluído com sucesso. |
| `NU_BACKUP_QUEUE_FILES_TOTAL` | Arquivos atualmente enfileirados para backup. |
| `VL_BACKUP_QUEUE_GB_TOTAL` | Volume atual da fila de backup. |
| `NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL` | Tarefas de backup em transferência no `FILE_TASK`. |
| `VL_BACKUP_QUEUE_RUNNING_GB_TOTAL` | Volume das transferências de backup em curso. |
| `NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL` | Tarefas de backup suspensas por indisponibilidade do host. |
| `VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL` | Volume das tarefas de backup suspensas. |
| `NU_PROCESSING_QUEUE_FILES_TOTAL` | Arquivos atualmente enfileirados para processamento. |
| `VL_PROCESSING_QUEUE_GB_TOTAL` | Volume atual da fila de processamento. |
| `NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL` | Tarefas em processamento no `FILE_TASK`. |
| `VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL` | Volume das tarefas em processamento. |
| `NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL` | Tarefas de processamento retidas para decisão manual. |
| `VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL` | Volume das tarefas de processamento frozen. |
| `NU_BACKUP_ERROR_FILES_CURRENT` | Arquivos cujo estado atual de backup é `ERROR`. |
| `NU_PROCESSING_ERROR_FILES_CURRENT` | Arquivos cujo estado atual de processamento é `ERROR`. |

`NU_LAST_DISCOVERY_FILE_COUNT` conta os registros persistidos após deduplicação.
Não criar uma variante `*_NEW_FILE_COUNT` com a mesma semântica.

### Consumo operacional do mês corrente

| Coluna | Descrição |
|---|---|
| `NU_BACKUP_DONE_THIS_MONTH` | Arquivos com backup concluído no mês, por `DT_BACKUP`. |
| `VL_BACKUP_DONE_GB_THIS_MONTH` | Volume de backup concluído no mês, por `DT_BACKUP`. |
| `NU_PROCESSING_DONE_THIS_MONTH` | Arquivos processados com sucesso no mês, por `DT_PROCESSED`. |
| `VL_PROCESSING_DONE_GB_THIS_MONTH` | Volume processado com sucesso no mês, por `DT_PROCESSED`. |

### Acervo histórico acumulado

| Coluna | Descrição |
|---|---|
| `NU_DISCOVERED_FILES_TOTAL` | Total acumulado de arquivos descobertos. |
| `VL_DISCOVERED_GB_TOTAL` | Volume acumulado de arquivos descobertos. |
| `NU_BACKUP_DONE_FILES_TOTAL` | Total acumulado de arquivos com backup concluído. |
| `VL_BACKUP_DONE_GB_TOTAL` | Volume acumulado de backup concluído. |
| `NU_PROCESSING_DONE_FILES_TOTAL` | Total acumulado de arquivos processados com sucesso. |
| `VL_PROCESSING_DONE_GB_TOTAL` | Volume acumulado de processamento concluído. |
| `NU_FACT_SPECTRUM_TOTAL` | Total acumulado de espectros gerados. |
| `NU_PAYLOAD_DELETED_FILES_TOTAL` | Artefatos do repositório removidos pelo garbage collector. |
| `VL_PAYLOAD_DELETED_GB_TOTAL` | Espaço liberado pelos artefatos removidos, pelo tamanho no servidor. |

Consumidores usam os nomes canônicos de contagem e volume definidos acima.

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

| Coluna | Descrição |
|---|---|
| `NA_CURRENT_LOCALITY_LABEL` | Localidade atual da estação. |
| `NA_CURRENT_SITE_LABEL` | Site atual associado à estação. |
| `NA_CURRENT_STATE_CODE` | UF atual associada ao site/localidade. |
| `VL_CURRENT_LATITUDE` | Latitude operacional atual. |
| `VL_CURRENT_LONGITUDE` | Longitude operacional atual. |

---

## Sinais Booleanos para o Zabbix

Há três ocorrências que o RF.Fusion deve publicar diretamente como booleanos,
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
introduzidos como códigos sem expandir o schema. Não criar colunas distintas para cada código de falha SSH.

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
[`queryFileMetadata_trapper.py`](../../src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py)
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

## Atualização consistente do snapshot

- `SERVER_CURRENT_SUMMARY` agrega métricas canônicas do snapshot. Não é a fonte
  semântica dos dados de uma estação.
- `HOST_MONTHLY_METRIC` mantém o acervo por `DT_FILE_CREATED_HOST`; não substitui
  totalizadores operacionais nem throughput do mês corrente.
- O resultado da última discovery é persistido no snapshot sem alterar a marca
  d'água técnica `BPDATA.HOST.DT_LAST_DISCOVERY`.
- Sinais SSH e GPS/GNSS são persistidos diretamente em `HOST_CURRENT_SNAPSHOT`.
  A escrita dos sinais e a troca do snapshot usam o mesmo lock curto MariaDB
  para preservar falhas e recuperações concorrentes.
- Não inferir evidência de falha ou recuperação sem uma avaliação válida do
  domínio. Valores iniciais dos sinais não substituem essa avaliação.
- Discovery, backup, processamento e backlog publicam invalidações por host
  em `SUMMARY_OUTBOX`; o worker de summary recalcula as projeções afetadas.
  Tasks legadas de estatísticas solicitam o mesmo refresh de escopo.
- Alterações de schema devem preservar os consumidores de [SUMMARY.md](SUMMARY.md).
  Antes de executar uma migração, conferir o estado real do ambiente e o
  procedimento dos [scripts de banco](../../src/mariadb/scripts/README.md).

## Validação das métricas

Comparar os totais do snapshot com `FILE_TASK_HISTORY`, o estado de execução
com `FILE_TASK` e os diagnósticos com ERROR, SUSPENDED e FROZEN. Validar primeiro
uma estação de referência. Reconciliações e agregações seguem as restrições de
recursos do [ambiente do appCataloga](../../src/appCataloga/.instructions/ENVIRONMENT.md).
