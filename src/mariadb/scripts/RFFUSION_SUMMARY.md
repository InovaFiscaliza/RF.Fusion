# RFFUSION_SUMMARY

Este documento descreve o schema `RFFUSION_SUMMARY` no estado atual do projeto e o significado de cada tabela e coluna.

## Objetivo

O `RFFUSION_SUMMARY` e um banco de leitura. Ele concentra read models derivados de `BPDATA` e `RFDATA` para reduzir joins pesados no Webfusion e no restante do RFFusion.

## Convencoes de nomenclatura

| Prefixo | Significado |
| --- | --- |
| `ID_` | identificador tecnico da linha |
| `FK_` | chave de referencia para outra entidade |
| `NA_` | nome, rotulo, texto ou codigo textual |
| `DT_` | data ou data/hora |
| `NU_` | numero inteiro, contador ou quantidade |
| `VL_` | valor numerico, volume, coordenada ou medida |
| `IS_` | flag booleana |

## Regras importantes de semantica

1. `HOST_MONTHLY_METRIC` e mensal por `DT_FILE_CREATED`. Ou seja: a linha do mes representa a coorte de arquivos criados naquele mes.
2. `HOST_CURRENT_SNAPSHOT.NU_BACKUP_DONE_THIS_MONTH` e `HOST_CURRENT_SNAPSHOT.VL_BACKUP_DONE_GB_THIS_MONTH` sao mensais por `DT_BACKUP`, considerando o mes UTC corrente.
3. `SERVER_CURRENT_SUMMARY.NU_BACKUP_DONE_THIS_MONTH` e `SERVER_CURRENT_SUMMARY.VL_BACKUP_DONE_GB_THIS_MONTH` sao a soma dos campos equivalentes do `HOST_CURRENT_SNAPSHOT`.
4. Os limites de mes usados nos procedimentos sao em UTC porque o refresh usa `UTC_TIMESTAMP()`.
5. Tabelas com sufixo `_shadow` sao staging tables internas do worker de refresh. Elas nao sao o contrato publico de leitura e podem divergir temporariamente da tabela principal durante migracoes ou trocas atomicas.

## Visao geral do fluxo

| Origem principal | Read model gerado |
| --- | --- |
| `BPDATA.HOST` | `HOST_CURRENT_SNAPSHOT`, parte de `SERVER_CURRENT_SUMMARY` |
| `BPDATA.FILE_TASK_HISTORY` | `HOST_MONTHLY_METRIC`, `HOST_ERROR_SUMMARY`, `SERVER_ERROR_SUMMARY`, backup do mes no snapshot |
| `BPDATA.FILE_TASK` | filas no `HOST_CURRENT_SNAPSHOT`, erros canonicos |
| `BPDATA.HOST_TASK` | erros canonicos |
| `RFDATA` espectro, site, equipamento e geografia | `SITE_EQUIPMENT_OBS_SUMMARY`, `HOST_LOCATION_SUMMARY`, `MAP_SITE_STATION_SUMMARY`, `MAP_SITE_SUMMARY` |
| `VW_ERROR_EVENT_CANONICAL` | `HOST_ERROR_SUMMARY`, `SERVER_ERROR_SUMMARY`, ultimo erro no snapshot do host |

## `SUMMARY_OUTBOX`

Finalidade: conjunto coalescido de escopos sujos pendentes consumido pelo worker Python.

| Coluna | Significado |
| --- | --- |
| `ID_OUTBOX` | identificador tecnico monotonicamente crescente da ultima publicacao pendente desse escopo |
| `NA_SCOPE_TYPE` | tipo do escopo sujo, por exemplo `host`, `site`, `equipment`, `reference_month` ou `full_reconcile` |
| `NA_SCOPE_VALUE` | chave do escopo sujo; host/site/equipment usam o ID em texto e `full_reconcile` usa `*` |
| `NA_SOURCE_HANDLER` | handler publicador para rastreabilidade diagnostica |
| `NA_REASON` | razao curta da invalidacao, usada apenas para diagnostico |
| `DT_CREATED_AT` | instante da ultima publicacao pendente desse escopo |

## `SUMMARY_WORKER_STATE`

Finalidade: checkpoint duravel e saude do consumidor Python do summary.

| Coluna | Significado |
| --- | --- |
| `NA_CONSUMER` | nome logico do consumidor |
| `ID_LAST_OUTBOX` | ultimo `ID_OUTBOX` processado com sucesso |
| `DT_LAST_START` | inicio do ultimo ciclo do worker |
| `DT_LAST_END` | fim do ultimo ciclo do worker |
| `DT_LAST_SUCCESS` | ultimo ciclo finalizado com sucesso |
| `DT_LAST_FAILURE` | ultimo ciclo finalizado com erro |
| `NU_LAST_BATCH_SIZE` | batch-size configurado no ultimo sucesso |
| `NU_LAST_EVENT_COUNT` | quantidade de eventos no ultimo sucesso |
| `NA_STATUS` | status resumido do worker, por exemplo `idle`, `running` ou `error` |
| `NA_ERROR_MESSAGE` | ultima mensagem de erro no nivel do worker |

## `SUMMARY_REFRESH_LOG`

Finalidade: historico rolling append-only das execucoes de refresh por objeto.

Observacao: esta tabela substitui o antigo `SUMMARY_REFRESH_STATE` como fonte
de telemetria por objeto. O estado mais recente e obtido pela ultima linha de
cada `NA_OBJECT_NAME`, enquanto a saude global do daemon continua em
`SUMMARY_WORKER_STATE`.

| Coluna | Significado |
| --- | --- |
| `ID_REFRESH_LOG` | identificador tecnico do evento de refresh |
| `NA_OBJECT_NAME` | objeto refrescado |
| `DT_STARTED_AT` | inicio da execucao |
| `DT_FINISHED_AT` | fim da execucao |
| `IS_SUCCESS` | resultado final da execucao |
| `NU_ROW_COUNT` | quantidade de linhas produzidas nessa execucao |
| `NA_SOURCE_HIGH_WATERMARK` | watermark textual gravado ao final da execucao |
| `NA_ERROR_MESSAGE` | erro registrado quando a execucao falhou |

## `HOST_EQUIPMENT_LINK_OVERRIDE`

Finalidade: override manual de vinculacao host <-> equipamento.

| Coluna | Significado |
| --- | --- |
| `ID_OVERRIDE` | identificador tecnico do override |
| `FK_HOST` | host forcado manualmente |
| `FK_EQUIPMENT` | equipamento forcado manualmente |
| `NA_OVERRIDE_REASON` | motivo do override manual |
| `IS_ACTIVE` | se o override esta ativo |
| `DT_CREATED_AT` | data de criacao do override |
| `DT_UPDATED_AT` | data da ultima alteracao do override |

## `HOST_EQUIPMENT_LINK`

Finalidade: tabela de casamento entre `BPDATA.HOST` e `RFDATA.DIM_SPECTRUM_EQUIPMENT`.

Origem principal: heuristicas de nome, assinatura CWSM e overrides manuais.

| Coluna | Significado |
| --- | --- |
| `ID_LINK` | identificador tecnico do vinculo |
| `FK_HOST` | host vinculado |
| `FK_EQUIPMENT` | equipamento vinculado |
| `NA_HOST_NAME` | nome do host usado no casamento |
| `NA_EQUIPMENT` | nome do equipamento usado no casamento |
| `NA_HOST_NAME_NORMALIZED` | nome do host normalizado para matching |
| `NA_EQUIPMENT_NAME_NORMALIZED` | nome do equipamento normalizado para matching |
| `NA_HOST_SIGNATURE` | assinatura derivada do host, usada para familias CWSM |
| `NA_EQUIPMENT_SIGNATURE` | assinatura derivada do equipamento |
| `NA_MATCH_TYPE` | heuristica vencedora do casamento, por exemplo `exact_normalized`, `cwsm_signature`, `prefix_match` ou override manual |
| `VL_MATCH_CONFIDENCE` | confianca numerica do casamento |
| `IS_PRIMARY_LINK` | indica o vinculo principal do host; e o mais usado pelos demais summaries |
| `IS_MANUAL_OVERRIDE` | indica que a linha veio de override manual |
| `IS_ACTIVE` | indica se o vinculo esta ativo para consumo |
| `DT_REFRESHED_AT` | instante em que a linha foi materializada |

## `SITE_EQUIPMENT_OBS_SUMMARY`

Finalidade: resumo observacional por par site/equipamento, baseado nas observacoes de espectro.

Granularidade: uma linha por `FK_SITE` + `FK_EQUIPMENT`.

| Coluna | Significado |
| --- | --- |
| `FK_SITE` | site observado |
| `FK_EQUIPMENT` | equipamento observado no site |
| `NA_SITE_NAME` | nome bruto do site, quando disponivel |
| `NA_SITE_LABEL` | rotulo amigavel do site para UI e mapas |
| `FK_COUNTY` | chave do county associado ao site |
| `FK_DISTRICT` | chave do district associado ao site |
| `NA_COUNTY_NAME` | nome do county |
| `NA_DISTRICT_NAME` | nome do district |
| `ID_STATE` | identificador do estado |
| `NA_STATE_NAME` | nome do estado |
| `NA_STATE_CODE` | UF do estado |
| `VL_LATITUDE` | latitude do site |
| `VL_LONGITUDE` | longitude do site |
| `VL_ALTITUDE` | altitude do site |
| `NU_GNSS_MEASUREMENTS` | quantidade de medicoes GNSS que sustentam a localizacao do site |
| `NA_EQUIPMENT` | nome do equipamento |
| `DT_FIRST_SEEN_AT` | primeira observacao conhecida desse equipamento nesse site |
| `DT_LAST_SEEN_AT` | observacao mais recente desse equipamento nesse site |
| `NU_SPECTRUM_COUNT` | quantidade total de espectros associados ao par site/equipamento |
| `ID_LAST_SPECTRUM` | ultimo identificador de espectro associado ao par |
| `IS_CURRENT_LOCATION` | `1` quando esse site e a localizacao atual inferida do equipamento |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `HOST_LOCATION_SUMMARY`

Finalidade: historico e estado atual de localizacao por host.

Origem principal: `HOST_EQUIPMENT_LINK` + `SITE_EQUIPMENT_OBS_SUMMARY`.

Granularidade: uma linha por `FK_HOST` + `FK_SITE`.

| Coluna | Significado |
| --- | --- |
| `FK_HOST` | host resumido |
| `FK_SITE` | site associado ao host |
| `NA_HOST_NAME` | nome do host |
| `NA_LOCALITY_LABEL` | rotulo textual da localidade mostrado para o host |
| `NA_SITE_LABEL` | rotulo do site |
| `FK_COUNTY` | chave do county da localidade |
| `FK_DISTRICT` | chave do district da localidade |
| `NA_COUNTY_NAME` | nome do county |
| `NA_DISTRICT_NAME` | nome do district |
| `ID_STATE` | identificador do estado |
| `NA_STATE_NAME` | nome do estado |
| `NA_STATE_CODE` | UF do estado |
| `VL_LATITUDE` | latitude do site associado |
| `VL_LONGITUDE` | longitude do site associado |
| `VL_ALTITUDE` | altitude do site associado |
| `DT_FIRST_SEEN_AT` | primeira observacao conhecida do host nesse site |
| `DT_LAST_SEEN_AT` | ultima observacao conhecida do host nesse site |
| `NU_SPECTRUM_COUNT` | total de espectros que sustentam a associacao host/site |
| `NU_MATCHED_EQUIPMENT_TOTAL` | quantidade de equipamentos vinculados ao host que sustentam essa localizacao |
| `IS_CURRENT_LOCATION` | `1` quando essa e a localizacao atual inferida do host |
| `IS_OFFLINE_SNAPSHOT` | flag de host offline capturada no momento do refresh, nao uma verdade historica do site |
| `VL_MAX_MATCH_CONFIDENCE` | maior confianca entre os vinculos host/equipamento usados nessa localizacao |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `MAP_SITE_STATION_SUMMARY`

Finalidade: resumo por estacao para renderizacao de mapa por site.

Granularidade: uma linha por `FK_SITE` + `FK_EQUIPMENT`.

| Coluna | Significado |
| --- | --- |
| `FK_SITE` | site da linha do mapa |
| `FK_EQUIPMENT` | equipamento representado |
| `FK_HOST` | host associado ao equipamento, quando conhecido |
| `NA_SITE_LABEL` | rotulo do site |
| `NA_EQUIPMENT` | nome do equipamento |
| `NA_HOST_NAME` | nome do host associado |
| `IS_OFFLINE` | snapshot atual do host offline/online |
| `IS_CURRENT_LOCATION` | `1` quando o equipamento esta na localizacao atual do host |
| `NA_MAP_STATE` | estado semantico da estacao no mapa: `online_current`, `online_previous`, `offline_current`, `offline_previous` ou `no_host` |
| `NU_STATE_PRIORITY` | prioridade numerica do `NA_MAP_STATE`: `0`, `1`, `2`, `3`, `4` nessa ordem |
| `DT_FIRST_SEEN_AT` | primeira observacao do equipamento nesse site |
| `DT_LAST_SEEN_AT` | ultima observacao do equipamento nesse site |
| `NU_SPECTRUM_COUNT` | numero de espectros associados a essa estacao no site |
| `NA_MATCH_TYPE` | tipo de casamento host/equipamento usado para esta linha |
| `VL_MATCH_CONFIDENCE` | confianca do casamento usado |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `MAP_SITE_SUMMARY`

Finalidade: resumo agregado por site para marcadores de mapa.

Origem principal: agregacao de `MAP_SITE_STATION_SUMMARY`.

Granularidade: uma linha por `FK_SITE`.

| Coluna | Significado |
| --- | --- |
| `FK_SITE` | site resumido |
| `NA_SITE_LABEL` | rotulo do site |
| `FK_COUNTY` | county do site |
| `FK_DISTRICT` | district do site |
| `NA_COUNTY_NAME` | nome do county |
| `NA_DISTRICT_NAME` | nome do district |
| `ID_STATE` | identificador do estado |
| `NA_STATE_NAME` | nome do estado |
| `NA_STATE_CODE` | UF do estado |
| `VL_LATITUDE` | latitude do marcador |
| `VL_LONGITUDE` | longitude do marcador |
| `VL_ALTITUDE` | altitude do marcador |
| `NU_GNSS_MEASUREMENTS` | quantidade de medicoes GNSS usadas na georreferencia do site |
| `NA_MARKER_STATE` | estado agregado do marcador no mapa, usando a mesma familia de valores de `NA_MAP_STATE` |
| `NU_STATION_COUNT` | quantidade de estacoes observadas no site |
| `NU_KNOWN_HOST_COUNT` | quantidade de estacoes do site que ja possuem host conhecido |
| `NU_ONLINE_CURRENT_COUNT` | quantidade de estacoes com host online e localizacao atual no site |
| `NU_ONLINE_PREVIOUS_COUNT` | quantidade de estacoes com host online, mas o site e historico |
| `NU_OFFLINE_CURRENT_COUNT` | quantidade de estacoes com host offline e localizacao atual no site |
| `NU_OFFLINE_PREVIOUS_COUNT` | quantidade de estacoes com host offline, mas o site e historico |
| `NU_NO_HOST_COUNT` | quantidade de estacoes sem host mapeado |
| `HAS_ONLINE_STATION` | `1` se existe pelo menos uma estacao online no site |
| `HAS_ONLINE_HOST` | `1` se existe pelo menos um host online associado ao site |
| `HAS_KNOWN_HOST` | `1` se existe pelo menos um host conhecido associado ao site |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `HOST_MONTHLY_METRIC`

Finalidade: metricas mensais por host ancoradas no mes de criacao do arquivo.

Origem principal: `BPDATA.FILE_TASK_HISTORY`.

Granularidade: uma linha por `FK_HOST` + `DT_REFERENCE_MONTH`.

Nota importante: `DT_REFERENCE_MONTH` vem de `DT_FILE_CREATED`, nao de `DT_BACKUP`.

| Coluna | Significado |
| --- | --- |
| `FK_HOST` | host resumido |
| `DT_REFERENCE_MONTH` | primeiro dia do mes de referencia da coorte de arquivos criados |
| `NA_HOST_NAME` | nome do host |
| `NU_DISCOVERED_FILES` | quantidade de arquivos descobertos nessa coorte mensal |
| `VL_DISCOVERED_GB` | volume total em GB dos arquivos descobertos nessa coorte |
| `NU_BACKUP_DONE_FILES` | quantidade de arquivos da coorte com `NU_STATUS_BACKUP = 0` |
| `VL_BACKUP_DONE_GB` | volume em GB dos arquivos da coorte com backup concluido |
| `NU_BACKUP_PENDING_FILES` | quantidade de arquivos da coorte com `NU_STATUS_BACKUP = 1` |
| `VL_BACKUP_PENDING_GB` | volume em GB dos arquivos da coorte com backup pendente |
| `NU_BACKUP_ERROR_FILES` | quantidade de arquivos da coorte com `NU_STATUS_BACKUP = -1` |
| `VL_BACKUP_ERROR_GB` | volume em GB dos arquivos da coorte com erro de backup |
| `NU_PROCESSING_DONE_FILES` | quantidade de arquivos da coorte com `NU_STATUS_PROCESSING = 0` |
| `VL_PROCESSING_DONE_GB` | volume em GB dos arquivos da coorte com processamento concluido |
| `NU_PROCESSING_PENDING_FILES` | quantidade de arquivos da coorte com `NU_STATUS_PROCESSING = 1` |
| `VL_PROCESSING_PENDING_GB` | volume em GB dos arquivos da coorte com processamento pendente |
| `NU_PROCESSING_ERROR_FILES` | quantidade de arquivos da coorte com `NU_STATUS_PROCESSING = -1` |
| `VL_PROCESSING_ERROR_GB` | volume em GB dos arquivos da coorte com erro de processamento |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `HOST_ERROR_SUMMARY`

Finalidade: agrupamento de erros por host e por assinatura canonica.

Origem principal: `VW_ERROR_EVENT_CANONICAL`.

Granularidade: uma linha por host + escopo + dominio + stage + codigo + hash do resumo.

| Coluna | Significado |
| --- | --- |
| `ID_HOST_ERROR_SUMMARY` | identificador tecnico da agregacao de erro |
| `FK_HOST` | host ao qual o erro pertence |
| `NA_HOST_NAME` | nome do host |
| `NA_ERROR_SCOPE` | escopo do erro, por exemplo `BACKUP`, `PROCESSING`, `BACKUP_QUEUE`, `PROCESSING_QUEUE` ou `HOST_TASK` |
| `NA_ERROR_DOMAIN` | dominio funcional do erro, quando classificado |
| `NA_ERROR_STAGE` | etapa do pipeline em que o erro ocorreu |
| `NA_ERROR_CODE` | codigo do erro |
| `NA_ERROR_SUMMARY_HASH` | hash SHA-256 do resumo canonico usado para agrupamento |
| `NA_ERROR_SUMMARY` | resumo canonico do erro |
| `NU_ERROR_COUNT` | quantidade total de ocorrencias desse grupo de erro |
| `DT_FIRST_SEEN_AT` | primeira ocorrencia conhecida do grupo |
| `DT_LAST_SEEN_AT` | ocorrencia mais recente do grupo |
| `NA_LAST_SOURCE_TABLE` | tabela de origem da ultima ocorrencia do grupo |
| `ID_LAST_SOURCE_ROW` | chave da ultima linha de origem que alimentou o grupo |
| `NA_LAST_ERROR_DETAIL` | detalhe textual da ultima ocorrencia |
| `NA_LAST_RAW_MESSAGE` | mensagem crua mais recente associada ao grupo |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `SERVER_ERROR_SUMMARY`

Finalidade: agrupamento de erros em nivel global, sem separar por host.

Origem principal: `VW_ERROR_EVENT_CANONICAL`.

| Coluna | Significado |
| --- | --- |
| `ID_SERVER_ERROR_SUMMARY` | identificador tecnico da agregacao global de erro |
| `NA_ERROR_SCOPE` | escopo do erro |
| `NA_ERROR_DOMAIN` | dominio funcional do erro |
| `NA_ERROR_STAGE` | etapa do pipeline |
| `NA_ERROR_CODE` | codigo do erro |
| `NA_ERROR_SUMMARY_HASH` | hash SHA-256 do resumo canonico usado para agrupamento |
| `NA_ERROR_SUMMARY` | resumo canonico do erro |
| `NU_ERROR_COUNT` | quantidade total de ocorrencias desse grupo no ambiente inteiro |
| `DT_FIRST_SEEN_AT` | primeira ocorrencia conhecida do grupo |
| `DT_LAST_SEEN_AT` | ocorrencia mais recente do grupo |
| `NA_LAST_SOURCE_TABLE` | tabela de origem da ultima ocorrencia |
| `ID_LAST_SOURCE_ROW` | chave da ultima linha de origem |
| `NA_LAST_ERROR_DETAIL` | detalhe textual da ultima ocorrencia |
| `NA_LAST_RAW_MESSAGE` | mensagem crua mais recente |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `HOST_CURRENT_SNAPSHOT`

Finalidade: snapshot atual por host, com foco operacional.

Origem principal: `BPDATA.HOST`, filas ativas, `HOST_LOCATION_SUMMARY`, `HOST_MONTHLY_METRIC`, `VW_ERROR_EVENT_CANONICAL` e volume de backup do mes por `DT_BACKUP`.

Granularidade: uma linha por host.

| Coluna | Significado |
| --- | --- |
| `ID_HOST` | identificador do host |
| `NA_HOST_NAME` | nome do host |
| `NA_HOST_ADDRESS` | endereco de rede do host |
| `NA_HOST_PORT` | porta configurada do host |
| `IS_OFFLINE` | flag atual de host offline |
| `IS_BUSY` | flag atual de host ocupado |
| `NU_PID` | PID atual do processo do host, quando houver |
| `DT_BUSY` | instante em que o host entrou em estado busy |
| `DT_LAST_FAIL` | instante da ultima falha operacional registrada no host |
| `DT_LAST_CHECK` | instante do ultimo host check |
| `NU_HOST_CHECK_ERROR` | contador ou ultimo codigo de erro do host check, conforme `BPDATA.HOST` |
| `DT_LAST_DISCOVERY` | instante da ultima descoberta de arquivos |
| `NU_DONE_FILE_DISCOVERY_TASKS` | quantidade registrada de discovery tasks concluidas |
| `NU_ERROR_FILE_DISCOVERY_TASKS` | quantidade registrada de discovery tasks com erro |
| `DT_LAST_BACKUP` | instante do ultimo backup registrado para o host |
| `NU_PENDING_FILE_BACKUP_TASKS` | quantidade atual de arquivos pendentes de backup no host |
| `NU_DONE_FILE_BACKUP_TASKS` | quantidade registrada de backups concluidos no host |
| `NU_ERROR_FILE_BACKUP_TASKS` | quantidade registrada de backups com erro no host |
| `NU_BACKUP_DONE_THIS_MONTH` | quantidade de arquivos com backup concluido no mes UTC corrente, usando `DT_BACKUP` |
| `VL_PENDING_BACKUP_GB` | volume atual pendente de backup do host, em GB |
| `VL_BACKUP_DONE_GB_THIS_MONTH` | volume em GB transferido com sucesso no mes UTC corrente, usando `DT_BACKUP` |
| `VL_DONE_BACKUP_GB` | volume total acumulado de backup concluido do host, em GB |
| `DT_LAST_PROCESSING` | instante do ultimo processamento registrado |
| `NU_PENDING_FILE_PROCESS_TASKS` | quantidade atual de arquivos pendentes de processamento |
| `NU_DONE_FILE_PROCESS_TASKS` | quantidade registrada de processamentos concluidos |
| `NU_ERROR_FILE_PROCESS_TASKS` | quantidade registrada de processamentos com erro |
| `NU_HOST_FILES` | total de arquivos descobertos do host; o refresh prefere a soma de `HOST_MONTHLY_METRIC` e usa `BPDATA.HOST.NU_HOST_FILES` como fallback |
| `NU_BACKUP_QUEUE_FILES_TOTAL` | quantidade de itens hoje na fila de backup (`FILE_TASK`) |
| `VL_BACKUP_QUEUE_GB_TOTAL` | volume em GB hoje na fila de backup |
| `NU_PROCESSING_QUEUE_FILES_TOTAL` | quantidade de itens hoje na fila de processamento |
| `VL_PROCESSING_QUEUE_GB_TOTAL` | volume em GB hoje na fila de processamento |
| `NU_MATCHED_EQUIPMENT_TOTAL` | quantidade de equipamentos ativos e primarios vinculados ao host |
| `NU_FACT_SPECTRUM_TOTAL` | total de fatos de espectro associados ao host via seus equipamentos vinculados |
| `FK_CURRENT_SITE` | site atual inferido para o host |
| `NA_CURRENT_SITE_LABEL` | rotulo do site atual |
| `NA_CURRENT_STATE_CODE` | UF do site atual |
| `VL_CURRENT_LATITUDE` | latitude do site atual |
| `VL_CURRENT_LONGITUDE` | longitude do site atual |
| `DT_CURRENT_SITE_LAST_SEEN` | ultima observacao do host no site atual |
| `NA_LAST_ERROR_SCOPE` | escopo do erro mais recente do host |
| `NA_LAST_ERROR_CODE` | codigo do erro mais recente do host |
| `NA_LAST_ERROR_SUMMARY` | resumo do erro mais recente do host |
| `DT_LAST_ERROR_AT` | instante do erro mais recente do host |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `SERVER_CURRENT_SUMMARY`

Finalidade: resumo operacional global do ambiente.

Origem principal: agregacao de `HOST_CURRENT_SNAPSHOT` e contagem de grupos em `SERVER_ERROR_SUMMARY`.

Granularidade: uma unica linha. O `ID_SUMMARY` esperado e `1`.

| Coluna | Significado |
| --- | --- |
| `ID_SUMMARY` | chave singleton do resumo global |
| `NA_CURRENT_MONTH_LABEL` | rotulo do mes UTC corrente no formato `YYYY-MM` |
| `NU_TOTAL_HOSTS` | total de hosts no snapshot |
| `NU_ONLINE_HOSTS` | total de hosts online |
| `NU_OFFLINE_HOSTS` | total de hosts offline |
| `NU_BUSY_HOSTS` | total de hosts ocupados |
| `NU_DISCOVERED_FILES_TOTAL` | soma global de `NU_HOST_FILES` |
| `NU_BACKUP_PENDING_FILES_TOTAL` | soma global de arquivos pendentes de backup |
| `VL_BACKUP_PENDING_GB_TOTAL` | soma global do volume pendente de backup em GB |
| `NU_BACKUP_ERROR_FILES_TOTAL` | soma global de arquivos com erro de backup no snapshot |
| `NU_BACKUP_QUEUE_FILES_TOTAL` | soma global de itens na fila de backup |
| `VL_BACKUP_QUEUE_GB_TOTAL` | soma global do volume em fila de backup |
| `NU_PROCESSING_PENDING_FILES_TOTAL` | soma global de arquivos pendentes de processamento |
| `NU_PROCESSING_DONE_FILES_TOTAL` | soma global de arquivos com processamento concluido |
| `NU_PROCESSING_ERROR_FILES_TOTAL` | soma global de arquivos com erro de processamento |
| `NU_PROCESSING_QUEUE_FILES_TOTAL` | soma global de itens na fila de processamento |
| `VL_PROCESSING_QUEUE_GB_TOTAL` | soma global do volume em fila de processamento |
| `NU_FACT_SPECTRUM_TOTAL` | soma global de fatos de espectro associados aos hosts |
| `NU_BACKUP_DONE_THIS_MONTH` | soma global de backups concluidos no mes UTC corrente, por `DT_BACKUP` |
| `VL_BACKUP_DONE_GB_THIS_MONTH` | soma global do volume transferido com sucesso no mes UTC corrente, por `DT_BACKUP` |
| `NU_BACKUP_ERROR_GROUPS` | quantidade de grupos de erro globais no escopo `BACKUP` |
| `NU_PROCESSING_ERROR_GROUPS` | quantidade de grupos de erro globais no escopo `PROCESSING` |
| `DT_REFRESHED_AT` | instante de materializacao da linha |

## `VW_ERROR_EVENT_CANONICAL`

Finalidade: view canonica de eventos de erro usada para consolidar erros por host e globalmente.

Observacao: esta e uma view, nao uma tabela persistida.

| Coluna | Significado |
| --- | --- |
| `NA_SOURCE_TABLE` | tabela fisica de origem do erro, por exemplo `FILE_TASK_HISTORY`, `FILE_TASK` ou `HOST_TASK` |
| `ID_SOURCE_ROW` | identificador da linha de origem |
| `NA_ERROR_SCOPE` | escopo canonico do erro |
| `FK_HOST` | host associado ao erro, quando houver |
| `NA_HOST_NAME` | nome do host associado |
| `DT_EVENT_AT` | melhor timestamp disponivel para representar o erro |
| `NU_STATUS_VALUE` | valor de status bruto da linha de origem |
| `NA_ERROR_DOMAIN` | dominio funcional do erro |
| `NA_ERROR_STAGE` | etapa do pipeline |
| `NA_ERROR_CODE` | codigo do erro |
| `NA_ERROR_SUMMARY` | resumo canonico do erro |
| `NA_ERROR_SUMMARY_HASH` | hash SHA-256 do resumo canonico |
| `NA_ERROR_DETAIL` | detalhe textual do erro |
| `NU_ERROR_CLASSIFIER_VERSION` | versao do classificador de erro que preencheu a linha |
| `NA_RAW_MESSAGE` | mensagem original ou mensagem crua da origem |

## Tabelas internas `_shadow`

Estas tabelas pertencem ao mecanismo de swap atomico do worker e nao devem ser usadas como fonte publica de leitura:

| Tabela interna | Contrato funcional |
| --- | --- |
| `HOST_CURRENT_SNAPSHOT_shadow` | espelho interno de `HOST_CURRENT_SNAPSHOT` |
| `HOST_EQUIPMENT_LINK_shadow` | espelho interno de `HOST_EQUIPMENT_LINK` |
| `HOST_ERROR_SUMMARY_shadow` | espelho interno de `HOST_ERROR_SUMMARY` |
| `HOST_LOCATION_SUMMARY_shadow` | espelho interno de `HOST_LOCATION_SUMMARY` |
| `HOST_MONTHLY_METRIC_shadow` | espelho interno de `HOST_MONTHLY_METRIC` |
| `MAP_SITE_STATION_SUMMARY_shadow` | espelho interno de `MAP_SITE_STATION_SUMMARY` |
| `MAP_SITE_SUMMARY_shadow` | espelho interno de `MAP_SITE_SUMMARY` |
| `SERVER_CURRENT_SUMMARY_shadow` | espelho interno de `SERVER_CURRENT_SUMMARY` |
| `SERVER_ERROR_SUMMARY_shadow` | espelho interno de `SERVER_ERROR_SUMMARY` |
| `SITE_EQUIPMENT_OBS_SUMMARY_shadow` | espelho interno de `SITE_EQUIPMENT_OBS_SUMMARY` |

Regra pratica: o significado das colunas de cada `_shadow` e o mesmo da tabela principal correspondente. O detalhe importante e operacional: a tabela `_shadow` pode estar vazia, em rebuild ou ate com schema transitoriamente diferente durante uma migracao. Para leitura de negocio, use sempre a tabela principal sem o sufixo.
