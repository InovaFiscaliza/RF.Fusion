# Planejamentos e pendências

Este diretório reúne propostas que abrangem várias aplicações. Os contratos
vigentes continuam nas instruções dos subprojetos e em `docs/architecture/`.
Cada proposta exige definição de escopo, atualização arquitetural e validação
antes de implementação. Remover itens concluídos; o histórico fica no Git.

## Ingestão manual e drive-test

[Planejamento da ingestão convencional, manual e de drive-test](PLANEJAMENTO_DRIVE_TEST.md).

Situação: planejamento, dependente de revisão arquitetural e migração.
Envolve WebFusion, appCataloga e RFDATA: entrada manual, identificação de hardware,
MD5, comparação de medições, kit/campanha e catálogo móvel. Os exemplos de
payload usados na proposta ficam em [drive-test/exemplos.json](drive-test/exemplos.json).

## Publicação operacional no Zabbix

Situação: integração pendente. O adaptador
[`queryFileMetadata_trapper.py`](../../src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py)
ainda solicita metadata ao gateway; a publicação completa do contrato de
[métricas operacionais](../architecture/METRICAS_OPERACIONAIS.md) exige:

- definir transporte por JSON com items dependentes ou items individuais;
- transportar métricas, timestamps, flags e evidências por host;
- configurar items, macros e triggers nos templates RFeye, CelPlan, ERMx e UMS;
- validar abertura e recuperação de Problems numa estação controlada.

O adaptador não calcula thresholds, não define severidade e não abre ou encerra
Problems diretamente. Essas responsabilidades permanecem no Zabbix.

## Manutenção estrutural do appCataloga

Situação: propostas pendentes, sem implementação autorizada por este índice.

- Avaliar renomes `host_handler/` → `host/`, `summary_handler/` → `summary/`
  e `server_handler/` → `runtime/`, incluindo o destino do transporte do gateway.
- Definir tipos de tasks distinguindo linhas de banco e contextos normalizados
  pelos workers antes de introduzir `TypedDict`.
- Substituir `_FIXED_SITE_UPDATE_AGGREGATOR` em `appAnalise/processing_bin.py`
  por contexto explícito, preservando persistência e comportamento.

## Vínculos entre domínios e eventos analíticos

Situação: avaliação futura, sem dependência para a ingestão ou métricas atuais.

- Avaliar a necessidade de um domínio `RFFUSION_BRIDGE` para vínculos oficiais
  entre hosts e equipamentos; não criar esse banco com base apenas nesta proposta.
- Quando houver produtor de eventos como `EMISSION_DETECTED` ou
  `EMISSION_RECURRENT`, avaliar se o Zabbix atende ao ciclo de tratamento ou
  se um domínio adicional é necessário. Não antecipar `ISSUESDB`, `ISSUE_CFG`
  ou worker de thresholds sem esse caso de uso.
