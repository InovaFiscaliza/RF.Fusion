# Contratos compartilhados do RFFUSION_SUMMARY

## Escopo e responsabilidades

`RFFUSION_SUMMARY` é o banco de modelos de leitura derivados de `BPDATA` e
`RFDATA`, consumidos pelo WebFusion, pelo cliente MATLAB appAnalise e pela
integração operacional com Zabbix. Não é o dono do ciclo de vida das filas.

A materialização pertence a `summary_handler/refresh_engine.py` e o SQL a
`db/dbHandlerSummary.py`, no runtime do appCataloga. As regras desse runtime
estão em [ARCHITECTURE.md](../../src/appCataloga/.instructions/ARCHITECTURE.md).
As regras do consumidor web estão na
[arquitetura do WebFusion](../../src/webfusion/.instructions/ARCHITECTURE.md).

## Compatibilidade externa

As tabelas abaixo são contratos públicos, também consumidos pelo cliente
MATLAB por `src/webfusion/DBHandler.m`:

- `HOST_LOCATION_SUMMARY`;
- `MAP_SITE_SUMMARY`;
- `MAP_SITE_STATION_SUMMARY`;
- `SITE_EQUIPMENT_OBS_SUMMARY`.

Não renomear tabelas, remover colunas consumidas, alterar granularidade ou
mudar silenciosamente a semântica. Novas semânticas exigem um modelo paralelo
ou versionado. Colunas opcionais, índices e backfills também exigem revisão
explícita de compatibilidade, preservando os consumidores existentes.

## Linha do tempo de localidades

`HOST_LOCATION_TIMELINE_SUMMARY` é um modelo de leitura adicional para `/host`.
Não substitui nem altera a granularidade de `HOST_LOCATION_SUMMARY`.

- Cada linha representa uma passagem consecutiva por um site, por estação.
- A sequência usa o início das medições em `FACT_SPECTRUM`, com site e ID do
  espectro como desempate determinístico. Uma mudança de site abre uma passagem;
  um retorno ao mesmo site gera outra linha.
- Início e fim são os limites das medições observadas nessa passagem. Não
  representam datas comprovadas de chegada/saída nem permanência sem interrupção.
- Intervalos sobrepostos em sites diferentes são preservados e sinalizados.
  Não cortar datas para simular uma trajetória sem conflitos.
- Considerar somente vínculos ativos e primários de `HOST_EQUIPMENT_LINK`.
  Espectros sem início ou site não permitem posicionamento cronológico.
- SQL e substituição transacional por estação pertencem a `dbHandlerSummary`;
  orquestração e sinalização de sobreposição ficam em `summary_handler/refresh_engine.py`.
- Reconciliação completa percorre as estações sequencialmente. Eventos de
  site/equipamento recalculam apenas estações afetadas, incluindo visitas antigas
  ao site invalidado e hosts explícitos do lote. Eventos puramente operacionais
  de host não varrem espectros. Correções retroativas recompõem a sequência.
  Uma atualização dirigida por host também pode ser solicitada pela API pública
  `SummaryRefreshEngine.refresh_location_timeline(host_ids={...})`.
- O WebFusion lê exclusivamente esse SUMMARY para a linha do tempo, em ordem
  decrescente de início. A API mantém `location_history`, com uma linha por passagem.

## Métricas e operação

O contrato de métricas, sinais, unidades e responsabilidade do Zabbix está em
[METRICAS_OPERACIONAIS.md](METRICAS_OPERACIONAIS.md).
