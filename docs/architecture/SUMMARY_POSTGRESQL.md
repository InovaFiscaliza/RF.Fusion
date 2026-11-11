# Modelo PostgreSQL do Summary

Este contrato substitui a estrutura física legada na instalação PostgreSQL.
A decisão foi autorizada na revisão do handler: não há exigência de
retrocompatibilidade SQL com WebFusion. O MariaDB existente não é alterado.
O significado das métricas permanece em [METRICAS_OPERACIONAIS.md](METRICAS_OPERACIONAIS.md).

## Propriedade e granularidade

Cadastros e sinais operacionais pertencem ao BPDATA; equipamentos, sites e
geografia pertencem ao RFDATA. O Summary guarda somente agregados caros de
recalcular. Não copia rótulos, coordenadas, credenciais, conectividade ou locks.

| Tabela derivada | Chave e conteúdo | Fonte |
|---|---|---|
| `HOST_EQUIPMENT_LINK` | Host/equipamento, classificação do vínculo e prioridade | Cruzamento automático dos cadastros |
| `SITE_EQUIPMENT_METRIC` | Site/equipamento, primeiro/último instante, contagem e último espectro | `FACT_SPECTRUM` |
| `HOST_LOCATION_VISIT` | Host/número de passagem, site, intervalo, contagem e sobreposição | Espectros e vínculo primário |
| `HOST_MONTHLY_METRIC` | Host/mês de criação do arquivo, contagens e volumes por fase | Histórico de arquivos |
| `HOST_METRIC` | Host, contagens/volumes de fila e acervo e throughput do mês identificado na linha | Fila e histórico |
| `HOST_ERROR_METRIC` | Host/fase/estado/código/hash, contagem e última ocorrência | Histórico e filas |

`HOST_MONTHLY_METRIC` usa o mês de **criação** do artefato. Throughput em
`HOST_METRIC` usa o mês operacional de backup/processamento. Essa diferença
é intencional: uma projeção não substitui a outra. `DT_REFERENCE_MONTH` impede
que um valor de throughput de um refresh anterior seja interpretado como mês
corrente depois da virada. Não somar buckets de acervo para obter throughput.

As tabelas `SUMMARY_OUTBOX`, `SUMMARY_WORKER_STATE` e `SUMMARY_REFRESH_LOG`
sustentam invalidações, checkpoint e auditoria limitada. As duas tabelas
`WEBFUSION_USAGE_LOG_CHECKPOINT` e `WEBFUSION_USAGE_METRIC_MONTHLY` conservam
seu contrato e são as únicas tabelas do Summary sujeitas a restore.

## Consultas e reconstrução

Não persistir `HOST_LOCATION_SUMMARY`, `MAP_SITE_STATION_SUMMARY`,
`MAP_SITE_SUMMARY`, `SERVER_CURRENT_SUMMARY`, `SERVER_ERROR_SUMMARY` ou
`HOST_CURRENT_SNAPSHOT`, nem criar views de compatibilidade com esses nomes.
Localidades e mapas combinam os agregados canônicos com os cadastros originais.
Totais de servidor são SUM/GROUP BY dos agregados por host. Localização atual
usa ordenação determinística por último instante, primeiro instante e site;
valores nulos ficam por último. Uma passagem continua sendo uma observação,
sem inferência de permanência e sem apagar sobreposições.

O handler segue a convenção `entidade_ação_qualificação`, com métodos por
entidade e CRUD protegido da base. Não expõe SQL em texto, substituição genérica
de qualquer tabela nem uma segunda API de sessão. Métodos compostos participam
da transação externa. Substituição por escopo faz DELETE e INSERT na mesma
transação; uma seleção vazia não significa reconstrução global.

`full_reconcile` reconstrói apenas as seis tabelas derivadas, em ordem de
vínculo, observação, visitas e agregados operacionais. Não apaga métricas de uso.
O checkpoint só cobre eventos observados antes do início da reconstrução;
eventos publicados durante o trabalho precisam permanecer para o próximo lote.
Não limpar indiscriminadamente o outbox depois do reconcile.

## Sinais e configuração operacional

Os campos de última discovery, SSH e GPS/GNSS ficam uma única vez em
`BPDATA.HOST`. Ausência de avaliação permanece distinguível por timestamp nulo;
o reconcile não inventa recuperação nem falha. Escritas parciais de sinais
não apagam evidências ausentes do payload. Não há lock de snapshot: sinais não
são mais copiados nem substituídos durante materialização.

## Cruzamento para o WebFusion

A relação host/equipamento tem contexto exclusivamente nas consultas do
WebFusion, que relacionam a estação operacional ao acervo analítico e às
localidades observadas. Não é cadastro operacional nem identidade de ingestão:
coleta, filas e processamento do appCataloga não dependem desse vínculo.
As identidades de host em BPDATA e de equipamento em RFDATA são independentes.
O worker do Summary apenas materializa esse modelo de leitura e seus agregados
dependentes; isso não atribui ao domínio operacional a propriedade da relação.

`HOST_EQUIPMENT_LINK` concentra o cruzamento automático entre `BPDATA.HOST` e
`RFDATA.DIM_SPECTRUM_EQUIPMENT`. Os candidatos seguem, em ordem de confiança,
nome normalizado exato, assinatura CWSM e prefixo. Empates na maior confiança
permanecem sem vínculo primário. O refresh substitui todos os vínculos na mesma
transação dos agregados dependentes; não preserva decisões manuais.
Não há tabela de overrides, flag de vínculo manual nem API para edição manual
desse cruzamento na instalação PostgreSQL. Os vínculos são reconstruíveis e
não exigem restore.

## Instalação e integração

O DDL cria um ambiente novo. Não executar DROP, alterar bancos compartilhados
ou iniciar reconcile para aplicar esta revisão. O motor e o worker Summary usam os métodos novos e
recebem conninfo explícito de `RFFUSION_PG_CONNINFO`; isso não converte o
WebFusion nem todos os demais workers para PostgreSQL. O lote de integração
de consumidores deve atualizar suas consultas e validar resultados; `full_reconcile` reconstrói dados, não reescreve SQL cliente.
Não manter aliases da API antiga para esconder essa dependência.

Restaurar juntas as duas tabelas de uso antes de retomar a leitura dos logs:
restaurar apenas contadores ou apenas offsets pode duplicar ou perder contagens.
Demais linhas derivadas, auditoria e checkpoint podem começar vazios. Volumes,
latência e concorrência precisam ser medidos no ambiente de ensaio; a redução
estrutural de cópias não constitui benchmark de desempenho.
