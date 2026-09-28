# Planejamento da carga manual e de drive-test

Status: proposta para implementação incremental. Este documento não altera o
contrato arquitetural vigente nem autoriza migrações ou mudanças em produção.
Data: 25/09/2026.

## Objetivo e decisões confirmadas

Transformar o upload no WebFusion em uma entrada de dados processáveis, inclusive
drive-test, sem exigir cadastro fictício no Zabbix e preservando a coleta fixa.

Decisões confirmadas pelo responsável:

- A estação móvel identifica um kit permanente, reutilizável em várias campanhas.
- A campanha tem identidade própria, separada do kit.
- O contorno geográfico deve ser calculado a partir dos pontos GPS.
- Os sites móveis devem usar `DIM_SPECTRUM_SITE`, com `FK_TYPE` apontando para
  `DIM_SITE_TYPE = Mobile`, e armazenar o polígono em `GEOGRAPHIC_PATH`.
- Arquivos RFEye podem crescer entre backups. Preservar a identidade lógica
  por origem/caminho/nome e a substituição transacional do resultado analítico
  ao processar uma nova versão. Hash identifica conteúdo, não o arquivo lógico.
- Comportamento confirmado: uma cópia parcial RFEye de 30 MB é processada
  corretamente, assim como a cópia posterior de 50 MB. Não exigir encerramento
  da aquisição, tamanho final ou estabilidade do arquivo na estação para aceitar
  essas cópias. Preservar o suporte atual a arquivos parciais válidos; a exigência
  de integridade da transferência diz respeito à cópia recebida, não ao término
  da medição na origem.
- Entradas confirmadas: upload manual de drive-test (ROMES, appAnalise e
  monitorRNI); upload manual de estações fixas conhecidas ou desconhecidas;
  fluxo convencional de descoberta e backup das estações pelo RFFusion.

## Matriz das entradas de dados

| Entrada | Canal | Tipo de aquisição | Identificação | Tratamento proposto |
|---|---|---|---|---|
| 1. Drive-test: ROMES, appAnalise, monitorRNI | Manual | Móvel | Kit permanente, instrumento, campanha e sessão | Resolver identidade móvel e site Mobile com contorno GPS |
| 2.1. Estação fixa conhecida | Manual | Fixa | Vínculo com estação existente | Reutilizar a identidade e as regras da estação, sem duplicar o cadastro |
| 2.2. Estação fixa desconhecida | Manual | Fixa | Identidade ainda não resolvida no catálogo | Registrar a origem e resolver/cadastrar a identidade fixa, sem inventar vínculo Zabbix |
| 3. Descoberta e backup convencionais | Automático | Conforme o contrato atual das estações | Host operacional já conhecido | Preservar descoberta, backup, versões parciais e reprocessamento existentes |

ROMES, appAnalise e monitorRNI são origens previstas para a entrada móvel. A
compatibilidade efetiva de cada formato com o parser será verificada com amostras;
esta classificação não afirma que todos já sejam suportados pelo processamento.

Tratar como eixos distintos:

- **Canal:** upload manual ou coleta automática. Indica como os bytes chegaram.
- **Tipo de aquisição:** fixa ou móvel. Indica a semântica da medição e do site.
- **Identidade:** resolvida ou pendente. Indica se a estação/kit já foi identificado.
- **Vínculo operacional:** presença ou ausência de host no Zabbix. Não substitui
  a identidade de uma estação no catálogo analítico.

Uma estação fixa pode existir no catálogo sem estar monitorada pelo Zabbix.
Antes da implementação, definir exatamente o que caracteriza uma estação
"conhecida": identidade analítica resolvida, host operacional ou ambos. Preservar
essa distinção na interface e nos contratos, mesmo quando hoje coincidem.

### Estação fixa conhecida recebida por upload

Reutilizar a identidade canônica e aplicar as mesmas regras de RFEye, CWSM e
ERMx/UMS quando o vínculo operacional correspondente estiver disponível. Não
presumir que o caminho de upload seja o caminho original na estação.

A deduplicação deve atravessar os canais: se a mesma versão já veio pelo backup,
o upload registra outra ocorrência, sem repetir o resultado analítico. Se for
uma versão diferente, resolver o vínculo com o arquivo lógico antes de substituir
seus espectros. A origem manual não ganha precedência automática sobre o backup,
e uma cópia antiga enviada depois não deve desfazer uma versão mais completa.

### Estação fixa desconhecida recebida por upload

Conservar o tipo fixo, os identificadores brutos do payload e a proveniência.
Proposta inicial: aceitar o recebimento e a extração de metadados, mantendo o
resultado pendente de identificação antes da publicação analítica definitiva.
Permitir associar a uma estação existente ou cadastrar uma identidade fixa
independente do Zabbix, mediante regra de cadastro ainda a definir.

Não usar um único cadastro "desconhecido" para todas as estações; não confundir
modelo do analisador, proximidade geográfica ou nome do arquivo com identidade
única. Se a estação for reconhecida posteriormente, reconciliar a identidade e
os resultados existentes em vez de cadastrar novamente os mesmos dados.

Para ERMx/UMS sem vínculo operacional conhecido, o modelo extraído pelo appAnalise
não permite inferir sozinho a estação. O ajuste de nome usado nas estações
conhecidas depende de informação que esse caso ainda não possui.

### Convergência dos fluxos

Os três canais funcionais convergem nos contratos de arquivo lógico, versão,
linhagem e persistência analítica. O transporte e a resolução inicial de origem
continuam específicos de cada entrada. A fila manual proposta atende às entradas
1, 2.1 e 2.2; o fluxo convencional conserva sua fila e seu ciclo de vida.

Site fixo mantém a resolução geográfica de medição fixa; site Mobile usa a
trajetória/contorno da aquisição. O canal manual não decide o tipo do site.

## Diagnóstico do código atual

- `createProcessingDB.sql` define `HOST.ID_HOST` como o ID do Zabbix. A fila e o
  histórico usam host/caminho/nome; o histórico exige `FK_HOST` não nulo.
- O worker `appCataloga_file_bin_process_appAnalise.py` lê host e hostname da
  tarefa e usa o hostname inclusive para decidir a exportação.
- `processing_bin.resolve_equipment_persistence_identity()` resolve RFEye/CWSM
  e substitui a identidade do analisador pela estação operacional em ERMx/UMS.
  O parser contém normalização de identificadores CWSM. Essas regras devem
  permanecer restritas ao fluxo fixo; não preencher hostname artificial no móvel.
- `dbHandlerRFM.insert_file()` identifica o artefato por volume/caminho/nome.
  O schema possui `NU_MD5`, mas esse método não o preenche. A simples existência
  da coluna não oferece deduplicação por conteúdo.
- `insert_spectrum()` aplica outra regra de idempotência, baseada em dimensões,
  parâmetros e intervalo temporal. A identidade de espectro inclui `FK_SITE`.
- Antes da inserção, `processing_bin._reset_reprocessed_file_lineage()` chama
  `dbHandlerRFM.reset_reprocessed_file_lineage()` dentro da transação de RFDATA.
  O reset considera origem, destino anterior e novo destino, remove seus vínculos
  e exclui somente espectros órfãos. Resultados ainda vinculados a outros arquivos
  são preservados. A nova resposta do appAnalise é autoritativa, inclusive quando
  a divisão em bandas/espectros muda; não se trata apenas de estender datas.
- `insert_site()` atualmente grava ponto, altitude e geografia administrativa;
  não preenche `FK_TYPE` nem `GEOGRAPHIC_PATH`.
- `_build_spectrum_site_data()` recebe GPS resumido por espectro e o transforma
  em listas de uma amostra. Essas listas não constituem a trajetória completa;
  o contrato com appAnalise precisa disponibilizar os pontos do percurso.
- `get_site_id()` busca o ponto mais próximo sem filtrar o tipo do site. Inserir
  sites móveis sem corrigir essa consulta também pode afetar futuras cargas fixas.
- No DDL versionado, `GEO_POINT` é obrigatório e `GEOGRAPHIC_PATH` é `BLOB`.
  A existência do campo não estabelece o formato binário esperado pelo consumidor.
- Parte dos summaries e do mapa depende de `HOST_EQUIPMENT_LINK`. Persistir dados
  móveis em RFDATA não garante que apareçam nas consultas e mapas existentes.

Diagnóstico baseado no código e no DDL versionados. Conferir o schema efetivo e
amostras reais de payload antes de elaborar as migrações.

## 1. Definir os contratos de identidade e origem

Separar os conceitos de origem, arquivo lógico e versão de conteúdo:

| Conceito | Identidade proposta | Responsabilidade |
|---|---|---|
| Kit móvel | ID interno permanente, independente do nome | Identificar a estação móvel |
| Instrumento | Fabricante/modelo/número de série quando disponíveis | Identificar o receptor utilizado |
| Campanha | ID próprio | Agrupar uma atividade de medição |
| Aquisição ou sessão | ID próprio vinculado à campanha e ao kit | Agrupar arquivos do mesmo percurso |
| Arquivo lógico | ID interno e identidade na origem | Reunir versões do mesmo arquivo em evolução |
| Versão de conteúdo | ID interno, SHA-256 e tamanho em bytes | Identificar uma cópia imutável recebida |
| Ocorrência de envio | ID por recebimento | Auditar remetente, nome apresentado e associação |

Proposta: catálogo próprio de kits móveis, com referências às dimensões
analíticas existentes, sem criar um `HOST` operacional fictício. Registrar o
instrumento e seu vínculo com o kit no momento da aquisição; trocar o analisador
não deve reescrever o histórico nem alterar a identidade permanente do kit.

No modo móvel, o upload informa kit, campanha e sessão. Identificadores confiáveis do payload
podem sugerir o vínculo; modelo do analisador, nome do arquivo e usuário remetente
não são identificadores únicos do kit. Divergências exigem resolução explícita.
Manter o identificador bruto retornado pelo appAnalise para auditoria.

Canal de entrada e tipo de aquisição são campos distintos: um arquivo enviado
manualmente também pode ser de estação fixa. Ausência de Zabbix não comprova que
um arquivo seja móvel. Registrar modo declarado e evidência extraída do payload.

Para estações fixas, manter a identidade lógica operacional existente por host,
caminho e nome. Para a carga manual, criar ou selecionar o arquivo lógico pela
origem de aquisição e por um identificador estável, quando disponível. Se não
houver evidência suficiente, exigir indicação explícita de que o envio substitui
uma versão anterior. Nome local, mesmo kit/campanha e maior tamanho não bastam
para concluir que dois arquivos pertencem à mesma sequência de versões.

Entrega: contratos de kit/instrumento/campanha/sessão/origem, arquivo lógico,
versão e política de vínculo. Definir tabelas somente após revisão dos contratos.

## 2. Separar recebimento, conteúdo e processamento

Proposta de fluxo:

`receber temporário → validar e calcular hash → resolver arquivo lógico/versão →`
`registrar envio → publicar cópia recebida → enfileirar → analisar →`
`substituir resultado ativo quando aplicável → atualizar summaries`

- Criar um registro por ocorrência de envio: usuário, data, nome original,
  estação/kit, contexto de campanha/sessão quando aplicável, tamanho em bytes e
  resultado. Reenvios conservam proveniência.
- Calcular SHA-256 no servidor, em leitura incremental. Não confiar apenas em
  hash fornecido pelo navegador ou MATLAB.
- Usar nome interno de armazenamento independente do nome original. Dois
  arquivos diferentes chamados `medicao.zip` devem poder coexistir.
- Publicar apenas cópias cuja transferência terminou e que passaram pela
  validação. Uma cópia íntegra de 30 MB de uma medição ainda em andamento pode
  ser processável, mesmo que o arquivo de origem cresça para 50 MB amanhã.
  Transferência incompleta e aquisição ainda não encerrada são condições distintas.
- Calcular o hash sobre a cópia recebida e imutável que será processada. Um hash
  não prova que a leitura de um arquivo modificado durante o backup foi coerente;
  preservar/validar o contrato de captura da fonte e registrar eventual instabilidade.
- Tornar a reserva do conteúdo atômica no banco, com restrição única adequada;
  duas requisições simultâneas não podem criar dois processamentos equivalentes.
- Definir reconciliação para falhas entre filesystem e banco: não existe uma
  transação única envolvendo ambos. Recuperar artefato sem fila e fila sem
  artefato sem anunciar conclusão falsa.
- Manter limites de arquivo e estabelecer limites de descompactação, arquivos
  internos e caminhos aceitos antes de processar ZIPs.

Entrega: upload durável, auditável e independente do nome físico, disponível
pela mesma API para navegador e MATLAB.

## 3. Idempotência, versões e substituição analítica

O hash complementa a identidade lógica; não substitui as chaves atuais de origem.
Trocar MD5 por SHA-256 não torna o identificador estável quando o conteúdo cresce.

| Situação | Interpretação | Ação proposta |
|---|---|---|
| Mesmo arquivo lógico, mesmos bytes | Mesma versão | Reutilizar o resultado válido ou retomar uma tentativa que falhou |
| Mesmo arquivo lógico, conteúdo alterado | Outra versão | Validar precedência e reprocessar para substituir o resultado ativo |
| Mesmo conteúdo, nome de envio diferente | Cópia de uma versão conhecida | Registrar ocorrência sem duplicar o resultado nem promover uma versão antiga |
| Conteúdo diferente, nome parecido ou maior tamanho | Relação ainda indeterminada | Resolver vínculo e precedência antes de substituir |
| Mesmos bytes, parser/perfil corrigido | Novo processamento da mesma versão | Substituir o resultado conforme política explícita de reprocessamento |

Exemplo RFEye: o arquivo lógico `(host, caminho, espectro.bin)` possui uma versão
de 30 MB com hash A e outra de 50 MB com hash B. Ambas pertencem ao mesmo arquivo
lógico. Após validar B como sucessora, seu resultado substitui o de A. Um reenvio
posterior de A não deve restaurar automaticamente o resultado menos completo.

Contratos necessários:

1. **Conteúdo:** SHA-256 e tamanho identificam uma cópia de bytes. Um hash
   diferente detecta alteração, mas não informa se a versão é mais nova ou melhor.
2. **Linhagem e precedência:** registrar relação entre versões e qual está ativa.
   Usar evidência da origem/aquisição e política explícita; não escolher só por
   tamanho ou ordem de chegada. Truncamento, reuso de nome, correção de metadados
   e alteração mantendo o tamanho precisam de tratamento próprio.
3. **Processamento:** vincular a execução ao arquivo lógico, versão, perfil e
   parser. Separar o hash físico do contexto analítico, que pode mudar o resultado.
4. **Medição normalizada:** preservar a relação entre originais, ZIPs e MATs
   derivados. Definir assinatura de medição somente após estudar o payload.

Preservar a semântica atual de substituição: validar o novo payload, remover os
vínculos do resultado substituído, excluir somente fatos órfãos, inserir/vincular
o resultado atualizado e confirmar a transação. Não fazer um append dos espectros
das duas versões nem depender apenas da deduplicação de `insert_spectrum()`.
Se a transação falhar, o resultado anterior deve continuar válido.

Serializar a publicação por arquivo lógico, ou usar uma condição atômica de
versão esperada, para impedir que um worker atrasado da versão antiga substitua
a versão nova. A identidade antiga por localização continua disponível para
resolver a linhagem, mesmo se as versões passarem a ter nomes físicos diferentes.

O histórico de versões/processamentos não deve manter indiscriminadamente
vínculos analíticos ativos dos resultados substituídos: isso impediria a limpeza
de órfãos. Separar auditoria do resultado vigente e definir retenção dos arquivos
físicos antigos, sem prometer armazenamento ilimitado ou apagar originais em uso.

Hash do ZIP inteiro não reconhece automaticamente o mesmo arquivo recompactado.
Hashes dos arquivos internos podem identificar membros iguais; equivalência
entre BIN e MAT exige dados normalizados ou linhagem explícita. Não deduplicar
medição apenas por horário, faixa de frequência, equipamento ou área do percurso.

Separar compartilhamento de conteúdo físico do vínculo analítico: se o mesmo
conteúdo for atribuído a outro kit/campanha, registrar conflito ou associação
validada, sem criar silenciosamente novos espectros. Não revelar metadados de
outros usuários na resposta de duplicidade sem verificar acesso.

Entrega inicial: detectar cópias renomeadas, reconhecer versões do mesmo arquivo
lógico e preservar a substituição analítica, inclusive sob concorrência.
Equivalência entre formatos será uma entrega posterior, com amostras de referência.

## 4. Processamento manual com resolução explícita da origem

Proposta preferida: fila de ingestão manual em BPDATA e worker próprio para
drive-test e estações fixas recebidas manualmente, com os
mesmos contratos de claim, conclusão, erro e recuperação da arquitetura. Isso
evita relaxar o histórico fixo ou inserir hosts fictícios apenas para satisfazer
as chaves estrangeiras. Confirmar essa escolha antes de criar tabelas ou módulos.

- O WebFusion recebe dados, consulta estados e solicita processamento; não
  executa o appAnalise dentro da requisição HTTP.
- O worker é dono do estado operacional da fila. Handlers fazem processamento
  de domínio; SQL permanece nos handlers de banco correspondentes.
- Reutilizar a integração e a persistência do appAnalise por interfaces explícitas.
  Separar somente as premissas fixas que impedem a carga móvel, sem refatoração ampla.
- O modo móvel usa kit/instrumento e uma política explícita de exportação por
  formato, sem passar pelo ajuste de nomes das estações do Zabbix.
- O modo fixo manual resolve a estação antes de aplicar as regras específicas
  da família. Quando o vínculo não estiver resolvido, manter a pendência de
  identificação em vez de aplicar um hostname fictício ou o modo móvel.
- Preservar original, metadados extraídos, versão do parser e vínculos dos derivados.
- Falha após commit em RFDATA e antes da conclusão em BPDATA deve permitir retry
  idempotente. Não presumir atomicidade entre conexões dos dois bancos.

Antes de implementar: atualizar `ARCHITECTURE.md` com os novos responsáveis,
contratos de tarefa e limites de reutilização. As regras das estações fixas e o
ciclo atual de suas tasks permanecem preservados.

## 5. Sites móveis, trajetória e polígono

Proposta de granularidade: um site móvel por aquisição/trecho contínuo definido,
podendo receber vários arquivos da mesma sessão. Uma campanha pode conter vários
sites móveis. Não usar um único site mutável para toda a vida do kit.
Confirmar como o usuário agrupa sessões e como identificar trechos descontínuos.

- Obter do appAnalise pontos GPS suficientes, com coordenadas, ordem/tempo e
  relação com os espectros. Conferir se os valores atuais representam amostras
  reais ou apenas resumos; um centroide não permite reconstruir um percurso.
- Conservar a trajetória original separadamente do contorno. O polígono é um
  resumo espacial e não significa que toda a sua área tenha sido medida.
- Calcular um contorno determinístico a partir de pontos válidos. Proposta
  inicial: envoltória convexa, com política documentada para outliers e lacunas.
  Avaliar contorno côncavo somente se houver requisito de maior aderência.
- Não fechar a trajetória simplesmente ligando o último ponto ao primeiro:
  isso pode criar um polígono cruzado e não representa necessariamente os limites.
- Validar fechamento do anel, orientação, coordenadas, área e ausência de
  autointerseções. Definir CRS e ordem longitude/latitude no contrato.
- Pontos insuficientes ou colineares não formam polígono válido. Registrar a
  aquisição com pendência geográfica, sem fabricar área ou classificá-la como fixa.
- Persistir `Mobile` por lookup controlado em `DIM_SITE_TYPE`, sem ID numérico
  presumido. Definir migração e tratamento dos sites legados com tipo nulo.
- Manter `GEO_POINT` como ponto representativo, sem utilizá-lo como chave de
  identidade do site móvel. Isolar lookup fixo e móvel para impedir fusões.
- Escolher e versionar a serialização de `GEOGRAPHIC_PATH` após verificar os
  leitores Python/MATLAB. Avaliar WKB, sem assumir que seja o formato atual.
- Avaliar capacidade da coluna: `BLOB` comporta até 65.535 bytes. Estimar com
  percursos reais e decidir entre simplificação controlada e migração de tipo.
- Campanhas podem atravessar municípios/UFs. Um ponto representativo não torna
  toda a aquisição pertencente a um único município. Definir filtro espacial e
  representação administrativa sem descartar medições válidas por essa condição.
- Se vários arquivos ampliarem o mesmo percurso, atualizar sua geometria sem
  mudar `ID_SITE`; reprocessamentos não devem criar novas identidades por pequenas
  diferenças no contorno calculado.
- Recalcular o contorno com as versões ativas da sessão. Ao substituir uma versão,
  retirar sua contribuição anterior antes de recompor a área; uma simples união
  acumulativa conservaria pontos incorretos removidos pela versão corrigida.
  Publicar geometria e summaries coerentes com a revisão analítica ativa e
  permitir reconstrução após falhas.

## 6. Consulta, interface e compatibilidade

- Evoluir a página de upload com kit, campanha, sessão e estados persistidos:
  recebimento, duplicidade, aguardando processamento, processando, concluído,
  falha e pendência de identificação/geografia.
- Oferecer seleção de aquisição móvel ou fixa. No modo móvel, solicitar o
  contexto de kit/campanha/sessão. No modo fixo, permitir selecionar uma estação
  conhecida ou indicar que ainda precisa ser identificada. A interface deve
  expor campos adequados à entrada sem exigir campanha móvel para dados fixos.
- Distinguir cancelamento da transmissão de cancelamento do processamento.
  O Stop atual não desfaz uma persistência já concluída.
- Exibir arquivos originais e derivados, estação móvel e área do drive-test.
- Auditar consultas que exigem host e os consumidores MATLAB. Incluir dados
  móveis sem criar vínculo fictício em `HOST_EQUIPMENT_LINK`.
- Preservar os contratos públicos de `HOST_LOCATION_SUMMARY`, `MAP_SITE_SUMMARY`,
  `MAP_SITE_STATION_SUMMARY` e `SITE_EQUIPMENT_OBS_SUMMARY`. Se houver mudança de
  granularidade ou significado, propor modelo paralelo/versionado.

## Sequência de entregas e critérios de aceite

| Etapa | Entrega | Critério de aceite |
|---|---|---|
| A | Amostras e contratos aprovados | Kit, campanha, sessão, GPS, arquivo lógico e precedência de versões definidos |
| B | Migrações aditivas e arquitetura | Sem quebra do fluxo fixo; rollback e compatibilidade revisados |
| C | Recebimento com hash e proveniência | Cópia renomeada reconhecida; versão nova vinculada; arquivos distintos com mesmo nome aceitos |
| D | Fila e processamento móvel | Processamento sem Zabbix; retry e concorrência sem duplicação |
| E | Sites Mobile e geometria | Polígono válido, tipo correto, trajetória preservada e nenhum merge com site fixo |
| F | Consultas, mapa e estados na página | Dados móveis acessíveis no WebFusion/MATLAB sem alterar os contratos fixos |

Todas as etapas devem considerar a matriz de entradas. Em particular, validar
a mesma versão recebida primeiro pelo backup e depois por upload, e na ordem
inversa; versão manual antiga após backup mais completo; duas estações fixas
desconhecidas com analisadores do mesmo modelo; reconhecimento posterior de uma
estação; e origem manual sem relação com o caminho original da coleta.

Na etapa A, levantar amostras anonimizadas de kits/formatos diferentes, inclusive
o mesmo conteúdo renomeado, recompactado e convertido. Inspecionar metadados e
DDL, evitando varredura de arquivos ou backfill pesado em produção.

Testar também: dois instrumentos do mesmo modelo; troca de analisador no kit;
mesmo kit em campanhas diferentes; várias partes de um percurso; mesma região
em sessões diferentes; GPS ausente/inválido; percurso cruzado; rota em linha;
trajeto com outlier; travessia municipal; falha de disco; cancelamento; envio
simultâneo; falha entre persistência e conclusão; regressão RFEye/CWSM/ERMx/UMS.

Casos obrigatórios de versionamento: RFEye crescendo de 30 para 50 MB; reenvio
dos 30 MB depois dos 50 MB; alteração com mesmo tamanho; truncamento/reuso de
nome; mudança de bandas na nova resposta; falha após reset e antes da inserção;
espectro compartilhado com outro arquivo; versões concorrentes terminando fora
de ordem; versão móvel corrigida removendo um ponto GPS; falha na publicação
entre bancos. Verificar ausência de soma indevida, regressão e perda do resultado
anterior em caso de erro.

Testes do WebFusion rodam no webserver com `/usr/local/bin/python`; testes
exclusivos do appCataloga no ambiente autorizado dele. Usar bancos e diretórios
isolados, monitorar recursos e não migrar dados históricos nesta fase.

## Decisões restantes antes da implementação

1. Granularidade e forma de seleção/criação da sessão, incluindo arquivos que
   contêm mais de um percurso ou kit.
2. Cadastro e governança de kits e campanhas; identificador de instrumento
   disponível em cada formato e tratamento de equipamento desconhecido.
3. Aprovação da fila manual separada e do modelo de ligação ao catálogo analítico.
4. Política inicial do contorno, qualidade GPS e representação de rotas degeneradas.
5. Formato de `GEOGRAPHIC_PATH`, capacidade e compatibilidade com consumidores.
6. Escopo de deduplicação entre usuários/campanhas, precedência de versões,
   substituição explícita no upload e retenção de snapshots/processamentos.
7. Critério de estação fixa conhecida, cadastro de estação fixa sem Zabbix e
   autorização para resolver identidades pendentes. Confirmar a proposta de
   impedir publicação analítica definitiva enquanto a identidade não for resolvida.

## Referências técnicas

- [BLOB no MariaDB](https://mariadb.com/docs/server/reference/data-types/string-data-types/blob).
- [Operações de construção geométrica](https://shapely.readthedocs.io/en/latest/constructive.html).
- [Casos degenerados da envoltória convexa](https://shapely.readthedocs.io/en/maint-1.6/manual.html).

Essas referências descrevem limites e conceitos; não implicam adoção de nova
dependência no projeto sem revisão arquitetural.
