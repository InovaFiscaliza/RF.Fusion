# Dados e integrações compartilhadas

Este arquivo orienta a identificação de produtores, consumidores e bootstrap
quando uma mudança atravessa aplicações. Os contratos normativos continuam em
[SUMMARY.md](../../docs/architecture/SUMMARY.md),
[METRICAS_OPERACIONAIS.md](../../docs/architecture/METRICAS_OPERACIONAIS.md)
e nas arquiteturas de cada aplicação.

Consulte ao alterar SQL, payload do gateway, métricas ou modelos de leitura.

## Escolher o dono do dado

| Dado | Fonte e papel | Superfícies que acompanham a mudança |
|---|---|---|
| Hosts e filas | `BPDATA`, controle operacional | handler BKP, worker/gateway e ações operacionais web |
| Arquivos e espectros | `RFDATA`, catálogo analítico | handler RFM, processamento appAnalise, pesquisa e downloads |
| Leituras derivadas | `RFFUSION_SUMMARY` | handler Summary, refresh engine, WebFusion, cliente MATLAB e Zabbix |
| Identidade e papéis | `WEBFUSION` | `auth/db_users.py`, serviço de identidade e APIs de usuários |

O nome do schema não é uma fronteira de chave estrangeira entre aplicações:
as relações entre schemas são coordenadas pelo código. `FILE_TASK` e
`FILE_TASK_HISTORY` possuem vínculos ao host, não uma FK entre si. Preserve a
atomicidade definida no contrato do appCataloga.

## Alterar uma estrutura ou consulta

1. Identifique a tabela e o handler proprietário. Localize produtores e
   consumidores com `rg -n -F '<TABELA>' src test docs/architecture`.
2. Leia o contrato correspondente antes de definir colunas, granularidade,
   unidades e semântica temporal. Summary é derivado; não substitui a fila.
3. Atualize o `create*DB.sql` correspondente para instalações novas. Trate a
   atualização de banco existente como operação separada: o bootstrap não é
   mecanismo de migração. Não execute esses scripts em banco compartilhado.
4. Preserve os consumidores públicos do summary, inclusive o cliente MATLAB
   previsto pelo contrato. Para localizar os consumidores disponíveis, busque
   o nome da tabela em todas as extensões, não apenas em Python. A localização
   do cliente externo exige a confirmação descrita abaixo.
5. Se o dado for derivado, acompanhe publicação/invalidação do escopo e leitura,
   não apenas o DDL. As rotas operacionais continuam usando seus serviços.
6. Valide o contrato com doubles e, quando SQL ou locks forem parte da mudança,
   planeje MariaDB isolado. SQLite não comprova essas propriedades.

**Alerta — bootstrap e atualização:** os quatro schemas têm criadores próprios.
Antes de propor reaplicar um criador para atualizar dados existentes, confira
as instruções do banco e o SQL da tabela:
`rg -n 'CREATE TABLE|ALTER TABLE|LOAD DATA' <script-sql>`.

O script de deploy MariaDB referencia e executa os quatro criadores, inclusive
`createWebFusionDB.sql`. Os READMEs que descrevem somente três cargas não
representam esse conjunto de chamadas. Consulte o script sem executá-lo.

## Adaptadores locais do Zabbix

`src/zabbix/root/usr/lib/zabbix/externalscripts/` contém código próprio, não o
interior do servidor Zabbix. Há scripts de consulta às estações e clientes do
gateway appCataloga; `scratch/` não comprova o protocolo ativo.

Para mudar um payload do gateway:

1. Localize o `query_tag` e seu despacho no gateway, depois os clientes com
   `rg -n -F '<query_tag>' src/zabbix src/appCataloga test`.
2. Preserve a composição de argumentos, filtro, codificação e framing da
   resposta do protocolo específico. `z_shared.argument` e `warning_msg`
   atendem vários clientes; não conclua que todos os protocolos são iguais.
3. A variante `queryFileMetadata_trapper.py` normaliza o filtro, envia JSON por
   TCP, extrai a resposta delimitada e publica em
   `appCataloga.discovery.json` pelo `zabbix_sender` local. Considere também
   o consumidor de saída padrão, não apenas o trapper.
4. Preserve as chaves de status e o mascaramento do valor de `passwd` na
   cópia da requisição adicionada ao resultado. O cliente não remove o campo
   nem demonstra sanitização geral de quaisquer outros dados sensíveis.
   Não copie valores de configuração ou credenciais.
5. Confira o template/item e os testes do contrato antes de publicar uma nova
   forma. Os thresholds, macros, triggers e ciclo de Problems pertencem ao
   Zabbix, conforme o contrato de métricas.

`queryappColeta.py` usa outro destino e semântica: cria uma requisição
`Key/ClientName/Request`, recebe dados com framing via `z_shared.receive_message`
e resume respostas de tarefas ou diagnóstico. Não o substitua pelo contrato
do gateway por semelhança de nome.

lacuna: a versão efetivamente implantada dos scripts e templates no servidor
não é determinada pela árvore versionada. A listagem
`rg --files src/zabbix -g '*.py'` identifica os arquivos disponíveis, não sua
ativação. Confirme a implantação com o responsável antes de alterar o protocolo.

lacuna: o contrato SUMMARY cita `src/webfusion/DBHandler.m`, mas
`rg --files src -g '*.m'` devolve zero caminhos. O README da API appAnalise
situa o cliente MATLAB no projeto consumidor. O cliente não foi conferido
nesta árvore; confirme sua localização com o responsável antes de alterar o
contrato público. A ausência na busca não revoga a obrigação de compatibilidade
nem autoriza substituir o consumidor por outro componente.

## Exemplares

| Caminho | O que ilustra | Como localizar o equivalente hoje |
|---|---|---|
| `src/mariadb/scripts/createProcessingDB.sql` | host, fila e histórico | criador que contém `USE BPDATA` |
| `src/mariadb/scripts/createMeasureDB.sql` | dimensões, fatos e seeds do catálogo | criador que contém `USE RFDATA` |
| `src/mariadb/scripts/createWebFusionDB.sql` | identidade e associação de papéis | criador que contém `USE WEBFUSION` |
| `src/mariadb/scripts/createFusionSummaryDB.sql` | outbox e modelos derivados | criador que contém `USE RFFUSION_SUMMARY` |
| `src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py` | cliente gateway e trapper | script que chama `send_to_zabbix_trapper` |
| `src/zabbix/root/usr/lib/zabbix/externalscripts/queryappColeta.py` | protocolo de estação | script que constrói `ClientName` e `Request` |

## Evidências

Comandos de evidência executados a partir da raiz em 2026-10-27; não executam
aplicações, SQL ou conexões remotas.

| Afirmação | Confiança | Onde foi comprovada | Comando | Alcance | Verificado em |
|---|---|---|---|---|---|
| Criadores separados dos schemas | medido | quatro criadores, um `CREATE DATABASE` por arquivo | `rg -l -g 'create*DB.sql' '^CREATE DATABASE IF NOT EXISTS ' src/mariadb/scripts \| wc -l` → 4 | criadores SQL | 2026-10-27 |
| Criadores sem declaração do banco | medido | zero contraexemplos | `rg --files-without-match -g 'create*DB.sql' '^CREATE DATABASE IF NOT EXISTS ' src/mariadb/scripts` → vazio | criadores SQL | 2026-10-27 |
| Deploy inclui WEBFUSION | medido | `install/mariaDB/linux/deploy-debian12-mariadb.sh:31,152` | `rg -n 'sqlWebFusion\|sqlProcessing\|sqlMeasure\|sqlFusionSummary' install/mariaDB/linux/deploy-debian12-mariadb.sh` | um script; não comprova sucesso em execução | 2026-10-27 |
| Helper compartilhado dos clientes Zabbix | medido | 6 importações em 6 de 7 clientes raiz; `siteData.py` diverge; config/helper não são clientes | `rg -l -g '*.py' -g '!scratch/**' '^import z_shared as zsh' src/zabbix/root/usr/lib/zabbix/externalscripts \| wc -l` → 6 | clientes raiz sem scratch | 2026-10-27 |
| Protocolo de cada destino | inferido | leitura integral de `queryFileMetadata_trapper.py:219` e `queryappColeta.py:134` | leitura dos clientes e handlers do destino confirma a sequência; validação real exige ambiente isolado | dois protocolos, sem garantia de implantação | 2026-10-27 |
| Ativação dos scripts | lacuna | sem consulta ao servidor | `rg --files src/zabbix -g '*.py'` lista 13 fontes, sem informação de ativação | apenas árvore local | 2026-10-27 |
| Localização do cliente MATLAB | lacuna | SUMMARY cita caminho que a busca não permite conferir; README da API o situa no consumidor | `rg --files src -g '*.m'` → zero caminhos; `rg -n 'DBHandler\.m\|projeto consumidor\|cliente MATLAB pertencem' docs/architecture/SUMMARY.md src/webfusion/api/appAnalise_api/README.md` | contrato e cliente externo, sem validação do projeto consumidor | 2026-10-27 |

## Descartado

| Achado | Motivo | Alcance medido | O que mudaria a decisão | Data |
|---|---|---|---|---|
| Novo dicionário de dados | fora da tarefa; existe skill específica | não medido | pedido explícito para dicionário | 2026-10-27 |
| Reescrever contratos do summary e métricas aqui | já cobertos pelas fontes normativas | contratos lidos | nova semântica aprovada em contrato | 2026-10-27 |
