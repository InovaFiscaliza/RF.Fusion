# API de consulta do appAnalise

Biblioteca de leitura do WebFusion que reproduz as consultas de
[`DBHandler.m`](../../.instructions/DBHandler.m). O appAnalise passa a enviar a
operação e seus filtros por HTTP. O SQL permanece no servidor.

Base pública: **`/rffusion/api/appanalise`**.

O nginx remove `/rffusion` antes de encaminhar a requisição. Por isso, o
blueprint Flask usa `/api/appanalise`; não acrescente o prefixo público duas
vezes. O registro está em `src/webfusion/app.py`.

## Responsabilidades e compatibilidade

- `routes.py`: blueprint, leitura dos parâmetros, validação HTTP e respostas.
- `filters.py`: filtros tipados e validação de valores recebidos.
- `db_handler.py`: classe `AppAnaliseDB`, consultas SQL parametrizadas e
  fechamento das conexões, usando as fábricas existentes de `webfusion/db.py`.
- `service.py`: serialização das respostas tabulares.
- `config.py`: prefixo, limites e constantes do contrato.

As rotas e serviços chamam somente métodos públicos do handler. O módulo não
abre conexões no import, não compartilha conexões entre requisições e não
introduz outro driver. Todas as operações são consultas; não há endpoint para
SQL arbitrário, alteração de tabelas ou manipulação de tasks.

Esta separação aplica a preservação do contrato externo estabelecida em
`ARCHITECTURE.md`, §15.1.1, e `INSTRUCTIONS.md`, §0.1. As consultas próprias da
interface web permanecem independentes porque seus campos e regras já diferem
das consultas MATLAB. O mapa é compartilhado em `api/map_api`; este pacote
não contém uma implementação paralela do mapa.

Depois da migração dos consumidores, a estrutura interna do summary poderá
evoluir por trás destes métodos, preservando campos, ordenação e significado
das respostas. Até lá, as tabelas consumidas diretamente pelo MATLAB continuam
sujeitas ao contrato existente. Alterações incompatíveis da API precisam de
uma nova versão.

## Operações

Os caminhos abaixo são relativos à base pública.

| Método no MATLAB | Método HTTP | Caminho | Resultado |
|---|---|---|---|
| `getStationSummary` | GET | `/stations/summary` | Tabela de estações com coordenadas |
| `getHostStats(hostId)` | GET | `/hosts/{hostId}/stats` | Tabela com zero ou uma linha |
| `getSpectrumEquipments(filters)` | GET ou POST | `/equipments` | Tabela de equipamentos |
| `getSpectrumStates(filters)` | GET ou POST | `/states` | Tabela com `LC_STATE` |
| `getSpectrumLocalities(equipmentId, filters)` | GET ou POST | `/localities` | Tabela de distritos e disponibilidade |
| `getSpectrumFileData(filters)` | GET ou POST | `/files` | Tabela de arquivos e paginação |
| `getSpectrumFileDataCount(filters)` | GET ou POST | `/files/count` | Objeto com `count` inteiro |
| `getSpectraByFileId(fileId)` | GET | `/files/{fileId}/spectra` | Tabela de espectros do arquivo |

GET recebe filtros na query string. POST recebe **o próprio objeto de filtros**
em JSON, com `Content-Type: application/json`. POST não aceita filtros também
na URL. As rotas exclusivamente GET não aceitam filtros.

## Filtros

Os nomes camelCase foram mantidos para facilitar o uso dos structs existentes.
Todos são opcionais. Omita valores ausentes ou envie `null`; não envie `NaN` ou
`NaT`. Texto vazio é tratado como ausente.

| Campo | Tipo HTTP/JSON | Semântica |
|---|---|---|
| `equipmentId` | Inteiro positivo | Equipamento |
| `siteId` | Inteiro positivo | Site |
| `districtId` | Inteiro ou lista de inteiros positivos | Distritos; GET também aceita `1,2` ou parâmetros repetidos |
| `stateCode` | Texto | Código de UF |
| `startDate` | Data ISO `AAAA-MM-DD` | Início do dia, inclusive |
| `endDate` | Data ISO `AAAA-MM-DD` | Fim do dia às `23:59:59`, inclusive |
| `freqStart` | Número finito | Limite inferior na unidade das colunas `NU_FREQ_*` |
| `freqEnd` | Número finito | Limite superior na unidade das colunas `NU_FREQ_*` |
| `description` | Texto | Busca sem distinção de maiúsculas; `%` e `_` conservam significado de LIKE |
| `page` | Inteiro positivo | Página a partir de 1; padrão 1 |
| `pageSize` | Inteiro de 1 a 1000 | Tamanho solicitado; padrão 50 |

Datas ISO com horário e sem fuso também são aceitas; somente o dia é usado,
como em `toSQLDate` do MATLAB. Não há conversão de fuso. Frequências negativas
são ignoradas nos predicados, preservando a regra existente. Os intervalos não
são invertidos ou corrigidos automaticamente.

Limites novos da interface HTTP: corpo de até 64 KiB, até 1000 distritos e
`pageSize` de até 1000. Valores inválidos são rejeitados, sem arredondamento
silencioso. Os valores válidos mantêm a semântica das consultas de referência.

Os endpoints de pesquisa aceitam o conjunto de campos acima para reutilizar um
mesmo struct, mas cada operação usa os campos que o método MATLAB usava:

- Equipamentos: `stateCode`, `siteId`, `districtId` no summary. O fallback para
  `RFDATA` usa apenas `stateCode` e `siteId`, conforme o código de referência.
- Estados: `equipmentId` e `siteId`.
- Localidades: sem equipamento nem UF retorna vazio, sem consultar o banco.
  Sem filtros de espectro, o summary usa apenas equipamento e UF. A consulta
  em `RFDATA` também aplica distrito, datas, frequências e descrição, ignorando
  `siteId`. Os resultados são agregados por distrito, não por site.
- Arquivos e contagem: todos os filtros de espectro; a contagem ignora paginação.

Equipamentos, estados e localidades usam o fallback somente quando a consulta
ao summary retorna vazia. Erro no banco não é convertido em resultado vazio.

### Adaptação necessária nas estatísticas de host

A validação do SQL no MariaDB confirmou que `BPDATA.HOST` já não contém
`NU_HOST_FILES`, `NU_PENDING_FILE_BACKUP_TASKS`, `NU_DONE_FILE_BACKUP_TASKS` e
`VL_PENDING_BACKUP_KB`, embora o arquivo MATLAB ainda consulte esses campos.

A API mantém esses quatro nomes **na resposta**, reconstruindo os valores a
partir de `FILE_TASK_HISTORY`, restrito ao host solicitado. Os predicados foram
conferidos na implementação anterior de `host_update_statistics`, anterior ao
commit `a4d422b`:

- Arquivos descobertos: `NU_STATUS_DISCOVERY = 0`.
- Backups pendentes: `NU_STATUS_BACKUP = 1`.
- Backups concluídos: `NU_STATUS_BACKUP = 0`.
- Volume pendente em KB: soma de `VL_FILE_SIZE_KB_HOST` quando discovery está
  concluída e backup está pendente.

Os nomes legados não são recriados nas tabelas. Identidade, conectividade e
datas continuam vindo de `HOST`. Hosts sem histórico recebem contadores zero.
Não se converte o volume em GB do snapshot para KB: o valor do snapshot já foi
arredondado e perderia precisão. Essa consulta passa a refletir o histórico no
instante da requisição, em vez dos antigos contadores armazenados. Sua latência
deve ser observada em hosts com histórico muito grande.

## Formato das respostas

Resultados tabulares mantêm os aliases do MATLAB e incluem os nomes das
colunas mesmo quando não há linhas:

```json
{
  "columns": ["ID_EQUIPMENT", "NA_EQUIPMENT"],
  "rows": [
    {"ID_EQUIPMENT": 12, "NA_EQUIPMENT": "Equipamento exemplo"}
  ]
}
```

- `rows` sempre é um array, inclusive com zero ou uma linha.
- SQL NULL vira JSON `null`; textos não são convertidos para números.
- Números são enviados como números JSON. `Decimal` é convertido para ponto
  flutuante, compatível com o consumo numérico usual do MATLAB. Identificadores
  inteiros não passam por conversão para `float` no servidor.
- Datas usam texto ISO, como `2026-09-11T14:30:00`, sem acrescentar um fuso que
  não existe no valor armazenado.
- Flags das tabelas conservam o tipo do driver, normalmente `0`/`1`.
- Hosts/arquivos inexistentes retornam tabelas vazias, como o `fetch` original.

`/files` acrescenta:

```json
{
  "pagination": {"page": 1, "page_size": 50, "has_more": true}
}
```

**Compatibilidade da paginação:** `rows` contém até `pageSize + 1` linhas,
assim como `getSpectrumFileData`. A última é a linha de verificação da próxima
página. O cliente deve apresentar somente as primeiras `pageSize` linhas.
Não se calcula a contagem total implicitamente; use `/files/count` se precisar.

A busca temporal seleciona espectros que se sobrepõem ao período; a busca por
frequência seleciona espectros contidos nos limites informados. Agregados de
arquivos são calculados sobre os espectros correspondentes aos filtros.
`/files/{fileId}/spectra` retorna todos os espectros do arquivo `reposfi`.

A consulta de contagem conserva o conjunto de JOINs original, que é menor que
o da listagem. Dados com referências inconsistentes podem causar divergência
entre os dois resultados; esta implementação não altera essa regra existente.

## Mapa compartilhado

O appAnalise usa a API comum do mapa:

- `/rffusion/api/map/stations`: objeto com `points`.
- `/rffusion/api/map/stations?include_details=true`: `points` e `site_details`.
- `/rffusion/api/map/stations/{siteId}`: detalhe de um site.

As rotas `/rffusion/api/appanalise/map`, `/map/sites` e `/map/stations` sob o
prefixo appAnalise foram removidas. Não há aliases ou consultas duplicadas.
Os métodos MATLAB que buscavam linhas brutas de sites e estações devem passar
para o dataset comum, que já monta os pontos e detalhes. O endpoint
`/stations/summary` deste pacote continua existindo: ele consulta
`HOST_LOCATION_SUMMARY`, com finalidade distinta do mapa por site.

Consulte o [contrato do mapa](../map_api/README.md) para filtros, campos e datas.
As regras de marcadores passam a ser as mesmas da interface WebFusion.

## Exemplos de consumo

```bash
curl --get 'https://SEU_SERVIDOR/rffusion/api/appanalise/files' \
  --data-urlencode 'equipmentId=12' \
  --data-urlencode 'startDate=2026-09-01' \
  --data-urlencode 'endDate=2026-09-11' \
  --data-urlencode 'pageSize=50'

curl 'https://SEU_SERVIDOR/rffusion/api/appanalise/localities' \
  -H 'Content-Type: application/json' \
  -d '{"equipmentId":12,"stateCode":"SP","districtId":[1,2]}'
```

Exemplo MATLAB para uma consulta tabular não vazia:

```matlab
baseUrl = 'https://SEU_SERVIDOR/rffusion/api/appanalise';
options = weboptions('ContentType', 'json', 'Timeout', 60);
response = webread([baseUrl '/equipments'], 'stateCode', 'SP', options);
rows = struct2table(response.rows);
```

Para POST, use `webwrite` com um struct de filtros e `MediaType` definido como
`application/json`. A autenticação exigida pelo proxy do ambiente precisa ser
incluída nas opções do cliente.

O exemplo não substitui o adaptador MATLAB: ele ainda precisa tratar tabelas
vazias usando `columns`, converter campos de data explicitamente e normalizar
`null` para os valores esperados pelo aplicativo. A precisão de identificadores
acima de 2^53 depende do decodificador JSON do cliente. O cache local `.mat` e o
cache de sessão continuam sendo responsabilidades do appAnalise.

## Acesso e erros

A biblioteca segue a política atual dos endpoints de leitura do WebFusion:
não adiciona cadastro de usuários, token próprio ou autenticação de banco no
cliente. O acesso pelo proxy continua sujeito à configuração de autenticação
do ambiente. O Flask, isoladamente, **não exige autenticação nestas rotas**;
uma política específica para clientes MATLAB precisa ser definida antes de
expor este serviço em outro perímetro de rede. Não houve mudança no nginx ou
nas regras de autorização dos módulos existentes.

Falhas de validação retornam 400; conteúdo de POST sem JSON retorna 415; corpo
acima do limite retorna 413. Falhas de consulta retornam 500 com mensagem
genérica. Exemplo:

```json
{"error":{"code":"query_failed","message":"Não foi possível consultar os dados."}}
```

Exceções durante o atendimento do blueprint retornam JSON. Erros de roteamento
404/405 continuam usando o tratamento padrão do Flask. Detalhes de exceções são
registrados no servidor e não enviados ao cliente. Não há cache de resultados
na biblioteca nem truncamento silencioso das consultas não paginadas.

## Implementação Python e otimizações

O contrato de dados é compatível com o cliente; a implementação não reproduz
os helpers de conversão de células e tipos do MATLAB. As funções `_parse_*`
validam exclusivamente entradas HTTP: strings de GET e valores JSON de POST.
A montagem do mapa pertence exclusivamente ao pacote `api/map_api`.

`match/case` é usado onde a escolha depende do tipo recebido, como serialização
JSON e formatos de `districtId`. Predicados de intervalo e filtros cumulativos
continuam usando `if`, pois várias condições podem ser aplicadas simultaneamente.

No acesso ao banco:

- O fallback de equipamentos usa `EXISTS` para testar a presença de espectros,
  evitando produzir todas as combinações de equipamento/espectro para depois
  remover duplicatas com `DISTINCT`.
- A contagem usa `COUNT(DISTINCT repos.ID_FILE)`, eliminando a tabela derivada
  que agrupava arquivos antes de contar.
- O filtro de distrito usa o site já relacionado à consulta. Na contagem, os
  JOINs de site/estado só entram quando os filtros correspondentes exigem.

Essas mudanças preservam as respostas e os fallbacks documentados. Planos
`EXPLAIN` verificam a estrutura de execução; ganhos de latência dependem dos
filtros, índices e volume real. Não foram criados índices ou caches adicionais.

Na comparação dos planos em 11/09/2026, a contagem deixou de materializar
`<derived2>`; com filtro de distrito, o plano também deixou de indicar
`Using temporary; Using filesort`. No fallback de equipamentos sem filtros,
o plano passou de `Distinct` com tabela temporária para `FirstMatch(e)`.
Essas observações descrevem os planos avaliados, não um benchmark de latência.

## Validação e implantação

Execute no container `webserver`, com o projeto montado em `/RF.Fusion`:

```bash
cd /RF.Fusion
/usr/local/bin/python -m unittest discover \
  -s test/tests/webfusion -p test_appanalise_api.py -v
```

Os testes isolam o acesso ao banco e cobrem filtros, fallbacks, parâmetros SQL,
fechamento de conexões, serialização, paginação, rotas e erros. O mapa é
validado separadamente nos testes de `map_api`. Eles não
substituem a comparação ponta a ponta com o appAnalise em MATLAB.

A implantação requer recarregar o processo WebFusion pelo procedimento do
ambiente. Não há migração de banco. O arquivo MATLAB de referência foi
preservado; a migração do cliente e a simplificação do summary são etapas
posteriores.
