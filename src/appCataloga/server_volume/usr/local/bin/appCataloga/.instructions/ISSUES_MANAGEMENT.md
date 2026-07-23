# ISSUES_MANAGEMENT

## Objetivo

Registrar o planejamento inicial da integração entre RF.Fusion/appCataloga e
Zabbix para geração de issues operacionais.

Este documento descreve a direção atual da arquitetura. Ele não implementa
comportamento de runtime.

---

## Decisão Principal

Para a primeira fase, a direção adotada é:

- **Zabbix é o dono da política operacional**
- **RF.Fusion consome e persiste a política efetiva**
- **um worker operacional avalia localmente as condições no `BPDATA`**
- **o resultado volta para o Zabbix**

Isso significa que o RF.Fusion **não** será o dono inicial dos thresholds
operacionais.

Os thresholds, overrides e habilitações devem continuar sendo definidos no
Zabbix, principalmente por:

- template
- macros de template
- override por host

---

## Motivação

Essa direção foi escolhida porque hoje o Zabbix já é o dono de:

- cadastro dos dispositivos
- IP
- porta
- templates por família de equipamento
- override por estação específica

Logo, faz mais sentido que ele também seja o dono da política operacional
inicial.

O RF.Fusion passa a ser:

- consumidor da política
- avaliador local da condição operacional
- fornecedor de resultado para o Zabbix

---

## Escopo Inicial

O foco inicial é apenas em **issues operacionais baseadas em `BPDATA`**.

Ficam fora da primeira etapa:

- issues analíticas baseadas em `RFDATA`
- issues futuras baseadas em `DTDATA`
- detecção de emissão
- recorrência de emissão

Esses itens só devem voltar para discussão quando existir um modelo analítico
mais sólido para eles.

---

## Alarmes Operacionais Prioritários

Os principais alarmes operacionais discutidos até agora são:

- `SSH_AUTH_FAILURE`
- `SSH_CONNECTIVITY_FAILURE`
- `NO_NEW_DISCOVERY`
- `GPS_GNSS_UNAVAILABLE`
- `EMPTY_GPS_DATA_RATE`
- `PROCESSING_ERROR_RATE`

### Leitura atual

- `SSH_AUTH_FAILURE`
  - falha de autenticação SSH da estação

- `SSH_CONNECTIVITY_FAILURE`
  - estação offline, unreachable ou com falha de conectividade

- `NO_NEW_DISCOVERY`
  - ausência de descoberta de arquivos novos acima do threshold esperado

- `GPS_GNSS_UNAVAILABLE`
  - recorrência de erro explícito de GPS inválido

- `EMPTY_GPS_DATA_RATE`
  - taxa de arquivos ou medições sem GPS válido

- `PROCESSING_ERROR_RATE`
  - taxa de erro de processamento associada à estação

---

## Papel do Zabbix

O Zabbix será o dono da política operacional.

### Isso inclui

- thresholds
- habilitação ou desabilitação do alarme
- macros por template
- override por host
- severidade operacional no template

### Isso não inclui

O Zabbix não deve ser o lugar onde a condição operacional é calculada a partir
do `BPDATA`.

Essa avaliação deve acontecer no RF.Fusion.

---

## Papel do RF.Fusion

O RF.Fusion deve:

1. consumir a política operacional do Zabbix
2. persistir a política efetiva localmente
3. avaliar as condições reais com base no `BPDATA`
4. enviar o resultado para o Zabbix

---

## Integração com Zabbix

### Modelo escolhido

O modelo de integração adotado é:

- o Zabbix configura a política
- o RF.Fusion avalia a condição
- o RF.Fusion envia valores para o Zabbix
- o Zabbix gera `Problems` a partir de items e triggers

### Regra prática de responsabilidade

Na prática, a divisão correta de responsabilidade fica assim:

- o Zabbix define quais condições estão habilitadas
- o Zabbix define thresholds, macros e overrides por host/template
- o RF.Fusion lê a política efetiva
- o RF.Fusion calcula as condições operacionais em Python
- o RF.Fusion publica métricas brutas e flags calculadas via trapper
- o Zabbix usa triggers simples para abrir ou fechar `Problems`

Exemplo de desenho desejado:

- o RF.Fusion calcula `no_new_discovery = 1`
- o Zabbix aplica uma trigger simples sobre esse valor
- o `Problem` nasce no Zabbix, não no RF.Fusion

### Observação importante

O fluxo correto no Zabbix não é “criar um `Problem` diretamente”.

O fluxo esperado é:

1. RF.Fusion envia um valor
2. esse valor cai em um item do Zabbix
3. uma trigger avalia o valor
4. o Zabbix cria ou fecha o `Problem`

### WebFusion e Zabbix como espelhos assimétricos

WebFusion e Zabbix podem exibir o mesmo estado operacional da estação, mas não
ocupam o mesmo papel no fluxo de tratamento.

A leitura atual é:

- o WebFusion espelha o estado operacional e o contexto da estação
- o Zabbix espelha o estado operacional e também oficializa o incidente

Na prática, isso significa:

- o WebFusion mostra métricas, contexto e condições operacionais
- o WebFusion pode mostrar um resumo simples de `Alarmes Ativos`
- o WebFusion não deve tentar virar o gestor oficial de issues
- o Zabbix é o lugar oficial para `Problems`, severidade, ack, comentário e
  histórico de tratamento

### Regra de exibição

O WebFusion pode exibir:

- estado operacional atual
- contexto da estação
- condições calculadas pelo RF.Fusion
- totalizador simples de `Alarmes Ativos`

O WebFusion não deve, por enquanto, tentar reproduzir integralmente:

- workflow de tratamento
- comentários de resolução
- ack
- trilha histórica de gestão do incidente

Esses elementos continuam sendo responsabilidade do Zabbix.

### Frase-guia

Como princípio de arquitetura e produto:

- o RF.Fusion calcula e publica a condição operacional
- o WebFusion mostra o contexto operacional da condição
- o Zabbix registra e gerencia o tratamento oficial do `Problem`

---

## Worker Dedicado

Para evitar espalhar alarme em vários pontos do código, a direção preferencial
é criar um worker dedicado:

- `appCataloga_operational_issues_management.py`

Esse worker deve centralizar a avaliação dos alarmes operacionais.

### Objetivo

- evitar lógica de alarme espalhada
- manter um ponto claro de manutenção
- avaliar localmente usando `BPDATA`
- publicar resultado para o Zabbix

### Direção de organização

- o entrypoint cuida do loop e da orquestração
- a lógica das regras deve ficar em módulo de domínio apropriado
- o SQL deve continuar dentro de métodos públicos do `dbHandlerBKP`

---

## Política Efetiva no RF.Fusion

Mesmo com o Zabbix sendo o dono da política, o RF.Fusion precisa persistir a
política efetiva localmente para o runtime.

Isso segue a mesma lógica já usada hoje quando o `appCataloga.py` recebe dados
do Zabbix e atualiza o `HOST`.

### Motivos

- o worker não deve depender do Zabbix em toda avaliação
- a política precisa estar disponível localmente
- o runtime precisa continuar funcionando mesmo com indisponibilidade
  temporária do Zabbix

---

## Direção para ISSUE_CFG

O `ISSUE_CFG` continua necessário, mas seu papel mudou.

Ele deixa de ser, neste momento, o dono original da política operacional.

Ele passa a existir dentro de um `ISSUESDB` unificado, com separação por
domínio.

Leitura atual:

- uma única base de issues
- tipos de issue separados por domínio
- configuração separada por tipo e entidade
- workers distintos para avaliação operacional e analítica

Isso significa que a separação entre operacional e analítico deve acontecer
principalmente em:

- `ISSUE_TYPE`
- `ISSUE_DOMAIN`
- workers de avaliação

e não, por enquanto, em duas tabelas físicas independentes de `ISSUE_CFG`.

### Leitura prática

O Zabbix continua sendo a fonte de verdade operacional.

O RF.Fusion persiste localmente a configuração efetiva para conseguir avaliar
os alarmes com estabilidade.

Para o momento, a direção mais escalável é:

- `ISSUESDB` unificado
- uma tabela de tipos de issue
- uma tabela única de configuração
- uma tabela própria de thresholds
- uma tabela de estado corrente da issue

Se no futuro o domínio analítico crescer muito e ficar estruturalmente
diferente do operacional, essa separação física pode ser revisada depois.

---

## Tabelas Conceituais Mínimas

Neste momento, o modelo mínimo segue simples.

### ISSUE_TYPE

Responsabilidade:

- catalogar os tipos de issue suportados pelo sistema
- separar operacional e analítico por domínio
- apontar qual rotina calcula cada tipo de issue

Campos conceituais mínimos:

- `ID_ISSUE_TYPE`
- `ISSUE_CODE`
- `ISSUE_DOMAIN`
- `ENTITY_SCOPE`
- `VALUE_TYPE`
- `EVALUATOR_CODE`
- `IS_ACTIVE`
- `DESCRIPTION`

### ISSUE_CFG

Responsabilidade:

- persistir a política efetiva aplicada a uma entidade
- armazenar habilitação, janela e parâmetros gerais do tipo de issue
- funcionar tanto para issues operacionais quanto analíticas

Campos conceituais mínimos:

- `ID_ISSUE_CFG`
- `FK_ISSUE_TYPE`
- `ENTITY_SCOPE`
- `ENTITY_KEY`
- `IS_ENABLED`
- `WINDOW_VALUE`
- `WINDOW_UNIT`
- `SOURCE_SYSTEM`
- `SOURCE_REF`
- `PARAMS_JSON`
- `UPDATED_AT`

### ISSUE_CFG_THRESHOLD

Responsabilidade:

- armazenar thresholds escaláveis sem fixar colunas rígidas por severidade
- permitir modelos como `LL`, `L`, `H`, `HH`
- suportar operadores simples e faixas

Campos conceituais mínimos:

- `ID_ISSUE_CFG_THRESHOLD`
- `FK_ISSUE_CFG`
- `LEVEL_CODE`
- `OPERATOR`
- `MIN_VALUE`
- `MAX_VALUE`
- `SEVERITY`
- `UPDATED_AT`

### ISSUE_CURRENT

Responsabilidade:

- armazenar o estado corrente calculado localmente
- registrar se a issue está ativa no momento
- manter o último valor, nível e resumo calculado
- servir de base para publicação no Zabbix e espelhamento no WebFusion

Campos conceituais mínimos:

- `ID_ISSUE_CURRENT`
- `FK_ISSUE_TYPE`
- `ENTITY_SCOPE`
- `ENTITY_KEY`
- `ISSUE_FINGERPRINT`
- `IS_ACTIVE`
- `CURRENT_VALUE`
- `CURRENT_LEVEL`
- `SEVERITY`
- `SUMMARY`
- `DETAILS`
- `FIRST_SEEN`
- `LAST_SEEN`
- `OPENED_AT`
- `CLOSED_AT`
- `EXTERNAL_SYSTEM`
- `EXTERNAL_ID`
- `EXTERNAL_URL`

---

## Contratos Iniciais do ISSUESDB

Para reduzir ambiguidade futura, ficam registrados os valores iniciais
recomendados para os principais contratos do `ISSUESDB`.

Esses valores ainda são de planejamento, mas já servem como base para:

- modelagem do schema
- desenho dos workers
- definição do payload para Zabbix
- cadastro inicial dos tipos de issue

### ISSUE_DOMAIN

Domínio funcional da issue.

Valores iniciais recomendados:

- `operational`
  - issues calculadas principalmente a partir de `BPDATA`,
    `HOST_CURRENT_SNAPSHOT` e estado operacional do catálogo
- `analytical`
  - issues calculadas principalmente a partir de `RFDATA` e, no futuro,
    outros domínios analíticos

Diretriz:

- o worker operacional deve consumir apenas `ISSUE_DOMAIN = operational`
- o worker analítico futuro deve consumir apenas `ISSUE_DOMAIN = analytical`

### ENTITY_SCOPE

Escopo da entidade monitorada pela issue.

Valores iniciais recomendados:

- `host`
  - estação operacional identificada no `BPDATA.HOST`
- `site`
  - local analítico identificado no `RFDATA.DIM_SPECTRUM_SITE`
- `equipment`
  - equipamento analítico identificado no
    `RFDATA.DIM_SPECTRUM_EQUIPMENT`
- `system`
  - escopo global do RF.Fusion, sem vínculo com uma única estação

Diretriz:

- fase inicial operacional:
  - priorizar `host`
- fase analítica futura:
  - usar principalmente `site` e `equipment`

### VALUE_TYPE

Tipo lógico do valor calculado pela issue.

Valores iniciais recomendados:

- `bool`
  - condição binária direta
- `count`
  - quantidade inteira
- `rate`
  - taxa calculada no intervalo
- `duration`
  - tempo decorrido ou duração de janela
- `volume`
  - volume de dados, preferencialmente em GB no contrato de issue
- `enum`
  - estado discreto controlado

Diretriz:

- sempre que possível, o cálculo de alarme deve produzir um valor
  comparável com threshold
- timestamps crus são métricas auxiliares, mas a issue deve preferir um valor
  derivado comparável, como:
  - horas desde a última discovery útil
  - quantidade de falhas na janela
  - taxa de erro na janela

### LEVEL_CODE

Os thresholds da `ISSUE_CFG_THRESHOLD` devem suportar níveis configuráveis sem
obrigar o uso de todos eles.

Valores iniciais recomendados:

- `LL`
- `L`
- `H`
- `HH`

Diretriz:

- uma issue pode usar apenas parte desses níveis
- não é obrigatório preencher todos os níveis para todo tipo de issue
- a severidade operacional final pode ser derivada do nível atingido

### OPERATOR

Operador de comparação do threshold.

Valores iniciais recomendados:

- `<`
- `<=`
- `>`
- `>=`
- `between`

Diretriz:

- preferir operadores simples para o primeiro ciclo
- `between` fica reservado para casos realmente dependentes de faixa

### EVALUATOR_CODE

Cada tipo de issue deve apontar para uma rotina de avaliação estável.

Direção inicial de nomenclatura:

- `operational.host_offline`
- `operational.no_new_discovery`
- `operational.ssh_auth_failure`
- `operational.processing_error_rate`
- `analytical.emission_detected`
- `analytical.emission_recurrent`

Diretriz:

- `EVALUATOR_CODE` é contrato lógico
- ele não precisa ser, necessariamente, o nome literal de uma função Python
- mas deve mapear de forma estável para uma rotina conhecida do worker

### Exemplos iniciais de mapeamento

#### Operacional

- `ISSUE_DOMAIN = operational`
- `ENTITY_SCOPE = host`
- `VALUE_TYPE = duration`
- caso típico:
  - `NO_NEW_DISCOVERY`
  - valor calculado:
    - horas desde a última discovery com arquivo novo

- `ISSUE_DOMAIN = operational`
- `ENTITY_SCOPE = host`
- `VALUE_TYPE = bool`
- caso típico:
  - `SSH_AUTH_FAILURE`
  - valor calculado:
    - houve ou não falha relevante na janela

#### Analítico

- `ISSUE_DOMAIN = analytical`
- `ENTITY_SCOPE = site`
- `VALUE_TYPE = count`
- caso típico:
  - `EMISSION_DETECTED`
  - valor calculado:
    - quantidade de ocorrências detectadas na janela

- `ISSUE_DOMAIN = analytical`
- `ENTITY_SCOPE = equipment`
- `VALUE_TYPE = rate`
- caso típico:
  - `EMISSION_RECURRENT`
  - valor calculado:
    - taxa ou frequência de recorrência na janela

### Decisão de planejamento atual

Fica adotada, neste momento, a seguinte linha:

- `ISSUESDB` unificado
- `ISSUE_TYPE` separa domínios
- `ISSUE_CFG` permanece única
- `ISSUE_CFG_THRESHOLD` guarda os níveis
- `ISSUE_CURRENT` guarda o estado corrente
- workers distintos consomem domínios distintos

Isso permite crescer sem duplicar schema cedo demais e sem espalhar a lógica
de alarmes pelo sistema.

---

## Fonte dos Dados Operacionais

O worker operacional deve usar `BPDATA` como fonte primária.

A leitura atual é que as estatísticas resumidas de `BPDATA.HOST` não são
suficientes, nem coesas o bastante, para serem a única base do monitoramento.

Por isso, a direção recomendada é:

- melhorar o contrato de monitoramento entregue ao Zabbix
- não depender apenas do snapshot atual da tabela `HOST`
- usar consultas e agregações mais claras a partir de:
  - `HOST`
  - `HOST_TASK`
  - `FILE_TASK`
  - `FILE_TASK_HISTORY`

### Papel do snapshot canônico

Para o cálculo operacional de issues, a direção atual é:

- usar `HOST_CURRENT_SNAPSHOT` como fonte primária para a maior parte das
  métricas canônicas
- complementar com consultas em `BPDATA` apenas quando o snapshot ainda não
  materializar algum sinal necessário

Isso reduz duplicação de regra de cálculo dentro do worker de issues e reforça
o papel do snapshot como contrato operacional.

---

## Planejamento do Entrypoint Operacional

O entrypoint operacional de issues deve ser planejado como:

- `appCataloga_operational_issues_management.py`

### Modelo de execução preferencial

A direção atual é que esse entrypoint seja um daemon periódico de avaliação,
e não um consumidor da fila operacional comum.

Motivos:

- cálculo de issue é periódico e idempotente
- o domínio de issues não deve inflar `HOST_TASK` e `FILE_TASK`
- a unidade natural de trabalho aqui é a configuração ativa por entidade, não
  uma task operacional tradicional
- o comportamento fica mais próximo do summary worker do que dos workers de
  backup e processamento

### Unidade lógica de avaliação

A unidade lógica do worker não é “uma task de fila”, e sim:

- um `ISSUE_TYPE`
- uma `ISSUE_CFG`
- uma `ENTITY_SCOPE`
- uma `ENTITY_KEY`

Na prática, o ciclo deve avaliar:

- todos os `ISSUE_CFG` ativos com `ISSUE_DOMAIN = operational`

### Papel do entrypoint

O entrypoint deve:

- controlar o loop periódico
- carregar a política efetiva ativa
- distribuir a avaliação por entidade
- persistir o estado corrente em `ISSUE_CURRENT`
- publicar o resultado estruturado para o Zabbix

### Papel da camada de domínio

A lógica de cálculo não deve ficar toda no entrypoint.

A direção recomendada é:

- o entrypoint orquestra
- o domínio resolve o valor de cada issue
- o DB handler apenas lê e escreve dados

### Fluxo canônico do daemon

Em alto nível, o ciclo do daemon deve seguir esta ordem:

1. iniciar o serviço e adquirir lock de worker
2. carregar `ISSUE_TYPE` e `ISSUE_CFG` ativos do domínio operacional
3. carregar os snapshots e contextos necessários para as entidades alvo
4. calcular cada issue configurada
5. resolver nível e severidade com base em `ISSUE_CFG_THRESHOLD`
6. atualizar `ISSUE_CURRENT`
7. montar o payload estruturado de publicação
8. enviar o resultado ao Zabbix
9. dormir até o próximo ciclo

### Contrato mínimo do resultado de avaliação

Cada evaluator operacional deve devolver, no mínimo:

- `issue_code`
- `issue_domain`
- `entity_scope`
- `entity_key`
- `current_value`
- `value_type`
- `is_active`
- `current_level`
- `severity`
- `summary`
- `details`
- `observed_at`

Diretriz:

- o evaluator nunca deve decidir diretamente sobre `Problem`
- ele calcula condição local
- o resultado local é que será persistido e publicado

---

## Planejamento do Cálculo de Issues e Alarmes

### Distinção entre issue, alarme e problem

Para evitar confusão de responsabilidade:

- `ISSUE_CURRENT`
  - é o estado local calculado pelo RF.Fusion
- alarme publicado
  - é a representação exportada ao Zabbix
- `Problem`
  - é o incidente oficial criado e tratado no Zabbix

Diretriz:

- o RF.Fusion calcula e persiste a issue
- o RF.Fusion publica o alarme estruturado
- o Zabbix abre ou fecha o `Problem`

### Dois modelos de cálculo

O desenho atual separa os issues em dois grupos.

#### 1. Issues booleanas

São condições binárias diretas.

Exemplos:

- `HOST_OFFLINE`
- `SSH_AUTH_FAILURE`
- `SSH_CONNECTIVITY_FAILURE`
- `GPS_GNSS_UNAVAILABLE`

Leitura prática:

- o evaluator produz `current_value = 0` ou `1`
- o threshold típico pode ser `>= 1`
- o nível ativo pode ser resolvido como `H` ou `HH`, conforme política

#### 2. Issues quantitativas

São condições cujo valor precisa ser comparado com thresholds.

Exemplos:

- `HOST_CHECK_STALE`
- `NO_NEW_DISCOVERY`
- `BACKUP_STALE`
- `PROCESSING_STALE`
- `BACKUP_ERRORS_OPEN`
- `PROCESSING_ERRORS_OPEN`
- `MONTHLY_BACKUP_QUOTA`

Leitura prática:

- o evaluator produz um número comparável
- o threshold resolve `L`, `LL`, `H`, `HH`
- a severidade final deriva do nível atingido

### Diretriz importante de modelagem

Quando o problema for de severidade crescente, a preferência é:

- um único `ISSUE_CODE`
- vários thresholds em `ISSUE_CFG_THRESHOLD`

e não vários códigos separados para warning e critical.

Exemplo preferencial:

- `MONTHLY_BACKUP_QUOTA`
  - `H` para atenção
  - `HH` para crítico

Em vez de:

- `MONTHLY_BACKUP_QUOTA_WARNING`
- `MONTHLY_BACKUP_QUOTA_CRITICAL`

### Ordem de cálculo recomendada

Para cada `ISSUE_CFG` ativo:

1. identificar o `EVALUATOR_CODE`
2. localizar a entidade alvo
3. carregar o valor-base do snapshot ou da query complementar
4. transformar o valor bruto em `current_value`
5. comparar com os thresholds configurados
6. resolver o nível mais grave atingido
7. definir `is_active`
8. montar `summary` e `details`
9. persistir em `ISSUE_CURRENT`

### Resolução de nível

Quando mais de um threshold for satisfeito ao mesmo tempo:

- deve prevalecer o nível mais grave

Ordem recomendada:

- `HH`
- `H`
- `L`
- `LL`

Observação:

- para issues quantitativas de crescimento, a ordem prática de gravidade tende
  a ficar concentrada em `H` e `HH`
- `L` e `LL` existem para casos em que valor baixo também representa problema

### Regras iniciais de cálculo por issue operacional

#### `HOST_OFFLINE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.is_offline`
- `VALUE_TYPE`
  - `bool`
- `current_value`
  - `1` quando offline
  - `0` quando online

#### `HOST_CHECK_STALE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_host_check_at`
- `VALUE_TYPE`
  - `duration`
- `current_value`
  - horas desde a última checagem operacional
- regra para nulo:
  - tratar como condição crítica ou sem dado, conforme política ativa

#### `NO_NEW_DISCOVERY`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_discovery_with_new_files_at`
- fallback possível:
  - `last_discovery_at`, quando a política ainda não tiver o campo mais rico
- `VALUE_TYPE`
  - `duration`
- `current_value`
  - horas desde a última discovery útil com arquivo novo

#### `BACKUP_STALE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_backup_success_at`
- `VALUE_TYPE`
  - `duration`
- `current_value`
  - horas desde o último backup com sucesso

#### `PROCESSING_STALE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_processing_success_at`
- `VALUE_TYPE`
  - `duration`
- `current_value`
  - horas desde o último processamento com sucesso

#### `BACKUP_ERRORS_OPEN`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.backup_error_open_count`
- `VALUE_TYPE`
  - `count`
- `current_value`
  - quantidade atual de erros abertos de backup

#### `PROCESSING_ERRORS_OPEN`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.processing_error_open_count`
- `VALUE_TYPE`
  - `count`
- `current_value`
  - quantidade atual de erros abertos de processamento

#### `SSH_AUTH_FAILURE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_ssh_auth_failure_at`
  - ou consulta equivalente enquanto o snapshot não materializar isso
- `VALUE_TYPE`
  - `bool`
- `current_value`
  - `1` quando houve falha relevante dentro da janela da política
  - `0` caso contrário

#### `SSH_CONNECTIVITY_FAILURE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_ssh_connectivity_failure_at`
  - ou consulta equivalente enquanto o snapshot não materializar isso
- `VALUE_TYPE`
  - `bool`
- `current_value`
  - `1` quando houve falha relevante dentro da janela da política
  - `0` caso contrário

#### `GPS_GNSS_UNAVAILABLE`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.last_gps_gnss_unavailable_at`
  - ou consulta equivalente enquanto o snapshot não materializar isso
- `VALUE_TYPE`
  - `bool`
- `current_value`
  - `1` quando houve ocorrência relevante dentro da janela da política
  - `0` caso contrário

#### `MONTHLY_BACKUP_QUOTA`

- fonte principal:
  - `HOST_CURRENT_SNAPSHOT.backup_done_gb_current_month`
- `VALUE_TYPE`
  - `volume`
- `current_value`
  - volume total de backup do mês corrente em GB

### Persistência do estado corrente

O `ISSUE_CURRENT` deve funcionar como o retrato mais recente da condição.

Direção inicial:

- uma linha por fingerprint lógico da issue
- fingerprint composto, no mínimo, por:
  - `ISSUE_CODE`
  - `ENTITY_SCOPE`
  - `ENTITY_KEY`

Regras de atualização:

- se a issue estiver ativa e ainda não existir:
  - criar linha com `FIRST_SEEN`, `OPENED_AT`, `LAST_SEEN`
- se a issue continuar ativa:
  - atualizar `CURRENT_VALUE`, `CURRENT_LEVEL`, `SEVERITY`, `SUMMARY`,
    `DETAILS`, `LAST_SEEN`
- se a issue deixar de estar ativa:
  - atualizar `IS_ACTIVE = 0`
  - preencher `CLOSED_AT`
  - manter `FIRST_SEEN` e `OPENED_AT` como histórico local mínimo

### Publicação para o Zabbix

O resultado publicado ao Zabbix deve ser simples e estável.

Direção preferencial:

- publicar um payload JSON estruturado por host
- deixar o Zabbix extrair items dependentes e triggers simples

Contrato lógico mínimo do payload:

- host ou entidade alvo
- timestamp de observação
- lista de issues calculadas
- para cada issue:
  - `active`
  - `level`
  - `severity`
  - `value`
  - `summary`

### Exemplo conceitual de payload

```json
{
  "host_id": 10369,
  "observed_at": "2026-07-23T22:30:00Z",
  "issues": {
    "HOST_OFFLINE": {
      "active": 0,
      "level": null,
      "severity": null,
      "value": 0,
      "summary": "Host online"
    },
    "NO_NEW_DISCOVERY": {
      "active": 1,
      "level": "H",
      "severity": "warning",
      "value": 36,
      "summary": "36 horas sem discovery útil"
    }
  }
}
```

### Observação sobre histórico

Neste momento, o planejamento fecha apenas:

- `ISSUE_CURRENT`
- publicação do estado atual

Uma tabela histórica de eventos de issue pode existir no futuro, mas não é
obrigatória para o primeiro ciclo.

---

## queryFileMetadata_trapper.py

O script atual:

- [`queryFileMetadata_trapper.py`](/RFFusion/src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py)

deve ser revisto no contexto dessa nova estratégia.

### Direção

Ele não deve continuar sendo visto apenas como um script de metadata de
arquivo.

Ele deve evoluir para o papel de adaptador de monitoramento entre RF.Fusion e
Zabbix.

### Objetivo da revisão

- melhorar o contrato de dados enviado ao Zabbix
- aproximar a integração do runtime do RF.Fusion
- usar trapper para atualização de estado operacional

---

## Lista Canônica Inicial de Dados Estruturados

Como ponto de partida para o contrato operacional enviado ao Zabbix via
trapper, a lista canônica inicial de dados estruturados fica assim.

### Estado operacional

- `last_host_check_at`
- `is_offline`

### Fluxo do pipeline

- `last_discovery_at`
- `last_discovery_file_count`
- `last_discovery_new_file_count`
- `last_discovery_with_new_files_at`
- `last_backup_success_at`
- `last_processing_success_at`
- `backup_queue_pending_count`
- `backup_queue_gb_total`
- `processing_queue_pending_count`
- `processing_queue_gb_total`
- `backup_error_open_count`
- `processing_error_open_count`

### Consumo operacional mensal

- `backup_done_gb_current_month`
- `backup_done_files_current_month`

### Acervo acumulado

- `discovered_files_total`
- `discovered_gb_total`
- `backup_done_files_total`
- `backup_done_gb_total`
- `processing_done_files_total`
- `processing_done_gb_total`
- `fact_spectrum_total`

### Eventos estruturados para alarmes

- `last_ssh_auth_failure_at`
- `last_ssh_connectivity_failure_at`
- `last_gps_gnss_unavailable_at`
- `no_new_discovery`

### Contexto operacional

- `current_locality_label`
- `current_site_label`
- `current_state_code`
- `current_geopoint_lat`
- `current_geopoint_lon`

### Detalhe opcional

- `last_backup_error_at`
- `last_backup_error_code`
- `last_processing_error_at`
- `last_processing_error_code`

### Observação atual

O campo `is_busy` não entra, por enquanto, na lista canônica do dashboard.

A leitura atual é que sua semântica ainda não é forte o bastante para uso
operacional confiável no painel principal, embora ele possa continuar existindo
internamente enquanto o fluxo for revisado.

O mesmo vale para um campo genérico como `last_error_summary`.

A leitura atual é que esse resumo amplo mistura falhas de `HOST_TASK` e
`FILE_TASK`, além de juntar naturezas diferentes de erro em um único campo
pouco confiável para dashboard.

Por isso, a direção atual é priorizar sinais mais específicos por domínio,
principalmente para:

- erro de backup
- erro de processamento

Os entrypoints do catálogo já persistem campos estruturados de erro para os
fluxos de `FILE_TASK`, como estágio, código e resumo canonizado. O contrato
operacional deve aproveitar essa estrutura em vez de promover um último erro
genérico como métrica principal.

O desenho final desses sinais ainda não está totalmente fechado, mas a direção
mais promissora hoje é trabalhar com:

- `*_error_at`
- `*_error_code`

e manter qualquer `*_error_summary` apenas como detalhe secundário, não como
item central do dashboard.

### Tabela resumida dos dados canônicos

| Nome | Micro descrição |
|---|---|
| `last_host_check_at` | Timestamp da última checagem operacional do host. |
| `is_offline` | Flag indicando se a estação está offline no estado atual. |
| `last_discovery_at` | Timestamp da última discovery concluída. |
| `last_discovery_file_count` | Quantidade total de arquivos encontrados na última discovery. |
| `last_discovery_new_file_count` | Quantidade de arquivos novos encontrados na última discovery. |
| `last_discovery_with_new_files_at` | Timestamp da última discovery que encontrou arquivo novo. |
| `last_backup_success_at` | Timestamp do último backup concluído com sucesso. |
| `last_processing_success_at` | Timestamp do último processamento concluído com sucesso. |
| `backup_queue_pending_count` | Quantidade de arquivos atualmente enfileirados para backup. |
| `backup_queue_gb_total` | Volume total atual da fila de backup em GB. |
| `processing_queue_pending_count` | Quantidade de arquivos atualmente enfileirados para processamento. |
| `processing_queue_gb_total` | Volume total atual da fila de processamento em GB. |
| `backup_error_open_count` | Quantidade de erros abertos de backup no estado corrente. |
| `processing_error_open_count` | Quantidade de erros abertos de processamento no estado corrente. |
| `backup_done_gb_current_month` | Volume total de backup concluído no mês corrente. |
| `backup_done_files_current_month` | Quantidade de arquivos com backup concluído no mês corrente. |
| `discovered_files_total` | Quantidade total acumulada de arquivos descobertos. |
| `discovered_gb_total` | Volume total acumulado de arquivos descobertos em GB. |
| `backup_done_files_total` | Quantidade total acumulada de arquivos com backup concluído. |
| `backup_done_gb_total` | Volume total acumulado de arquivos com backup concluído em GB. |
| `processing_done_files_total` | Quantidade total acumulada de arquivos processados com sucesso. |
| `processing_done_gb_total` | Volume total acumulado de arquivos processados com sucesso em GB. |
| `fact_spectrum_total` | Quantidade total acumulada de espectros gerados. |
| `last_ssh_auth_failure_at` | Timestamp da última falha de autenticação SSH detectada. |
| `last_ssh_connectivity_failure_at` | Timestamp da última falha de conectividade SSH detectada. |
| `last_gps_gnss_unavailable_at` | Timestamp da última ocorrência de GPS/GNSS indisponível. |
| `no_new_discovery` | Flag calculada indicando ausência de discovery útil segundo a política ativa. |
| `current_locality_label` | Nome textual da localidade atual da estação. |
| `current_site_label` | Nome textual do site atual associado à estação. |
| `current_state_code` | UF atual associada ao site/localidade da estação. |
| `current_geopoint_lat` | Latitude atual da estação no contexto operacional. |
| `current_geopoint_lon` | Longitude atual da estação no contexto operacional. |
| `last_backup_error_at` | Timestamp do erro mais recente de backup. |
| `last_backup_error_code` | Código estruturado do erro mais recente de backup. |
| `last_processing_error_at` | Timestamp do erro mais recente de processamento. |
| `last_processing_error_code` | Código estruturado do erro mais recente de processamento. |

### Tabela resumida das condições de alarme prioritárias

| Nome | Micro descrição |
|---|---|
| `HOST_OFFLINE` | Estação marcada como offline no estado operacional atual. |
| `HOST_CHECK_STALE` | Checagem operacional desatualizada além do threshold configurado. |
| `NO_NEW_DISCOVERY` | Estação sem discovery útil dentro da janela esperada pela política. |
| `BACKUP_STALE` | Backup sem sucesso recente dentro da janela esperada pela política. |
| `PROCESSING_STALE` | Processamento sem sucesso recente dentro da janela esperada pela política. |
| `BACKUP_ERRORS_OPEN` | Existência de erro aberto de backup acima da política tolerada. |
| `PROCESSING_ERRORS_OPEN` | Existência de erro aberto de processamento acima da política tolerada. |
| `SSH_AUTH_FAILURE` | Falha de autenticação SSH relevante para ação operacional. |
| `SSH_CONNECTIVITY_FAILURE` | Falha de conectividade SSH relevante para ação operacional. |
| `GPS_GNSS_UNAVAILABLE` | Ocorrência operacional de GPS/GNSS indisponível com relevância de alarme. |
| `MONTHLY_BACKUP_QUOTA_WARNING` | Consumo mensal de backup acima do limite de atenção. |
| `MONTHLY_BACKUP_QUOTA_CRITICAL` | Consumo mensal de backup acima do limite crítico. |

---

## O que Fica Fora da Primeira Versão

Para evitar excesso de suposição, ficam fora do escopo inicial:

- issues analíticas em `RFDATA`
- emissão detectada
- emissão recorrente
- anexos, screenshots ou evidência binária
- workflow complexo de ticket
- catálogo detalhado além do necessário para o operacional inicial

---

## Perguntas em Aberto

As definições abaixo ainda precisam ser refinadas:

1. Como a política operacional será lida do Zabbix e persistida localmente?
2. O `appCataloga.py` atual deve absorver essa sincronização ou isso merece um
   fluxo separado?
3. Quais consultas públicas do `dbHandlerBKP` precisam ser criadas para cada
   alarme?
4. Quais campos do `HOST` continuam úteis e quais devem deixar de ser base
   principal para monitoramento?
5. Qual será o payload operacional final entregue ao Zabbix?

---

## Próximos Passos Recomendados

1. Definir o contrato operacional mínimo entre Zabbix e RF.Fusion.
2. Definir como persistir a política efetiva em `ISSUE_CFG`.
3. Definir o escopo do `appCataloga_operational_issues_management.py`.
4. Definir as queries públicas do `dbHandlerBKP` para os alarmes da fase 1.
5. Revisar o `queryFileMetadata_trapper.py` à luz do novo contrato.
6. Definir os items e triggers do Zabbix para os alarmes operacionais
   prioritários.

---

## Planejamento da Migração de Métricas Operacionais

Este tópico consolida o planejamento da reorganização de métricas entre
`BPDATA`, `RFFUSION_SUMMARY`, WebFusion e Zabbix.

### Premissa

Nenhuma alteração de código deve ser executada antes de mapear todos os pontos
de impacto do runtime e dos consumidores.

O risco principal aqui não é apenas mudar um método isolado como
`host_update_statistics`, mas quebrar a coerência entre:

- schema de `BPDATA`
- schema de `RFFUSION_SUMMARY`
- worker `appCataloga_summary_database.py`
- `dbHandlerSummary.py`
- WebFusion
- integração com Zabbix

### Macrofases aprovadas

O programa de evolução fica dividido em duas macrofases:

- **Fase 1**
  - reorganizar e consolidar as métricas canônicas
  - estabilizar `HOST_CURRENT_SNAPSHOT` como contrato operacional
  - ajustar os consumidores de métricas
- **Fase 2**
  - planejar e implementar o domínio de issues operacionais
  - introduzir `ISSUESDB` e `ISSUES_CFG`
  - criar o entrypoint responsável por calcular as condições de alarme

Enquanto a Fase 1 não estiver fechada, a Fase 2 não deve avançar para
implementação.

### Modelo semântico aprovado

As decisões abaixo ficam registradas como base para a futura implementação:

- `BPDATA.HOST` deve representar principalmente o estado operacional corrente
  do runtime.
- `RFFUSION_SUMMARY.HOST_MONTHLY_METRIC` deve continuar representando acervo
  por mês de geração do arquivo, com base em `DT_FILE_CREATED_HOST`.
- `RFFUSION_SUMMARY.HOST_CURRENT_SNAPSHOT` deve ser o snapshot canônico para
  dashboard e integração operacional.
- `RFFUSION_SUMMARY.SERVER_CURRENT_SUMMARY` deve ser tratado como agregação
  derivada e simplificada, não como origem semântica principal.
- métricas de volume operacional devem usar `VL_FILE_SIZE_KB_HOST`, porque é o
  artefato realmente movimentado do host para o servidor.
- a transformação de `.zip` para `.mat` não deve alterar a contabilidade do
  volume operacional transferido.

### Regras semânticas já fechadas

- acervo mensal:
  - usar `DT_FILE_CREATED_HOST`
- operação do mês corrente:
  - backup do mês usa `DT_BACKUP`
  - processamento do mês deve usar `DT_PROCESSED`
- volume movimentado:
  - sempre usar `VL_FILE_SIZE_KB_HOST`
- tabelas de compatibilidade externa:
  - `HOST_LOCATION_SUMMARY`
  - `MAP_SITE_SUMMARY`
  - `MAP_SITE_STATION_SUMMARY`
  - `SITE_EQUIPMENT_OBS_SUMMARY`
  - não devem ter semântica alterada de forma incompatível

### Direção provisória para domínio de bridge

Há um incômodo arquitetural válido em manter elementos oficiais dentro de
`RFFUSION_SUMMARY`.

A leitura atual passa a ser:

- `RFFUSION_SUMMARY` deve permanecer como read model
- `RFFUSION_SUMMARY` deve conter métricas, snapshots, resumos e payloads de
  dashboard
- vínculos oficiais entre domínios não devem nascer nem morar
  conceitualmente no summary

Por isso, entra como direção provisória avaliar um banco dedicado de bridge.

### Nome provisório do banco

O nome `BRIDGE_BPDATA_RFDATA` parece estreito demais para a arquitetura
esperada no médio prazo.

Como direção de planejamento, faz mais sentido trabalhar com algo do tipo:

- `RFFUSION_BRIDGE`

ou outro nome equivalente que continue válido quando novos domínios entrarem,
como por exemplo um futuro `DTDATA`.

### Pontos de bridge já identificados

Até o momento, os pontos mais claros de bridge entre `BPDATA` e `RFDATA`
observados no código são os seguintes.

#### 1. Host operacional para equipamento analítico

Esse é o caso mais forte.

Hoje ele aparece materializado em:

- `HOST_EQUIPMENT_LINK`
- `HOST_EQUIPMENT_LINK_OVERRIDE`

A função semântica real aqui é reconciliar:

- `BPDATA.HOST.NA_HOST_NAME`
- `RFDATA.DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT`

Leitura atual:

- esse vínculo tem cara de bridge oficial
- ele não parece pertencer conceitualmente ao `RFFUSION_SUMMARY`
- ele é candidato forte a migrar futuramente para `RFFUSION_BRIDGE`

Direção de MVP:

- `HOST_EQUIPMENT_LINK` é o candidato principal
- `HOST_EQUIPMENT_LINK_OVERRIDE` não entra como peça central do desenho
- override manual só deve voltar se aparecer necessidade operacional real

#### 2. Identidade do arquivo operacional para a linhagem analítica

Existe um segundo bridge importante, mais sutil, ligado à linhagem dos
arquivos.

Hoje o fluxo de processamento:

- registra o arquivo fonte vindo do host em `RFDATA.DIM_SPECTRUM_FILE`
- registra o artefato final do repositório também em `RFDATA.DIM_SPECTRUM_FILE`
- liga esses arquivos aos espectros por `RFDATA.BRIDGE_SPECTRUM_FILE`

Além disso, o lado operacional em `BPDATA` já consulta `RFDATA.DIM_SPECTRUM_FILE`
para reconciliar estado de fila e artefatos existentes no repositório.

Leitura atual:

- aqui existe sim um acoplamento cross-domain real
- mas o núcleo semântico principal ainda é de linhagem analítica
- portanto, esse ponto não precisa necessariamente sair do `RFDATA`

Em outras palavras:

- isso é um bridge entre domínios
- mas não necessariamente um bridge que precise virar tabela em
  `RFFUSION_BRIDGE`

#### 3. Regras de inferência por família de host

Também existem regras em Python que fazem ponte entre identidade operacional e
persistência analítica, principalmente para:

- tipo de arquivo por hostname
- persistência do equipamento por família de estação
- inferência de tipo de equipamento

Exemplos observados:

- `get_file_type_id_by_hostname(...)`
- `resolve_equipment_persistence_identity(...)`
- `get_or_create_spectrum_equipment(...)`

Leitura atual:

- isso é bridge semântico em nível de regra
- mas não é necessariamente bridge físico em nível de tabela
- deve ser tratado como lógica de normalização e reconciliação, não como read
  model

### O que não parece bridge oficial

Nem todo ponto que junta `BPDATA` e `RFDATA` deve virar bridge oficial.

Os seguintes casos parecem derivados e devem continuar fora do domínio de
bridge oficial:

- `HOST_LOCATION_SUMMARY`
- `MAP_SITE_STATION_SUMMARY`
- `MAP_SITE_SUMMARY`
- totais de espectros por host
- localidade atual da estação
- site atual da estação

Esses elementos parecem ser:

- projeções
- agregações
- read models para dashboard

e não vínculos canônicos entre domínios.

### Diretriz provisória

Fica registrada a seguinte direção arquitetural provisória:

- `RFFUSION_SUMMARY` continua sendo apenas summary/read model
- vínculos oficiais cross-domain devem ser avaliados para um domínio próprio
- o primeiro candidato real para sair do summary é `HOST_EQUIPMENT_LINK`
- a linhagem `arquivo -> espectro` continua, por enquanto, como parte nativa
  do `RFDATA`
- regras de inferência por hostname/família continuam sendo tratadas como
  regras de normalização, não como summary

### Backlog planejado para domínio de bridge

Sem implementar agora, entra no backlog oficial do planejamento a execução
futura desta alteração arquitetural.

#### Item prioritário de backlog

- criar um domínio `RFFUSION_BRIDGE`
- migrar `HOST_EQUIPMENT_LINK` para esse domínio
- manter `RFFUSION_SUMMARY` como consumidor desse vínculo, e não como seu dono

#### Ordem desejada desse backlog

1. fechar o contrato final do vínculo `host -> equipment`
2. criar o schema mínimo do `RFFUSION_BRIDGE`
3. mover `HOST_EQUIPMENT_LINK` para o domínio de bridge
4. adaptar o summary worker para consumir o bridge
5. revisar consumidores de WebFusion e métricas derivadas

#### Observação

Essa execução continua fora do escopo imediato.

A decisão aqui é apenas:

- colocar a migração no backlog oficial
- tratar essa mudança como direção arquitetural desejada
- evitar que `HOST_EQUIPMENT_LINK` seja visto como destino final dentro de
  `RFFUSION_SUMMARY`

### Mapa preliminar de impacto

#### 1. `/src/mariadb/scripts/createProcessingDB.sql`

Hoje a tabela `BPDATA.HOST` mistura:

- estado operacional corrente
- timestamps úteis de operação
- contadores históricos agregados
- volumes agregados

Isso precisa ser revisado porque parte dessas métricas já é melhor
representada em `RFFUSION_SUMMARY`.

Impactos esperados no planejamento:

- decidir quais colunas de `HOST` continuam obrigatórias
- decidir quais colunas passam a ser apenas legadas durante transição
- evitar remoção prematura de colunas ainda lidas por runtime ou WebFusion

#### 2. `/src/mariadb/scripts/createFusionSummaryDB.sql`

Esse arquivo é um dos principais pontos de mudança futura.

Planejamento esperado:

- consolidar `HOST_CURRENT_SNAPSHOT` como contrato canônico operacional
- incluir métricas ainda faltantes, principalmente as derivadas por
  `DT_PROCESSED`
- revisar se `SERVER_CURRENT_SUMMARY` continua existindo como tabela física ou
  se fica apenas como agregação simples do snapshot

#### 3. `/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerBKP.py`

O método `host_update_statistics(self, host_id: int)` é apenas um exemplo do
impacto.

Hoje ele recalcula em `HOST` vários dados agregados vindos de
`FILE_TASK_HISTORY`.

Se o papel de `HOST` for reduzido para estado operacional corrente, esse
método provavelmente precisará:

- encolher bastante
- mudar de responsabilidade
- ou até ser substituído por outro fluxo mais explícito

Mas isso não pode ser decidido isoladamente sem revisar os consumidores.

#### 4. `/src/appCataloga/server_volume/usr/local/bin/appCataloga/host_handler/host_runtime.py`

Hoje esse módulo chama `db.host_update_statistics(...)`.

Se a responsabilidade desse método mudar, o fluxo do host task de atualização
de estatísticas também muda.

Ou seja, não basta alterar o `dbHandlerBKP`; o orquestrador que chama esse
passo também entra no impacto.

#### 5. `/src/appCataloga/server_volume/usr/local/bin/appCataloga/summary_handler/refresh_engine.py`

Esse é provavelmente o ponto mais sensível da migração.

Hoje o `refresh_engine`:

- lê `BPDATA.HOST`
- lê `FILE_TASK`
- lê `FILE_TASK_HISTORY`
- lê `HOST_MONTHLY_METRIC`
- monta `HOST_CURRENT_SNAPSHOT`
- depois agrega tudo em `SERVER_CURRENT_SUMMARY`

Logo, qualquer revisão de semântica em `HOST`, `HOST_CURRENT_SNAPSHOT` ou
`HOST_MONTHLY_METRIC` impacta diretamente:

- `_refresh_host_current_snapshot`
- `_refresh_server_current_summary`
- a ordem e o significado dos objetos do summary worker

#### 6. `/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_summary_database.py`

O loop do worker pode até não mudar muito, mas ele depende do contrato dos
objetos de summary e da estabilidade do `refresh_engine`.

Se mudarmos a composição dos snapshots, esse worker precisa ser revisado junto
para garantir:

- ordem correta de refresh
- logs coerentes
- comportamento previsível em full reconcile e incremental

#### 7. `/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerSummary.py`

Mesmo sendo um handler mais genérico, ele participa diretamente do runtime do
summary worker.

O impacto aqui não é necessariamente reescrever a classe, mas revisar:

- métodos afetados por novos objetos ou novas colunas
- documentação do contrato do summary worker
- compatibilidade com a sequência de refresh planejada

#### 8. `/src/webfusion/modules/host`

O módulo `/host` consome fortemente:

- `SERVER_CURRENT_SUMMARY`
- `HOST_CURRENT_SNAPSHOT`
- `HOST_MONTHLY_METRIC`

Logo, qualquer mudança de nomenclatura, semântica ou granularidade nessas
tabelas impacta o dashboard e o drill-down de estação.

#### 9. `/src/webfusion/modules/task` e `/src/webfusion/modules/maintenance`

Existem telas operacionais que ainda leem campos de `BPDATA.HOST` como contexto
de conectividade e operação.

Mesmo que o foco principal da migração seja o summary, esses módulos também
precisam entrar no checklist para não perder contexto útil do operador.

#### 10. `/src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py`

O contrato atual do trapper ainda está muito preso à ideia de metadata de
consulta.

Na migração planejada, ele deve passar a conversar melhor com o contrato
canônico de métricas operacionais e alarmes calculados.

#### 11. `/src/webfusion/DBHandler.m`

O método `getHostStats` existe no arquivo, mesmo que hoje não esteja em uso no
`appAnalise`.

Portanto, ele deve ser tratado como compatibilidade dormente:

- não é o principal limitador atual
- mas precisa entrar na revisão final antes de remover colunas antigas de
  `BPDATA.HOST`

#### 12. Documentação de apoio

Também entram no impacto:

- `/src/mariadb/scripts/README.md`
- documentação do summary worker
- documentação das métricas canônicas

### O que pode quebrar se a mudança for feita sem migração em fases

- `HOST_CURRENT_SNAPSHOT` pode ficar parcialmente vazio ou incoerente
- `SERVER_CURRENT_SUMMARY` pode somar métricas com semânticas misturadas
- o dashboard `/host` pode exibir números corretos com nomes errados
- o WebFusion pode continuar lendo colunas legadas já sem significado útil
- o Zabbix pode continuar recebendo um payload antigo incompatível com a lista
  canônica
- tarefas operacionais podem continuar dependendo de colunas antigas de `HOST`
  sem que isso esteja visível no início da mudança

### Estratégia de execução recomendada

#### Fase 1. Congelar o contrato canônico

Antes de qualquer refatoração de runtime, fechar formalmente:

- nomes finais das métricas canônicas
- semântica de cada métrica
- origem de cada métrica
- consumidor principal de cada métrica

#### Fase 2. Expandir o summary sem remover legado

Primeiro passo de implementação futura:

- adicionar no summary os campos novos necessários
- manter paralelamente os campos antigos durante a transição
- evitar remoções em `HOST` e em `HOST_CURRENT_SNAPSHOT` nessa fase

#### Fase 3. Ajustar os produtores

Depois:

- revisar `refresh_engine.py`
- revisar o fluxo que hoje usa `host_update_statistics`
- revisar os pontos que alimentam snapshot, mensal e agregados

#### Fase 4. Ajustar os consumidores

Somente depois dos produtores estabilizados:

- WebFusion `/host`
- WebFusion `/task`
- WebFusion `/maintenance`
- integração Zabbix trapper

#### Fase 5. Reduzir `BPDATA.HOST`

Só no final:

- remover dependência semântica de métricas históricas em `HOST`
- manter em `HOST` apenas o que fizer sentido como estado operacional corrente
- avaliar quais colunas podem ser oficialmente descontinuadas

### Segunda macrofase planejada: ISSUES e alarmes operacionais

Depois de estabilizar a parte de métricas, entra a segunda macrofase.

Essa etapa não deve começar antes de:

- fechar a lista canônica final de métricas
- estabilizar `HOST_CURRENT_SNAPSHOT`
- estabilizar o payload operacional levado ao Zabbix
- revisar os consumidores principais de métricas

#### Fase 6. Definir o papel do `ISSUESDB`

O banco de issues deve ser planejado como domínio próprio e separado dos
bancos operacionais e analíticos.

Direção atual:

- `BPDATA` continua sendo a base operacional transacional do catálogo
- `RFDATA` continua sendo a base analítica de espectros
- `RFFUSION_SUMMARY` continua sendo read model para dashboards e consultas
- `ISSUESDB` passa a ser o domínio persistente das políticas e do estado das
  issues

O objetivo é evitar espalhar política e estado de alarme em:

- `BPDATA`
- `RFDATA`
- `RFFUSION_SUMMARY`
- código Python sem persistência dedicada

#### Fase 7. Fechar o mínimo viável de schema de issues

Como direção de planejamento, o mínimo viável desta fase deve considerar pelo
menos:

- `ISSUES_CFG`
  - política efetiva persistida localmente
  - thresholds
  - habilitação
  - severidade
  - janela de avaliação
  - parâmetros específicos por tipo de issue
- tabela de estado de issue
  - pode continuar se chamando `ISSUE`
  - ou outro nome equivalente, se isso for revisado depois
  - o importante é persistir o estado corrente avaliado localmente

Nesta etapa, ainda não é necessário fechar todos os detalhes do modelo físico,
mas é necessário fechar:

- responsabilidade do `ISSUES_CFG`
- responsabilidade da tabela de estado
- relacionamento do `ISSUESDB` com host, estação e domínio monitorado

#### Fase 8. Definir o entrypoint de cálculo operacional

Deve existir um entrypoint dedicado para cálculo local das condições de alarme.

Direção atual:

- nome provável:
  - `appCataloga_operational_issues_management.py`
- responsabilidade:
  - ler a política efetiva persistida em `ISSUES_CFG`
  - consultar `BPDATA` e, quando aplicável, o snapshot operacional
  - calcular flags e condições estruturadas
  - persistir o estado das issues
  - devolver o resultado para integração com Zabbix

A lógica de alarme não deve ficar espalhada em vários entrypoints do catálogo.

#### Fase 9. Integrar cálculo local com Zabbix

Depois do domínio de issues existir localmente:

- o RF.Fusion calcula a condição
- persiste a política efetiva
- persiste ou atualiza o estado da issue
- publica o resultado estruturado para o Zabbix
- o Zabbix abre ou fecha o `Problem`

Isso mantém a regra já aprovada:

- o Zabbix continua sendo o dono da política operacional
- o RF.Fusion continua sendo o calculador da condição operacional
- o Zabbix continua sendo o gestor oficial do `Problem`

### Pendências que ainda precisam ser fechadas

- lista final de colunas que permanecem em `BPDATA.HOST`
- lista final de colunas novas ou ajustadas em `HOST_CURRENT_SNAPSHOT`
- decisão sobre o futuro de `SERVER_CURRENT_SUMMARY`
- decisão sobre a criação ou não de um banco `RFFUSION_BRIDGE`
- decisão sobre quais vínculos entram de fato no domínio de bridge oficial
- decisão sobre quais alarmes ficarão somente como flags e quais terão também
  timestamps auxiliares
- revisão final dos consumidores reais antes de qualquer remoção de schema
- definição final do escopo do `ISSUESDB`
- definição final do schema mínimo de `ISSUES_CFG`
- definição do estado mínimo que será persistido por issue
- definição final do entrypoint responsável pelo cálculo dos alarmes

### Diretriz final deste planejamento

O próximo passo não é editar código.

O próximo passo é usar este mapa para abrir uma revisão controlada dos pontos
afetados, fechar primeiro a Fase 1 de métricas e contratos, e só depois abrir
a execução da Fase 2 de issues e alarmes.
