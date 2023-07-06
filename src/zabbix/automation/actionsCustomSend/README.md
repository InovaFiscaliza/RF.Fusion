# Documentação

Script para envio de mensagens personalizadas das GRs no MS Teams. Recebe dados do banco de dados __hosts.db__.

Execução do script: 

- Login no Zabbix.
- Leitura de __time_select.json__ .
- Definição de horário mais antigo para um alarme ser recuperado.
- Recuperando dados Sharepoint.
- Itera por canal, e então para cada host deste canal recupera a lista de problemas ocorridos.
- Se necessário, filtra os problemas.
- Trata os dados e então repassa como alerta.
- Envia histórico de ocorrências. Nessa estrutura de try/catch, se algo dá errado então adicionamos no log do BD que a execução falhou.
- Envia histórico de quanto tempo cada estação de cada GR ficou offline (Envia todo dia 28 e todo dia 14 de cada mês).
- Fim do script.

Commit do dia 12/08 no meu Github foi o último a enviar mensagens individuais e não listas!

# Arquivos em Json para configuração

## webhooks.json

Neste arquivo são declarados todos os webhooks para os canais do Teams. Existe um para cada GR, e um para cada UO.

Na main, a filtragem de GRs é dada por meio da busca de hosts, passando o ID de cada grupo ao qual o host que se deseja buscar pertence.

## time_selection.json

__tempo_automatico__: booleano, caso seja 1, a busca de incidentes ocorre desde o último horário declarado em "__ultima_execucao__". Caso essa variável seja, 1, as variáveis "__select_ontem__" e "__dif_minima_minutos__" serão ignoradas. 

__select_ontem__: booleano, caso seja 1, a busca de incidentes ocorre nas últimas 24 horas.

__dif_minima_minutos__: caso a variável anterior esteja em zero, essa variável fará a seleção de início de buscas. Deverá ser passado há quantos minutos atrás a busca por incidentes começará.

__ultima_execucao__: momento da última execução em Unix Timestamp.

## autenticador.json

Deverá ser passada as variáveis "__url__", "__usuario__" e "__senha__" do Zabbix.

### Problemas ao se recuperar hosts Zabbix em planilhas Sharepoint!

A formatação diverge. Neste script, tudo é tratado em _lowercase_. Existe alguns templates que acabam se adequando às todas as estações:

Os hosts que estão em "Estações e Servidores" são encontrados por meio de seus nomes de RFEye. O modelo é:

- RFEye00XXXX
- RFEye00XXXX_OVPN

Na hora de retornar o nome do que tem conexão normal e OVPN, ele retira o OVPN do nome do host, já que a comparação é na estrutura:

> if string1 in string2

No final, dá no mesmo e evita erros nas comparações.

Os hosts que estão em "Enlaces Fixos" apresentam algumas apresentações diferentes, como:

- AAA 1234567
- AAA/BB/12345
- AAA_BB_12345
- AAA-BB-12345

Nos últimos dois casos, tudo passa a ser "/" como no Zabbix. Se está entre "()", esses caracteres são retirados.

----

# Scripts de auxílio 

## status_estacoes.py

Descobrir, por GR, tempo de inatividade no mes de cada estação. Depende de variável __INICIO_BUSCA_ESTACAO_FUNCIONANDO__, igual a "x" dias de busca para trás (30 dias).
Declaração da função responsável por essa funcionalidade, __define_inatividade.py__

## filtro_de_alertas.py

Serve para diminuir a redundância nos alarmes. Muitos alarmes são problemas que ocorreram mas foram resolvidos logo em sequência. Esse tipo de mensagem dupla vira uma mensagem unificada.
Neste arquivo há uma única função que tem como input a lista de alertas por host em dado período que foram recuperados do Zabbix.

### Código de status das Mensagens

Se a ocorrência foi resolvida, a mensagem repassada no Zabbix será:

> ✅ Resolvido ✅

Se a ocorrência ainda é um problema, a mensagem repassada no Zabbix será:

> ❌ Problema ❌

Quando uma ocorrência é resolvida logo em seguida, a mensagem gerada (se filtrada) será:

> ✅⚠ Problema já ocorreu e já foi resolvido! ⚠✅

## arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo.

Utilizam-se as seguintes variáveis para modificar como o script funciona:

- __ENVIA_MENSAGENS_EXTRA__: Quando ativado (1), envia mensagens redundantes de problemas que ocorreram e já foram resolvidos em dado período de tempo.
- __MODO_DEBUG__: Quando ativado (1), exibe prints na tela.

## zabbix_login.py

Função __zabbix_login__ retorna a variável __zapi__, servindo para realizar chamadas de API. Depende o json de autenticação. 

## seletor_grs.py

Função __seleciona_GR__ retorna o __webhook__ correto dependendo do grupo de hosts que se está analisando, também se retorna a variável que será usada para a __filtragem__.

### Inputs 

- Dados dos webhooks, recuperados do json __webhooks.json__.  
- Url do webhook que será atualizada.
- Tag que será usada para filtro de hosts (id do grupo de hosts).
- Contador "i", selecionando a variável da iteração.

## print_json.py

Realiza o print com formatação correta de um json.

### Inputs

- Texto em Json, sem quebra de linhas.

## salva_horario.py

Salva o horário da última execução do script para automação de horários! Salvando o horário da última execução, o script não tratará de alertas já conhecidos.

### Inputs

- Horário a ser salvo em __time_selection.json__, em Unix Timestamp.

## retorna_local.py

Com dados retirados do Sharepoint, função declarada neste arquivo deve retornar dados relacionados ao local do host.

### Inputs

- host_no_zabbix: string em *lowercase* do nome do host recuperado no Zabbix.
- dados_EstServ: dados extraídos de __Planilha de Estações e Servidores__.
- dados_EnlacesFix: dados extraídos de __Planilha de Elances Fixos__ no Sharepoint.

----

# Dependências

- Python (v3.9.10)
- difflib
- time
- json
- datetime
- sys
- os
- pandas
- sqlite3
- logging
- pyzabbix

# Observação

Requisitar que quem for receber as mensagens no Teams mude as Notificações do canal de interesse de "Personalizado" para "Todas as atividades".

# Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── actionsCustomSend.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── aplicacao
│   ├── __pycache__
│   │   ├── __init__.cpython-39.pyc
│   │   └── Alertas.cpython-39.pyc
│   ├── __init__.py
│   └── Alertas.py
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   ├── filtro_de_alertas.cpython-39.pyc
│   │   ├── print_json.cpython-39.pyc
│   │   ├── retorna_local.cpython-39.pyc
│   │   ├── salva_horario.cpython-39.pyc
│   │   ├── seletor_grs.cpython-39.pyc
│   │   ├── status_estacoes.cpython-39.pyc
│   │   └── zabbix_login.cpython-39.pyc
│   ├── arquivos_locais.py
│   ├── filtro_de_alertas.py
│   ├── print_json.py
│   ├── retorna_local.py
│   ├── salva_horario.py
│   ├── seletor_grs.py
│   ├── status_estacoes.py
│   └── zabbix_login.py
├── etl
│   ├── __pycache__
│   │   ├── __init__.cpython-39.pyc
│   │   └── Load.cpython-39.pyc
│   ├── __init__.py
│   └── Load.py
├── autenticador.json
├── exec_log.log
├── main.py
├── README.md
├── time_select.json
└── webhooks.json
Obs: hosts.db alocado no diretório junto da pasta com o script.

````
