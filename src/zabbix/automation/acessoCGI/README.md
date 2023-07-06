# Documentação

Script de acesso aos conteúdos das CGIs das estações, para atualização de conteúdo em Banco de Dados.
Agrega daos na tabela "cgis".


### Explicação de *main.py*

- Início do script -> Acesso a BD

- Itera por cada host no Zabbix.

- Para cada host no Zabbix, tenta acessar o IP.

- Tenta autenticar com uma par usuario/senha se é de MG, com outro par usuário/senha para o resto das estações.

- Adiciona as estações acessadas para uma lista. Se a estação já foi acessada antes (independente de OVPN), a estação não é acessada duas vezes. 

- Cria e exporta dataframe.

- Salva no BD.

- Fim do script. 

----

# Arquivos Auxiliares

## time_selection.json

__ultima_execucao__: momento da última execução em Unix Timestamp.

## salva_horario.py

Salva o horário da última execução do script para automação de horários! Salvando o horário da última execução, o script não tratará de alertas já conhecidos.

## print_json.py

Realiza o print com formatação correta de um json, para debug apenas.

## retorna_endereco.py

Recebe o par *latitude* e *longitude*, retorna uma *string única* com identificadores do endereço completo do local (País, estado, cidade, CEP e outros).

## arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo. 

````

AUTH_DEFAULT -> formato "usuario:senha", serve para maioria das estações
AUTH_MG -> formato "usuario:senha", serve para estações em Minas Gerais

MG_TAG = '31' -> tag de minas gerais no Zabbix (aparece nos grupos aos quais as estações pertencem)

TIME_SELECT = r"/time_select.json" -> nome de arquivo local
HOST_BD = r"/../hosts.db" -> nome de arquivo local

MOSTRAR_PRINTS = 1 -> 0 não mostra os prints, 1 mostra os prints!

````

## acesso_estacoes.py

Inputs das funções -> *url base* a ser acessada (formato "http://" + ip da estação), mais par de *autenticação* (usuário e senha pro login).

#### Função define_nome

Com o IP da endereço da estação, retorna nome da estação.

Acesso -> *url base* + '/cgi-bin/unitname.cgi'

Retorna uma *string* apenas.

#### Função address

Com o IP da endereço da estação, retorna *mac address*, *vpn* e *tuns*.

Acesso -> *url base* + '/cgi-bin/ifconfig.cgi'


#### Função gpsstatus

Com o IP da endereço da estação, retorna dados de localização da estação. 

Variáveis de *latitude*, *longitude*, *memória livre* e *endereço IP*.

Acesso -> *url base* + '/cgi-bin/status.cgi'

Uso de expressão regular exemplificado pelo [link][expressao_reg].

#### Função apps

Com o IP da endereço da estação, retorna lista de aplicativos instalados, junto de seu status atual (se está rodando na estação ou não).

Acesso -> *url base* + '/cgi-bin/apps_list.cgi'

Após tratamento, a variável salva no Banco de Dados é uma *string*, mas o output completo da função é uma *lista*.

## Dependências

- Python (v3.9.10)
- urllib3
- sys
- re
- json
- pandas
- os
- time
- sqlite3
- logging
- datetime
- geopy

----

## Visualização da Árvore de Arquivos

````

.
├── __pycache__
│   └── retorna_endereco.cpython-39.pyc
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── acessoCGI.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── auxiliar
│   ├── __pycache__
│   │   ├── acesso_estacoes.cpython-39.pyc
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   ├── print_json.cpython-39.pyc
│   │   └── salva_horario.cpython-39.pyc
│   ├── acesso_estacoes.py
│   ├── arquivos_locais.py
│   ├── print_json.py
│   └── salva_horario.py
├── extract
│   ├── __pycache__
│   │   └── Extract.cpython-39.pyc
│   └── Extract.py
├── load
│   ├── __pycache__
│   │   └── Load.cpython-39.pyc
│   └── Load.py
├── transform
│   ├── __pycache__
│   │   └── Transform.cpython-39.pyc
│   └── Transform.py
├── exec_log.log
├── main.py
├── README.md
└── time_select.json
Obs: hosts.db alocado no diretório junto da pasta com o script.
````

Créditos pela busca de endereço por coordenada: contribuidores do [OpenStreetMap][openmap].

----

[openmap]: "https://www.openstreetmap.org/copyright"

[expressao_reg]: "https://www.debuggex.com/r/TJj8-qCdlj7ohH2H"
