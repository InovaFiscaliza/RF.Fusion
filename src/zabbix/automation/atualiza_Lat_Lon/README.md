# Documentação

Script para atualização de dados de inventário dosHosts no Zabbix.

Execução do script: 

- Login no Zabbix (zabbix_login.py).
- Acesso a BD (elt/Load.py).
- Updade de modo de inventario (aplicacao/Atualiza_modo_inventario.py).
- Update de coordenadas (aplcacao/Atualiza_coordenadas.py).
- Notificação sobre Latitudes e Longitudes que não bateram (main.py)
- Fim de script

# Arquivos em Json para configuração

## webhooks.json

Neste arquivo são declarados todos os webhooks para os canais do Teams. Existe um para Notificações Oficiais, e um para Testes.

## autenticador.json

Deverá ser passada as variáveis "__url__", "__usuario__" e "__senha__" do Zabbix. Também existem duas urls para o zabbix, dependendo da origem da execução do script, isso pode ser atualizado.

# Scripts de auxílio

## arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo.

## print_json.py

Realiza o print com formatação correta de um json.

### Inputs

- Texto em Json, sem quebra de linhas.

## zabbix_login.py

Função __zabbix_login__ retorna a variável __zapi__, servindo para realizar chamadas de API. Depende o json de autenticação. 

----

# Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── adicionaHosts.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── aplicacao
│   ├── __pycache__
│   │   ├── __init__.cpython-39.pyc
│   │   ├── Atualiza_coordenadas.cpython-39.pyc
│   │   └── Atualiza_modo_inventario.cpython-39.pyc
│   ├── __init__.py
│   ├── Atualiza_coordenadas.py
│   └── Atualiza_modo_inventario.py
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   └── zabbix_login.cpython-39.pyc
│   ├── arquivos_locais.py
│   ├── print_json.py
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
└── webhooks.json

Obs: hosts.db alocado no diretório junto da pasta com o script.

````