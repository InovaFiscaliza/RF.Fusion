# Documentação

Script de verificação de BD. Levantamento de inconsistência entre diferentes fontes de dados, criação de relacionamentos no BD.

----

# Explicação de *main.py*

- Início do script -> Acesso a BD

- Levantamento de dados quantitativos sobre BD

- Mapeando dados entre Zabbix e Sharepoint -> Levanta dados que vão ser usados para preencher tabela *relacao_zabbix*. Esse controle para checagem Sharepoint para o Zabbix ocorre por meio da criação de algumas colunas novas no dataframe. Os dataframes de dados no Zabbix recebem booleanos a serem acessados depois, indicando se foram achados no Zabbix ou não. O dataframe do Zabbix já relaciona os ids no sharepoint (seja estação ou enlace) do elemento de rede representado em dado índice. Esses dados serão diretamente usados na criação da tabela *relacao_zabbix* no BD.

- Busca inversa, de Sharepoint para Zabbix -> Levantamento do que faltou no Zabbix. Primeiro percorre Enlaces Fixos, e depois Estações e Servidores. O que falta estar no Zabbix é adicionado em listas com prefixo "append".

- Manda mensagem no Teams notificando verificação do BD (busca do que faltou no Zabbix.)

- Mapeando dados entre Estações e Locais -> Levanta dados que serão criados para preencher tabela *relacao_locais*. Compara campo "referencia" em tabela "Locais" e "local_nome" em tabela de "Estações".

- Mapeando dados entre CGIs e Estações -> Levanta dados que serão criados para preencher tabela *relacao_cgis*. Itera pelos dados dos CGIs e depois pelos dados das Estações, comparando respectivamente campos "nome" e "id_rede". Notifica inconsistências no Teams. 

- Pega do dataframe de dados do Zabbix as colunas que serão usadas para fazer a tabela (como listas), e depois extende essas listas com as listas de prefixo "append". Essas listas com prefixo "append" são itens que estão no sharepoint mas não no Zabbix, e o que estava no dataframe são itens que estão no Zabbix mas, por algum erro, poderiam estar no Sharepoint.

- Agora, no final, acessa o BD e cria os relacionamentos entre dados: __relacao_zabbix__, __relacao_locais__, __relacao_cgis__, __log_execucao__.

- Fim do script.

----

# Arquivos em Json para configuração

## webhooks.json

Neste arquivo são declarados todos os webhooks para os canais do Teams. Existe um para cada GR, e um para cada UO.

Na main, a filtragem de GRs é dada por meio da busca de hosts, passando o ID de cada grupo ao qual o host que se deseja buscar pertence.

----

# Scripts de auxílio 

## arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo.

## salva_horario.py

Salva o horário da última execução do script para automação de horários! Salvando o horário da última execução, o script não tratará de alertas já conhecidos.

# Dependências

- Python (v3.9.10)
- time
- pandas
- datetime
- sys
- sqlite3
- os
- re
- logging
- json
- difflib
- pymsteams (v0.2.1) 

----

# Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── misc.xml
│   ├── modules.xml
│   ├── verifica_BD.iml
│   └── workspace.xml
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   └── salva_horario.cpython-39.pyc
│   ├── arquivos_locais.py
│   └── salva_horario.py
├── load
│   ├── __pycache__
│   │   └── Load.cpython-39.pyc
│   └── Load.py
├── transform
│   ├── __pycache__
│   │   └── Transform.cpython-39.pyc
│   └── Transform.py
├── exec_log.log
├── log_execucao.json
├── main.py
├── README.md
└── webhooks.json
Obs: hosts.db alocado no diretório junto da pasta com o script.

````
