
# Documentação

## No final, esse script recebe os dados do Zabbix e coloca (seja criando ou atualizando) no banco de dados *SQLite hosts.db* . Banco de dados no diretório acima das pasta de execução.

Para mais informações sobre o hosts.db, acessar [link][WikiGit].

### Explicação de *main.py*

- Início do script -> Login no zabbix por meio de biblioteca PyZabbix.

- Itera por cada host no Zabbix.

- Para cada host no Zabbix, recupera problemas, alertas, host_interface e outros dados.

- Cria e exporta dataframe.

- Salva no BD.

- Fim do script. 

## Arquivos auxiliares

### arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo.

### zabbix_login.py

Função __zabbix_login__ retorna a variável __zapi__, servindo para realizar chamadas de API. Usa o arquivo "__autenticador.json__".

### salva_horario.py

Salva horário da última execução em log __(log_execucao.json)__ no formato Unix Timestamp.

### limpa_dados.py

Limpa nomes de rfeye, que aparecem fora do formato "refeyeXXXXXX", e nomes de estações que apresentam inconsistências com caracteres tipo "-" e "_".

O objetivo da limpeza é padronização.

### print_json.py

Arquivo que mantém função de print estruturado de arquivo JSON.

### log_execucao.json

Guarda o horário da última execução do script (índice ultima_execucao).

### autenticador.json

Deverá ser passada as variáveis "__url__", "__usuario__" e "__senha__" do Zabbix.

----

## Criando IDs para as tabelas

__id_bd_zabbix__: Módulo por 4 de hash que tem como input a seguinte concatenação de strings: 

````

Para cada linha no dataframe:
    
    id =  hash("nome do host" + "id do host") % 4

    lista de hashs.adiciona(id)

Adiciona lista de hashs no dataframe 

````


O objetivo da elaboração dos IDs dessa forma é evitar IDs como sendo inteiros autoincrementados. Cada ID é equivalente a um conjunto dinâmico de dados que o descreve, cada vez que o script roda o ID gerado no final é o mesmo. Funiona pois a função padrão de hash da **hashlib** no Python é determinística. Sabendo as regras de como um ID é formado, é possível o recalcular, porém não recuperar os dados que o geram dado o resultado puro da hash.

## Dataframe que salva os dados do Zabbix

````

    dadosZabbbix = {
        "id_bd_zabbix": id_bd,
        "nome": nome, 
        "host": host, 
        "disponivel_no_zabbix": host_disponivel_zabbix, 
        "host_id": id, 
        "host_ip": ip,
        "grupos": grupo_lista, 
        "conexao_OVPN": ovpn,
        "erros_ultimas_24h": erro_recente,
        "host_disponivel_snmp": host_disponivel_snmp, 
        "host_disponivel_jmx": host_disponivel_jmx,
        "host_disponivel_ipmi": host_disponivel_ipmi, 
        "templates_vinculados": templates,
        "ultimo_problema_ocorrido": dif_erro, 
        "qtd_problemas_graves": quantidade_problemas, 
        "qtd_prolemas_total": qtd_prolemas_total, 
        "ultimo_ocorrido": lista_ultimo_ocorrido
    }


````

----

## Dependências

- Python (v3.9.10)
- time
- datetime
- pandas
- os
- hashlib
- logging
- pyzabbix

----

## Visualização da Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── criaBD_Zabbix.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   ├── limpa_dados.cpython-39.pyc
│   │   ├── print_json.cpython-39.pyc
│   │   ├── salva_horario.cpython-39.pyc
│   │   └── zabbix_login.cpython-39.pyc
│   ├── arquivos_locais.py
│   ├── limpa_dados.py
│   ├── print_json.py
│   ├── salva_horario.py
│   └── zabbix_login.py
├── extract
│   ├── __pycache__
│   │   └── Extract.cpython-39.pyc
│   └── Extract.py
├── load
│   ├── __pycache__
│   │   └── Load.cpython-39.pyc
│   └── Load.py
├── autenticador.json
├── exec_log.log
├── log_execucao.json
├── main.py
└── README.md
Obs: hosts.db alocado no diretório junto da pasta com o script.
````

[WikiGit]: "https://github.com/gui1080/testes_PyZabbix_FISF3/wiki/Hosts-Database"
