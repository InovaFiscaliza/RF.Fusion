# Documentação

No final, esse script recebe os dados das planilhas ("Estações e Servidores" e "Enlaces Fixos") e coloca (seja criando ou atualizando) no banco de dados *SQLite hosts.db* . Banco de dados no diretório acima das pasta de execução.

### Explicação de *main.py*

- Início do script -> Limpeza e recuperação de dados Sharepoint via API.

- Limpeza dos dados (dataframe pandas).

- Acesso ao BD (salva dataframes "dados_EstServ" e "dados_EnlacesFix").

- Fim do script. 

----

## Arquivos auxiliares

### arquivos_locais.py

Serve para manter variáveis que se referenciam a arquivos locais que podem ser atualizadas com o tempo.

### autenticador.py

Dados para consumo de API do Sharepoint (se guarda endereços, a lista a ser baixada e credenciais). 

### dados_sharepoint.py

Acesso ao Sharepoint por meio de API, usando credenciais disponíveis no arquivo *autenticador.py*. 
Primeiro se autentica (função *authenticate*), pega a lista (função *get_sp_list*) e baixa a lista (função *download_list_items*), posteriormente transformada em um dataframe pandas.

### limpa_dados.py

Retira redundância de estações que são do tipo "rfeye". É comum que neste registro seja salvo algo além do nome no formato "RFeyeXXYYY", logo essa é uma função de limpeza para padronização. 

### salva_horario.py

Salva horário da última execução em log __(log_execucao.json)__ no formato Unix Timestamp.

### exec_log.json

Guarda o horário da última execução do script (índice ultima_execucao). 

----

## Criando IDs para as tabelas

__id_bd_estserv__: Módulo por 4 de hash que tem como input a seguinte concatenação de strings: 

````

Para cada linha no dataframe:
    
    id =  hash("Local:UF" + "Local" + "ID de rede") % 4

    lista de hashs.adiciona(id)

Adiciona lista de hashs no dataframe 

````

__id_bd_enlacesfix__: Módulo por 4 de hash que tem como input a seguinte concatenação de strings: "Local:UF" + "Local" + "Designação do Circuito"

````

Para cada linha no dataframe:
    
    id = hash("Local:UF" + "Local" + "Designação do Circuito") % 4

    lista de hashs.adiciona(id)

Adiciona lista de hashs no dataframe 

````

O objetivo da elaboração dos IDs dessa forma é evitar IDs como sendo inteiros autoincrementados. Cada ID é equivalente a um conjunto dinâmico de dados que o descreve, cada vez que o script roda o ID gerado no final é o mesmo. Funiona pois a função padrão de hash da **hashlib** no Python é determinística. Sabendo as regras de como um ID é formado, é possível o recalcular, porém não recuperar os dados que o geram dado o resultado puro da hash. 

O objetivo da elaboração dos IDs dessa forma é evitar IDs como sendo inteiros autoincrementados. Cada ID é equivalente a um conjunto dinâmico de dados que o descreve, cada vez que o script roda o ID gerado no final é o mesmo. Funiona pois a função padrão de hash da **hashlib** no Python é determinística. Sabendo as regras de como um ID é formado, é possível o recalcular, porém não recuperar os dados que o geram dado o resultado puro da hash. 

----

## Dicionário de equivalências dos nomes dos campos das planilhas no banco de dados

````
dicionario_tabela_EnlacesFix = {'ID_BD_EnlacesFix': 'id_bd_enlacesfix',
                             'Local:UF': 'local_uf',
                             'Local:Município': 'local_municipio',
                             "Local": "local_nome",
                             "IP Público da Estação": "ip_publico_da_estacao",
                             "Designação do Circuito": "designacao_do_circuito",
                             "Designação do Roteador": "designacao_do_roteador",
                             "IP Público do Roteador": "ip_publico_do_roteador",
                             "IP Gateway": "ip_gateway",
                             "Máscara de Rede": "mascara_de_rede",
                             "Contrato": "contrato",
                             "Tecnologia de Acesso": "tecnologia_de_acesso",
                             "Situação do Enlace": "situacao_enlace",
                             "Responsável Anatel": "responsavel_anatel",
                             "Referência Suspensão": "referencia_suspensao",
                             "Data Suspensão": "data_suspensao",
                             "Referência Solicitação": "referencia_solicitacao",
                             "Data Solicitação": "data_solicitacao",
                             "Referência Homologação": "referencia_homologacao",
                             "Data Homologação": "data_homologacao",
                             "Referência TRD": "referencia_trd",
                             "Data TRD": "data_trd",
                             "Observações": "observacoes",
                             "Pendência": "pendencia",
                             "Ações a serem adotadas": "acoes_a_serem_adotadas",
                             "Responsável na Anatel pela ação": "responsavel_na_anatel_pela_acao",
                             "Modificado": "modificado",
                             "Modificado por": "modificado_por",
                             "Meses Pagos": "meses_pagos",
                             "Versão": "versao"}
                             
dicionario_tabela_EstServ = {'ID_BD_EstServ': 'id_bd_estserv',
                          'Local:UF': 'local_uf',
                          'Local:Município': 'local_municipio',
                          "Local": "local_nome",
                          "ID de rede": "id_de_rede",
                          "Detentor": "detentor",
                          "Tipo de Estação": "tipo_de_estacao",
                          "Marca": "marca",
                          "Modelo": "modelo",
                          "Patrimônio": "patrimonio",
                          "Nº Série": "n_serie",
                          "Situação do Equipamento": "situacao_equipamento",
                          "Diagnóstico": "diagnostico",
                          "Versão FW/SW": "versao_fw_sw",
                          "Altura e configuração de antenas": "altura_e_configuracao_de_antenas",
                          "IP OpenVPN": "ip_openvpn",
                          "Data Chave OpenVPN": "data_chave_openvpn",
                          "ID OpenVPN": "id_openvpn",
                          "Observações": "observacoes",
                          "Pendência": "pendencia",
                          "Ações a serem adotadas": "acoes_a_serem_adotadas",
                          "Responsável na Anatel pela ação": "responsavel_na_anatel_pela_acao",
                          "Modificado": "modificado",
                          "Modificado por": "modificado_por",
                          "Versão": "versao",
                          "Instrumento Fiscaliza": "instrumento_fiscaliza",  # colunas novas
                          "Local:Referência_original": "local_ref_original",
                          "Local:Latitude": "local_lat",
                          "Local:Longitude": "local_lon",
                          "Local:Referência": "local_ref",
                          "Status de Aprovação": "status",
                          "IP OpenVPN (não editar)": "ip_ovpn"}
                             
````

----

## Dependências

- Python (v3.9.10)
- BeautifulSoup
- shareplum
- re
- json 
- sys
- os
- logging
- sqlite3
- pandas

----

## Visualização da Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── criaBD_SP_Planilhas.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   ├── autenticador.cpython-39.pyc
│   │   ├── dados_sharepoint.cpython-39.pyc
│   │   ├── limpa_dados.cpython-39.pyc
│   │   └── salva_horario.cpython-39.pyc
│   ├── arquivos_locais.py
│   ├── autenticador.py
│   ├── dados_sharepoint.py
│   ├── limpa_dados.py
│   └── salva_horario.py
├── extract
│   ├── __pycache__
│   │   └── Extract.cpython-39.pyc
│   └── Extract.py
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
