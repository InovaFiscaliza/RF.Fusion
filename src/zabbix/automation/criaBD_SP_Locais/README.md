
# Documentação

No final, esse script recebe os dados das planilhas (Locais) e coloca (seja criando ou atualizando) no banco de dados *SQLite hosts.db* . Banco de dados no diretório acima das pasta de execução.

### Explicação de *main.py*

- Início do script -> Limpeza e recuperação de dados Sharepoint via API.

- Limpeza dos dados (dataframe pandas).

- Acesso ao BD (salva dataframe "dados_Locais").

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

__id_bd_locais__: Módulo por 4 de hash que tem como input a seguinte concatenação de strings:  

````

Para cada linha no dataframe:
    
    id =  hash("Logradouro" + "CEP" + "Municipio") % 4

    lista de hashs.adiciona(id)

Adiciona lista de hashs no dataframe 

````

O objetivo da elaboração dos IDs dessa forma é evitar IDs como sendo inteiros autoincrementados. Cada ID é equivalente a um conjunto dinâmico de dados que o descreve, cada vez que o script roda o ID gerado no final é o mesmo. Funiona pois a função padrão de hash da **hashlib** no Python é determinística. Sabendo as regras de como um ID é formado, é possível o recalcular, porém não recuperar os dados que o geram dado o resultado puro da hash. 

----

## Dicionário de equivalências dos nomes dos campos das planilhas no banco de dados

````
dicionario_tabela_Locais = {'ID_BD_Locais': 'id_bd_locais',
                         'UF': 'uf',
                         'Município': 'municipio',
                         'Referência': 'referencia',
                         'Bairro': 'bairro',
                         'Logradouro': 'logradouro',
                         'Número': 'numero',
                         'Complemento': 'complemento',
                         'CEP': 'cep',
                         'Atendimento': 'atendimento',
                         'Responsável pelo Local': 'responsavel_local',
                         'Situação Local': 'situacao_local',
                         'Contrato/Cessão': 'contrato_cessao',
                         'Contrato/Cessão:Instrumento.': 'contrato_cessao_instrumento',
                         'Contrato/Cessão: Situação': 'contrato_cessao_situacao',
                         'Responsável Anatel': 'responsavel_anatel',
                         'Latitude': 'lat',
                         'Longitude': 'lon',
                         'Observações': 'observacoes',
                         'Pendência': 'pendencia',
                         'Ações a serem adotadas': 'acoes_a_serem_adotadas',
                         'Responsável na Anatel pela ação': 'responsavel_acao_na_anatel',
                         'Modificado': 'modificado',
                         'Modificado por': 'modificado_por',
                         'Versão': 'versao',
                         'Status de Aprovação': 'status_aprovacao'}
````

----

## Dependências

- Python (v3.9.10)
- sys
- datetime
- os
- sqlite3
- json
- Pandas (junto do "openpyxl")
- pyzabbix (v1.0.0)
- hashlib
- shareplum (acesso por API do Sharepoint)
- logging
- bs4 (BeautifulSoup)

----

## Visualização da Árvore de Arquivos

````
.
├── __pycache__
├── .idea
│   ├── inspectionProfiles
│   │   └── profiles_settings.xml
│   ├── .gitignore
│   ├── criaBD_SP_Locais.iml
│   ├── misc.xml
│   ├── modules.xml
│   └── workspace.xml
├── auxiliar
│   ├── __pycache__
│   │   ├── arquivos_locais.cpython-39.pyc
│   │   ├── autenticador.cpython-39.pyc
│   │   ├── dados_sharepoint.cpython-39.pyc
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
├── load
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
