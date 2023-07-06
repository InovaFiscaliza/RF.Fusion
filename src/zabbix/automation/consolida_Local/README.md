# Documentação

Script de 

----

# Explicação de *main.py*

- Início do script -> Acesso a BD

- ...

- ...

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
- tabulate

----

# Árvore de Arquivos

````

...

````