import mysql.connector

# Conecte-se ao banco de dados
conn = mysql.connector.connect(host='localhost', user='appCataloga', password='<app_pass>', db='RFDATA')

# Crie um cursor
cur = conn.cursor()

# Execute uma consulta para obter todas as tabelas no banco de dados
cur.execute("SHOW TABLES")

# Obtenha uma lista de todas as tabelas
tables = [table[0] for table in cur.fetchall()]

# Crie um dicionário para armazenar a estrutura
structure = {}

# Para cada tabela, execute uma consulta para obter os nomes das colunas
for table in tables:
    cur.execute(f"SHOW COLUMNS FROM {table}")
    
    # Armazene os nomes das colunas no dicionário
    structure[table] = [column[0] for column in cur.fetchall()]

# Feche a conexão
conn.close()

print(structure)