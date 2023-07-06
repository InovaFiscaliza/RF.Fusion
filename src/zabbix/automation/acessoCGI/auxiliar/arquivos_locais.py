# --------------------------------------------------
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

'''
## Página web da estação:
- usuário: admin
- senha: VER PAINEL PRPDU"CAO

#### Para Minas Gerais
- usuário: admin
- senha: VER PAINEL PRODUCAO
'''

AUTH_DEFAULT = 'admin:VER PAINEL PRODUCAO'
AUTH_MG = 'ADMIN:VER PAINEL PRODUCAO'

# MG tem uma senha diferente, é a tag "31"
# (Isso aparece como parte da string que é guardada na tabela "dados_zabbix", coluna "grupos")

MG_TAG = '31'

TIME_SELECT = r"/time_select.json"
HOST_BD = r"/../hosts.db"

# 1 mostra os prints, 0 não mostra
MOSTRAR_PRINTS = 1
