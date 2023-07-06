# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

TIME_SELECT = r"/time_select.json"
WEBHOOK = r"/webhooks.json"
HOST_BD = r"/../hosts.db"
AUTH_FILE = r"/autenticador.json"
RODANDO_SERVIDOR = 1

# 1 == MOSTRA MENSG ENVIADA, 0 == NÃO MOSTRA MENSAGEM ENVIADA
MODO_DEBUG = 0

# 1 == MOSTRA PROBLEMAS QUE FORAM GERADOS E RESOLVIDOS, 0 == NÃO ENVIA ESSA MENSAGEM EXTRA
ENVIA_MENSAGENS_EXTRA = 0

# A busca de quanto tempo cada estação ficou offline vai ser feita a partir de quantos dias atrás?
# Menos de 30 dias atrás não vai retornar um resultado bom
INICIO_BUSCA_ESTACAO_FUNCIONANDO = 30