from pyzabbix import ZabbixAPI
from arquivos_locais import AUTENTICADOR, RODANDO_SERVIDOR
import json
import sys
import os


# Login no zabbix por meio de biblioteca PyZabbix
# ------------------------------------------------------------------------

def zabbix_login():
    CREDENTIALS = AUTENTICADOR

    print("Iniciando script de alertas com PyZabbix!\n")

    with open(os.path.dirname(__file__) + CREDENTIALS, "r") as arq_senha:
        dados = json.load(arq_senha)
        user = str(dados["usuario"])
        senha = str(dados["senha"])

        url_testes = str(dados["url"])
        url_server = str(dados["url_no_servidor"])
        if RODANDO_SERVIDOR == 1:
            url = url_server
        else:
            url = url_testes


        # login
        zapi = ZabbixAPI(url)
        zapi.login(user, senha)

    return zapi