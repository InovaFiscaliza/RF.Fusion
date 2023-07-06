# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

from pyzabbix import ZabbixAPI
import json
import sys
import os
from auxiliar.arquivos_locais import AUTH_FILE, RODANDO_SERVIDOR

# Login no zabbix por meio de biblioteca PyZabbix
# ------------------------------------------------------------------------

def zabbix_login():

    print("Iniciando script de alertas com PyZabbix!\n")

    try:
        # recuperando dados
        with open(os.path.dirname(__file__) + "/../" + AUTH_FILE, "r") as arq_senha:
            dados = json.load(arq_senha)
            token = str(dados["api_token"])

            url_servidor = str(dados["url_servidor"])
            url_teste = str(dados["url"])

            if RODANDO_SERVIDOR == 1:
                url = url_servidor
            else:
                url = url_teste

            # login
            zapi = ZabbixAPI(url)
            zapi.login(api_token=token)

    except:
        print("\n\nErro na chamada de API para login!\n\n")
        print("Ocorreu: " + str(sys.exc_info()[0]))

    finally:
        print("Login realizado com sucesso!")
        print("Conectado no Zabbix na versão %s" % zapi.api_version())

    return zapi
