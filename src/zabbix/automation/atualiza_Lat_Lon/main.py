# --------------------------------------------------
# Script de criação de hosts
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

# funções locais
from auxiliar.zabbix_login import zabbix_login
from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG, WEBHOOK, TESTES

# dependências secundárias
import time
import os
import json
import sqlite3
import logging
import pymsteams
from datetime import datetime
from tabulate import tabulate

from etl import Load
from aplicacao import Atualiza_modo_inventario, Atualiza_coordenadas, Atualiza_gruposZabbix

# centro da integração do script, PyZabbix

def main():


    # Login no zabbix por meio de biblioteca PyZabbix
    # ------------------------------------------------------------------------
    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script de alertas com PyZabbix!\n")

    zapi = zabbix_login()

    # Criando corpo das mensagens que será enviada no Teams
    # ------------------------------------------------------------------------

    arq_hooks = open(os.path.dirname(__file__) + WEBHOOK, "r")
    dados_webhooks = json.load(arq_hooks)

    if TESTES == 1:

        url_webhook = str(dados_webhooks["Testes"])

    else:

        url_webhook = str(dados_webhooks["Notificacoes"])

    minhaMensagem = pymsteams.connectorcard(url_webhook)

    # Início do script -> Acesso a BD
    # ------------------------------------------------------------------------

    dados_EstServ, dados_EnlacesFix, dados_Zabbix, dados_CGIs, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_RelacaoLocais, dados_Locais = Load.load()

    # Updade de modo de inventario
    # -----------------------------------------------

    Atualiza_modo_inventario.modo_inventario(logging, dados_Zabbix, zapi)

    # Update de coordenadas
    # -----------------------------------------------

    n_bateu = 0
    hosts_nao_correspondentes = []
    hosts_nao_correspondentes.append(["Nome Host", "Host ID"])
    qtd_hosts_atualizados = 0

    qtd_hosts_atualizados, hosts_nao_correspondentes, n_bateu = Atualiza_coordenadas.coordenadas(logging, zapi, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_Zabbix, dados_EnlacesFix, dados_EstServ, dados_CGIs, dados_RelacaoLocais, dados_Locais, minhaMensagem)

    # -----------------------------------------------
    # Notificação sobre Latitudes e Longitudes que não bateram

    logging.debug("Repassando mensagem de Latitudes e Longitudes que não bateram")

    minhaMensagem.title("Coordenadas de Hosts que não correspondem!")
    info_html = str(tabulate(hosts_nao_correspondentes, headers='firstrow', tablefmt='grid'))
    minhaMensagem.text(info_html)

    try:

        minhaMensagem.send()

        time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Erro no envio")

    finally:

        if MODO_DEBUG == 1:
            print("Mensagem enviada!")

    # Atualiza os grupos dos Hosts
    # -----------------------------------------------

    hosts_atualizados = Atualiza_gruposZabbix.grupos_zabbix(logging, zapi, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_Zabbix, dados_EnlacesFix, dados_EstServ, dados_CGIs, dados_RelacaoLocais, dados_Locais, minhaMensagem)

    # -----------------------------------------------

    logging.debug("Fechando BD!")

    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    try:

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('atualiza_Lat_Lon', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)

    except:

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('actionsCustomSend', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

    finally:

        conn.commit()
        conn.close()

    # -----------------------------------------------

if __name__ == '__main__':

    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    duracao = (fim - inicio)/60
    print("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % duracao)


