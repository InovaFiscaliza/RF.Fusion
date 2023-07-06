# --------------------------------------------------
# Script de verificação de BD
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

# dependências secundárias
import time
import pandas as pd # dependência openpyxl
from datetime import datetime, timedelta, date
import sys
import sqlite3
import os
import re
import logging
import json
import difflib

from auxiliar.salva_horario import salva_horario
from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG, WEBHOOK, TESTES

import pymsteams
from tabulate import tabulate

from load import Load
#from transform import Transform


def main():

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script de alertas com PyZabbix!\n")

    # Início do script -> Acesso a BD
    # ------------------------------------------------------------------------

    try:

        dados_EstServ, dados_EnlacesFix, dados_Zabbix, dados_CGIs, relacao_CGIs, relacao_Zabbix = Load.load(logging)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('consolida_Local', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)
        conn.close()
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Fim do acesso ao Banco de Dados")

    # ------------------------------------------------------------------------

    '''
    "relacao_CGIs" -> bate "id_de_rede" de estserv com "nome" da CGIs e cria relacionamento
    '''

    # ip de cgi e estserv bate
    estacoes_cgi_ip_eq = 0

    # ip de cgi e estserv difere
    estacoes_cgi_ip_dif = 0

    # cgi tem eq com estserv
    estacoes_cgi_id_eq = 0

    # cgi tem eq com zabbix
    estacoes_cgi_zabbix_eq = 0

    estacoes_cgis_nao_batem = []
    estacoes_cgis_nao_batem.append(["ID CGI", "Nome", "IP Zabbix", "IP CGI", "IP VPN CGI"])

    for index_relacao_cgi, row_relacao_cgi in relacao_CGIs.iterrows():

        id_cgi = str(row_relacao_cgi["presente_cgis"])
        id_estserv = str(row_relacao_cgi["presente_estserv"])

        # significa que o host acessado pela CGI tem equivalencia com o sharepoint, em "estserv"
        if id_estserv != "0000":

            estacoes_cgi_id_eq = estacoes_cgi_id_eq + 1

            ip_cgi = str(dados_CGIs.loc[dados_CGIs["id_bd_cgis"] == id_cgi, ["ip"]].values[0][0])

            ip_vpn_cgi = str(dados_CGIs.loc[dados_CGIs["id_bd_cgis"] == id_cgi, ["vpn"]].values[0][0])

            id_zabbix = str(relacao_Zabbix.loc[relacao_Zabbix["presente_estserv"] == id_estserv, ["id_bd_zabbix"]].values[0][0])

            # entrada em "estações e servidores" tem equivalencia com zabbix
            if id_zabbix != "0000":

                ip_zabbix = str(dados_Zabbix.loc[dados_Zabbix["id_bd_zabbix"] == id_zabbix, ["host_ip"]].values[0][0])

                estacoes_cgi_zabbix_eq = estacoes_cgi_zabbix_eq + 1

                # o zabbix está consumindo ou o ip da estação ou sua vpn
                if (difflib.SequenceMatcher(None, ip_cgi, ip_zabbix).ratio() > 0.8) or (difflib.SequenceMatcher(None, ip_vpn_cgi, ip_zabbix).ratio() > 0.8):
                    if MODO_DEBUG == 1:
                        print(ip_cgi)
                        print(ip_zabbix)
                        estacoes_cgi_ip_eq = estacoes_cgi_ip_eq + 1

                # a estação não bateu nem com ip publico ou vpn no acesso do zabbix
                else:

                    # estacoes que não bateram
                    estacoes_cgi_ip_dif = estacoes_cgi_ip_dif + 1

                    # salvar informação do host que não bateu
                    estacoes_cgis_nao_batem.append([id_cgi, str(dados_CGIs.loc[dados_CGIs["id_bd_cgis"] == id_cgi, ["nome"]].values[0][0]), ip_zabbix, ip_cgi, ip_vpn_cgi])

    if MODO_DEBUG == 1:

        # ip não bate
        print("\n\n\nip não bate -> " + str(estacoes_cgi_ip_dif))
        # ip bate
        print("ip bate -> " + str(estacoes_cgi_ip_eq))
        # equivalencia com sharepoint
        print("equivalencia com sharepoint -> " + str(estacoes_cgi_id_eq))
        # equivalencia com zabbix
        print("equivalencia com zabbix -> " + str(estacoes_cgi_zabbix_eq))
        print("\n\n\n")
        print(estacoes_cgis_nao_batem)
        print("\n\n\n")

    # ------------------------------------------------------------------------
    # parte 1 -> avisa no Teams que não bateu

    # informação formatada
    info_html = "Hosts acessados por CGI, que se equivaleram a Hosts no Zabbix: " + str(estacoes_cgi_zabbix_eq) + "\n" + "Hosts com diferença no IP entre tabela de CGIs e Zabbix: " + str(estacoes_cgi_ip_dif) + "\n\n" + str(tabulate(estacoes_cgis_nao_batem, headers='firstrow', tablefmt='grid'))

    arq_hooks = open(os.path.dirname(__file__) + WEBHOOK, "r")
    dados_webhooks = json.load(arq_hooks)

    if TESTES == 1:
        url_webhook = str(dados_webhooks["Testes"])

    else:
        url_webhook = str(dados_webhooks["Notificacoes"])

    # rodar toda semana
    try:

        today = date.today()
        dia = int(today.strftime("%d"))
        print(dia)

        if (dia % 7) == 0 or (dia == 1):

            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("🧭 Inconsistências nos IPs entre CGIs e Zabbix! Dia " + str(date.today().strftime("%d/%m/%Y")) + " 🧭")
            minhaMensagem.text(info_html)
            minhaMensagem.addLinkButton("Planilhas Sharepoint", "https://anatel365.sharepoint.com/sites/lista.fisf.publico/SitePages/Rede-de-Monitoramento-Remoto.aspx")
            minhaMensagem.addLinkButton("Zabbix", "http://zabbixsfi.anatel.gov.br/")

            minhaMensagem.send()

            if MODO_DEBUG == 1:
                minhaMensagem.printme()

            time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Erro ao enviar mensagem!")

        conn = sqlite3.connect(os.path.dirname(__file__) + '/../hosts.db')
        c = conn.cursor()

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('consolida_Local', '" + agora + "', 'False');"

        c.execute(sql_string)

        conn.commit()
        time.sleep(1)

        conn.close()
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Mensagem enviada!")


    # ------------------------------------------------------------------------
    # parte 2 -> muda no Zabbix!

    # ------------------------------------------------------------------------
    # salva execução

    try:
        conn = sqlite3.connect(os.path.dirname(__file__) + '/../hosts.db')
        c = conn.cursor()

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('consolida_Local', '" + agora + "', 'True');"

        c.execute(sql_string)

        conn.commit()
        time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Erro ao salvar última execução")

    finally:

        conn.commit()
        conn.close()

# ------------------------------------------------------------------------

if __name__ == '__main__':
    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()
    
    # última execução
    salva_horario(inicio)

    duracao = (fim - inicio) / 60
    if MODO_DEBUG == 1:
        print("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))

    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
