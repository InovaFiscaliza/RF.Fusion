# dependências secundárias
import time
import logging
import sqlite3
import os
import pandas as pd
import json
from tabulate import tabulate

from auxiliar.arquivos_locais import WEBHOOK, HOST_BD, MODO_DEBUG
import pymsteams

def main():

    # Login no zabbix por meio de biblioteca PyZabbix
    # ------------------------------------------------------------------------
    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script de envio de Histórico!\n")

    arq_hooks = open(os.path.dirname(__file__) + WEBHOOK, "r")
    dados_webhooks = json.load(arq_hooks)


    # Recuperando dados
    # ------------------------------------------------------------------------
    # Update: interação com banco de dados

    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    try:

        c.execute('''
                        SELECT * FROM log_execucao
                ''')

        # Dados de Estações e Servidores
        dados_Historico = pd.DataFrame(c.fetchall(), columns=["script_id", "ultima_execucao", "exec_sucesso"])

        logging.debug(dados_Historico.info())

        time.sleep(2)

    except:

        if MODO_DEBUG == 1:
            print("Erro")

    finally:

        if MODO_DEBUG == 1:
            print("Dados históricos recuperados!")

        conn.commit()
        conn.close()

    info_alertas = []

    info_alertas.append(['Script', 'Última Execução', 'Status'])

    for index_hist, row_hist in dados_Historico.iterrows():

        info_alertas.append([str(row_hist["script_id"]), str(row_hist["ultima_execucao"]), str(row_hist["exec_sucesso"])])

    info_html = str(tabulate(info_alertas, headers='firstrow', tablefmt='grid'))

    logging.debug(str(info_html))

    url_webhook = str(dados_webhooks["Notificacoes"])
    minhaMensagem = pymsteams.connectorcard(url_webhook)
    minhaMensagem.title("Execuções de scripts de hoje!")
    minhaMensagem.text(info_html)

    try:
        minhaMensagem.send()

        time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Erro no envio")

    finally:

        if MODO_DEBUG == 1:
            print("Fim do Script")


# Inicio da execução do script
#--------------------------------------------------------------------------

if __name__ == '__main__':

    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    duracao = (fim - inicio)/60
    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
