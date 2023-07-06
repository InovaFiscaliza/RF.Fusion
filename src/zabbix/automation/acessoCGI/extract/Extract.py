import pandas as pd
from auxiliar.arquivos_locais import HOST_BD, MOSTRAR_PRINTS
import sqlite3
import os
import time
import sys

def extract_cgis(logging):

    conn = sqlite3.connect(os.path.dirname(__file__) + "/../" + HOST_BD)
    c = conn.cursor()

    try:

        c.execute('''
                        SELECT * FROM dados_zabbix
                ''')

        # Dados de Estações e Servidores
        dados_Zabbix = pd.DataFrame(c.fetchall(), columns=["id_bd_zabbix", "nome", "host", "disponivel_no_zabbix", "host_id", "host_ip", "grupos", "conexao_OVPN", "erros_ultimas_24h", "templates_vinculados", "ultimo_problema_ocorrido", "qtd_problemas_graves", "ultimo_ocorrido"])

        logging.debug(dados_Zabbix.info())

        time.sleep(5)

    except:

        if MOSTRAR_PRINTS == 1:
            print("Erro ao abrir BD!")

        logging.warning("Ocorreu um erro ao abrir o banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))

    finally:

        conn.commit()
        conn.close()

    # tirar do dataframe tudo que não é rfeye
    dados_Zabbix = dados_Zabbix[dados_Zabbix["host"].str.contains('rfeye', case=False, na=False)]

    # vai pro começo do dataframe as conexoes ovpn
    dados_Zabbix = dados_Zabbix.sort_values(by=['conexao_OVPN'])

    return dados_Zabbix