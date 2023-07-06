# --------------------------------------------------
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

import sys
import os
import time
import sqlite3
import logging
from datetime import datetime

from auxiliar.arquivos_locais import HOST_BD, MOSTRAR_PRINTS
from auxiliar.salva_horario import salva_horario

from extract import Extract
from transform import Transform
from load import Load

def main():

    if MOSTRAR_PRINTS == 1:
        print("Iniciando acesso às estações!")

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando acesso às estações!")

    # ----------------------------------------------------------------------------
    # Início do script -> Acesso a BD

    dados_Zabbix =  Extract.extract_cgis(logging)

    # ----------------------------------------------------------------------------

    # contadores de acesso
    conectou = 0
    nao_conectou = 0

    # listas que vão para o dataframe ao final do script
    list_ids = []
    list_nome = []
    list_lat = []
    list_lon = []
    list_free_mem = []
    list_ip = []
    list_mac = []
    list_vpn = []
    list_tuns = []
    list_apps = []
    #list_endereco = []

    # lista para checar o que foi acessado durante a execução
    acessados = []

    conectou, nao_conectou, list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps, acessados = Transform.transform_acesso_cgis(dados_Zabbix, logging, conectou, nao_conectou, list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps, acessados)


    # Cria e exporta dataframe
    # ------------------------------------------------------------------------

    df = Load.load_df_cgis_BD(list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps)

    # Salva em BD
    # ------------------------------------------------------------------------

    logging.debug("Criando/alterando banco de dados!")

    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    # criando tabela dados_zabbix
    c.execute(
        'CREATE TABLE IF NOT EXISTS cgis (id_bd_cgis VARCHAR(4) PRIMARY KEY, nome VARCHAR(255), lat VARCHAR(255), lon VARCHAR(255), free_mem VARCHAR(255), ip VARCHAR(255), mac VARCHAR(255), vpn VARCHAR(255), tuns VARCHAR(255), list_apps VARCHAR(255), endereco VARCHAR(255))')

    conn.commit()
    time.sleep(1)

    # criando tabela log (caso não exista)
    c.execute('CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
    conn.commit()
    time.sleep(2)
    
    # bota na tabela de log que execucao foi um sucesso
    # update if row exists, else insert
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                "VALUES ('acessoCGI', '" + agora +"', 'True');"

    c.execute(sql_string)
    
    conn.commit()
    time.sleep(1)

    try:

        if df.empty: 
            if MOSTRAR_PRINTS == 1:
                print("Dataframe de informações a serem salvas voltou vazio!")
            logging.debug("Dataframe de informações a serem salvas voltou vazio!")
        else:
            df.to_sql('cgis', conn, if_exists='replace', index=False)


    except:

        if MOSTRAR_PRINTS == 1:
            print("Erro na conexão com banco de dados!\n" + str(sys.exc_info()[0]))
        
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                    "VALUES ('acessoCGI', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

        logging.warning("Erro na conexão com banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))

    finally:

        logging.debug("Encerrando conexão com BD!")

        conn.close()

#--------------------------------------------------------------------------

if __name__ == '__main__':

    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    salva_horario(inicio)

    duracao = (fim - inicio) / 60

    if MOSTRAR_PRINTS == 1:
        print("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))

    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
