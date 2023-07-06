# --------------------------------------------------
# Script de criação/atualização de BD, com dados do Zabbix
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

# dependências secundárias
import time
from datetime import datetime
import sys
import sqlite3
import os
import logging

from auxiliar.salva_horario import salva_horario
from auxiliar.zabbix_login import zabbix_login
from auxiliar.arquivos_locais import MOSTRAR_PRINTS

from extract import Extract
from load import Load

# Declaração da main
# --------------------------------------------------------------------------


def main():

    # Login no zabbix por meio de biblioteca PyZabbix
    # ------------------------------------------------------------------------

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8',
                        format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script!\n")

    try:

        zapi = zabbix_login()
        logging.debug("Conectado no Zabbix na versão %s" % zapi.api_version())

    except:

        if MOSTRAR_PRINTS == 1:
            print("Erro no Login do Zabbix!")

        logging.warning("Erro no login!")
        logging.warning("Ocorrido: " + str(sys.exc_info()[0]))
        exit(1)


    # ------------------------------------------------------------------------

    if MOSTRAR_PRINTS == 1:
        print("Iniciando busca Zabbix -> Sharepoint")

    # Itera por cada host no Zabbix
    # ------------------------------------------------------------------------
    # Loop itera por todos os hosts de zabbix, fazendo querys individuais para cada um
    # buscando problemas, alertas, templates, grupos e interfazer

    try:

        id_bd, nome, host, id, ip, grupo_lista, host_disponivel_zabbix, erro_recente, templates,  ovpn, dif_erro, lista_ultimo_ocorrido, quantidade_problemas, qtd_prolemas_total, hoje, inicio_query, inicio_query_problemas = Extract.extract(logging, zapi)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + '/../hosts.db')
        c = conn.cursor()
        # criando tabela log (caso não exista)

        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        conn.commit()
        time.sleep(2)

        # bota na tabela de log que execucao foi um sucesso
        # update if row exists, else insert
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('criaBD_Zabbix', '" + agora + "', 'False');"

        c.execute(sql_string)

        conn.commit()
        time.sleep(1)
        quit()

    finally:

        if MOSTRAR_PRINTS == 1:
            print("Fim da busca por itens do Zabbix!")

    # Cria e exporta dataframe
    # ------------------------------------------------------------------------

    try:
        df = Load.load(id_bd, nome, host, host_disponivel_zabbix, id, ip, grupo_lista, ovpn, erro_recente, templates, dif_erro, quantidade_problemas, qtd_prolemas_total, lista_ultimo_ocorrido)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + '/../hosts.db')
        c = conn.cursor()
        # criando tabela log (caso não exista)

        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        conn.commit()
        time.sleep(2)

        # bota na tabela de log que execucao foi um sucesso
        # update if row exists, else insert
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('criaBD_Zabbix', '" + agora + "', 'False');"

        c.execute(sql_string)

        conn.commit()
        time.sleep(1)
        quit()

    finally:

        if MOSTRAR_PRINTS == 1:
            print("Fim da exportação de dataframe!")

    # Salva no BD
    # ------------------------------------------------------------------------

    logging.debug("Criando/alterando banco de dados!")
    
    # ele trabalha com o banco de dados na pasta de cima, que ai tem outras pastas
    # nesse diretório se alimentando desse banco de dados!
    conn = sqlite3.connect(os.path.dirname(__file__) + '/../hosts.db')
    c = conn.cursor()

    # criando tabela dados_zabbix
    c.execute('CREATE TABLE IF NOT EXISTS dados_zabbix (id_bd_zabbix VARCHAR(4) PRIMARY KEY, nome VARCHAR(255), host VARCHAR(255), disponivel_no_zabbix VARCHAR(255), host_id VARCHAR(25), host_ip VARCHAR(25), grupos VARCHAR(255), conexao_OVPN VARCHAR(255), erros_ultimas_24h VARCHAR(25), templates_vinculados VARCHAR(255), texto_ultimo_problema_ocorrido VARCHAR(255), qtd_problemas_graves VARCHAR(25), qtd_prolemas_total VARCHAR(25), ultimo_ocorrido VARCHAR(255))')

    conn.commit()
    time.sleep(2)

    # criando tabela log (caso não exista)
    c.execute('CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
    conn.commit()
    time.sleep(2)
    
    # bota na tabela de log que execucao foi um sucesso
    # update if row exists, else insert
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                "VALUES ('criaBD_Zabbix', '" + agora +"', 'True');"

    c.execute(sql_string)
    
    conn.commit()
    time.sleep(1)

    
    try:

        if df.empty:

            if MOSTRAR_PRINTS == 1:
                print("Dataframe vazio, melhor não mudar o BD!")
            logging.debug("Dataframe vazio, melhor não mudar o BD!")

        else:
            df.to_sql('dados_zabbix', conn, if_exists='replace', index=False)


    except:
        
        logging.warning("Erro na conexão com banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))
        
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                    "VALUES ('criaBD_Zabbix', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:

        logging.debug("Encerrando conexão com BD!")
        
    # ------------------------------------------------------------------------

if __name__ == '__main__':

    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    # última execução
    salva_horario(inicio)

    duracao = (fim - inicio) / 60
    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
