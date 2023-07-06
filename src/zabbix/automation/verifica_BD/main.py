# --------------------------------------------------
# Script de verificação de BD
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

# dependências secundárias
import time
import pandas as pd # dependência openpyxl
from datetime import datetime
import sys
import sqlite3
import os
import re
import logging
import json

from auxiliar.salva_horario import salva_horario
from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG, WEBHOOK, TESTES

import pymsteams

from load import Load
from transform import Transform


def main():

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script de alertas com PyZabbix!\n")

    # Início do script -> Acesso a BD
    # ------------------------------------------------------------------------

    try:

        dados_EstServ, dados_EnlacesFix, dados_Zabbix, dados_Locais, dados_CGIs = Load.load(logging)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Fim do acesso ao Banco de Dados")

    # Criando corpo das mensagens que será enviada no Teams
    # ------------------------------------------------------------------------

    arq_hooks = open(os.path.dirname(__file__) + WEBHOOK, "r")
    dados_webhooks = json.load(arq_hooks)

    if TESTES == 1:

        url_webhook = str(dados_webhooks["Testes"])

    else:

        url_webhook = str(dados_webhooks["Notificacoes"])

    minhaMensagem = pymsteams.connectorcard(url_webhook)

    # Levantamento de dados quantitativos sobre BD
    # ------------------------------------------------------------------------

    qtd_zabbix = int(len(dados_Zabbix))
    qtd_EnlacesFix = int(len(dados_EnlacesFix))
    qtd_EstServ = int(len(dados_EstServ))
    qtd_Locais = int(len(dados_Locais))
    qtd_CGIs = int(len(dados_CGIs))

    logging.debug("Estações no Sharepoint: " + str(qtd_EstServ + qtd_EnlacesFix))
    logging.debug("Estações no Zabbix: " + str(qtd_zabbix))
    logging.debug("Locais cadastrados: " + str(qtd_Locais))
    logging.debug("CGIs de estações acessadas: " + str(qtd_CGIs))

    mensagem_inicial = "Estações no Sharepoint: " + str(qtd_EstServ + qtd_EnlacesFix) + "\n\nEstações no Zabbix: " + str(qtd_zabbix) + "\n\nLocais cadastrados: " + str(qtd_Locais) + "\n\nCGIs de estações acessadas: " + str(qtd_CGIs)

    # ZABBIX -> SHAREPOINT
    # Mapeia entre Zabbix e Sharepoint
    # ------------------------------------------------------------------------
    # Se busca no Zabbix o que está ou não no Sharepoint

    try:

        dados_Zabbix, dados_EnlacesFix, dados_EstServ, qtd_hosts_achados = Transform.transform_Zabbix_Sharepoint(dados_Zabbix, dados_EnlacesFix, dados_EstServ, logging)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Fim: busca no Zabbix o que está ou não no Sharepoint")

    # Busca inversa, de Sharepoint para Zabbix -> Levantamento do que faltou no Zabbix
    # ------------------------------------------------------------------------

    # tudo onde o relacionamento não bate é atribuído "0000" nos códigos

    # a idéia é que ele não vai dar criar a mesma entrada 2 vezes (como no loop acima),
    # mas novos casos de estações fora do zabbix podem ser encontrados!
    # o que faltar vai para uma lista, para ser anexada depois

    if MODO_DEBUG == 1:
        print("Checando Enlaces Fixos e o Zabbix...")

    # Batendo enlaces fixos com Zabbix
    # ------------------------------------------------------------------------
    # "designação_do_circuito" em estações e servidores equivale a "host" no Zabbix.

    append_ids_relacao = []
    append_idsZabbix = []
    append_presente_estserv = []
    append_presente_enlacesfix = []

    faltou_no_zabbix = 0

    try:

        dados_Zabbix, dados_EnlacesFix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix = Transform.transform_Zabbix_Enlaces(dados_Zabbix, dados_EnlacesFix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:
        if MODO_DEBUG == 1:
            print("Fim: Batendo enlaces fixos com Zabbix")

    # Batendo estações e servidores com Zabbix
    # ------------------------------------------------------------------------
    # "id_de_rede" em estações e servidores equivale a "host" no Zabbix.

    if MODO_DEBUG == 1:
        print("Realizando busca reversa: vendo o que faltou no Zabbix mas que consta em Estações e Servidores/Enlaces Fixos")

    try:

        dados_EstServ, dados_Zabbix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix = Transform.transform_Zabbix_EstServ(dados_EstServ, dados_Zabbix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Fim: Batendo estserv com Zabbix")

    logging.debug("Quantidade de Hosts mapeados do Zabbix para o Sharepoint com sucesso!: " + str(qtd_hosts_achados))
    logging.debug("Quantidade de Hosts no Sharepoint: " + str(qtd_zabbix))
    logging.debug("Não achei no Zabbix: " + str(faltou_no_zabbix))

    mensagem_inicial = mensagem_inicial + "\n\nQuantidade de Hosts mapeados do Zabbix para o Sharepoint com sucesso!: " + str(qtd_hosts_achados) + "\n\nQuantidade de Hosts no Sharepoint: " + str(qtd_zabbix) + "\n\nEstações não achadas no Zabbix: " + str(faltou_no_zabbix) + "\n\n-------------------------------\n\n🕰 " + str(datetime.now())

    # Manda mensagem no Teams notificando verificação do BD
    # ------------------------------------------------------------------------

    minhaMensagem.title("🧾 Verificação do Banco de Dados! 🧾")
    minhaMensagem.text(mensagem_inicial)
    minhaMensagem.send()

    time.sleep(1)

    # Mapeando dados entre Estações e Locais
    # ------------------------------------------------------------------------
    # intenção é criar tabela relacao_locais
    # Equivalência entre campo "referencia" em Locais e "local_nome" em Estações e Servidores

    if MODO_DEBUG == 1:
        print("Fazendo relacao_locais...")

    ids_relacao_local = []
    ids_locais = []
    ids_estserv = []
    ids_enlaces = []
    locais_batem = []

    try:

        dados_Locais, dados_EnlacesFix, dados_EstServ, locais_batem, ids_locais, ids_estserv, ids_enlaces, ids_relacao_local = Transform.transform_EstServ_Locais(dados_Locais, dados_EnlacesFix, dados_EstServ, locais_batem, ids_locais, ids_estserv, ids_enlaces, ids_relacao_local)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'True');"

        c.execute(sql_string)
        time.sleep(1)
        quit()

    finally:

        if MODO_DEBUG == 1:
            print("Fim: Mapeando dados entre Estações e Locais")

    # Mapeando dados entre CGIs e Estações
    # ------------------------------------------------------------------------
    # Equivalencia entre "nome" nos dados das CGIs e "id_de_rede" nos dados de Estações e Servidores

    if MODO_DEBUG == 1:
        print("Fazendo relacao_cgis")

    id_relacao_cgi_estserv = []
    ids_cgis = []
    ids_estserv_achados = []

    try:

        dados_CGIs, dados_EstServ, ids_cgis, ids_estserv_achados, id_relacao_cgi_estserv, minhaMensagem = Transform.transform_EstServ_CGIs(dados_CGIs, dados_EstServ, ids_cgis, ids_estserv_achados, id_relacao_cgi_estserv, minhaMensagem)

    except:

        conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
        c = conn.cursor()

        # criando tabela de log se n existir
        c.execute(
            'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
        time.sleep(1)

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('verifica_BD', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)
        #quit()

    finally:
        if MODO_DEBUG == 1:
            print("Fim: Mapeando dados entre CGIs e Estações")

    del minhaMensagem

    # cria tabela de relacionamento entre tabelas atuais
    # relacao_zabbix
    # ------------------------------------------------------------------------
    # Pega do dataframe as colunas que serão usadas para fazer a tabela, e depois bota as listas com prefixo "append".
    # Essas listas com prefixo "append" são itens que estão no sharepoint mas não no Zabbix

    if MODO_DEBUG == 1:
        print("Fazendo relacao_zabbix...")

    dados_id_bd_relacao = []
    dados_bd_zabbix = [] #dados_Zabbix.drop(["id_bd_zabbix"], axis=1)
    dados_presente_estserv = [] #dados_Zabbix.drop("presente_estserv", axis=1)
    dados_presente_enlacesfix = [] #dados_Zabbix.drop("presente_enlacesfix", axis=1)

    for index_zabbix, row_zabbix in dados_Zabbix.iterrows():

        dados_bd_zabbix.append(row_zabbix["id_bd_zabbix"])
        dados_presente_estserv.append(row_zabbix["presente_estserv"])
        dados_presente_enlacesfix.append(row_zabbix["presente_enlacesfix"])
        
        input_hash = str(row_zabbix["id_bd_zabbix"]) + str(row_zabbix["presente_estserv"]) + str(row_zabbix["presente_enlacesfix"])
        
        id_relacao = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)
        
        dados_id_bd_relacao.append(id_relacao)

    dados_id_bd_relacao.extend(append_ids_relacao)
    dados_bd_zabbix.extend(append_idsZabbix)
    dados_presente_estserv.extend(append_presente_estserv)
    dados_presente_enlacesfix.extend(append_presente_enlacesfix)

    # Acessa o BD
    # ------------------------------------------------------------------------
    # cria os relacionamentos

    if MODO_DEBUG == 1:
        print("\n\nCriando/alterando banco de dados!")
    logging.debug("\n\nCriando/alterando banco de dados!")

    # ele trabalha com o banco de dados na pasta de cima, que ai tem outras pastas
    # nesse diretório se alimentando desse banco de dados!
    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    # criando tabela relacao_zabbix
    
    c.execute('CREATE TABLE IF NOT EXISTS relacao_zabbix (id_bd_relacao VARCHAR(4) PRIMARY KEY, id_bd_zabbix VARCHAR(4), presente_estserv VARCHAR(4), presente_enlacesfix VARCHAR(4))')

    conn.commit()
    time.sleep(1)

    # criando tabela relacao_locais
    c.execute('CREATE TABLE IF NOT EXISTS relacao_locais (id_relacao_local VARCHAR(4) PRIMARY KEY, id_bd_locais VARCHAR(4), id_bd_estserv VARCHAR(4), id_bd_enlaces VARCHAR(4), local_correspondente VARCHAR(155 ))')

    conn.commit()
    time.sleep(1)

    # criando tabela relacao_cgis
    c.execute('CREATE TABLE IF NOT EXISTS relacao_cgis ( id_relacao_cgi_estserv VARCHAR(4) PRIMARY KEY, ids_cgis VARCHAR(4), ids_estserv_achados VARCHAR(4))')

    conn.commit()
    time.sleep(1)

    # criando tabela de log se n existir
    c.execute('CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
    time.sleep(1)

    # bota na tabela de log que execucao foi um sucesso
    # update if row exists, else insert

    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                 "VALUES ('verifica_BD', '" + agora +"', 'True');"

    c.execute(sql_string)

    # formata dados para passar pro BD

    relacao_zabbix = {"id_bd_relacao": dados_id_bd_relacao, "id_bd_zabbix":dados_bd_zabbix, "presente_estserv":dados_presente_estserv, "presente_enlacesfix":dados_presente_enlacesfix}

    relacao_locais = {"id_relacao_local": ids_relacao_local, "presente_locais": ids_locais, "presente_estserv": ids_estserv, "presente_enlacesfix": ids_enlaces, "local_correspondente": locais_batem}

    relacao_cgis = {"id_relacao_cgi_estserv":id_relacao_cgi_estserv, "presente_cgis":ids_cgis, "presente_estserv":ids_estserv_achados}

    df_relacao_zabbix = pd.DataFrame(relacao_zabbix)

    df_relacao_locais = pd.DataFrame(relacao_locais)

    df_relacao_cgis = pd.DataFrame(relacao_cgis)

    # passa tudo para string
    df_relacao_zabbix = df_relacao_zabbix.astype('string')

    df_relacao_locais = df_relacao_locais.astype('string')

    df_relacao_cgis = df_relacao_cgis.astype('string')

    # testando consistência
    assert int(len(df_relacao_zabbix)) == qtd_zabbix, "Teste de consistência falho!"

    try:

        if MODO_DEBUG == 1:
            print("Passando dados para BD!")
        logging.debug("Passando dados para BD!")

        # relacao_zabbix
        if df_relacao_zabbix.empty:
            if MODO_DEBUG == 1:
                print("Dataframe de relacao_zabbix estava vazio, melhor não atualizar o BD...")
            logging.debug("Dataframe de relacao_zabbix estava vazio, melhor não atualizar o BD...")

        else:
            df_relacao_zabbix.to_sql('relacao_zabbix', conn, if_exists='replace', index=False)
            time.sleep(1)

        # relacao_locais
        if df_relacao_locais.empty:
            if MODO_DEBUG == 1:
                print("Dataframe de relacao_locais estava vazio, melhor não atualizar o BD...")
            logging.debug("Dataframe de relacao_locais estava vazio, melhor não atualizar o BD...")

        else:
            df_relacao_locais.to_sql('relacao_locais', conn, if_exists='replace', index=False)
            time.sleep(1)

        # relacao_cgis
        if df_relacao_cgis.empty:

            if MODO_DEBUG == 1:
                print("Dataframe de relacao_cgis estava vazio, melhor não atualizar o BD...")
            logging.debug("Dataframe de relacao_cgis estava vazio, melhor não atualizar o BD...")

        else:
            df_relacao_cgis.to_sql('relacao_cgis', conn, if_exists='replace', index=False)
            time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Erro na conexão com banco de dados!")
            print("Ocorreu: " + str(sys.exc_info()[0]))

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                    "VALUES ('verifica_BD', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

        logging.debug("Erro na conexão com banco de dados!")
        logging.debug("Ocorreu: " + str(sys.exc_info()[0]))

    finally:

        if MODO_DEBUG == 1:
            print("Encerrando conexão com BD!")

        logging.debug("Encerrando conexão com BD!")

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
