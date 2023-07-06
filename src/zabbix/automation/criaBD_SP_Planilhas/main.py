# --------------------------------------------------
# Script de criação/atualização de BD
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

# dependências secundárias
import time
import pandas as pd  # dependência openpyxl
from datetime import datetime
import sys
import sqlite3
import os
import logging

from auxiliar.salva_horario import salva_horario
from auxiliar.limpa_dados import limpa_dados_estserv, limpa_registros_SP
from auxiliar.arquivos_locais import HOST_BD, MOSTRAR_PRINTS

from extract import Extract
from transform import Transform

# Declaração da main
# --------------------------------------------------------------------------

def main():

    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8',
                        format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script!\n")

    # Estações e Servidores/Enlaces
    # ------------------------------------------------------------------------

    dados_EstServ, dados_EnlacesFix = Extract.extract_EstServ_Enlaces()

    # ------------------------------------------------------------------------

    # ignorar onde o id de rede é nulo
    # dados_EstServ = dados_EstServ.dropna(subset=["Local"])
    #dados_EstServ = dados_EstServ.dropna(subset=['ID de rede'])

    dados_EstServ = Transform.transform_EstServ(logging, dados_EstServ)

    # Enlaces Fixos de Dados (FISF3ElancesFix)
    # ------------------------------------------------------------------------

    # leva em conta só onde os enlaces fixos estão ativos
    # dados_EnlacesFix = dados_EnlacesFix.loc[dados_EnlacesFix["Situação do Enlace"] == "Ativado"]

    # ignorar onde a designação de circuito
    # dados_EnlacesFix = dados_EnlacesFix.dropna(subset=["Local"])

    dados_EnlacesFix = Transform.transform_EnlacesFix(logging, dados_EnlacesFix)

    # Acesso ao BD
    # LOAD
    # ------------------------------------------------------------------------

    logging.debug(dados_EnlacesFix.dtypes)
    logging.debug(dados_EstServ.dtypes)

    # print(type(dados_EstServ))
    # print(type(dados_EnlacesFix))

    logging.debug("\n\nCriando/alterando banco de dados!")

    # ele trabalha com o banco de dados na pasta de cima, que ai tem outras pastas
    # nesse diretório se alimentando desse banco de dados!
    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    # criando tabela estserv
    c.execute(
        'CREATE TABLE IF NOT EXISTS estserv (id_bd_estserv VARCHAR(4) PRIMARY KEY, local_uf VARCHAR(5), local_municipio VARCHAR(225), local_nome VARCHAR(225), id_de_rede VARCHAR(225), detentor VARCHAR(225), tipo_de_estacao VARCHAR(225), marca VARCHAR(225), modelo VARCHAR(225), patrimonio VARCHAR(225), n_serie VARCHAR(225), situacao_equipamento VARCHAR(225), versao_fw_sw VARCHAR(225), altura_e_configuracao_de_antenas VARCHAR(225), ip_openvpn VARCHAR(225), data_chave_openvpn VARCHAR(225), id_openvpn VARCHAR(225), observacoes VARCHAR(225), pendencia VARCHAR(225), acoes_a_serem_adotadas VARCHAR(225), responsavel_na_anatel_pela_acao VARCHAR(225), modificado VARCHAR(225), modificado_por VARCHAR(225), versao VARCHAR(225), instrumento_fiscaliza VARCHAR(225), local_ref_original VARCHAR(225), local_lat VARCHAR(225), local_lon VARCHAR(225), local_ref VARCHAR(225), status VARCHAR(225), ip_ovpn VARCHAR(225))')
    conn.commit()
    time.sleep(1)

    # criando tabela enlacesfix
    c.execute(
        'CREATE TABLE IF NOT EXISTS enlacesfix (id_bd_enlacesfix VARCHAR(4) PRIMARY KEY,local_uf VARCHAR(5), local_municipio VARCHAR(225), local_nome VARCHAR(225), ip_publico_da_estacao VARCHAR(225), designacao_do_circuito VARCHAR(225), designacao_do_roteador VARCHAR(225), ip_publico_do_roteador VARCHAR(225), ip_gateway VARCHAR(225), mascara_de_rede VARCHAR(225), contrato VARCHAR(225), tecnologia_de_acesso VARCHAR(225), situacao_enlace VARCHAR(225), resopnsavel_anatel VARCHAR(225), referencia_suspensao VARCHAR(225), data_suspensao VARCHAR(225), referencia_solicitacao VARCHAR(225), data_solicitacao VARCHAR(225), referencia_homologacao VARCHAR(225), data_homologacao VARCHAR(225), referencia_trd VARCHAR(225), data_trd VARCHAR(225), observacoes VARCHAR(225), pendencia VARCHAR(225), acoes_a_serem_adotadas VARCHAR(225), responsavel_na_anatel_pela_acao VARCHAR(225), modificado VARCHAR(225), modificado_por VARCHAR(225), meses_pagos VARCHAR(225), versao VARCHAR(225))')
    conn.commit()
    time.sleep(1)

    # criando tabela log (caso não exista)
    c.execute(
        'CREATE TABLE IF NOT EXISTS log_execucao ( script_id VARCHAR(100) PRIMARY KEY, ultima_execucao VARCHAR(100), exec_sucesso VARCHAR(100))')
    conn.commit()
    time.sleep(1)

    # bota na tabela de log que execucao foi um sucesso
    # update if row exists, else insert
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                 "VALUES ('criaBD_Planilhas', '" + agora + "', 'True');"

    c.execute(sql_string)

    conn.commit()
    time.sleep(1)

    try:

        if dados_EstServ.empty:
            if MOSTRAR_PRINTS == 1:
                print("Dataframe de informações a serem salvas voltou vazio (dados_EstServ)!")
            logging.debug("Dataframe de informações a serem salvas voltou vazio (dados_EstServ)!")

        else:
            dados_EstServ.to_sql('estserv', conn, if_exists='replace', index=False)

        if dados_EnlacesFix.empty:
            if MOSTRAR_PRINTS == 1:
                print("Dataframe de informações a serem salvas voltou vazio (dados_EnlacesFix)!")

            logging.debug("Dataframe de informações a serem salvas voltou vazio (dados_EnlacesFix)!")

        dados_EnlacesFix.to_sql('enlacesfix', conn, if_exists='replace', index=False)


    except:
        logging.warning("Erro na conexão com banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('criaBD_Planilhas', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

    finally:

        logging.debug("Encerrando conexão com BD!")

        conn.close()


# Inicio da execução do script
# --------------------------------------------------------------------------

if __name__ == '__main__':
    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    # última execução
    salva_horario(inicio)

    duracao = (fim - inicio) / 60
    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))