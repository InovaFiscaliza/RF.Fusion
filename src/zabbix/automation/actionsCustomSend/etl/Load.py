import os
import sqlite3
import time
from datetime import datetime
import pandas as pd
import sys

from auxiliar.arquivos_locais import HOST_BD

def load(logging):

    conn = sqlite3.connect(os.path.dirname(__file__) + "/../" + HOST_BD)
    c = conn.cursor()


    c.execute('''
        SELECT * FROM estserv
    ''')

    # Dados de Estações e Servidores
    dados_EstServ = pd.DataFrame(c.fetchall(), columns=["local_uf", "local_municipio", "local_nome", "id_de_rede", "instrumento_fiscaliza", "detentor", "tipo_de_estacao", "marca", "modelo", "patrimonio", "n_serie", "situacao_equipamento", "versao_fw_sw", "altura_e_configuracao_de_antenas", "ip_openvpn", "data_chave_openvpn", "id_openvpn", "observacoes", "pendencia", "acoes_a_serem_adotadas", "responsavel_na_anatel_pela_acao", "modificado", "modificado_por", "versao", "local_ref_original", "local_lat", "local_lon", "local_ref", "status", "id_bd_estserv"])

    logging.debug(dados_EstServ.info())

    time.sleep(1)

    c.execute('''
       SELECT * FROM enlacesfix
    ''')

    # Dados de planilha de Enlaces Fixos
    dados_EnlacesFix = pd.DataFrame(c.fetchall(), columns=["local_uf", "local_municipio", "local_nome", "ip_publico_da_estacao", "designacao_do_circuito", "designacao_do_roteador", "ip_publico_do_roteador", "ip_gateway", "mascara_de_rede", "contrato", "situacao_enlace", "responsavel_anatel", "referencia_solicitacao", "data_solicitacao", "referencia_homologacao", "data_homologacao", "referencia_trd", "data_trd", "observacoes", "pendencia", "acoes_a_serem_adotadas", "modificado", "modificado_por", "meses_pagos", "versao", "tecnologia_de_acesso", "referencia_suspensao", "data_suspensao", "responsavel_na_anatel_pela_acao", "id_bd_enlacesfix"])

    logging.debug(dados_EnlacesFix.info())

    try:
        # bota na tabela de log que execucao foi um sucesso
        # update if row exists, else insert
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                     "VALUES ('actionsCustomSend', '" + agora +"', 'True');"

        c.execute(sql_string)

        conn.commit()
        time.sleep(1)

    except:

        logging.warning("Ocorreu um erro ao abrir o banco de dados!")
        logging.warning("Ocorreu: " + str(sys.exc_info()[0]))

    finally:


        conn.close()

        return dados_EstServ, dados_EnlacesFix