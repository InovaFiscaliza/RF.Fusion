
import logging
import sqlite3
import pandas as pd
import sys
import os
import time

from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG, WEBHOOK, TESTES

def load(logging):

    conn = sqlite3.connect(os.path.dirname(__file__) + "/../" + HOST_BD)
    c = conn.cursor()

    try:

        # --------------------------------------------

        c.execute('''
                        SELECT * FROM estserv
                ''')

        # Dados de Estações e Servidores
        dados_EstServ = pd.DataFrame(c.fetchall(), columns=["local_uf", "local_municipio", "local_nome", "id_de_rede",
                                                            "instrumento_fiscaliza", "detentor", "tipo_de_estacao",
                                                            "marca", "modelo", "patrimonio", "n_serie",
                                                            "situacao_equipamento", "versao_fw_sw",
                                                            "altura_e_configuracao_de_antenas", "ip_openvpn",
                                                            "data_chave_openvpn", "id_openvpn", "observacoes",
                                                            "pendencia", "acoes_a_serem_adotadas",
                                                            "responsavel_na_anatel_pela_acao", "modificado",
                                                            "modificado_por", "versao", "local_ref_original",
                                                            "local_lat", "local_lon", "local_ref", "status",
                                                            "id_bd_estserv"])

        if MODO_DEBUG == 1:
            print(dados_EstServ.info())
        logging.debug(dados_EstServ.info())

        time.sleep(1)

        # --------------------------------------------

        c.execute('''
                        SELECT * FROM enlacesfix
                ''')

        # Dados de planilha de Enlaces Fixos
        dados_EnlacesFix = pd.DataFrame(c.fetchall(),
                                        columns=["local_uf", "local_municipio", "local_nome", "ip_publico_da_estacao",
                                                 "designacao_do_circuito", "designacao_do_roteador",
                                                 "ip_publico_do_roteador", "ip_gateway", "mascara_de_rede", "contrato",
                                                 "situacao_enlace", "responsavel_anatel", "referencia_solicitacao",
                                                 "data_solicitacao", "referencia_homologacao", "data_homologacao",
                                                 "referencia_trd", "data_trd", "observacoes", "pendencia",
                                                 "acoes_a_serem_adotadas", "modificado", "modificado_por",
                                                 "meses_pagos", "versao", "tecnologia_de_acesso",
                                                 "referencia_suspensao", "data_suspensao",
                                                 "responsavel_na_anatel_pela_acao", "id_bd_enlacesfix"])

        if MODO_DEBUG == 1:
            print(dados_EnlacesFix.info())
        logging.debug(dados_EnlacesFix.info())

        time.sleep(1)

        # --------------------------------------------

        c.execute('''
                        SELECT * FROM dados_zabbix
                ''')

        # Dados de planilha de Enlaces Fixos
        dados_Zabbix = pd.DataFrame(c.fetchall(),
                                    columns=["id_bd_zabbix", "nome", "host", "disponivel_no_zabbix", "host_id",
                                             "host_ip", "grupos", "conexao_OVPN", "erros_ultimas_24h",
                                             "templates_vinculados", "ultimo_problema_ocorrido", "qtd_problemas_graves",
                                             "ultimo_ocorrido"])

        if MODO_DEBUG == 1:
            print(dados_Zabbix.info())
        logging.debug(dados_Zabbix.info())

        time.sleep(1)

        # --------------------------------------------

        c.execute(''' 
                        SELECT * FROM cgis
                ''')

        # dados de planilhas de CGIs
        dados_CGIs = pd.DataFrame(c.fetchall(),
                                  columns=['id_bd_cgis', 'nome', 'lat', 'lon', 'free_mem', 'ip', 'mac', 'vpn', 'tuns',
                                           'apps'])

        if MODO_DEBUG == 1:
            print(dados_CGIs.info())
        logging.debug(dados_CGIs.info())

        time.sleep(1)

        # --------------------------------------------

        c.execute(''' 
                                SELECT * FROM relacao_cgis
                        ''')

        # dados de planilhas de CGIs
        relacao_CGIs = pd.DataFrame(c.fetchall(),
                                  columns=['id_relacao_cgi_estserv', 'presente_cgis', 'presente_estserv'])

        if MODO_DEBUG == 1:
            print(relacao_CGIs.info())
        logging.debug(relacao_CGIs.info())

        time.sleep(1)

        # --------------------------------------------

        c.execute(''' 
                                        SELECT * FROM relacao_zabbix
                                ''')

        # dados de planilhas de CGIs
        relacao_Zabbix = pd.DataFrame(c.fetchall(),
                                    columns=['id_bd_relacao', 'id_bd_zabbix', 'presente_estserv', 'presente_enlacesfix'])

        if MODO_DEBUG == 1:
            print(relacao_Zabbix.info())
        logging.debug(relacao_Zabbix.info())

        time.sleep(1)

        # --------------------------------------------

    except:

        if MODO_DEBUG == 1:
            print("Ocorreu um erro ao abrir o banco de dados!")
            print("Ocorreu: " + str(sys.exc_info()[0]))
        logging.debug("Ocorreu um erro ao abrir o banco de dados!")
        logging.debug("Ocorreu: " + str(sys.exc_info()[0]))

    finally:

        conn.commit()
        conn.close()

        return dados_EstServ, dados_EnlacesFix, dados_Zabbix, dados_CGIs, relacao_CGIs, relacao_Zabbix
