from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG
import pandas as pd
import sqlite3
import time
import logging
import os
import sys

def load():

    conn = sqlite3.connect(os.path.dirname(__file__) + "/../" + HOST_BD)
    c = conn.cursor()

    try:

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

        # Dados de relacionamento EstServ -> CGIs
        c.execute('''
                                    SELECT * FROM relacao_cgis
                                ''')

        dados_CGIsBD_Relacao = pd.DataFrame(c.fetchall(),
                                            columns=["id_relacao_cgi_estserv", "presente_cgis", "presente_estserv"])

        if MODO_DEBUG == 1:
            print(dados_CGIsBD_Relacao.info())

        logging.debug(dados_CGIsBD_Relacao.info())

        time.sleep(1)

        # Dados de relacionamento Zabbix -> Planilhas Sharepoint
        c.execute('''
                                    SELECT * FROM relacao_zabbix
                                ''')

        dados_ZabbixBD_Relacao = pd.DataFrame(c.fetchall(),
                                              columns=["id_bd_relacao", "id_bd_zabbix", "presente_estserv",
                                                       "presente_enlacesfix"])

        if MODO_DEBUG == 1:
            print(dados_ZabbixBD_Relacao.info())

        logging.debug(dados_ZabbixBD_Relacao.info())

        time.sleep(1)

        # Dados de relacionamento Locais -> Planilhas Sharepoint
        c.execute('''
                                            SELECT * FROM relacao_locais
                                        ''')

        dados_RelacaoLocais = pd.DataFrame(c.fetchall(),
                                              columns=["id_relacao_local", "presente_locais", "presente_estserv", "presente_enlacesfix", "local_correspondente"])

        if MODO_DEBUG == 1:
            print(dados_RelacaoLocais.info())

        logging.debug(dados_RelacaoLocais.info())

        time.sleep(1)

        c.execute('''
                                SELECT * FROM locais
                        ''')

        # Dados de planilhas de Locais
        dados_Locais = pd.DataFrame(c.fetchall(),
                                    columns=['uf', 'municipio', 'referencia', 'bairro', 'logradouro', 'numero',
                                             'complemento', 'cep', 'atendimento', 'responsavel_local', 'situacao_local',
                                             'contrato_cessao', 'contrato_cessao_instrumento',
                                             'contrato_cessao_situacao', 'responsavel_anatel', 'lat', 'lon',
                                             'pendencia', 'modificado', 'modificado_por', 'versao', 'status_aprovacao',
                                             'observacoes', 'acoes_a_serem_adotadas', 'responsavel_acao_na_anatel',
                                             'id_bd_locais'])

        if MODO_DEBUG == 1:
            print(dados_Locais.info())
        logging.debug(dados_Locais.info())

        time.sleep(1)

    except:

        if MODO_DEBUG == 1:
            print("Ocorreu um erro ao abrir o banco de dados!")
            print("Ocorreu: " + str(sys.exc_info()[0]))
        logging.debug("Ocorreu um erro ao abrir o banco de dados!")
        logging.debug("Ocorreu: " + str(sys.exc_info()[0]))

    finally:

        conn.commit()
        conn.close()

        return dados_EstServ, dados_EnlacesFix, dados_Zabbix, dados_CGIs, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_RelacaoLocais, dados_Locais
