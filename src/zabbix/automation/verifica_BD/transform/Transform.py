
from difflib import SequenceMatcher
from auxiliar.arquivos_locais import HOST_BD, MODO_DEBUG, WEBHOOK, TESTES
import logging
import pandas as pd
import time
import re

# ZABBIX -> SHAREPOINT
# Mapeia entre Zabbix e Sharepoint
# ------------------------------------------------------------------------

def transform_Zabbix_Sharepoint(dados_Zabbix, dados_EnlacesFix, dados_EstServ, logging):

    if MODO_DEBUG == 1:
        print("Mapeando dados entre Zabbix e Sharepoint!")

    # criando essas entradas no dataframe do zabbix para depois recueprar o que não estã sendo monitorado
    qtd_hosts_achados = 0

    dados_Zabbix["achado_sharepoint"] = "0"

    dados_Zabbix["presente_estserv"] = "0000"

    dados_Zabbix["presente_enlacesfix"] = "0000"

    dados_EnlacesFix["achado_zabbix"] = ""
    dados_EnlacesFix.assign(achado_zabbix="0")

    dados_EstServ["achado_zabbix"] = ""
    dados_EstServ.assign(achado_zabbix="0")

    for index_zabbix, row_zabbix in dados_Zabbix.iterrows():

        # achar o host do zabbix no sharepoint

        host_zabbix = str(row_zabbix["host"]).lower()

        if MODO_DEBUG == 1:
            print("Tentando achar " + host_zabbix + " no Sharepoint.")
        logging.debug("Tentando achar " + host_zabbix + " no Sharepoint.")

        for index_enlace, row_enlace in dados_EnlacesFix.iterrows():

            host_achado = 0

            host_enlace = str(row_enlace["designacao_do_circuito"]).lower()

            if host_enlace in host_zabbix:

                host_achado = 1
                row_zabbix["achado_sharepoint"] = 1
                row_zabbix["presente_enlacesfix"] = str(row_enlace["id_bd_enlacesfix"])
                row_zabbix["presente_estserv"] = "0000"

                # para controle interno, depois pode-se tratar o que faltou
                row_enlace["achado_zabbix"] = 1

                qtd_hosts_achados = qtd_hosts_achados + 1

                if MODO_DEBUG == 1:
                    print(host_enlace + " ACHADO")
                logging.debug(host_enlace + " ACHADO")

                break

            else:

                host_achado = 0

        if host_achado == 0:

            if MODO_DEBUG == 1:
                print(host_zabbix + "Não estava na Planilha de Enlaces Fixos!")
                print("Buscando em Estações e Servidores!")

            logging.debug(host_zabbix + "Não estava na Planilha de Enlaces Fixos!")
            logging.debug("Buscando em Estações e Servidores!")

            for index_estserv, row_estserv in dados_EstServ.iterrows():

                host_achado = 0

                host_estserv = str(row_estserv["id_de_rede"]).lower()

                if host_estserv in host_zabbix:

                    host_achado = 1

                    row_zabbix["achado_sharepoint"] = 1
                    row_zabbix["presente_enlacesfix"] = "0000"
                    row_zabbix["presente_estserv"] = str(row_estserv["id_bd_estserv"])

                    row_estserv["achado_zabbix"] = 1

                    # para controle interno, depois pode-se tratar o que faltou
                    qtd_hosts_achados = qtd_hosts_achados + 1

                    if MODO_DEBUG == 1:
                        print(host_estserv + " ACHADO")
                    logging.debug(host_estserv + " ACHADO")

                    break

                else:

                    host_achado = 0

            if host_achado == 1:

                if MODO_DEBUG == 1:
                    print(host_zabbix + " achado em Estações e Servidores!")
                logging.debug(host_zabbix + " achado em Estações e Servidores!")

            else:

                host_achado = 0

                if MODO_DEBUG == 1:
                    print("Não foi achado " + host_zabbix + " no Sharepoint!")
                logging.debug("Não foi achado " + host_zabbix + " no Sharepoint!")

                row_zabbix["achado_sharepoint"] = 0

        else:

            if MODO_DEBUG == 1:
                print(host_zabbix + " achado em Enlaces Fixos!")
            logging.debug(host_zabbix + " achado em Enlaces Fixos!")

    return dados_Zabbix, dados_EnlacesFix, dados_EstServ, qtd_hosts_achados

# Batendo enlaces fixos com Zabbix
# ------------------------------------------------------------------------

def transform_Zabbix_Enlaces(dados_Zabbix, dados_EnlacesFix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix):

    for index_enlace, row_enlace in dados_EnlacesFix.iterrows():

        host_achado = 0

        host_enlace = str(row_enlace["designacao_do_circuito"]).lower()
        host_enlace = host_enlace.replace("/", "")
        host_enlace = host_enlace.replace(" ", "")
        host_enlace = host_enlace.replace("-", "")

        for index_zabbix, row_zabbix in dados_Zabbix.iterrows():

            # achar o host do zabbix no sharepoint

            host_zabbix = str(row_zabbix["host"]).lower()
            host_zabbix = host_zabbix.replace("/", "")
            host_zabbix = host_zabbix.replace(" ", "")
            host_zabbix = host_zabbix.replace("-", "")

            if host_enlace in host_zabbix:

                host_achado = 1
                break

            else:

                host_achado = 0

        if (host_achado == 1):

            if MODO_DEBUG == 1:
                print(host_enlace + " achado no Zabbix!")
            logging.debug(host_enlace + " achado no Zabbix!")

        elif row_enlace["achado_zabbix"] == 0:

            if MODO_DEBUG == 1:
                print(host_enlace + " NÃO foi achado no Zabbix!")
            logging.debug(host_enlace + " NÃO foi achado no Zabbix!")

            faltou_no_zabbix = faltou_no_zabbix + 1

            # fazer id no BD
            input_hash = str(row_enlace["id_bd_enlacesfix"]) + "0000"

            id_relacao = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

            append_ids_relacao.append(id_relacao)
            append_idsZabbix.append("0000")
            append_presente_estserv.append("0000")
            append_presente_enlacesfix.append(row_enlace["id_bd_enlacesfix"])

    return dados_Zabbix, dados_EnlacesFix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix

# Batendo estações e servidores com Zabbix
# ------------------------------------------------------------------------

def transform_Zabbix_EstServ(dados_EstServ, dados_Zabbix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix):

    for index_estserv, row_estserv in dados_EstServ.iterrows():

        host_achado = 0

        host_estserv = str(row_estserv["id_de_rede"]).lower()

        for index_zabbix, row_zabbix in dados_Zabbix.iterrows():

            # achar o host do zabbix no sharepoint

            host_zabbix = str(row_zabbix["host"]).lower()

            if host_estserv in host_zabbix:

                host_achado = 1
                break

            else:

                host_achado = 0

        if (host_achado == 1):

            if MODO_DEBUG == 1:
                print(host_estserv + " achado no Zabbix!")
            logging.debug(host_estserv + " achado no Zabbix!")

        elif row_estserv["achado_zabbix"] == 0:

            if MODO_DEBUG == 1:
                print(host_estserv + " NÃO foi achado no Zabbix!")
            logging.debug(host_estserv + " NÃO foi achado no Zabbix!")

            faltou_no_zabbix = faltou_no_zabbix + 1

            # fazer id no BD
            input_hash = str(row_estserv["id_bd_estserv"]) + "0000"

            id_relacao = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

            append_ids_relacao.append(id_relacao)
            append_idsZabbix.append("0000")
            append_presente_estserv.append(row_estserv["id_bd_estserv"])
            append_presente_enlacesfix.append("0000")

    return dados_EstServ, dados_Zabbix, append_ids_relacao, append_idsZabbix, append_presente_estserv, append_presente_enlacesfix, faltou_no_zabbix

# Mapeando dados entre Estações e Locais
# ------------------------------------------------------------------------
# intenção é criar tabela relacao_locais

def transform_EstServ_Locais(dados_Locais, dados_EnlacesFix, dados_EstServ, locais_batem, ids_locais, ids_estserv, ids_enlaces, ids_relacao_local):

    for index_locais, row_locais in dados_Locais.iterrows():

        estserv_achado_local = 0
        enlace_achado_local = 0

        referencia_local = str(row_locais["referencia"]).lower()
        referencia_local_municipio = str(row_locais["municipio"]).lower()

        # para cada entrada de local, ver a equivalência na planilha de estações
        for index_estserv, row_estserv in dados_EstServ.iterrows():

            referencia_estacao = str(row_estserv["local_nome"]).lower()
            referencia_estacao_municipio = str(row_estserv["local_municipio"]).lower()

            # bate nome e municipio
            if (referencia_estacao_municipio in referencia_local_municipio):

                if (SequenceMatcher(None, referencia_estacao, referencia_local).ratio() > 0.9):

                    estserv_achado_local = 1

                    row_locais["presente_estserv"] = row_estserv["id_bd_estserv"]

                    estacao_correspondente = str(row_estserv["id_bd_estserv"])

                    break

        # para cada entrada de local, ver a equivalência na planilha de enlaces
        for index_enlace, row_enlace in dados_EnlacesFix.iterrows():

            referencia_enlace = str(row_enlace["local_nome"]).lower()
            referencia_enlace_municipio = str(row_enlace["local_municipio"]).lower()

            # bate nome e municipio
            if (referencia_enlace_municipio in referencia_local_municipio):

                if (SequenceMatcher(None, referencia_enlace, referencia_local).ratio() > 0.9):

                    if MODO_DEBUG == 1:
                        print("Sequence matcher aceito -> " + str((SequenceMatcher(None, referencia_enlace, referencia_local).ratio())))

                    enlace_achado_local = 1

                    enlace_correspondente = str(row_enlace["id_bd_enlacesfix"])

                    break

        if enlace_achado_local == 0:

            enlace_correspondente = "0000"


        if estserv_achado_local == 0:

            estacao_correspondente = "0000"
            row_locais["presente_estserv"] = "0000"

        # fazer id no BD
        input_hash = str(row_locais["id_bd_locais"]) + str(row_locais["referencia"])

        id_relacao_local = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

        # atualiza listas
        ids_relacao_local.append(id_relacao_local)
        locais_batem.append("Local achado!")
        ids_locais.append(row_locais["id_bd_locais"])
        ids_estserv.append(estacao_correspondente)
        ids_enlaces.append(enlace_correspondente)

        estserv_achado_local = 0
        enlace_achado_local = 0

    return dados_Locais, dados_EnlacesFix, dados_EstServ, locais_batem, ids_locais, ids_estserv, ids_enlaces, ids_relacao_local

# Mapeando dados entre CGIs e Estações
# ------------------------------------------------------------------------

def transform_EstServ_CGIs(dados_CGIs, dados_EstServ, ids_cgis, ids_estserv_achados, id_relacao_cgi_estserv, minhaMensagem):

    # para cada entrada de local, ver a equivalência na parte de cgis
    for index_cgi, row_cgi in dados_CGIs.iterrows():

        cgi_achado_local = 0
        nome_cgi = str(row_cgi["nome"]).lower()

        try:

            # regex para limpar dado, pegar apenas parte da string no formato "rfeyexxyyyy"
            rfeye_cgi = re.findall('rfeye+[\d].....', nome_cgi).pop(0)

        except AttributeError:

            # rfeye não encontrado no padrão passado
            rfeye_cgi = 'rfeye000000'
            pass

        for index_estserv, row_estserv in dados_EstServ.iterrows():

            id_rede = str(row_estserv['id_de_rede']).lower()

            if rfeye_cgi == id_rede:

                ids_cgis.append(row_cgi["id_bd_cgis"])
                ids_estserv_achados.append(row_estserv['id_bd_estserv'])

                cgi_achado_local = 1


                if MODO_DEBUG == 1:
                    print("Página CGI de " + rfeye_cgi + " achada em lista de Estações e servidores")

                logging.debug("Página CGI de " + rfeye_cgi + " achada em lista de Estações e servidores")

                # fazer id no BD
                input_hash = str(row_cgi["id_bd_cgis"]) + str(row_estserv["id_bd_estserv"])

                id_relacao_cgi = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

                id_relacao_cgi_estserv.append(id_relacao_cgi)

        if cgi_achado_local == 0:

            if MODO_DEBUG == 1:
                print("Página CGI de " + rfeye_cgi + " NÃO achada em lista de Estações e servidores")

            logging.debug("Página CGI de " + rfeye_cgi + " NÃO achada em lista de Estações e servidores")

            ids_cgis.append(row_cgi["id_bd_cgis"])
            ids_estserv_achados.append("0000")

            # fazer id no BD
            input_hash = str(row_cgi["id_bd_cgis"]) + "0000"

            id_relacao_cgi = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

            id_relacao_cgi_estserv.append(id_relacao_cgi)

            # Manda mensagem notificando isso!
            minhaMensagem.title(" 😨 Página CGI de " + rfeye_cgi + " NÃO achada em lista de Estações e Servidores")
            minhaMensagem.text("\n ❌ Não foi encontrado na planilha Estações e Servidores nenhum ID de Rede " + rfeye_cgi + "\n\n 🗺 Endereço recuperado com GeoPy: " + str(row_cgi["endereco"]) + "\n\n-------------------------------\n\n🕰 " + str(datetime.now()))
            minhaMensagem.send()

            time.sleep(1)

            if MODO_DEBUG == 1:
                minhaMensagem.printme()

    return dados_CGIs, dados_EstServ, ids_cgis, ids_estserv_achados, id_relacao_cgi_estserv, minhaMensagem