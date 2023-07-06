from auxiliar.acesso_estacoes import define_nome, gpsstatus, address, apps
from auxiliar.arquivos_locais import AUTH_DEFAULT, AUTH_MG, MG_TAG, MOSTRAR_PRINTS
import re

# recupera e trata daods de CGIs
def transform_acesso_cgis(dados_Zabbix, logging, conectou, nao_conectou, list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps, acessados):

    # Itera por cada host no Zabbix.
    # ------------------------------------------------------------------------

    for index_zabbix, row_zabbix in dados_Zabbix.iterrows():

        ip_zabbix = str(row_zabbix["host_ip"]).lower()
        grupos_zabbix = str(row_zabbix["grupos"]).lower()

        if MOSTRAR_PRINTS == 1:
            print("iterando por " + str(row_zabbix["nome"]))
            print(ip_zabbix + " " + grupos_zabbix)

        logging.debug("Passando por " + ip_zabbix)

        url_base = 'http://' + ip_zabbix

        # checa acesso com autenticador de MG
        if MG_TAG in grupos_zabbix:

            nome_estacao = define_nome(url_base, AUTH_MG)
            #time.sleep(1)

            # se foi possível pegar o nome, então o script vai pegar o resto dos dados
            if nome_estacao != "False":

                if MOSTRAR_PRINTS == 1:
                    print("Foi possível acessar estação! (MG)")
                logging.debug(ip_zabbix + " acessado com sucesso!")

                input_hash = str(nome_estacao)

                nome_estacao_zabbix = str(row_zabbix["host"]).lower()

                # controle do que foi acessado é independente de ser ovpn
                # se acessou 1 vez, não acessa dnv
                if "_ovpn" in nome_estacao_zabbix:

                    nome_estacao_zabbix.replace("", "_ovpn")

                if nome_estacao_zabbix in acessados:
                    # não precisa acessar
                    print(nome_estacao_zabbix + "já acessado!")

                else:
                    # acessar o resto dos dados
                    acessados.append()

                    resultado_id = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

                    list_ids.append(resultado_id)

                    conectou = conectou + 1

                    nome_estacao = re.sub(r"^b'", '', nome_estacao)
                    nome_estacao = re.sub(r"\\n'$", '', nome_estacao)

                    list_nome.append(nome_estacao)

                    lat, lon, free_mem, ip = gpsstatus(url_base, AUTH_MG)
                    list_lat.append(lat)
                    list_lon.append(lon)
                    list_free_mem.append(free_mem)
                    list_ip.append(ip)

                    #time.sleep(1)

                    mac, vpn, tuns = address(url_base, AUTH_MG)
                    list_mac.append(mac)
                    list_vpn.append(vpn)
                    list_tuns.append(tuns)

                    #time.sleep(1)

                    apps_usados = apps(url_base, AUTH_MG)

                    nome_dos_apps = ""
                    for app in apps_usados:
                        nome_dos_apps = nome_dos_apps + app["binary"] + " (" + str(app["running"]) + ")" + ", "

                    list_apps.append(nome_dos_apps[:-2])
                    print(nome_dos_apps[:-2])

                    #time.sleep(1)

            else:

                nao_conectou = nao_conectou + 1

        # checa acesso com autenticador padrão
        else:

            nome_estacao = define_nome(url_base, AUTH_DEFAULT)
            #time.sleep(1)

            if nome_estacao != "False":

                if MOSTRAR_PRINTS == 1:
                    print("Foi possível acessar estação! (Autenticação normal)")

                logging.debug(ip_zabbix + " acessado com sucesso!")

                input_hash = str(nome_estacao)

                nome_estacao_zabbix = str(row_zabbix["host"]).lower()

                # controle do que foi acessado é independente de ser ovpn
                # se acessou 1 vez, não acessa dnv
                if "_ovpn" in nome_estacao_zabbix:
                    nome_estacao_zabbix.replace("", "_ovpn")

                if nome_estacao_zabbix in acessados:
                    # não precisa acessar
                    print(nome_estacao_zabbix + "já acessado!")

                else:

                    resultado_id = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

                    list_ids.append(resultado_id)

                    conectou = conectou + 1

                    nome_estacao = re.sub(r"^b'", '', nome_estacao)
                    nome_estacao = re.sub(r"\\n'$", '', nome_estacao)

                    list_nome.append(nome_estacao)

                    lat, lon, free_mem, ip = gpsstatus(url_base, AUTH_DEFAULT)
                    list_lat.append(lat)
                    list_lon.append(lon)
                    list_free_mem.append(free_mem)
                    list_ip.append(ip)

                    #time.sleep(1)

                    mac, vpn, tuns = address(url_base, AUTH_DEFAULT)
                    list_mac.append(mac)
                    list_vpn.append(vpn)
                    list_tuns.append(tuns)

                    #time.sleep(1)

                    apps_usados = apps(url_base, AUTH_DEFAULT)

                    if apps_usados != "False":

                        nome_dos_apps = ""
                        for app in apps_usados:
                            nome_dos_apps = nome_dos_apps + app["binary"] + " (" + str(app["running"]) + ")" + ", "

                        list_apps.append(nome_dos_apps[:-2])

            else:

                nao_conectou = nao_conectou + 1

                if MOSTRAR_PRINTS == 1:
                    print("NÃO foi possível acessar estação " + ip_zabbix)

                logging.debug("NÃO foi possível acessar estação " + ip_zabbix)

    if MOSTRAR_PRINTS == 1:

        print("\n\n--------------------------------------")
        print("Achou: " + str(conectou))
        print("Não achou: " + str(nao_conectou))
        print("--------------------------------------\n\n")

    logging.debug("Achou: " + str(conectou))
    logging.debug("Não achou: " + str(nao_conectou))

    return conectou, nao_conectou, list_ids, list_nome, list_lat, list_lon, list_free_mem, list_ip, list_mac, list_vpn, list_tuns, list_apps, acessados
