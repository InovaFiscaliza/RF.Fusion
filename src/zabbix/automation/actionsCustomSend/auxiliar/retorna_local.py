# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

from auxiliar.arquivos_locais import MODO_DEBUG
import re

def busca_local_do_host(host_no_zabbix, dados_EstServ, dados_EnlacesFix):

    flag_desconhecido = 0
    hostachado = 0

    if "_ovpn" in host_no_zabbix:
        
        ovpn = 1
        host_no_zabbix = host_no_zabbix.replace("_ovpn", "")
        
    else:
    
        ovpn = 0

    # ESTSERV
    for index, row in dados_EstServ.iterrows():

        host_no_sharepoint = str(row["id_de_rede"])

        if host_no_zabbix in host_no_sharepoint:

            estado_host = str(row["local_uf"])
            municipio_host = str(row["local_municipio"])
            local_host = str(row["local_nome"])

            if MODO_DEBUG == 1:
                print("---------------------")
                print("Host do Zabbix encontrado no Sharepoint! (Estações e Servidores)")
                print(host_no_zabbix)
                print(estado_host)
                print(municipio_host)
                print(local_host)
                print("---------------------")
                

            hostachado = 1
            break

    if hostachado == 1:

        if MODO_DEBUG == 1:
            print("Achou endereço em EstServ!")

    else:

        if MODO_DEBUG == 1:
            print("\n")
            print("Não encontrei host " + host_no_zabbix + " na planilha de Estações e Servidores")
            print("Hora de buscar nos Enlaces Fixos!")
            print("\n")
            

        #hostachado = 0
        #quantidade_hosts_nao_encontrados = quantidade_hosts_nao_encontrados + 1

        # ENLACES FIXOS
        for index, row in dados_EnlacesFix.iterrows():

            circuito_sharepoint = str(row["designacao_do_circuito"])

            # remove "_", "-" e "/" de host_id e circuito_sharepoint
            host_no_zabbix = str(re.sub('\ |\_|\-|\/', '', host_no_zabbix)).lower()
            circuito_sharepoint = str(re.sub('\ |\_|\-|\/', '', circuito_sharepoint)).lower()

            if circuito_sharepoint in host_no_zabbix:
                # print("Achei host do Zabbix " + host_no_zabbix + " na planilha Sharepoint de Enlaces Fixos!")

                estado_host = str(row["local_uf"])
                municipio_host = str(row["local_municipio"])
                local_host = str(row["local_nome"])

                if MODO_DEBUG == 1:
                    print("---------------------")
                    print("Host do Zabbix encontrado no Sharepoint! (Enlaces Fixos)")
                    print(host_no_zabbix)
                    print(estado_host)
                    print(municipio_host)
                    print(local_host)
                    print("---------------------")
                    

                hostachado = 1
                break

        if hostachado == 1:

            hostachado = 0

        else:
            #print(host_no_zabbix + " não encontrado!")
            estado_host = ""
            municipio_host = ""
            local_host = ""
            flag_desconhecido = 1
    
    return local_host, municipio_host, estado_host, ovpn, flag_desconhecido