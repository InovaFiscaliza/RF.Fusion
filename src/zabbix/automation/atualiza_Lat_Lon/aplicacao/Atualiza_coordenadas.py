from auxiliar.arquivos_locais import MODO_DEBUG
from geopy.geocoders import Nominatim
import time
import pandas as pd


def coordenadas(logging, zapi, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_Zabbix, dados_EnlacesFix, dados_EstServ, dados_CGIs, dados_RelacaoLocais, dados_Locais, minhaMensagem):

    n_bateu = 0
    hosts_nao_correspondentes = []
    hosts_nao_correspondentes.append(["Nome Host", "Host ID"])
    qtd_hosts_atualizados = 0


    if MODO_DEBUG == 1:
        print("Atualizando Latitudes e Longitudes de todos os hosts")

    logging.debug("Atualizando Latitudes e Longitudes de todos os hosts")

    for index_dados_Zabbix, row_dados_Zabbix in dados_Zabbix.iterrows():

        if MODO_DEBUG == 1:
            print("----------------------")

        id_bd_cgi = "0000"

        host_id = str(row_dados_Zabbix["host_id"])
        id_bd_zabbix = str(row_dados_Zabbix["id_bd_zabbix"])

        if MODO_DEBUG == 1:
            print(str(row_dados_Zabbix["host"]))

        logging.debug("Iterando por " + host_id)

        id_bd_estserv = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_bd_zabbix, ["presente_estserv"]].values[0][0])

        if MODO_DEBUG == 1:
            print("Id BD EstServ " + id_bd_estserv)

        # É estserv
        # tenta ver se tem id de CGI. 
        if id_bd_estserv != "0000":

            try:
                id_bd_cgi = str(dados_CGIsBD_Relacao.loc[dados_CGIsBD_Relacao["presente_estserv"] == id_bd_estserv, ["presente_cgis"]].values[0][0])

            except:
                id_bd_cgi = "0000"

            if MODO_DEBUG == 1:
                print("Id BD CGI " + id_bd_cgi)

        # Enlace!
        else:

            id_bd_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_bd_zabbix, ["presente_enlacesfix"]].values[0][0])

            if id_bd_enlace != "0000":

                try:
                    id_local_enlace = str(dados_RelacaoLocais.loc[dados_RelacaoLocais["presente_enlacesfix"] == id_bd_enlace, ["presente_locais"]].values[0][0])
                except:
                    id_local_enlace = "0000"

                nome_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_bd_enlace, ["local_nome"]].values[0][0])
                municipio = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_bd_enlace, ["local_municipio"]].values[0][0])
                uf_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_bd_enlace, ["local_uf"]].values[0][0])


                # tag

                print(id_bd_enlace)
                print(id_local_enlace)

                if id_local_enlace != "0000":

                    try:
                        lat_enlace = str(dados_Locais.loc[dados_Locais["id_bd_locais"] == int(id_local_enlace), ["lat"]].values[0][0])
                        lon_enlace =  str(dados_Locais.loc[dados_Locais["id_bd_locais"] == int(id_local_enlace), ["lon"]].values[0][0])
                    except:
                        lat_enlace = ""
                        lon_enlace = ""

                # para atualizar o nome dos enlaces periódicamente!

                if MODO_DEBUG == 1:
                    print("hostid -> " + str(host_id))
                    print(nome_enlace)
                    print("lat -> " + str(lat_enlace))
                    print("lon -> " + str(lon_enlace))

                logging.debug("Enlace: " + nome_enlace)

                try:
    
                    result = zapi.host.update({
                    # host no zabbix!
                    "hostid": host_id,
                    # dados a serem atualizados!
                    "inventory": {
                        "site_city": uf_enlace + " - " + municipio,
                        "site_state": uf_enlace,
                        "location_lat": str(lat_enlace),
                        "location_lon": str(lon_enlace)
                        }
                    })
                
                    time.sleep(1)

                    result = zapi.host.update({
                        # host no zabbix!
                        "hostid": host_id,
                        "tags":[{
                            "tag": "Local",
                            "value": nome_enlace
                        },
                        {
                            "tag": "Município",
                            "value": municipio
                        }]
                    })
    
                    if MODO_DEBUG == 1:
                        print("update campos de Host ID " + host_id)
                        #print(result)
    
                except:
    
                    if MODO_DEBUG == 1:
                        print("Erro ao mudar 'name' para host ID - " + host_id)
    
                finally:
    
                    time.sleep(1)
                

        # caso tenha sido recuperado a estação tanto em EstServ como em CGIs...
        if id_bd_cgi != "0000" and id_bd_estserv != "0000":

            # local
            local_uf_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_uf"]].values[0][0])
            local_municipio_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_municipio"]].values[0][0])

            local_nome_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_nome"]].values[0][0])

            
            # recupera lat e lon tanto de uma fonte como outra, e compara

            lat_cgi = str(dados_CGIs.loc[dados_CGIs["id_bd_cgis"] == id_bd_cgi, ["lat"]].values[0][0])
            lon_cgi = str(dados_CGIs.loc[dados_CGIs["id_bd_cgis"] == id_bd_cgi, ["lon"]].values[0][0])

            lat_estserv = str(
                dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_lat"]].values[0][0])[0:12]
            lon_estserv = str(
                dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_lon"]].values[0][0])[0:12]

            if (lat_cgi[0:6] == lat_estserv[0:6]) and (lon_cgi[0:6] == lon_estserv[0:6]):

                if MODO_DEBUG == 1:
                    print("Coordenadas batem!")

            else:

                n_bateu = n_bateu + 1

                if lat_cgi != "False" and lon_cgi != "False":
                    hosts_nao_correspondentes.append([str(row_dados_Zabbix["host"]), int(host_id)])

                if MODO_DEBUG == 1:
                    print("Problema nas coordenadas")

            if MODO_DEBUG == 1:
                print("Coordenadas CGIs")
                print(lat_cgi)
                print(lon_cgi)
                print("Coordenadas EstServ")
                print(lat_estserv)
                print(lon_estserv)
                print("host_id - " + str(host_id))

            logging.debug("Coordenadas CGIs")
            logging.debug(lat_cgi)
            logging.debug(lon_cgi)
            logging.debug("Coordenadas EstServ")
            logging.debug(lat_estserv)
            logging.debug(lon_estserv)
            logging.debug("host_id - " + str(host_id))

            if lat_cgi != "False" and lon_cgi != "False":
                
                # pega o endereço
                geolocator = Nominatim(user_agent="coordinateconverter")
                endereco = str(lat_cgi) + ", " + str(lon_cgi)
                location = geolocator.reverse(endereco)
                estado_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_uf"]].values[0][0]).lower()
                estado_recuperado = str(location.raw["address"]["ISO3166-2-lvl4"]).lower()

                if MODO_DEBUG == 1:
                    print(location.address)

            else:
                
                # pega o endereço
                geolocator = Nominatim(user_agent="coordinateconverter")
                endereco = str(lat_estserv) + ", " + str(lon_estserv)
                location = geolocator.reverse(endereco)
                estado_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_uf"]].values[0][0]).lower()

                estado_recuperado = ""

                if MODO_DEBUG == 1:
                    print(location.address)

            # recupera estado cadastrado no sharepoint e compara com o novo
            estado_sharepoint = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_uf"]].values[0][0]).lower()

            if estado_recuperado != "":

                if str(estado_sharepoint) == str(estado_recuperado[3:5]):
                    # estados batem com o que foi recuperado pela CGI!
                    if MODO_DEBUG == 1:
                        print("Coordenada recuperada da CGI bate com o que foi cadastrado no Sharepoint!")

                else:

                    # avisa que o estado mudou!
                    if MODO_DEBUG == 1:
                        print("Os estados recuperados batem (entre CGI e EstServ) - hostid > " + str(host_id))

                    # AVISA NO TEAMS MUDANÇA!

                    try:

                        minhaMensagem.title("🧾 Mudança de UF de um Host! 🧾")
                        minhaMensagem.text("Há uma disparidade entre UF de um host!\n\nHostID - " + str(host_id) + "\n\nNome no Zabbix- " + str(row_dados_Zabbix["nome"]) + "\n\nUF recuperado pela coordenada da CGI - " + str(estado_recuperado) + "\n\nUF recuperado do Sharepoint - " + str(estado_estserv))
                        minhaMensagem.send()


                    except:

                        if MODO_DEBUG == 1:
                            print("Erro ao avisar que mudança de estado de um host ocorreu - hostid > " + str(host_id))


                    finally:

                        time.sleep(1)

                        if MODO_DEBUG == 1:
                            print("mensagem referente a mudança de UF do host enviada - hostid > " + str(host_id))

            try:

                result = zapi.host.update({
                    # host no zabbix!
                    "hostid": host_id,
                    # dados a serem atualizados!
                    "inventory": {
                        "site_city": local_uf_estserv + " - " + local_municipio_estserv,
                        "site_state": local_uf_estserv,
                        "location_lat": str(lat_cgi),
                        "location_lon": str(lon_cgi),
                        "notes": "Dados recuperados de Banco de Dados em tabela de CGIs!\n\n" + str(location.address)
                    }
                })
                
                time.sleep(1)

                result = zapi.host.update({
                    # host no zabbix!
                    "hostid": host_id,
                    "tags":[{
                        "tag": "Local",
                        "value": local_nome_estserv
                    },
                    {
                        "tag": "Município",
                        "value": local_municipio_estserv
                    }]
                })

                time.sleep(1)
                
                if MODO_DEBUG == 1:
                    print(result)
                del result

            except:
                if MODO_DEBUG == 1:
                    print("Erro ao mudar 'Lat' e 'Lon' para host " + host_id)

            finally:

                time.sleep(1)

            qtd_hosts_atualizados = qtd_hosts_atualizados + 1

        # caso tenha sido recuperada a estação apenas em EstServ
        elif id_bd_cgi == "0000" and id_bd_estserv != "0000":

            # local
            local_nome_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_nome"]].values[0][0])
            local_uf_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_uf"]].values[0][0])
            local_municipio_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_municipio"]].values[0][0])

            # recupera lat e lon de EstServ, e atualiza Zabbix
            lat_estserv = str(
                dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_lat"]].values[0][0])[0:12]
            lon_estserv = str(
                dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_bd_estserv, ["local_lon"]].values[0][0])[0:12]

            if MODO_DEBUG == 1:
                print(lat_estserv)
                print(lon_estserv)
                print("host_id - " + str(host_id))

            logging.debug("Coordenadas EstServ")
            logging.debug(lat_estserv)
            logging.debug(lon_estserv)
            logging.debug("host_id - " + str(host_id))


            if lat_estserv != "" or lon_estserv != "":

                # pega o endereço
                geolocator = Nominatim(user_agent="coordinateconverter")
                endereco = str(lat_estserv) + ", " + str(lon_estserv)
                location = geolocator.reverse(endereco)
                print(location.address)

            # atualiza o zabbix!

            try:
                
                result = zapi.host.update({
                    # host no zabbix!
                    "hostid": host_id,
                    # dados a serem atualizados!
                    "inventory": {
                        "site_city": local_uf_estserv + " - " + local_municipio_estserv,
                        "site_state": local_uf_estserv,
                        "location_lat": str(lat_estserv),
                        "location_lon": str(lon_estserv),
                        "notes": "Dados recuperados de Banco de Dados em tabela de Estações e Servidores!\n\n" + str(location.address)
                    }
                })
                
                time.sleep(1)
                
                result = zapi.host.update({
                    # host no zabbix!
                    "hostid": host_id,
                    "tags":[{
                        "tag": "Local",
                        "value": local_nome_estserv
                    },
                    {
                        "tag": "Município",
                        "value": local_municipio_estserv
                    }]
                })
                
                time.sleep(1)

                if MODO_DEBUG == 1:
                    print(result)
                del result

            except:

                print("Erro ao mudar 'Lat' e 'Lon' para host " + host_id)

            finally:

                time.sleep(1)

            qtd_hosts_atualizados = qtd_hosts_atualizados + 1

    if MODO_DEBUG == 1:
        print("-----------")
        print("Hosts atualizados - " + str(qtd_hosts_atualizados))
        print("Hosts que as coordenadas entre CGI e Sharepoint não bateram - " + str(hosts_nao_correspondentes))
        print("-----------")

    return qtd_hosts_atualizados, hosts_nao_correspondentes, n_bateu