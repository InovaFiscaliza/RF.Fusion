from auxiliar.arquivos_locais import MODO_DEBUG
from auxiliar.atualizaListaGrupos import atualiza_grupos
import time
import pandas as pd


def grupos_zabbix(logging, zapi, dados_CGIsBD_Relacao, dados_ZabbixBD_Relacao, dados_Zabbix, dados_EnlacesFix, dados_EstServ, dados_CGIs, dados_RelacaoLocais, dados_Locais, minhaMensagem):
    """
        Atualiza os grupos de todos os hosts no Zabbix.
        :param logging: variável de criação de arquivo de log.
        :param zapi: variável de acesso ao Zabbix.
        :param dados_CGIsBD_Relacao: equivalências entre CGIs e Estações e Servidores.
        :param dados_ZabbixBD_Relacao: equivalências entre Sharepoint e hosts no Zabbix.
        :param dados_Zabbix: dataframe de dados do Zabbix.
        :param dados_EnlacesFix: dataframe de dados de Enlace Fixo.
        :param dados_EstServ: dataframe de dados do Estações e Servidores.
        :param dados_CGIs: dataframe de dados de CGIs.
        :param dados_RelacaoLocais: dataframe de dados do Zabbix.
        :param dados_Locais: equivalências entre Sharepoint e seus Locais.
        :param minhaMensagem: variável que gerencia a minha mensagem.
        :returns: Quantidade de hosts atualizados
    """
    
    qtd_hosts = 0

    # Itera por todos os hosts do Zabbix
    for index_dados_Zabbix, row_dados_Zabbix in dados_Zabbix.iterrows():

        dados = zapi.hostgroup.get(hostids=str(row_dados_Zabbix["host_id"]))
        id_zabbix = str(row_dados_Zabbix["id_bd_zabbix"])

        if MODO_DEBUG == 1:
            print("-----")
            print(str(row_dados_Zabbix["host_id"]))

        grupos_extraidos = []
        tipo = "vazio"
        estado_estacao = ""

        if len(dados) > 0:

            for i in range(len(dados)):

                grupo = dados.pop(0)
                id_grupo = str(grupo["groupid"])

                if id_grupo == "51":
                    tipo = "enlace"

                if id_grupo == "52":
                    tipo = "estacao"

                grupos_extraidos.append({"groupid": id_grupo})

        if MODO_DEBUG == 1:
            print(tipo)

        # -----------------------------------
        # Atualiza tipo de Enlace baseado em tecnologia
        # groupid=71 -> Fibra Óptica
        # groupid=72 -> Satélite
        # groupid=73 -> Ethernet
        # groupid=74 -> Cabo Coaxial
        # groupid=75 -> Enlace Terrestre
        # groupid=78 -> xDSL
        # groupid=82 -> SMP
        # groupid=83 -> Não disponível
        # ainda pode ser um "nan"
        # -----------------------------------
        if tipo == "enlace":

            if MODO_DEBUG == 1:
                print("É um enlace! Se atualizam grupos de contrato!")

            try:

                id_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_enlacesfix"]].values[0][0])

                tecnologia_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_enlace, ["tecnologia_de_acesso"]].values[0][0]).lower()

            except:

                if MODO_DEBUG == 1:

                    print("Erro ao encontrar enlace")

                tecnologia_enlace = "erro"

            # atualiza por grupo dado o que foi recuperado do sharepoint
            if "fibra óptica" in tecnologia_enlace:

                if {"groupid": "71"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "71"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["72", "73", "74", "75", "78", "82", "83"])

            elif "satélite" in tecnologia_enlace:

                if {"groupid": "72"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "72"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "73", "74", "75", "78", "82", "83"])

            elif "ethernet" in tecnologia_enlace:

                if {"groupid": "73"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "73"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "74", "75", "78", "82", "83"])

            elif "cabo coaxial" in tecnologia_enlace:

                if {"groupid": "74"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "74"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "75", "78", "82", "83"])

            elif "enlace terrestre" in tecnologia_enlace:

                if {"groupid": "75"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "75"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "74", "78", "82", "83"])

            elif "xdsl" in tecnologia_enlace:

                if {"groupid": "78"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "78"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "74", "75", "82", "83"])
                
            elif "smp" in tecnologia_enlace:

                if {"groupid": "82"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "82"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "74", "75", "78", "83"])

            elif "não disponível" in tecnologia_enlace:

                if {"groupid": "83"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "83"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "74", "75", "78", "82"])

            elif tecnologia_enlace == "nan":
                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["71", "72", "73", "74", "75", "78", "82", "83"])

        # -----------------------------------
        # Atualiza tipo de Enlace baseado em contrato
        # groupid=16 -> Enlaces Oi
        # groupid=15 -> Enlaces Claro
        # groupid=67 -> Claro Fixo (Contrato 161/2018)
        # groupid=68 -> Oi Fixo (Contrato 163/2018)
        # groupid=70 -> Sem Suporte Formal
        # groupid=69 -> UD da Anatel
        # -----------------------------------

        if tipo == "enlace":

            if MODO_DEBUG == 1:
                print("É um enlace! Se atualizam grupos de contrato!")


            try:

                id_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_enlacesfix"]].values[0][0])

                contrato_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_enlace, ["contrato"]].values[0][0]).lower()

            except:

                if MODO_DEBUG == 1:

                    print("Erro ao encontrar enlace")
                contrato_enlace = "erro"

            # atualiza por grupo dado o que foi recuperado do sharepoint
            if "claro" in contrato_enlace:

                if {"groupid": "67"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "67"})
                if {"groupid": "15"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "15"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["16", "68", "69", "70"])

            elif "oi" in contrato_enlace:

                if {"groupid": "16"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "16"})
                if {"groupid": "68"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "68"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["15", "67", "69", "70"])

            elif "rede ou local cedido sem suporte formal" in contrato_enlace:

                if {"groupid": "70"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "70"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["16", "68", "15", "67", "69"])

            elif "unidade administrativa da anatel" in contrato_enlace:

                if {"groupid": "69"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "69"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["16", "68", "15", "67", "70"])

        # -----------------------------------
        # Atualiza grupo de RFeye
        # groupid=66 -> RFeye
        # groupid=17 -> RFEye Nodes
        # -----------------------------------

        nome_rfeye = str(row_dados_Zabbix["host"])

        # atualiza grupo de rfeye se o host tem rfeye no nome
        if "rfeye" in nome_rfeye:

            if ({"groupid", "17"} in grupos_extraidos) and ({"groupid", "66"} in grupos_extraidos):
                # os dois grupos foram designados! RFeye Node e RFeye
                if MODO_DEBUG == 1:
                    print("Esse host já é identificado como RFEye! Hostid -> " + str(row_dados_Zabbix["host_id"]))

            elif ({"groupid", "17"} in grupos_extraidos):

                if MODO_DEBUG == 1:
                    print("Falta adicionar grupo 'RFeye'! Hostid -> " + str(row_dados_Zabbix["host_id"]))
                grupos_extraidos.append({"groupid": "66"})

            elif ({"groupid", "66"} in grupos_extraidos):

                if MODO_DEBUG == 1:
                    print("Falta adicionar grupo 'RFeye Nodes'! Hostid -> " + str(row_dados_Zabbix["host_id"]))
                grupos_extraidos.append({"groupid": "17"})

            else:

                if MODO_DEBUG == 1:
                    print("Falta adicionar ambos os grupos referentes a RFEye: 'RFeye Nodes' e 'RFEye'! Hostid -> " + str(row_dados_Zabbix["host_id"]))

                grupos_extraidos.append({"groupid": "17"})
                grupos_extraidos.append({"groupid": "66"})

        # se host não tem rfeye no nome, pode tirar grupos de rfeye
        else:   

            try:

                if ({"groupid", "17"} in grupos_extraidos):

                    grupos_extraidos = grupos_extraidos.remove({"groupid", "17"})

                if ({"groupid", "66"} in grupos_extraidos):

                    grupos_extraidos = grupos_extraidos.remove({"groupid", "66"})

            except:

                if MODO_DEBUG == 1:
                    print("Erro ao extrair grupos '17' e '66' (grupos de RFeye) do hostid -> " + str(row_dados_Zabbix["host_id"]))

            finally:

                if MODO_DEBUG == 1:
                    print("Fim da extração de grupos de RFeye do hostid -> " + str(row_dados_Zabbix["host_id"]))

        # -----------------------------------
        # se é estação, mas não é RFeye, atualiza grupo de tipo de estação!
        # groupid=18 -> MIAer
        # groupid=76 -> CelWireless RMU
        # groupid=77 -> UMS300
        # -----------------------------------

        if (tipo == "vazio") and ("rfeye" not in str(row_dados_Zabbix["host"])):
        
            if MODO_DEBUG == 1:
                print("É uma estação, não se trata de uma rfeye!")

            try:

                id_estserv = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_estserv"]].values[0][0])

                tipo_estserv = str(dados_EstServ.loc[dados_EstServ["id_bd_enlacesfix"] == id_estserv, ["tipo_de_estacao"]].values[0][0]).lower()

            except:

                if MODO_DEBUG == 1:

                    print("Erro ao encontrar enlace")

                tipo_estserv = "erro"

            # recupera tipo de estserv e atualiza grupo
            if "miaer" in tipo_estserv:

                if {"groupid": "18"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "18"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["76", "77"])

            elif "rmu" in tipo_estserv:

                if {"groupid": "76"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "76"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["77", "18"])

            elif "ums" in tipo_estserv:

                if {"groupid": "77"} not in grupos_extraidos:
                    grupos_extraidos.append({"groupid": "77"})

                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["76", "18"])

        # -----------------------------------
        # Atualiza grupo de Enlace e de Estação para Hosts Zabbix que não tem tipo cadastrado
        # groupid=52 -> Estação
        # groupid=51 -> Enlace
        # -----------------------------------

        if tipo == "vazio":

            try:

                id_estserv = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_estserv"]].values[0][0])

            except:

                id_estserv = "erro"

            try:

                id_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_enlacesfix"]].values[0][0])

            except:

                id_enlace = "erro"

            # é um estserv, mas constava enlace!
            if (id_estserv != "erro") and (id_enlace == "erro") and ({"groupid": "51"} in grupos_extraidos):
                # AVISA NO TEAMS MUDANÇA!

                try:

                    minhaMensagem.title("🧾 Mudança de Host de Enlace para Estação! 🧾")
                    minhaMensagem.text("Para host " + + " no Zabbix, const aque se trata de uma Estação, mas estava cadastrado como Enlace!")
                    minhaMensagem.send()


                except:

                    if MODO_DEBUG == 1:
                        print("Não foi possivel avisar no Teams que host no zabbix mudou de tipo estação/enlace!")


                finally:

                    time.sleep(1)

                    if MODO_DEBUG == 1:
                        print("Fim de notificação de mudança de tipo de host de enlace/estação!")


            # é um enlace, mas constava estserv
            elif (id_estserv == "erro") and (id_enlace != "erro"):

                try:

                    minhaMensagem.title("🧾 Mudança de Host de Estação para Enlace! 🧾")
                    minhaMensagem.text("Para host " + + " no Zabbix, consta que se trata de um Enlace, mas estava cadastrado como Estação!")
                    minhaMensagem.send()


                except:

                    if MODO_DEBUG == 1:
                        print("Não foi possivel avisar no Teams que host no zabbix mudou de tipo estação/enlace!")


                finally:

                    time.sleep(1)

                    if MODO_DEBUG == 1:
                        print("Fim de notificação de mudança de tipo de host de enlace/estação!")

            # é um estserv
            if (id_estserv != "erro") and (id_enlace == "erro") and ({"groupid": "52"} in grupos_extraidos):
                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["52", "51"])
                grupos_extraidos.append({"groupid": "52"})

            # é um enlace
            elif (id_estserv == "erro") and (id_enlace != "erro"):
                grupos_extraidos = atualiza_grupos(grupos_extraidos, ["52", "51"])
                grupos_extraidos.append({"groupid": "51"})

        # -----------------------------------

        # groupid por UF
        uf_dict = {
            "ac": "22",
            "al": "23",
            "am": "21",
            "ap": "24",
            "ba": "25",
            "ce": "26",
            "df": "27",
            "es": "28",
            "go": "29",
            "ma": "30",
            "mg": "31",
            "ms": "32",
            "mt": "33",
            "pa": "34",
            "pb": "35",
            "pe": "36",
            "pi": "37",
            "pr": "38",
            "rj": "20",
            "rn": "39",
            "ro": "40",
            "rr": "41",
            "rs": "42",
            "sc": "43",
            "se": "44",
            "sp": "19",
            "to": "45"
        }

        # UPDATE DE UF DE ESTAÇÃO
        if tipo == "estacao":

            try:

                id_estserv = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_estserv"]].values[0][0])

                uf_estacao = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_estserv, ["lucal_uf"]].values[0][0]).lower()

            except:

                uf_estacao = "erro"

            if uf_estacao != "erro":

                try:
                    # remove da lista todos os grupos de estado
                    grupos_extraidos = atualiza_grupos(grupos_extraidos,
                                                       ["22", "23", "21", "24", "25", "26", "27", "28", "29", "30", "31",
                                                        "32", "33", "34", "35", "36", "37", "38", "20", "39", "40", "41",
                                                        "42", "43", "44", "19", "45"])

                    # adiciona grupo de estado na lista
                    grupos_extraidos.append({"groupid": str(uf_dict[uf_estacao])})

                except:

                    if MODO_DEBUG == 1:
                        print("Erro ao atualizar lista de hostid -> " + str(row_dados_Zabbix["host_id"]))

        # UPDATE DE UF DE ENLACE
        elif tipo == "enlace":

            try:

                id_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_enlacesfix"]].values[0][0])

                uf_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_enlace, ["local_uf"]].values[0][0]).lower()

            except:

                uf_enlace = "erro"

            if uf_enlace != "erro":

                try:

                    # remove da lista todos os grupos de estado
                    grupos_extraidos = atualiza_grupos(grupos_extraidos,
                                                       ["22", "23", "21", "24", "25", "26", "27", "28", "29", "30", "31",
                                                        "32", "33", "34", "35", "36", "37", "38", "20", "39", "40", "41",
                                                        "42", "43", "44", "19", "45"])

                    # adiciona grupo de estado na lista
                    grupos_extraidos.append({"groupid": str(uf_dict[uf_enlace])})

                except:

                    if MODO_DEBUG == 1:
                        print("Erro ao atualizar lista de hostid -> " + str(row_dados_Zabbix["host_id"]))


        # Grupos de classificação para Estações (groupid = 52)
        # -----------------------------------
        # groupid=48 -> Defeito
        # groupid=54 -> Ativo
        # groupid=55 -> Análise Pendente
        # groupid=56 -> Baixa
        # groupid=57 -> Calibração
        # groupid=58 -> Nomádico
        # groupid=59 -> Reserva Técnica
        # -----------------------------------

        if tipo == "estacao":

            try:

                id_estserv = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_estserv"]].values[0][0])

                if MODO_DEBUG == 1:
                    print("id_estserv ->" + str(id_estserv))

                estado_estacao = str(dados_EstServ.loc[dados_EstServ["id_bd_estserv"] == id_estserv, ["situacao_equipamento"]].values[0][0]).lower()

            except:

                estado_estacao = "erro"

            # tags 48/54/55/56/57/58/59 são excludentes entre si
            # host não pode repetir classificação

            if MODO_DEBUG == 1:
                print(estado_estacao)

            if estado_estacao != "erro":

                if estado_estacao == "defeito":
                    grupos_extraidos.append({"groupid": "48"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["54", "55", "56", "57", "58", "59"])

                elif estado_estacao == "ativo":
                    grupos_extraidos.append({"groupid": "54"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "55", "56", "57", "58", "59"])

                elif estado_estacao == "análise pendente":
                    grupos_extraidos.append({"groupid": "55"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "54", "56", "57", "58", "59"])

                elif estado_estacao == "baixa":
                    grupos_extraidos.append({"groupid": "56"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "54", "55", "57", "58", "59"])

                elif estado_estacao == "calibração":
                    grupos_extraidos.append({"groupid": "57"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "54", "55", "56", "58", "59"])

                elif estado_estacao == "nomádico":
                    grupos_extraidos.append({"groupid": "58"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "54", "55", "56", "57", "59"])

                elif estado_estacao == "reserva técnica":
                    grupos_extraidos.append({"groupid": "59"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["48", "54", "55", "56", "57", "58"])

            try:

                result = zapi.host.update({
                    # host no zabbix!
                    "hostid": str(row_dados_Zabbix["host_id"]),
                    # dados a serem atualizados!
                    "groups": grupos_extraidos
                })

                time.sleep(1)

            except:

                if MODO_DEBUG == 1:
                    print("Erro ao atualizar grupo de hostid - " + str(row_dados_Zabbix["host_id"]))

        # Grupos de classificação para Enlaces (groupid = 51)
        # -----------------------------------
        # Ativado
        # Desativado
        # Em execução
        # Oscioso
        # Suspenso
        # Suspenso Temporariamente
        # -----------------------------------

        elif tipo == "enlace":

            try:

                id_enlace = str(dados_ZabbixBD_Relacao.loc[dados_ZabbixBD_Relacao["id_bd_zabbix"] == id_zabbix, ["presente_enlacesfix"]].values[0][0])

                if MODO_DEBUG == 1:
                    print("id_enlace ->" + str(id_enlace))

                estado_enlace = str(dados_EnlacesFix.loc[dados_EnlacesFix["id_bd_enlacesfix"] == id_enlace, ["situacao_enlace"]].values[0][0]).lower()

            except:

                estado_enlace = "erro"

            if MODO_DEBUG == 1:
                print(estado_enlace)

            if estado_enlace != "erro":

                if estado_enlace == "ativado":
                    grupos_extraidos.append({"groupid": "60"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["61", "62", "63", "64", "65"])

                elif estado_enlace == "desativado":
                    grupos_extraidos.append({"groupid": "61"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["60", "62", "63", "64", "65"])

                elif estado_enlace == "em execução":
                    grupos_extraidos.append({"groupid": "62"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["60", "61", "63", "64", "65"])

                elif estado_enlace == "oscioso":
                    grupos_extraidos.append({"groupid": "63"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["60", "61", "62", "64", "65"])

                elif estado_enlace == "suspenso":
                    grupos_extraidos.append({"groupid": "64"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["60", "61", "62", "63", "65"])

                elif estado_enlace == "suspenso temporariamente":
                    grupos_extraidos.append({"groupid": "65"})
                    grupos_extraidos = atualiza_grupos(grupos_extraidos, ["60", "61", "62", "63", "64"])

                try:

                    result = zapi.host.update({
                        # host no zabbix!
                        "hostid": str(row_dados_Zabbix["host_id"]),
                        # dados a serem atualizados!
                        "groups": grupos_extraidos
                    })

                    time.sleep(1)

                except:

                    if MODO_DEBUG == 1:
                        print("Erro ao atualizar grupo de hostid - " + str(row_dados_Zabbix["host_id"]))

        # -----------------------------------

        if MODO_DEBUG == 1:
            print(grupos_extraidos)

    return qtd_hosts