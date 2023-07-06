from auxiliar.print_json import json_print
from auxiliar.arquivos_locais import MOSTRAR_PRINTS
from datetime import datetime, timedelta
from auxiliar.limpa_dados import limpa_dados_zabbix

def extract(logging, zapi):


    # ------------------------------------------------------------------------
    # Lista das variáveis que serão recuperadas para o BD

    # chave do BD
    id_bd = []

    # dados simples recuperados
    nome = []
    host = []
    id = []
    ip = []
    grupo_lista = []

    # Análise de propriedade h["status"] == 0
    host_disponivel_zabbix = []

    # lista de booleanos de erros rcentes
    erro_recente = []

    # busca de templates atrelados a cada host
    templates = []

    # booleano, indicia se é um host cadastrado como ovpn
    ovpn = []

    # diferença entre momento atual e último erro
    dif_erro = []

    # enunciado do último ocorrido
    lista_ultimo_ocorrido = []

    # quantidade de problemas severos no último ano
    quantidade_problemas = []

    # quantidade de problemas no total no último ano
    qtd_prolemas_total = []

    # variáveis de horario (serve para as buscas no Zabbix)
    hoje = datetime.today()
    inicio_query = datetime.timestamp(hoje - timedelta(days=730))
    inicio_query_problemas = datetime.timestamp(hoje - timedelta(days=365))

    for h in zapi.host.get(output="extend"):

        logging.debug("Acessando -> " + str(h["name"]))

        # HASH QUE GERA O ID
        # adiciona coluna de ID
        input_hash = str(h['hostid']) + " " + str(h["name"])

        resultado_id = str(abs(hash(input_hash)) % (10 ** 4)).zfill(4)

        # PROBLEMAS DE MAIOR PRIORIDADE
        # busca dados de problemas do host
        problemas = zapi.problem.get(time_from=inicio_query_problemas,
                                     # início da busca é da data definida previamente, até agora
                                     hostids=str(h['hostid']),
                                     severities=[3, 4, 5])  # não buscar incidentes de baixíssima prioridade!

        quantidade_problemas.append(len(problemas))

        # PROBLEMAS NO TOTAL
        problemas = zapi.problem.get(time_from=inicio_query_problemas,
                                     # início da busca é da data definida previamente, até agora
                                     hostids=str(h['hostid']))

        qtd_prolemas_total.append(len(problemas))

        if len(problemas) > 0:

            erro_recente.append("True")

        else:

            erro_recente.append("False")

        if MOSTRAR_PRINTS == 1:
            json_print(h)

        # ALTERTA MAIS RECENTE
        # mostra erro mais recente, no formato do alerto que foi passado
        lista_alertas = zapi.alert.get(time_from=inicio_query,
                                       # início da busca é da data definida previamente, até agora
                                       hostids=str(h['hostid']))

        # tratamento caso existam problemas recentes no hosto
        if len(lista_alertas) > 0:

            alerta_mais_recente = lista_alertas.pop()

            subject_alert = str(alerta_mais_recente["subject"])

            # problema mais recente ainda esta aberto?
            if subject_alert.find("Resolvido") != -1:
                # o problema mais recente foi resolvido!
                dif_erro.append("Resolvido!")

            else:

                dif_erro.append("Pendente")

            subject_alert = subject_alert + str(alerta_mais_recente["message"])

            lista_ultimo_ocorrido.append(subject_alert)

        else:

            dif_erro.append("Sem erros achados nos últimos 2 anos!")
            lista_ultimo_ocorrido.append("Sem status!")

        # TEMPLATES
        h_templates = zapi.template.get(hostids=str(h["hostid"]),
                                        output="extend")

        string_templates = ""
        # tratar templates
        if (len(h_templates) == 0):
            templates.append("Não existem templates atrelados.")

        # TRATAMENTO PARA LISTAGEM DE TEMPLATES
        for i in range(len(h_templates)):

            template = h_templates.pop(0)
            string_templates = string_templates + str(template["name"]) + "(id " + str(template["templateid"]) + ")"

            if i != len(h_templates):
                string_templates = string_templates + ", "
            else:
                string_templates = string_templates + "."

        # BUSCA DE GRUPOS DO HOST
        h_group = zapi.hostgroup.get(hostids=str(h["hostid"]),
                                     output="extend")

        # tratar quantidade de grupos variável
        string_groups = ""

        if len(h_group) > 0:

            for i in range(len(h_group)):

                grupo = h_group.pop(0)
                string_groups = string_groups + str(grupo["groupid"])

                string_groups = string_groups + ", "

            string_groups = string_groups[:-2]
            string_groups = string_groups + "."

        else:

            string_groups = "Sem grupo definido!"

        # INTERFACES
        # retorna uma lista de interfaces, mesmo buscando 1 host
        h_interface = zapi.hostinterface.get(hostids=str(h["hostid"]),
                                             output="extend")

        if len(h_interface) > 0:

            interface = h_interface.pop(0)

            if MOSTRAR_PRINTS == 1:
                json_print(interface)

        else:
            interface = "Indefinido!"

        # formata nome do host e salva dados
        # verifica se conexão é ovpn
        host_no_zabbix = str(h["host"]).lower()

        host_no_zabbix = limpa_dados_zabbix(host_no_zabbix)

        logging.debug("Host no zabbix -> " + str(host_no_zabbix))

        if "_ovpn" in host_no_zabbix:

            ovpn.append("True")

        else:

            ovpn.append("False")

        # adiciona dados compilados a listas de dados
        # no final as listas irão compor o dataframe
        id_bd.append(resultado_id)
        nome.append(str(h["name"]))
        host.append(host_no_zabbix)
        id.append(str(h["hostid"]))
        ip.append(str(interface["ip"]))
        grupo_lista.append(string_groups)
        templates.append(string_templates)

        # verifica disponibilidade de host
        if str(h["status"]) == "0":

            host_disponivel_zabbix.append("False")

        else:

            host_disponivel_zabbix.append("True")

    return id_bd, nome, host, id, ip, grupo_lista, host_disponivel_zabbix, erro_recente, templates, ovpn, dif_erro, lista_ultimo_ocorrido, quantidade_problemas, qtd_prolemas_total, hoje, inicio_query, inicio_query_problemas