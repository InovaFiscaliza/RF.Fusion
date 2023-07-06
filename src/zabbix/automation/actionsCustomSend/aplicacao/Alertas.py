
import sys
import time
from datetime import datetime, timedelta, date
import pymsteams
from tabulate import tabulate

from auxiliar.arquivos_locais import MODO_DEBUG, ENVIA_MENSAGENS_EXTRA
from auxiliar.seletor_grs import seleciona_GR
from auxiliar.filtro_de_alertas import filtra_alertas
from auxiliar.retorna_local import busca_local_do_host

def envia(logging, zapi, inicio_query, dados_webhooks, dados_EstServ, dados_EnlacesFix):

    historico_de_alertas = []

    num_canais = 27

    url_webhook = ""
    tags = ""
    responsavel = ""

    hoje = datetime.today()
    inicio_historico_problemas = datetime.timestamp(hoje - timedelta(days=365))

    for i in range(num_canais):

        # define o grupo de hosts e variável do webhook no msteams
        url_webhook, tags, responsavel = seleciona_GR(dados_webhooks, i)

        logging.debug("\n\nItera por todos os hosts em " + tags + "!\n\n")

        info_alertas = []
        info_alertas.append(['ID', 'Local', 'Status', 'Horário'])

        hosts_monitorados = 0

        for h in zapi.host.get(output="extend", groupids=tags):

            hosts_monitorados = hosts_monitorados + 1

            lista_alertas = zapi.alert.get(time_from=inicio_query,
                                           # início da busca é da data definida previamente, até agora
                                           hostids=str(h['hostid']))

            # Se necessário, filtra os problemas
            lista_alertas_filtrada = filtra_alertas(lista_alertas)

            logging.debug("Quantidade de alertas em " + str(h['name']) + " desde o início passado para o script: " + str(len(lista_alertas)))

            # se há alerta novo, se itera por cada um deles, fazendo um .pop(0) na lista até esvaziar
            if len(lista_alertas) > 0:

                for alerta in lista_alertas_filtrada:

                    logging.debug(alerta)

                    # recuperando horário e formatando o dia
                    horario_ocorrencia = datetime.fromtimestamp(int(alerta["clock"]))
                    horario_ocorrencia = horario_ocorrencia.strftime("%d/%m/%y %X")
                    logging.debug("Momento da ocorrência: " + str(horario_ocorrencia))

                    # Trata os dados e então repassa como alerta

                    '''
                    >dados relevantes retornados no objeto de alertas
                    # message   = mensagem a ser passada padrão
                    # alertid   = id do alerta
                    # clock     = hora da ocorrência

                    >dados importantes recuperados no json de hosts
                    "host"    = sigla, como "GNA, RJO, RFeye00WXYZ"
                    "hostid"  = id, valor numérico
                    "name"    = Nome em si devidamente (com designação de circuito)
                    "snmp_available" = 0 é sim, 1 é não
                    "ipmi_available" = 0 é sim, 1 é não
                    '''

                    subject_alert = str(alerta["subject"])

                    # definindo se é um alerta de problema finalizado ou aberto!
                    if subject_alert.find("Problema já ocorreu e já foi resolvido!") != -1:
                        status = "✅⚠ Problema já ocorreu e já foi resolvido! ⚠✅"

                    else:

                        if subject_alert.find("Resolvido") != -1:
                            status = "✅ Resolvido"
                        else:
                            if "ICMP" in str(alerta["message"]):

                                status = "❌ Sem Comunicação"

                            elif "SSH" in str(alerta["message"]):
                                status = "❌ Sem SSH"
                            else:
                                status = "❌ Problema"

                    logging.debug("Buscando " + str(h['name']).lower())

                    # devemos recuperar os dados de local da planilha, deste host
                    # se passará
                    local, municipio, estado, ovpn, flag = busca_local_do_host(str(h['name']).lower(), dados_EstServ, dados_EnlacesFix)

                    mensagem = "STATUS -> " + status + "\n\n" + str(alerta["message"]) + "\n\nHost id :" + str(h['hostid']) + "\n\nHorário: " + str(horario_ocorrencia)

                    if "Problema já ocorreu" in status and ENVIA_MENSAGENS_EXTRA == 0:
                        if MODO_DEBUG == 1:
                            print("Não precisa repassar esse alerta para a lista -> " + str(h['hostid']) + "\nStatus -> " + status)

                    else:
                        if "Resolvido" in status and ENVIA_MENSAGENS_EXTRA == 0:
                            if MODO_DEBUG == 1:
                                print("Não precisa repassar esse alerta para a lista -> " + str(h['hostid']) + "\nStatus -> " + status)

                        else:
                            if "Problema" in status:
                                status = "❌ Erro"

                            # o host com erro não vai aparecer duas vezes no quadro!
                            if str(h['name']) in str(info_alertas):
                                if MODO_DEBUG == 1:
                                    print("Esa estação já foi adicionada uma vez, não precisa botar 2 vezes...")

                            else:
                                info_alertas.append([str(h['name']), str(local + " (" + municipio + ")"), status, str(horario_ocorrencia)])

                    mensagem_resumo_historico = "STATUS: " + status + " | " + str(h['hostid']) + " | Horário: " + str(horario_ocorrencia) + "\n\n"
                    historico_de_alertas.append(mensagem_resumo_historico)

        # depois do for que pega todas as mensagens por canal
        if len(info_alertas) > 1:

            # trata e manda
            for i in range(len(info_alertas)):

                if info_alertas[i][1] == "":
                    info_alertas[i][1] = "------"

            agora = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

            problemas = zapi.problem.get(time_from=inicio_historico_problemas,
                                         groupids=tags,
                                         severities=[3, 4, 5])

            quantidade_erros = len(problemas)

            info_html = "Hosts Monitorados: " + str(hosts_monitorados) + "\n\n" + "Hosts com alertas novos: " + str(len(info_alertas) - 1) + "\n\nQuantidade de incidentes existentes com idade de até 1 ano: " + str(quantidade_erros) + "\n\n" + str(tabulate(info_alertas, headers='firstrow', tablefmt='grid'))

            if MODO_DEBUG == 1:
                print(info_html)

            # Mensagem de Erro!
            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("🔥 Ocorrências! Dia " + str(date.today().strftime("%d/%m/%Y")) + " 🔥")
            minhaMensagem.text(info_html)
            minhaMensagem.addLinkButton("Planilhas Sharepoint", "https://anatel365.sharepoint.com/sites/lista.fisf.publico/SitePages/Rede-de-Monitoramento-Remoto.aspx")
            minhaMensagem.addLinkButton("Fiscaliza", "https://sistemas.anatel.gov.br/fiscaliza/projects/suporte")
            minhaMensagem.addLinkButton("Wiki FISF", "https://anatel365.sharepoint.com/sites/lista.fisf.publico/Suporte_a_Fiscalizao/Wiki%20de%20Suporte%20%C3%A0%20Fiscaliza%C3%A7%C3%A3o.aspx")
            minhaMensagem.addLinkButton("Zabbix", "http://zabbixsfi.anatel.gov.br/")

            try:

                minhaMensagem.send()

                if MODO_DEBUG == 1:
                    minhaMensagem.printme()

                time.sleep(2)

            except:

                logging.warning("\n\nErro no envio da tabela de ocorrências!\n\n")
                logging.warning("Ocorrido: " + str(sys.exc_info()[0]))

        else:

            agora = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            problemas = zapi.problem.get(time_from=inicio_historico_problemas,
                                         groupids=tags,
                                         severities=[3, 4, 5])

            quantidade_erros = len(problemas)

            mensagem_padrao = "Hosts Monitorados: " + str(hosts_monitorados) + "\n\n" + "Hosts com alertas novos: 0 " + "\n\nQuantidade de incidentes existentes com idade de até 1 ano: " + str(quantidade_erros) + "\n\n"

            # Mensagem padrão de que não teve erro!
            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("✅ Ocorrências! Dia " + str(date.today().strftime("%d/%m/%Y")) + " ✅")
            minhaMensagem.text(mensagem_padrao)
            minhaMensagem.addLinkButton("Planilhas Sharepoint", "https://anatel365.sharepoint.com/sites/lista.fisf.publico/SitePages/Rede-de-Monitoramento-Remoto.aspx")
            minhaMensagem.addLinkButton("Fiscaliza", "https://sistemas.anatel.gov.br/fiscaliza/projects/suporte")
            minhaMensagem.addLinkButton("Wiki FISF", "https://anatel365.sharepoint.com/sites/lista.fisf.publico/Suporte_a_Fiscalizao/Wiki%20de%20Suporte%20%C3%A0%20Fiscaliza%C3%A7%C3%A3o.aspx")
            minhaMensagem.addLinkButton("Zabbix", "http://zabbixsfi.anatel.gov.br/")

            try:

                minhaMensagem.send()

                if MODO_DEBUG == 1:
                    minhaMensagem.printme()

                time.sleep(2)

            except:

                logging.warning("\n\nErro no envio da tabela de ocorrências!\n\n")
                logging.warning("Ocorrido: " + str(sys.exc_info()[0]))


    return historico_de_alertas