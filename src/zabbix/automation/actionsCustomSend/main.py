# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

# funções locais
from auxiliar.seletor_grs import seleciona_GR
from auxiliar.print_json import json_print
from auxiliar.salva_horario import salva_horario
from auxiliar import zabbix_login
from auxiliar.status_estacoes import define_inatividade

from auxiliar.arquivos_locais import TIME_SELECT, WEBHOOK, HOST_BD, MODO_DEBUG, ENVIA_MENSAGENS_EXTRA, INICIO_BUSCA_ESTACAO_FUNCIONANDO

# dependências secundárias
import time
import json
from datetime import datetime, timedelta, date
import sys
import os
import pandas as pd
import sqlite3
import logging

# centro da integração do script, PyMSTeams e PyZabbix
import pymsteams  # https://pypi.org/project/pymsteams/

from etl import Load
from aplicacao import Alertas

# Declaração da main
#--------------------------------------------------------------------------

def main():

    # Login no zabbix por meio de biblioteca PyZabbix
    # ------------------------------------------------------------------------
    logging.basicConfig(filename='exec_log.log', filemode='w+', encoding='utf-8', format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    logging.debug("Iniciando script de alertas com PyZabbix!\n")

    zapi = zabbix_login.zabbix_login()

    # Leitura de time_select.json
    # ------------------------------------------------------------------------
    # recupera-se em arquivo JSON o período para a query ser realizada

    try:
        with open(os.path.dirname(__file__) + TIME_SELECT, "r") as arq_tempo:
            dados = json.load(arq_tempo)

            # booleano "a query inicia 24 atrás?"
            seleciona_ontem = int(dados["select_ontem"])

            # inicio da busca, x minutos atrás
            min_atras = int(dados["dif_minima_minutos"])

            # momento da ultima execução
            ultima_execucao = int(dados["ultima_execucao"])

            # seleciona tempo automatico, se será usado "ultima_execucao"
            tempo_automatico = int(dados["tempo_automatico"])

    except:
        logging.warning("\n\nErro!\n\n")
        logging.warning("Erro no acesso de arquivo json para seleção de tempo na execução!")

    # ONTEM
    # pega-se a data de início
    hoje = datetime.today()
    ontem = hoje - timedelta(days=1)

    # transforma variável "ontem" em timestamp e passa pros problemas
    ontem = datetime.timestamp(ontem)

    # transforma variável de "hoje" para comparação na recuperação dos dados
    # hoje = datetime.timestamp(hoje)

    # X MINUTOS ATRÁS
    minutos_atras = datetime.now() - timedelta(minutes=min_atras)

    # transformando em "timestamp"
    minutos_atras = datetime.timestamp(minutos_atras)

    if tempo_automatico != 1:

        # caso não se for usar o tempo automatico
        if seleciona_ontem == 1:

            inicio_query = ontem

        else:

            inicio_query = minutos_atras

    else:

        # se a ultima execucao foi "0", é a primeira execução o um erro.
        # Reinicia mandando os alertas desde "ontem"
        if ultima_execucao == 0:

            inicio_query = ontem

        else:

            inicio_query = ultima_execucao
        
        
    # Recuperando dados Sharepoint
    # ------------------------------------------------------------------------
    # Update: interação com banco de dados

    dados_EstServ, dados_EnlacesFix = Load.load(logging)

    # ALERTAS
    # ------------------------------------------------------------------------
    # Itera por canal, e então para cada host deste canal recupera a lista de problemas ocorridos

    arq_hooks = open(os.path.dirname(__file__) + WEBHOOK, "r")
    dados_webhooks = json.load(arq_hooks)

    historico_de_alertas = Alertas.envia(logging, zapi, inicio_query, dados_webhooks, dados_EstServ, dados_EnlacesFix)

    # ------------------------------------------------------------------------

    # abre a conexão apara recuperar dados do zabbix se necessário, e para botar o histórico de execução no BD
    conn = sqlite3.connect(os.path.dirname(__file__) + HOST_BD)
    c = conn.cursor()

    # aviso mensal de atividade das estações
    print("Hoje - " + str(date.today().strftime("%d")))

    hoje = datetime.today()
    inicio_query = hoje - timedelta(days=INICIO_BUSCA_ESTACAO_FUNCIONANDO)
    inicio_query = datetime.timestamp(inicio_query)

    if (str(hoje.strftime("%d")) == "28") or (str(hoje.strftime("%d")) == "14"):

        if MODO_DEBUG == 1:
            print("Recuperando o histórico de cada estação.")

        # DADOS DO ZABBIX
        c.execute('''
                    SELECT * FROM dados_zabbix
                  ''')

        # Dados do Zabbix
        dados_Zabbix = pd.DataFrame(c.fetchall(), columns=["id_bd_zabbix", "nome", "host", "disponivel_no_zabbix", "host_id",
                                                            "host_ip", "grupos", "conexao_OVPN", "erros_ultimas_24h",
                                                            "host_disponivel_snmp", "host_disponivel_jmx", "host_disponivel_ipmi",
                                                            "templates_vinculados", "ultimo_problema_ocorrido", "qtd_problemas_graves",
                                                            "qtd_problemas_total", "ultimo_ocorrido"])

        if MODO_DEBUG == 1:
            print(dados_Zabbix.info())
            
        logging.debug(dados_Zabbix.info())

        time.sleep(2)

        num_canais = 27

        url_webhook = ""
        tags = ""
        responsavel = ""

        hoje = datetime.today()
        inicio_query_offline = hoje - timedelta(days=INICIO_BUSCA_ESTACAO_FUNCIONANDO)
        inicio_query_offline = datetime.timestamp(inicio_query_offline)

        for i in range(num_canais):

            hosts_monitorados = 0
            string_tempo_offline = "Busca de " + str(INICIO_BUSCA_ESTACAO_FUNCIONANDO) + " dias atrás!\n\n\n"

            # define o grupo de hosts e variável do webhook no msteams
            url_webhook, tags, responsavel = seleciona_GR(dados_webhooks, i)

            logging.debug("\n\nItera por todos os hosts em " + tags + "!\n\n")

            if MODO_DEBUG == 1:
                print("Tag - " + tags)
                print("Estado - " + responsavel)

            # para cada host, de cada grupo
            for h in zapi.host.get(output="extend", groupids=tags):

                hosts_monitorados = hosts_monitorados + 1

                try:

                    lista_alertas = zapi.alert.get(time_from=inicio_query_offline,
                                                # início da busca é da data definida previamente, até agora
                                                hostids=str(h['hostid']))

                    json_print(lista_alertas)

                    # a porcentagem se refere ao inicio da busca, "# INICIO_BUSCA_ESTACAO_FUNCIONANDO" dias atrás
                    porcentagem_tempo_offline, dias_offline = define_inatividade(lista_alertas)

                    # recupera nome do host
                    nome_zabbix = str(dados_Zabbix.loc[dados_Zabbix["host_id"] == str(h['hostid']), ["nome"]].values[0][0])

                    # adiciona mensagem na string
                    if dias_offline != "0.0":
                        string_tempo_offline = string_tempo_offline + "⚠ " + nome_zabbix + " - Ficou offline " + porcentagem_tempo_offline + " do tempo (" + dias_offline + " dias).\n\n"

                except:

                    if MODO_DEBUG == 1:
                        print("Não foi possível recuperar os dados de " + str(h['hostid']))

            # manda alerta no Teams
            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("⏱ Tempo offline dos hosts - " + responsavel + " ⏱")
            minhaMensagem.text(string_tempo_offline + "\n\n-------------------------------\n\n🕰 " + str(datetime.now()))
            minhaMensagem.send()

            time.sleep(1)

    # ------------------------------------------------------------------------

    # Envio de sumário de ocorrências
    try:

        # mensagem caso algum alerta tenha sido repassado
        if len(historico_de_alertas) > 0:

            if ENVIA_MENSAGENS_EXTRA == 1:
                mensagem = "Histórico: \n\nAlertas repassados em " + str(hoje.strftime("%d/%m/%y %X")) + "\n\n" + "Quantidade de alertas: " + str(len(historico_de_alertas)) + "\n\n" + "Mensagens de problemas já resolvidos foram repassadas!" + "\n\n"
            else:
                mensagem = "Histórico: \n\nAlertas repassados em " + str(hoje.strftime("%d/%m/%y %X")) + "\n\n" + "Quantidade de alertas: " + str(len(historico_de_alertas)) + "\n\n" + "Mensagens de problemas já resolvidos NÃO foram repassadas!" + "\n\n"

            for i in range(len(historico_de_alertas)):
                mensagem = mensagem + historico_de_alertas[i] + "\n\n"

            mensagem = mensagem + "\n\nPyZabix " + str(zapi.api_version())

            url_webhook = str(dados_webhooks["Geral"])
            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("\n\n🔔Histórico de ocorrências!🔔\n\n")
            minhaMensagem.text(mensagem)
            minhaMensagem.send()

            if MODO_DEBUG == 1:
                minhaMensagem.printme()

            time.sleep(2)

        # alerta avisando que não houve alertas novos
        else:

            inicio_busca = datetime.fromtimestamp(inicio_query)
            inicio_busca = inicio_busca.strftime("%d/%m/%y %X")

            mensagem = "Não ocorreu novos alertas desde " + str(inicio_busca) + " até o momento!"

            mensagem = mensagem + "\n\nPyZabix " + str(zapi.api_version())

            url_webhook = str(dados_webhooks["Geral"])
            minhaMensagem = pymsteams.connectorcard(url_webhook)
            minhaMensagem.title("\n\n⚠️Histórico de ocorrências!⚠️ \n\n")
            minhaMensagem.text(mensagem)
            minhaMensagem.send()

            if MODO_DEBUG == 1:
                minhaMensagem.printme()

            time.sleep(2)

    except:

        logging.warning("\n\nErro no envio da mensagem final!\n\n")
        logging.warning("Ocorrido: " + str(sys.exc_info()[0]))
        
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        sql_string = "INSERT OR REPLACE INTO log_execucao (script_id, ultima_execucao, exec_sucesso)" \
                    "VALUES ('actionsCustomSend', '" + agora + "', 'False');"

        c.execute(sql_string)
        time.sleep(1)

    finally:

        conn.commit()
        conn.close()


# Inicio da execução do script
#--------------------------------------------------------------------------

if __name__ == '__main__':

    # contabiliza o tempo de execução!
    inicio = time.time()
    main()
    fim = time.time()

    # salva-se o horário do inicio da execução
    # sabe-se que se iterou por todos os alertas até este presente momento!
    # na próxima execução deste script, deverá ser levantado se algo novo ocorreu depois deste momento
    salva_horario(inicio)

    duracao = (fim - inicio)/60
    logging.debug("\n\n\nFim da execução!\n\nDuração da execução deste script: %f minutos." % (duracao))
    