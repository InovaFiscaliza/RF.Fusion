# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

import difflib
from auxiliar.arquivos_locais import MODO_DEBUG, INICIO_BUSCA_ESTACAO_FUNCIONANDO
from datetime import datetime, timedelta, date

def define_inatividade(lista_alertas):

    porcentagem_tempo_offline = "0%"
    dias_offline = "0.0"

    if len(lista_alertas) < 2:
        return porcentagem_tempo_offline, dias_offline

    else:
        # se nessa lista, para mesmo actionid
        # se em dada mensagem, se em subject há "Problema" e na próxima mensagem há "Resolvido" em subject
        # se strings em "message" são 75% iguais, temos um par problema/solução
        # subtração enter ocorrências é o tempo da estação offline

        # rfeye002179 está instável
        lista_periodos_offline = []

        for alerta in lista_alertas:

            evento_atual = alerta["eventid"]

            for alerta_resolvido in lista_alertas:

                if "Resolvido" in alerta_resolvido["subject"]:

                    evento_resolvido = alerta_resolvido["p_eventid"]

                    if MODO_DEBUG == 1:
                        print("comparando -> " + evento_resolvido + " / " + evento_atual)

                    temp = difflib.SequenceMatcher(None, evento_atual, evento_resolvido)

                    if MODO_DEBUG == 1:

                        print(temp.ratio())

                    if temp.ratio() == 1.0:

                        if MODO_DEBUG == 1:

                            print("BATEU")
                            print("---------------\n\nBATEU\n\n" + "evento_atual - " + evento_atual + "\nevento_resolvido - " + evento_resolvido + "\n\n---------------")

                        lista_periodos_offline.append([alerta["clock"], alerta_resolvido["clock"]])



        if len(lista_periodos_offline) > 1:

            limpeza_pendente = True

            while limpeza_pendente == True:

                k = 1
                for i in range(len(lista_periodos_offline)):

                    # queremos a diferença entre
                    # alerta["clock"] e alerta_anterior["clock"]

                    if k == 1:

                        par_antigo = lista_periodos_offline[i]
                        k = 0

                    else:

                        par_atual = lista_periodos_offline[i]

                        '''
                        [    |----------------------|        ] entre solução e problema, teve esse periodo off
                        [          |--------------------|    ] entre solução e problema, teve esse OUTRO periodo off
                        =
                        [    |--------------------------|    ] essa é a equivalencia, não se deve contar os dois períodos sobrepostos 
                        '''
                        if ( int(par_atual[0]) > int(par_antigo[0]) ) and ( int(par_atual[1]) > int(par_antigo[1]) ) and (int(par_atual[0]) < int(par_antigo[1])):

                            lista_periodos_offline.pop(i)
                            lista_periodos_offline.pop(i-1)
                            lista_periodos_offline.append([ par_antigo[0], par_atual[1] ])

                            break

                        '''
                        [        |----------------------|    ] entre solução e problema, teve esse periodo off
                        [    |--------------------|          ] entre solução e problema, teve esse OUTRO periodo off
                        =
                        [    |--------------------------|    ] essa é a equivalencia, não se deve contar os dois períodos sobrepostos 
                        '''

                        if (int(par_atual[0]) < int(par_antigo[0])) and (int(par_atual[1]) < int(par_antigo[1])) and (int(par_atual[1]) < int(par_antigo[0])):

                            lista_periodos_offline.pop(i)
                            lista_periodos_offline.pop(i - 1)
                            lista_periodos_offline.append([ par_atual[0], par_antigo[1] ])

                            break

                        # k = 1
                        par_antigo = par_atual

                    if i == len(lista_periodos_offline) - 1:

                        limpeza_pendente = False

        '''
        ssh? portas e se está online
            
        O objetivo da limpeza é ter uma lista de períodos do tipo:
            
        [ |--------------------------|   |--------------------------|     |--------------------------|]
            
        Sem sobreposição de timestamp!
            
        '''

        if len(lista_periodos_offline) > 0:

            if MODO_DEBUG == 1:

                print(lista_periodos_offline)

            delta_total = 0
            for par_horario in lista_periodos_offline:

                inicio = datetime.fromtimestamp(int(par_horario[0]))
                fim = datetime.fromtimestamp(int(par_horario[1]))

                delta = fim - inicio

                delta_total = delta_total + delta.total_seconds()


        porcentagem_tempo_offline = str(round((((delta_total / 60) / (60 * 24)) * 100) / INICIO_BUSCA_ESTACAO_FUNCIONANDO, 2)) + "%"
        dias_offline = str( round( (delta_total / 60) / (60 * 24), 2 ) )

        if MODO_DEBUG == 1:

            print("Dias totais -> " + dias_offline)
            # dias totais - x%
            # INICIO_BUSCA_ESTACAO_FUNCIONANDO - 100%
            print("Porcentagem do tempo que a estação ficou offline -> " + porcentagem_tempo_offline)

    return porcentagem_tempo_offline, dias_offline
