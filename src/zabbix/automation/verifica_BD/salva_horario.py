# --------------------------------------------------
# Script de verificação de BD
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

import json
import sys

# função auxiliar que salva horário da ultima varredura nos alertas
def salva_horario(inicio):

    try:
        with open("log_execucao.json", "r+") as arq_tempo:

            dados_tempo = json.load(arq_tempo)

            dados_tempo["ultima_execucao"] = int(inicio)

            # rewind
            arq_tempo.seek(0)
            json.dump(dados_tempo, arq_tempo)
            arq_tempo.truncate()

    except:
        print("\n\nErro na abertura e gravação de arquivo JSON de tempo!")
        print("Ocorrido: " + str(sys.exc_info()[0]))
