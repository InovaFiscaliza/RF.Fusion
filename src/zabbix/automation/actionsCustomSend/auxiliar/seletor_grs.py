# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

def seleciona_GR(dados_webhooks, i):

    if i == 0:
        # GR01
        # SP
        url_webhook = str(dados_webhooks["GR01"])
        tags = "19"
        estado = "GR01 - SP"

    elif i == 1:
        # gr02
        # RJ
        url_webhook = str(dados_webhooks["GR02"])
        tags = "20"
        estado = "GR02 - RJ"

    elif i == 2:
        # gr02 uo021
        # ES
        url_webhook = str(dados_webhooks["GR02 - UO02.1"])
        tags = "28"
        estado = "GR02 UO021 - ES"

    elif i == 3:
        # gr03
        # PR
        url_webhook = str(dados_webhooks["GR03"])
        tags = "38"
        estado = "GR03 - PR"

    elif i == 4:
        # gr03 uo031
        # SC
        url_webhook = str(dados_webhooks["GR03 - UO03.1"])
        tags = "43"
        estado = "GR03 UO031 - SC"

    elif i == 5:
        # gr04
        # MG
        url_webhook = str(dados_webhooks["GR04"])
        tags = "31"
        estado = "GR04 - MG"

    elif i == 6:
        # gr05
        # RS
        url_webhook = str(dados_webhooks["GR05"])
        tags = "42"
        estado = "GR05 - RS"

    elif i == 7:
        # gr06
        # PE
        url_webhook = str(dados_webhooks["GR06"])
        tags = "36"
        estado = "GR06 - PE"

    elif i == 8:
        # gr06 uo061
        # AL
        url_webhook = str(dados_webhooks["GR06 - UO06.1"])
        tags = "23"
        estado = "GR06 UO061 - AL"

    elif i == 9:
        # gr06 uo062
        # PB
        url_webhook = str(dados_webhooks["GR06 - UO06.2"])
        tags = "35"
        estado = "GR06 UO062 - PB"

    elif i == 10:
        # gr07
        # GO
        url_webhook = str(dados_webhooks["GR07"])
        tags = "29"
        estado = "GR07 - GO"

    elif i == 11:
        # gr07 uo071
        # MT
        url_webhook = str(dados_webhooks["GR07 - UO07.1"])
        tags = "33"
        estado = "GR07 UO071 -MT"

    elif i == 12:
        # gr07 uo072
        # MS
        url_webhook = str(dados_webhooks["GR07 - UO07.2"])
        tags = "32"
        estado = "GR07 UO072 -MS"

    elif i == 13:
        # gr07 uo073
        # TO
        url_webhook = str(dados_webhooks["GR07 - UO07.3"])
        tags = "45"
        estado = "GR07 UO073 - TO"

    elif i == 14:
        # gr08
        # BA
        url_webhook = str(dados_webhooks["GR08"])
        tags = "25"
        estado = "GR08 - BA"

    elif i == 15:
        # gr08 uo081
        # SE
        url_webhook = str(dados_webhooks["GR08 - UO08.1"])
        tags = "44"
        estado = "GR08 UO081 - SE"

    elif i == 16:
        # gr09
        # CE
        url_webhook = str(dados_webhooks["GR09"])
        tags = "26"
        estado = "GR09 - CE"

    elif i == 17:
        # gr09 uo091
        # RN
        url_webhook = str(dados_webhooks["GR09 - UO09.1"])
        tags = "39"
        estado = "GR09 UO091 - RN"

    elif i == 18:
        # gr09 uo092
        # PI
        url_webhook = str(dados_webhooks["GR09 - UO09.2"])
        tags = "37"
        estado = "GR09 UO092 - PI"

    elif i == 19:
        # gr10
        # PA
        url_webhook = str(dados_webhooks["GR10"])
        tags = "34"
        estado = "GR10 - PA"

    elif i == 20:
        # gr010 uo101
        # MA
        url_webhook = str(dados_webhooks["GR10 - UO10.1"])
        tags = "30"
        estado = "GR10 UO101 - MA"

    elif i == 21:
        # gr010 uo102
        # AP
        url_webhook = str(dados_webhooks["GR10 - UO10.2"])
        tags = "24"
        estado = "GR09 UO102 - AP"

    elif i == 22:
        # gr011
        # AM
        url_webhook = str(dados_webhooks["GR11"])
        tags = "21"
        estado = "GR11 - AM"

    elif i == 23:
        # gr011 uo111
        # RO
        url_webhook = str(dados_webhooks["GR11 - UO11.1"])
        tags = "40"
        estado = "GR11 UO111 - RO"

    elif i == 24:
        # gr011 uo112
        # AC
        url_webhook = str(dados_webhooks["GR11 - UO11.2"])
        tags = "22"
        estado = "GR11 UO112 - AC"

    elif i == 25:
        # gr011 uo113
        # RR
        url_webhook = str(dados_webhooks["GR11 - UO11.3"])
        tags = "41"
        estado = "GR11 UO113 - RR"

    elif i == 26:
        # uo001
        # DF
        url_webhook = str(dados_webhooks["UO001.1"])
        tags = "27"
        estado = "UO001 - DF"

    return url_webhook, tags, estado
