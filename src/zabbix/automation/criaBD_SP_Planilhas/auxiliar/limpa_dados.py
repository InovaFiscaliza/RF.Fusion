# --------------------------------------------------
# Script de criação/atualização de BD
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_PyZabbix_FISF3
# --------------------------------------------------

import re

def limpa_dados_estserv(nome):
    
    if "rfeye" in nome:
        nome = nome[:11]

    return nome

# Consumindo dados do Shareopint, algumas estações estão retornando erros...
def limpa_registros_SP(registro):

    # na UF de Estações e Servidores, por exemplo: "23746#;RJ" deve ser apenas "RJ"
    if re.match(r'^\d{1,};#', str(registro)):
        registro_limpo = str(re.sub(r'^\d{1,};#', '', registro))
        return registro_limpo
    else:
        # na UF de Estações e Servidores, por exemplo: "string#;RJ" deve ser apenas "RJ"
        if re.match(r'^string;#', str(registro)):
            registro_limpo = str(re.sub(r'^string;#', '', registro))
            return registro_limpo
        else:
            return registro