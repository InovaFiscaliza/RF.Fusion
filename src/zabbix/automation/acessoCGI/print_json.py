# --------------------------------------------------
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------

import json

# função auxiliar para realizar print de JSON
def json_print(
        obj
):
    """
        Realiza um print formatado de uma resposta em JSON.
        :param obj: JSON
    """

    # dando print da string formatada de forma legível

    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)
