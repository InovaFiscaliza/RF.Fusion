from auxiliar.arquivos_locais import MODO_DEBUG

def atualiza_grupos(grupos_extraidos, grupos_para_retirar):
    # grupo_para_retirar-> lista de strings com as groupids pra tirar
    # grupos_extraidos -> lista de [{"groupid", "x"}, {"groupid", "y"}]
    # retirar grupo passado


    for grupo in grupos_para_retirar:

        try:
            grupos_extraidos = grupos_extraidos.remove({"groupid", grupo})
        except:

            if MODO_DEBUG == 1:
                print("Esse grupo não estava na lista -> " + str(grupo))

    return grupos_extraidos