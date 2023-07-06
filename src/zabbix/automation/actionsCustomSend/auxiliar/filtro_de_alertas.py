# --------------------------------------------------
# Script de envio de Alertas
# Guilherme Braga, 2022
# https://github.com/gui1080/testes_Anatel
# --------------------------------------------------


from difflib import SequenceMatcher
from auxiliar.arquivos_locais import MODO_DEBUG

def filtra_alertas(lista_alertas):

    if len(lista_alertas) < 2:
        return lista_alertas
    else:
        lista_final = lista_alertas.copy()
        # se nessa lista, para mesmo actionid
        # se em dada mensagem, se em subject há "Problema" e na próxima mensagem há "Resolvido" em subject
        # se strings em "message" são 75% iguais, vamos retirar esse item da lisat

        rodar = True
        while rodar == True:
            i = 0

            # tem que atualizar por causa do pop, se dá vários pop acaba dando erro
            # Exemplo
            # i = 4
            # A B C D E   <- conteúdo
            # 0 1 2 3 4   <- índice
            # pop(i)
            # A B C D   <- conteúdo
            # 0 1 2 3   <- índice
            # pop(i)
            # Erro!

            # solução proposta -> Itera, incrementa contador. Dá pop, faz break no loop.
            # Copia lista para variável nova. Itera, repeat.

            lista_alertas = lista_final.copy()
            for alerta in lista_alertas:

                # salva o primeiro alerta
                if i == 0:
                    alerta_anterior = alerta

                else:

                    # vai comparando com o alerta anterior e atualizando
                    if str(alerta["actionid"]) == str(alerta_anterior["actionid"]):
                        if "Problema" in str(alerta_anterior["subject"]) and "Resolvido" in str(alerta["subject"]):
                            if SequenceMatcher(None, str(alerta_anterior["message"]), str(alerta["message"])).ratio() > 0.75:

                                nova_msg = "Problema já ocorreu e já foi resolvido!"
                                alerta["subject"] = nova_msg

                                lista_final.pop(i - 1)

                                if MODO_DEBUG == 1:

                                    print("actionid -> " + str(alerta_anterior["actionid"]))
                                    print("Entradas repetidas foram deletadas!")

                                break


                i = i + 1
                if len(lista_alertas) == i:
                    rodar = False
                    break

    return lista_final